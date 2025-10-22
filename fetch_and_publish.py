#!/usr/bin/env python3
import os
import json
import sys
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen
import xml.etree.ElementTree as ET

import feedparser
from dateutil import parser as dateparser

# CONFIG
FEEDS = {
    "bd": "https://evilgodfahim.github.io/bd/articles.xml",
    "master": "https://evilgodfahim.github.io/Longreads/filtered.xml"
}

# Output filenames
OUTFILES = {
    "bd": "daily_bd.xml",
    "master": "daily_master.xml"
}

LAST_SEEN_FILES = {
    "bd": "last_seen_bd.json",
    "master": "last_seen_master.json"
}

# How far back to consider "new" (24 hours)
DELTA = timedelta(hours=24)

# Helper functions ---------------------------------------------------------
def safe_fetch(url):
    try:
        with urlopen(url, timeout=30) as r:
            return r.read()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None

def parse_entry_id(entry):
    # Prefer stable id/guid, then link, then composed title+date
    if 'id' in entry and entry.id:
        return entry.id
    if 'guid' in entry and entry.guid:
        return entry.guid
    if 'link' in entry and entry.link:
        return entry.link
    # fallback: title + published
    title = entry.get('title', '')
    pub = entry.get('published', '') or entry.get('updated', '')
    return f"{title}||{pub}"

def parse_entry_date(entry):
    # Try common fields: published_parsed / published / updated / updated_parsed
    # Use dateutil parser for string fields. If missing, return None.
    for key in ('published_parsed', 'updated_parsed'):
        v = entry.get(key)
        if v:
            try:
                # feedparser time tuple -> datetime
                dt = datetime(*v[:6], tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
    for key in ('published', 'updated', 'pubDate', 'date'):
        v = entry.get(key)
        if v:
            try:
                dt = dateparser.parse(v)
                if dt.tzinfo is None:
                    # assume UTC if naive
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass
    return None

def load_last_seen(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return {}

def save_last_seen(path, data):
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

def build_rss_xml(channel_title, channel_link, items):
    # Build a minimal RSS 2.0 document
    rss = ET.Element('rss', version='2.0')
    chan = ET.SubElement(rss, 'channel')
    ET.SubElement(chan, 'title').text = channel_title
    ET.SubElement(chan, 'link').text = channel_link
    ET.SubElement(chan, 'description').text = f"Daily feed: {channel_title}"
    ET.SubElement(chan, 'lastBuildDate').text = datetime.now(timezone.utc).astimezone().isoformat()

    for it in items:
        i = ET.SubElement(chan, 'item')
        if it.get('title'):
            ET.SubElement(i, 'title').text = it.get('title')
        if it.get('link'):
            ET.SubElement(i, 'link').text = it.get('link')
        if it.get('id'):
            ET.SubElement(i, 'guid').text = it.get('id')
        if it.get('published'):
            try:
                ET.SubElement(i, 'pubDate').text = it.get('published').astimezone(timezone.utc).isoformat()
            except Exception:
                ET.SubElement(i, 'pubDate').text = str(it.get('published'))
        if it.get('summary'):
            s = ET.SubElement(i, 'description')
            s.text = it.get('summary')

    # Pretty printing
    return ET.tostring(rss, encoding='utf-8', xml_declaration=True)

# Main logic ---------------------------------------------------------------
def process_feed(feed_key, feed_url):
    print(f"Processing {feed_key} -> {feed_url}")
    raw = safe_fetch(feed_url)
    if raw is None:
        print(f"Skipping {feed_key} (fetch failed).")
        return False

    parsed = feedparser.parse(raw)
    entries = parsed.entries or []
    now = datetime.now(timezone.utc)
    cutoff = now - DELTA

    last_seen_path = LAST_SEEN_FILES[feed_key]
    last_seen = load_last_seen(last_seen_path)  # mapping id -> iso timestamp

    new_items = []
    updated_last_seen = dict(last_seen)  # will update

    for e in entries:
        eid = parse_entry_id(e)
        edate = parse_entry_date(e)
        # If we have edate, use cutoff time check; else treat unknown date as possibly new (but also check last_seen)
        is_new = False
        if eid not in last_seen:
            # not seen before
            if edate:
                if edate >= cutoff:
                    is_new = True
                else:
                    is_new = False
            else:
                # no date -> consider new if not seen before
                is_new = True
        else:
            # seen before: check if published after stored timestamp (rare), or else skip
            try:
                stored_iso = last_seen[eid]
                stored_dt = dateparser.parse(stored_iso)
                if stored_dt.tzinfo is None:
                    stored_dt = stored_dt.replace(tzinfo=timezone.utc)
                if edate and edate > stored_dt:
                    # updated newer than stored entry and within 24 hours?
                    if edate >= cutoff:
                        is_new = True
            except Exception:
                # if stored value can't be parsed, be conservative and skip
                is_new = False

        if is_new:
            item = {
                'id': eid,
                'title': e.get('title'),
                'link': e.get('link'),
                'summary': e.get('summary') or e.get('description') or '',
                'published': edate or now
            }
            new_items.append(item)

        # Always update last_seen with entry's latest known timestamp (or now)
        stamp = None
        if edate:
            stamp = edate.isoformat()
        else:
            # try to use updated / published string fields or now
            stamp = (e.get('published') or e.get('updated') or now.isoformat())
        updated_last_seen[eid] = stamp

    # Write output RSS containing only new items (if any)
    outfn = OUTFILES[feed_key]
    if new_items:
        rss_bytes = build_rss_xml(f"daily_{feed_key}", feed_url, new_items)
        with open(outfn, 'wb') as fh:
            fh.write(rss_bytes)
        print(f"Wrote {len(new_items)} new items to {outfn}")
    else:
        # create empty feed with no items (optional) or leave previous. We'll write an empty feed to keep determinism.
        rss_bytes = build_rss_xml(f"daily_{feed_key}", feed_url, [])
        with open(outfn, 'wb') as fh:
            fh.write(rss_bytes)
        print(f"No new items for {feed_key}. Wrote empty feed to {outfn}")

    # Save last_seen
    save_last_seen(last_seen_path, updated_last_seen)
    return True

def main():
    any_ok = False
    for k, url in FEEDS.items():
        ok = process_feed(k, url)
        any_ok = any_ok or ok
    if not any_ok:
        print("No feeds processed successfully.", file=sys.stderr)
        sys.exit(1)
    else:
        print("Completed processing feeds.")

if __name__ == '__main__':
    main()
