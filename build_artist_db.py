#!/usr/bin/env python3
"""
SubPulse — Full Artist Image Database Builder

Builds a complete image database of every RA artist with 20+ followers
who has appeared on at least one event in the last 24 months.

This ships with the app. Users see instant images from first load.

Usage:
  cd ~/Documents/GitHub/booking
  pip3 install Pillow --break-system-packages
  python3 server.py  # in another terminal
  python3 build_artist_db.py

Runtime: ~2-4 hours first run (rate-limited to be respectful to RA).
Re-run monthly to pick up new artists — skips already-fetched ones.

Output:
  img/artists/*.webp     — 100x100 thumbnails (~3-5KB each)
  artist_images.json     — index mapping norm(name) → slug
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path
from io import BytesIO
from datetime import datetime, timedelta

try:
    from PIL import Image
except ImportError:
    print("Install Pillow first: pip3 install Pillow --break-system-packages")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────
SERVER = "http://localhost:8000"
IMG_DIR = Path("img/artists")
INDEX_FILE = Path("artist_images.json")
PROGRESS_FILE = Path("_artist_db_progress.json")
THUMB_SIZE = (100, 100)
MIN_FOLLOWERS = 20
MONTHS_BACK = 12
RA_RATE = 1.0  # seconds between RA page scrapes

# Top cities by event volume (RA area IDs)
# Covers ~85% of European RA volume
CITIES = {
    "London": 13,
    "Berlin": 34,
    "Barcelona": 44,
    "Amsterdam": 29,
    "Paris": 43,
    "Madrid": 149,
    "Manchester": 45,
    "Milan": 109,
    "Lisbon": 67,
    "Ibiza": 25,
    "Glasgow": 49,
    "Dublin": 47,
    "Hamburg": 37,
    "Munich": 41,
    "Bristol": 48,
    "Vienna": 35,
    "Prague": 152,
    "Brussels": 76,
    "Leeds": 54,
    "Warsaw": 139,
    "Budapest": 147,
    "Copenhagen": 58,
    "Cologne": 40,
    "Rome": 188,
    "Naples": 327,
}

RA_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html",
}


def norm(s):
    return re.sub(r'[^a-z0-9]', '', s.lower())


def slug(name):
    return re.sub(r'[^a-z0-9]', '', name.lower())


def ra_graphql(query, variables=None):
    """Call RA GraphQL via the local proxy."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{SERVER}/api/ra/graphql",
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "SubPulse/1.0"},
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                return None


def fetch_city_artists(city_name, area_id):
    """Fetch all unique artist names from a city's events over the last 24 months."""
    artists = {}  # norm(name) → display name
    now = datetime.now()

    for month_offset in range(MONTHS_BACK):
        start = now - timedelta(days=30 * (month_offset + 1))
        end = now - timedelta(days=30 * month_offset)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        page = 1
        while True:
            query = """query Q($f:FilterInputDtoInput,$fo:FilterOptionsInputDtoInput,$p:Int,$s:Int){
              eventListings(filters:$f,filterOptions:$fo,pageSize:$s,page:$p){
                data{event{artists{name}}}
                totalResults
              }
            }"""
            variables = {
                "f": {
                    "areas": {"eq": area_id},
                    "listingDate": {"gte": start_str, "lte": end_str},
                },
                "fo": {},
                "p": page,
                "s": 50,
            }
            result = ra_graphql(query, variables)
            if not result:
                break

            listings = result.get("data", {}).get("eventListings", {})
            data = listings.get("data", [])
            if not data:
                break

            for item in data:
                ev = item.get("event", {})
                for a in ev.get("artists", []):
                    name = a.get("name", "").strip()
                    if name:
                        n = norm(name)
                        if n and len(n) >= 2 and n not in artists:
                            artists[n] = name

            total = listings.get("totalResults", 0)
            if page * 50 >= total:
                break
            page += 1
            time.sleep(0.3)  # light rate limiting for event fetches

        sys.stdout.write(f"\r  {city_name}: month {month_offset+1}/{MONTHS_BACK} — {len(artists)} unique artists")
        sys.stdout.flush()

    print()
    return artists


def get_follower_count(name):
    """Get RA follower count for an artist."""
    s = slug(name)
    result = ra_graphql(
        "query A($s:String){artist(slug:$s){id followerCount}}",
        {"s": s},
    )
    if result:
        artist = result.get("data", {}).get("artist")
        if artist and artist.get("followerCount") is not None:
            return artist["followerCount"]
    return None


def get_ra_image_url(artist_slug):
    """Scrape og:image from RA artist page."""
    url = f"https://ra.co/dj/{artist_slug}"
    try:
        req = urllib.request.Request(url, headers=RA_HDRS)
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            html,
        )
        if not m:
            m = re.search(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                html,
            )
        if m:
            img = m.group(1)
            if "default" not in img.lower() and "placeholder" not in img.lower():
                return img
    except Exception:
        pass
    return None


def download_and_resize(url, output_path):
    """Download image, center-crop to square, resize to 100x100 WebP."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read()
        img = Image.open(BytesIO(data)).convert("RGB")
        w, h = img.size
        side = min(w, h)
        left, top = (w - side) // 2, (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
        img = img.resize(THUMB_SIZE, Image.LANCZOS)
        img.save(output_path, "WEBP", quality=80)
        return True
    except Exception as e:
        return False


def load_progress():
    """Load progress from previous run (crash-proof)."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"fetched_cities": [], "all_artists": {}, "checked_followers": {}, "phase": "cities"}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def main():
    print("═══════════════════════════════════════════════")
    print("  SubPulse — Artist Image Database Builder")
    print("═══════════════════════════════════════════════\n")

    # Check server is running
    try:
        urllib.request.urlopen(f"{SERVER}/", timeout=3)
    except Exception:
        print(f"Server not running at {SERVER}")
        print("Start it first: python3 server.py")
        sys.exit(1)

    IMG_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing index
    index = {}
    if INDEX_FILE.exists():
        with open(INDEX_FILE) as f:
            index = json.load(f)

    # Load progress
    prog = load_progress()

    # ── Phase 1: Fetch artists from all cities ──────────────────
    if prog["phase"] == "cities":
        print(f"Phase 1: Fetching artists from {len(CITIES)} cities ({MONTHS_BACK} months each)\n")
        all_artists = prog.get("all_artists", {})

        for city_name, area_id in CITIES.items():
            if city_name in prog.get("fetched_cities", []):
                print(f"  {city_name}: already scanned, skipping")
                continue

            city_artists = fetch_city_artists(city_name, area_id)
            all_artists.update(city_artists)
            prog["fetched_cities"].append(city_name)
            prog["all_artists"] = all_artists
            save_progress(prog)

        print(f"\nTotal unique artists across all cities: {len(all_artists)}")
        prog["phase"] = "followers"
        save_progress(prog)

    # ── Phase 2: Check follower counts ──────────────────────────
    all_artists = prog["all_artists"]
    checked = prog.get("checked_followers", {})
    qualified = {}  # norm → name (20+ followers)

    if prog["phase"] == "followers":
        unchecked = {n: name for n, name in all_artists.items() if n not in checked and n not in index}
        print(f"\nPhase 2: Checking RA follower counts ({len(unchecked)} to check, {len(checked)} already done)\n")

        total = len(unchecked)
        for i, (n, name) in enumerate(unchecked.items()):
            fc = get_follower_count(name)
            checked[n] = fc if fc is not None else -1

            if fc is not None and fc >= MIN_FOLLOWERS:
                qualified[n] = name
                sys.stdout.write(f"\r  [{i+1}/{total}] {name} — {fc} followers ✓")
            else:
                fc_str = str(fc) if fc is not None else "?"
                sys.stdout.write(f"\r  [{i+1}/{total}] {name} — {fc_str} followers  ")

            sys.stdout.flush()
            time.sleep(0.3)

            if (i + 1) % 100 == 0:
                prog["checked_followers"] = checked
                save_progress(prog)

        prog["checked_followers"] = checked
        prog["phase"] = "images"
        save_progress(prog)
        print()

    # Also include already-checked qualified artists
    for n, fc in checked.items():
        if fc >= MIN_FOLLOWERS and n in all_artists and n not in index:
            qualified[n] = all_artists[n]

    print(f"\nQualified artists (20+ followers): {len(qualified)}")
    print(f"Already in index: {len(index)}")
    to_fetch = {n: name for n, name in qualified.items() if n not in index}
    print(f"Images to fetch: {len(to_fetch)}\n")

    # ── Phase 3: Download images ────────────────────────────────
    if not to_fetch:
        print("All images already fetched!")
    else:
        print(f"Phase 3: Downloading and resizing images\n")
        success = 0
        no_image = 0
        errors = 0

        for i, (n, name) in enumerate(to_fetch.items()):
            s = slug(name)
            out_path = IMG_DIR / f"{s}.webp"

            if out_path.exists():
                index[n] = s
                success += 1
                continue

            img_url = get_ra_image_url(s)
            if not img_url:
                no_image += 1
                sys.stdout.write(f"\r  [{i+1}/{len(to_fetch)}] {name} — no image")
                sys.stdout.flush()
                time.sleep(RA_RATE)
                continue

            if download_and_resize(img_url, out_path):
                size_kb = out_path.stat().st_size / 1024
                index[n] = s
                success += 1
                sys.stdout.write(f"\r  [{i+1}/{len(to_fetch)}] {name} — {size_kb:.0f}KB ✓     ")
            else:
                errors += 1
                sys.stdout.write(f"\r  [{i+1}/{len(to_fetch)}] {name} — download failed")

            sys.stdout.flush()
            time.sleep(RA_RATE)

            if (i + 1) % 50 == 0:
                with open(INDEX_FILE, "w") as f:
                    json.dump(index, f)
                print(f"\n  [saved: {len(index)} in index]")

        print()

    # ── Final save ──────────────────────────────────────────────
    with open(INDEX_FILE, "w") as f:
        json.dump(index, f)

    total_files = len(list(IMG_DIR.glob("*.webp")))
    total_mb = sum(f.stat().st_size for f in IMG_DIR.glob("*.webp")) / (1024 * 1024) if total_files else 0

    print(f"\n═══ DONE ═══")
    print(f"  Artists in index:  {len(index)}")
    print(f"  Image files:       {total_files}")
    print(f"  Total size:        {total_mb:.1f}MB")
    print(f"  Index file:        {INDEX_FILE}")
    print(f"  Image directory:   {IMG_DIR}/")
    print(f"\nCommit img/ and artist_images.json to your repo.")
    print(f"Delete {PROGRESS_FILE} when satisfied with results.")


if __name__ == "__main__":
    main()
