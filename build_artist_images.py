#!/usr/bin/env python3
"""
SubPulse — Artist Image Pre-Builder

Downloads RA artist photos, resizes to 100x100 WebP thumbnails,
stores in img/artists/ for instant loading with zero API calls at runtime.

Usage:
  cd ~/Documents/GitHub/booking
  pip3 install Pillow --break-system-packages
  python3 build_artist_images.py

What it does:
  1. Loads your city event cache from the running server (or fetches fresh)
  2. Extracts unique artists, checks RA follower count ≥ 30
  3. Downloads RA og:image for each qualifying artist
  4. Resizes to 100x100 WebP thumbnails (~3-5KB each)
  5. Saves to img/artists/{slug}.webp
  6. Generates artist_images.json index

Re-run monthly to pick up new artists. ~1 second per artist.
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

try:
    from PIL import Image
except ImportError:
    print("❌ Pillow not installed. Run: pip3 install Pillow --break-system-packages")
    sys.exit(1)

# Config
SERVER = "http://localhost:8001"
IMG_DIR = Path("img/artists")
INDEX_FILE = Path("artist_images.json")
THUMB_SIZE = (100, 100)
MIN_FOLLOWERS = 30
RATE_LIMIT = 1.0  # seconds between RA page fetches

RA_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html",
}

def norm(s):
    return re.sub(r'[^a-z0-9]', '', s.lower())

def name_to_slug(name):
    return re.sub(r'[^a-z0-9]', '', name.lower())

def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "SubPulse/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def get_ra_image_url(slug):
    """Scrape og:image from RA artist page."""
    url = f"https://ra.co/dj/{slug}"
    try:
        req = urllib.request.Request(url, headers=RA_HDRS)
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html)
        if not m:
            m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html)
        if m:
            img = m.group(1)
            if "default" in img.lower() or "placeholder" in img.lower():
                return None
            return img
    except Exception:
        pass
    return None

def download_and_resize(url, output_path):
    """Download image and save as 100x100 WebP thumbnail."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read()
        img = Image.open(BytesIO(data))
        img = img.convert("RGB")
        # Center crop to square first
        w, h = img.size
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
        img = img.resize(THUMB_SIZE, Image.LANCZOS)
        img.save(output_path, "WEBP", quality=80)
        return True
    except Exception as e:
        print(f"    ✗ Download/resize failed: {e}")
        return False

def get_follower_count(name):
    """Get RA follower count via the proxy."""
    slug = name_to_slug(name)
    try:
        query = json.dumps({
            "query": "query A($slug:String){artist(slug:$slug){id followerCount}}",
            "variables": {"slug": slug}
        }).encode()
        req = urllib.request.Request(
            f"{SERVER}/api/ra/graphql",
            data=query,
            headers={"Content-Type": "application/json", "User-Agent": "SubPulse/1.0"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        artist = d.get("data", {}).get("artist")
        if artist and artist.get("followerCount") is not None:
            return artist["followerCount"]
    except Exception:
        pass
    return None

def load_city_cache():
    """Try to load events from the server's cache endpoint, or from a local file."""
    # Try fetching from a local cache export
    cache_file = Path("ra_cache_export.json")
    if cache_file.exists():
        print(f"Loading cache from {cache_file}...")
        with open(cache_file) as f:
            data = json.load(f)
        return data.get("events", data) if isinstance(data, dict) else data
    
    print("No cache file found. You need to export your city cache first.")
    print("")
    print("In the browser console (on SubPulse), run:")
    print('  copy(JSON.stringify(JSON.parse(localStorage.getItem("libro_ra_cache_v3"))))')
    print("")
    print("Then paste into a file: ra_cache_export.json")
    print("Then re-run this script.")
    return None

def main():
    print("═══ SubPulse Artist Image Builder ═══\n")
    
    # Create output directory
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load existing index to skip already-fetched artists
    existing = {}
    if INDEX_FILE.exists():
        with open(INDEX_FILE) as f:
            existing = json.load(f)
        print(f"Existing index: {len(existing)} artists")
    
    # Load cache
    events = load_city_cache()
    if not events:
        return
    
    print(f"Loaded {len(events)} events")
    
    # Extract unique artists
    artist_map = {}  # norm(name) → display name
    for ev in events:
        for a in (ev.get("artists") or []):
            name = a if isinstance(a, str) else (a.get("name", "") if isinstance(a, dict) else str(a))
            if name:
                n = norm(name)
                if n and len(n) >= 2 and n not in artist_map:
                    artist_map[n] = name
    
    print(f"Found {len(artist_map)} unique artists")
    
    # Filter: skip already indexed
    to_process = {n: name for n, name in artist_map.items() if n not in existing}
    print(f"New artists to process: {len(to_process)}")
    
    if not to_process:
        print("Nothing new to process. Done!")
        return
    
    # Process artists
    success = 0
    skipped_followers = 0
    skipped_no_image = 0
    errors = 0
    index = dict(existing)  # start from existing
    
    total = len(to_process)
    for i, (n, name) in enumerate(to_process.items()):
        slug = name_to_slug(name)
        out_path = IMG_DIR / f"{slug}.webp"
        
        # Skip if file already exists
        if out_path.exists():
            index[n] = slug
            success += 1
            continue
        
        print(f"[{i+1}/{total}] {name}", end="", flush=True)
        
        # Check follower count (rate limited by RA proxy)
        fc = get_follower_count(name)
        if fc is not None and fc < MIN_FOLLOWERS:
            print(f" — {fc} followers, skipping")
            skipped_followers += 1
            continue
        
        fc_str = f" ({fc} followers)" if fc is not None else ""
        
        # Get RA image URL
        img_url = get_ra_image_url(slug)
        if not img_url:
            print(f"{fc_str} — no RA image")
            skipped_no_image += 1
            time.sleep(RATE_LIMIT)
            continue
        
        # Download and resize
        if download_and_resize(img_url, out_path):
            size_kb = out_path.stat().st_size / 1024
            print(f"{fc_str} — ✓ {size_kb:.1f}KB")
            index[n] = slug
            success += 1
        else:
            errors += 1
        
        time.sleep(RATE_LIMIT)
        
        # Save index periodically
        if (i + 1) % 50 == 0:
            with open(INDEX_FILE, "w") as f:
                json.dump(index, f)
            print(f"  [saved index: {len(index)} artists]")
    
    # Final save
    with open(INDEX_FILE, "w") as f:
        json.dump(index, f)
    
    total_files = len(list(IMG_DIR.glob("*.webp")))
    total_size_mb = sum(f.stat().st_size for f in IMG_DIR.glob("*.webp")) / (1024 * 1024)
    
    print(f"\n═══ DONE ═══")
    print(f"  New images: {success}")
    print(f"  Skipped (low followers): {skipped_followers}")
    print(f"  Skipped (no RA image): {skipped_no_image}")
    print(f"  Errors: {errors}")
    print(f"  Total in index: {len(index)}")
    print(f"  Total files: {total_files}")
    print(f"  Total size: {total_size_mb:.1f}MB")
    print(f"\nFiles: {IMG_DIR}/")
    print(f"Index: {INDEX_FILE}")

if __name__ == "__main__":
    main()
