#!/usr/bin/env python3
"""Probe Dice.fm API endpoints to see what actually works"""
import urllib.request, json, time

HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

def fetch(url, label):
    print(f"\nTrying: {label}")
    print(f"  URL: {url}")
    try:
        req = urllib.request.Request(url, headers=HDRS)
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            print(f"  ✓ Status 200")
            print(f"  Keys: {list(data.keys())[:5]}")
            print(f"  Sample: {json.dumps(data)[:300]}")
            return data
    except urllib.error.HTTPError as e:
        print(f"  ✗ HTTP {e.code}: {e.reason}")
        try: print(f"  Body: {e.read()[:200]}")
        except: pass
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return None

# Test 1: Internal artist events API
fetch("https://api.dice.fm/api/v1/artists/ben-ufo/events?page[size]=10", "Internal artist events (slug)")

time.sleep(1)

# Test 2: Public events search
fetch("https://api.dice.fm/api/v1/events?page[size]=5&filter[query]=Ben+UFO", "Events search by artist name")

time.sleep(1)

# Test 3: Public events by city
fetch("https://api.dice.fm/api/v1/events?page[size]=5&filter[location_name]=Barcelona&filter[tags]=electronic", "Barcelona electronic events")

time.sleep(1)

# Test 4: Dice public API (different base)
fetch("https://dice.fm/api/v1/events?page[size]=5&filter[location_name]=Barcelona", "Dice public API events")

time.sleep(1)

# Test 5: Artist page scrape
fetch("https://dice.fm/artist/ben-ufo-qnkap", "Artist page")

time.sleep(1)

# Test 6: Dice web app API
fetch("https://api.dice.fm/api/v2/events?page[size]=5&filter[location_name]=Barcelona", "v2 API")

print("\nDone.")
