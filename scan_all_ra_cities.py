#!/usr/bin/env python3
"""
Scan ALL RA area IDs (1-500) and list every city with events.
Finds the top 80+ European cities by event count.

Usage: python3 scan_all_ra_cities.py
Takes ~3 minutes (500 quick API calls).
"""
import json, urllib.request, time, sys

RA_URL = "https://ra.co/graphql"
RA_HDRS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://ra.co/events",
    "Origin": "https://ra.co",
}

Q = """query C($a:Int!){eventListings(filters:{areas:{eq:$a}},pageSize:1){totalResults data{event{venue{area{id name urlName country{name urlCode}}}}}}}"""

results = []
print("Scanning RA area IDs 1-500...")
print("This takes ~3 minutes.\n")

for aid in range(1, 501):
    body = json.dumps({"query": Q, "variables": {"a": aid}}).encode()
    req = urllib.request.Request(RA_URL, data=body, headers=RA_HDRS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            d = json.loads(resp.read())
        total = d.get("data", {}).get("eventListings", {}).get("totalResults", 0)
        if total > 0:
            events = d.get("data", {}).get("eventListings", {}).get("data", [])
            area = {}
            if events:
                area = events[0].get("event", {}).get("venue", {}).get("area", {}) or {}
            name = area.get("name", "?")
            country = area.get("country", {}).get("name", "?") if area.get("country") else "?"
            country_code = area.get("country", {}).get("urlCode", "?") if area.get("country") else "?"
            results.append({
                "id": aid,
                "name": name,
                "country": country,
                "country_code": country_code,
                "events": total,
            })
            sys.stdout.write(f"\r  Scanned {aid}/500 — found {len(results)} cities")
            sys.stdout.flush()
    except Exception as e:
        pass
    # Small delay to be polite
    if aid % 20 == 0:
        time.sleep(0.3)

print(f"\n\nFound {len(results)} areas total.\n")

# Sort by event count
results.sort(key=lambda x: -x["events"])

# European country codes
EU_CODES = {
    "uk", "gb", "de", "fr", "es", "it", "nl", "be", "pt", "at", "ch", "ie",
    "dk", "se", "no", "fi", "pl", "cz", "hu", "ro", "gr", "hr", "bg", "sk",
    "si", "lt", "lv", "ee", "ge", "ua", "rs", "ba", "me", "mk", "al", "mt",
    "cy", "is", "lu", "li", "tr"
}

print("=" * 80)
print("ALL EUROPEAN CITIES (sorted by events)")
print("=" * 80)
print(f"{'ID':>5} {'City':<30} {'Country':<20} {'Events':>8}")
print("-" * 80)

eu_cities = []
for r in results:
    cc = r["country_code"].lower()
    if cc in EU_CODES:
        eu_cities.append(r)
        print(f"{r['id']:>5} {r['name']:<30} {r['country']:<20} {r['events']:>8}")

print(f"\nTotal European cities with events: {len(eu_cities)}")

print("\n" + "=" * 80)
print("NON-EUROPEAN CITIES (for reference)")
print("=" * 80)
for r in results:
    cc = r["country_code"].lower()
    if cc not in EU_CODES:
        print(f"{r['id']:>5} {r['name']:<30} {r['country']:<20} {r['events']:>8}")

# Generate JS code
print("\n" + "=" * 80)
print("COPY-PASTE JS — Top European cities for AREAS constant:")
print("=" * 80)
print("const AREAS={")
for r in eu_cities:
    slug = r["name"].lower().replace(" ", "-").replace("'", "")
    # Simple slug: lowercase, ascii-safe
    slug2 = r["name"].lower()
    for ch in "''àáâãäåèéêëìíîïòóôõöùúûüýÿñ":
        slug2 = slug2.replace(ch, "")
    slug2 = slug2.strip().replace("  ", " ").replace(" ", " ")
    js_key = repr(slug2) if " " in slug2 else slug2
    print(f"  {js_key}:{r['id']}, // {r['country']} — {r['events']} events")
print("};")
