import requests, json, time
from datetime import datetime

# Config
EVENT_DATE = "2026-06-26"
MONTHS_BACK = 7   # Nov 26, 2025
MONTHS_FORWARD = 3  # Sep 26, 2026
RA_AREA = 20  # Barcelona

start_date = "2025-11-26"
end_date = "2026-09-26"
same_day = "2026-06-26"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://ra.co/events/es/barcelona",
    "Origin": "https://ra.co",
}
QUERY = """query Q($f:FilterInputDtoInput,$fo:FilterOptionsInputDtoInput,$p:Int,$s:Int){
  eventListings(filters:$f,filterOptions:$fo,pageSize:$s,page:$p){
    data{id listingDate event{id title date venue{id name area{id name}} artists{id name}}}
    totalResults
  }
}"""

def fetch_events(area_filter=True, date_gte=None, date_lte=None, label=""):
    """Fetch all events from RA GraphQL with pagination"""
    all_events = []
    page = 1
    while True:
        filters = {"listingDate": {"gte": date_gte, "lte": date_lte}}
        if area_filter:
            filters["areas"] = {"eq": RA_AREA}
        
        variables = {"f": filters, "fo": {}, "p": page, "s": 50}
        try:
            r = requests.post("https://ra.co/graphql", json={"query": QUERY, "variables": variables}, headers=HEADERS, timeout=30)
            r.raise_for_status()
            d = r.json()
        except Exception as e:
            print(f"  ERROR page {page}: {e}")
            break
        
        listings = d.get("data", {}).get("eventListings", {})
        events = listings.get("data", [])
        total = listings.get("totalResults", 0)
        
        if not events:
            break
        
        for el in events:
            ev = el.get("event") or {}
            all_events.append(el)
        
        if page % 20 == 0:
            print(f"  {label} page {page}... {len(all_events)}/{total}")
        
        if page * 50 >= total or page >= 200:
            break
        page += 1
        time.sleep(0.5)
    
    print(f"  {label}: {len(all_events)} events from {page} pages")
    return all_events

# ═══════════════════════════════════════════════
# PART 1: Barcelona events (7mo back, 3mo forward)
# ═══════════════════════════════════════════════
print(f"PART 1: BCN events {start_date} → {end_date}")
bcn_events = fetch_events(area_filter=True, date_gte=start_date, date_lte=end_date, label="BCN")

bcn_artists = {}
for el in bcn_events:
    ev = el.get("event") or {}
    event_date = (ev.get("date") or "")[:10]
    venue = (ev.get("venue") or {}).get("name", "?")
    area = ((ev.get("venue") or {}).get("area") or {}).get("name", "")
    if "barcelona" not in area.lower():
        continue
    for a in ev.get("artists") or []:
        name = (a.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in bcn_artists:
            bcn_artists[key] = {"name": name, "appearances": []}
        existing = bcn_artists[key]["appearances"]
        if not any(x["date"] == event_date and x["venue"] == venue for x in existing):
            existing.append({"date": event_date, "venue": venue})

print(f"  → {len(bcn_artists)} unique BCN artists")

# ═══════════════════════════════════════════════
# PART 2: Worldwide events on June 26 only
# ═══════════════════════════════════════════════
print(f"\nPART 2: Worldwide events on {same_day}")
world_events = fetch_events(area_filter=False, date_gte=same_day, date_lte=same_day, label="WORLD")

world_artists = {}
for el in world_events:
    ev = el.get("event") or {}
    event_date = (ev.get("date") or "")[:10]
    venue = (ev.get("venue") or {}).get("name", "?")
    area = ((ev.get("venue") or {}).get("area") or {}).get("name", "?")
    for a in ev.get("artists") or []:
        name = (a.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in world_artists:
            world_artists[key] = {"name": name, "gigs": []}
        existing = world_artists[key]["gigs"]
        if not any(x["venue"] == venue for x in existing):
            existing.append({"venue": venue, "area": area})

print(f"  → {len(world_artists)} unique artists playing worldwide on {same_day}")

# ═══════════════════════════════════════════════
# PART 3: Combine and save
# ═══════════════════════════════════════════════
output = {
    "generated": datetime.now().isoformat(),
    "event_date": EVENT_DATE,
    "bcn_window": {"from": start_date, "to": end_date, "months_back": MONTHS_BACK, "months_forward": MONTHS_FORWARD},
    "same_day_date": same_day,
    "stats": {
        "bcn_artists": len(bcn_artists),
        "bcn_appearances": sum(len(a["appearances"]) for a in bcn_artists.values()),
        "worldwide_same_day_artists": len(world_artists),
    },
    "bcn_exclusions": bcn_artists,
    "same_day_exclusions": world_artists,
}

with open("exclusions_verified.json", "w") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"\n{'='*50}")
print(f"SAVED: exclusions_verified.json")
print(f"BCN artists: {len(bcn_artists)}")
print(f"BCN appearances: {output['stats']['bcn_appearances']}")
print(f"Worldwide same-day: {len(world_artists)}")

# Sanity checks
print(f"\n{'='*50}")
print("SANITY CHECKS:")
for check in ["dam swindle", "crazy p", "folamour", "dave lee"]:
    if check in bcn_artists:
        apps = bcn_artists[check]["appearances"]
        print(f"  BCN ✓ {bcn_artists[check]['name']}: {', '.join(a['date'] + ' @ ' + a['venue'] for a in apps)}")
    else:
        print(f"  BCN ✗ {check}: not in BCN exclusions")
    if check in world_artists:
        gigs = world_artists[check]["gigs"]
        print(f"  JUN26 ✓ {world_artists[check]['name']}: {', '.join(g['venue'] + ' (' + g['area'] + ')' for g in gigs)}")
    else:
        print(f"  JUN26 ✗ {check}: not playing June 26")

# Check dates are correct
all_dates = [ap["date"] for a in bcn_artists.values() for ap in a["appearances"]]
all_dates.sort()
print(f"\n  Date range in data: {all_dates[0]} → {all_dates[-1]}")
print(f"  Expected range:     {start_date} → {end_date}")
before = sum(1 for d in all_dates if d < start_date)
after = sum(1 for d in all_dates if d > end_date)
print(f"  Dates outside window: {before} before, {after} after")
