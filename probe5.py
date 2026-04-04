#!/usr/bin/env python3
"""Verify: fetch area events with promoters field, filter by promoter ID"""
import urllib.request, json, time

BASE = "http://localhost:8000/api/ra/graphql"

def gql(query, variables=None):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(BASE, data=body,
          headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

# New query — includes promoters field on each event
Q_WITH_PROMOTERS = '''query Q($f:FilterInputDtoInput,$fo:FilterOptionsInputDtoInput,$p:Int,$s:Int){
  eventListings(filters:$f,filterOptions:$fo,pageSize:$s,page:$p){
    data{
      id listingDate
      event{
        id title date
        venue{id name}
        artists{id name}
        promoters{id name}
        interestedCount
      }
    }
    totalResults
  }
}'''

print("=== Fetch Barcelona events page 1, filter client-side by promoter ID 103095 ===")
r = gql(Q_WITH_PROMOTERS, {
    "f": {"areas": {"eq": 20}, "listingDate": {"gte": "2024-01-01", "lte": "2026-12-31"}},
    "fo": {}, "p": 1, "s": 50
})
evs = r.get("data",{}).get("eventListings",{}).get("data",[])
total = r.get("data",{}).get("eventListings",{}).get("totalResults",0)
print(f"Total Barcelona events: {total}")

# Filter by promoter ID
brunch_events = []
for el in evs:
    ev = el.get("event",{})
    promos = ev.get("promoters") or []
    promo_ids = [str(p.get("id","")) for p in promos]
    if "103095" in promo_ids:
        brunch_events.append(ev)

print(f"Brunch Electronik events on page 1: {len(brunch_events)}")
for e in brunch_events[:5]:
    artists = [a["name"] for a in (e.get("artists") or [])]
    promos = [p["name"] for p in (e.get("promoters") or [])]
    print(f"  {e.get('date','?')[:10]} @ {e.get('venue',{}).get('name','?')[:25]}")
    print(f"    Artists: {', '.join(artists[:3])}")
    print(f"    Promoters: {', '.join(promos)}")

print("\n=== Also check: does PREVIOUS return more with year? ===")
for year in [2025, 2024, 2023]:
    q = '{ promoter(id: 103095) { events(type: ARCHIVE, year: '+str(year)+') { id title date venue { name } artists { name } } } }'
    r = gql(q)
    if r.get("errors"):
        print(f"  ARCHIVE {year}: {r['errors'][0].get('message','?')[:80]}")
    else:
        evs = r.get("data",{}).get("promoter",{}).get("events") or []
        print(f"  ARCHIVE {year}: {len(evs)} events")
        for e in evs[:2]:
            artists = [a["name"] for a in (e.get("artists") or [])]
            print(f"    {e.get('date','?')[:10]} — {e.get('title','?')[:40]} — {', '.join(artists[:2])}")
    time.sleep(0.5)

print("\nDone.")
