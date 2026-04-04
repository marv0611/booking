#!/usr/bin/env python3
"""
Probe RA GraphQL to find the correct promoter filter field.
Run while server.py is running: python3 probe_promoter.py
"""
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

print("\n=== STEP 1: Get all FilterInput fields ===")
r = gql('''{ 
  __type(name: "FilterInputDtoInput") { 
    name 
    inputFields { 
      name 
      type { name kind ofType { name kind } } 
    } 
  } 
}''')
fields = r.get("data",{}).get("__type",{}).get("inputFields",[])
if fields:
    print(f"FilterInputDtoInput has {len(fields)} fields:")
    for f in fields:
        t = f.get("type",{})
        tname = t.get("name") or t.get("ofType",{}).get("name","?")
        print(f"  {f['name']}: {tname}")
else:
    print("No fields found or error:", r)

time.sleep(1)

print("\n=== STEP 2: Try known promoter IDs with different filter shapes ===")
# Brunch Electronik = 103095 (verified from ra.co/promoters/103095)
pid = 103095
shapes = [
    ("promoters.id.eq",    {"f":{"promoters":{"id":{"eq":pid}},"listingDate":{"gte":"2024-01-01","lte":"2025-12-31"}},"fo":{},"p":1,"s":3}),
    ("promoter.id.eq",     {"f":{"promoter":{"id":{"eq":pid}},"listingDate":{"gte":"2024-01-01","lte":"2025-12-31"}},"fo":{},"p":1,"s":3}),
    ("promoterId.eq",      {"f":{"promoterId":{"eq":pid},"listingDate":{"gte":"2024-01-01","lte":"2025-12-31"}},"fo":{},"p":1,"s":3}),
    ("organizers.id.eq",   {"f":{"organizers":{"id":{"eq":pid}},"listingDate":{"gte":"2024-01-01","lte":"2025-12-31"}},"fo":{},"p":1,"s":3}),
]

Q = 'query Q($f:FilterInputDtoInput,$fo:FilterOptionsInputDtoInput,$p:Int,$s:Int){eventListings(filters:$f,filterOptions:$fo,pageSize:$s,page:$p){data{id listingDate event{id title date artists{id name}}}totalResults}}'

for label, variables in shapes:
    print(f"\nTrying: {label}")
    r = gql(Q, variables)
    total = r.get("data",{}).get("eventListings",{}).get("totalResults",0)
    data = r.get("data",{}).get("eventListings",{}).get("data",[])
    errors = r.get("errors",[])
    if errors:
        print(f"  Error: {errors[0].get('message','?')[:100]}")
    elif total > 0:
        print(f"  ✓ SUCCESS! {total} total events")
        for ev in data[:2]:
            e = ev.get("event",{})
            artists = [a["name"] for a in e.get("artists",[])]
            print(f"    {e.get('date','?')[:10]} — {e.get('title','?')[:40]} — {', '.join(artists[:3])}")
    else:
        print(f"  0 results (no error — wrong field name or wrong ID)")
    time.sleep(0.5)

print("\n=== STEP 3: Try fetching promoter profile directly ===")
promoter_queries = [
    ("promoter by id", '{ promoter(id: 103095) { id name } }'),
    ("promoters list",  '{ promoters(ids: [103095]) { id name } }'),
    ("organizer",       '{ organizer(id: 103095) { id name } }'),
]
for label, q in promoter_queries:
    print(f"\nTrying: {label}")
    r = gql(q)
    if r.get("data") and any(v for v in r["data"].values() if v):
        print(f"  ✓ {json.dumps(r['data'])[:200]}")
    elif r.get("errors"):
        print(f"  Error: {r['errors'][0].get('message','?')[:100]}")
    else:
        print(f"  No data")
    time.sleep(0.3)

print("\nDone. Share the output.")
