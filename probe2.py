#!/usr/bin/env python3
"""Probe promoter profile fields and event connection"""
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

print("\n=== STEP 1: What fields does the Promoter type have? ===")
r = gql('{ __type(name: "Promoter") { name fields { name type { name kind ofType { name kind } } } } }')
fields = r.get("data",{}).get("__type",{}).get("fields",[])
if fields:
    print(f"Promoter type has {len(fields)} fields:")
    for f in fields:
        t = f.get("type",{})
        tname = t.get("name") or t.get("ofType",{}).get("name","?")
        print(f"  {f['name']}: {tname}")
else:
    print("No fields:", r)

time.sleep(1)

print("\n=== STEP 2: Try fetching promoter with events ===")
queries = [
    ("promoter with eventListings", '{ promoter(id: 103095) { id name eventListings { totalResults data { id listingDate event { title date artists { name } } } } } }'),
    ("promoter with events", '{ promoter(id: 103095) { id name events { id title date artists { name } } } }'),
    ("promoter with listings", '{ promoter(id: 103095) { id name listings { id title } } }'),
    ("promoter followers", '{ promoter(id: 103095) { id name followCount contentUrl } }'),
]
for label, q in queries:
    print(f"\nTrying: {label}")
    r = gql(q)
    if r.get("errors"):
        print(f"  Error: {r['errors'][0].get('message','?')[:120]}")
    elif r.get("data"):
        print(f"  ✓ {json.dumps(r['data'])[:300]}")
    time.sleep(0.5)

print("\n=== STEP 3: Check what the name field on eventListings filter actually does ===")
# Try filtering by promoter name instead of ID
r = gql('''{ eventListings(
    filters: { name: { eq: "Brunch Electronik" }, listingDate: { gte: "2024-01-01", lte: "2025-12-31" } }
    filterOptions: {}
    page: 1
    pageSize: 5
) { totalResults data { id listingDate event { title date venue { name } artists { name } } } } }''')
total = r.get("data",{}).get("eventListings",{}).get("totalResults",0)
data = r.get("data",{}).get("eventListings",{}).get("data",[])
errors = r.get("errors",[])
print(f"\nFilter by name='Brunch Electronik': {total} results")
if errors: print(f"  Error: {errors[0].get('message','?')[:100]}")
for ev in data[:3]:
    e = ev.get("event",{})
    print(f"  {e.get('date','?')[:10]} @ {e.get('venue',{}).get('name','?')} — {e.get('title','?')[:40]}")

time.sleep(0.5)

print("\n=== STEP 4: Try the title filter ===")
r = gql('''{ eventListings(
    filters: { title: { term: "Brunch Electronik" }, listingDate: { gte: "2024-01-01", lte: "2025-12-31" } }
    filterOptions: {}
    page: 1
    pageSize: 5
) { totalResults data { id listingDate event { title date venue { name } artists { name } } } } }''')
total = r.get("data",{}).get("eventListings",{}).get("totalResults",0)
data = r.get("data",{}).get("eventListings",{}).get("data",[])
errors = r.get("errors",[])
print(f"\nFilter by title term 'Brunch Electronik': {total} results")
if errors: print(f"  Error: {errors[0].get('message','?')[:100]}")
for ev in data[:3]:
    e = ev.get("event",{})
    print(f"  {e.get('date','?')[:10]} @ {e.get('venue',{}).get('name','?')} — {e.get('title','?')[:50]}")

print("\nDone.")
