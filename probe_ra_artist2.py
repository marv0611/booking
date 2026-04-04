#!/usr/bin/env python3
"""Find RA artist ID from name via eventListings"""
import urllib.request, json, time

BASE = "http://localhost:8000/api/ra/graphql"

def gql(q, v=None):
    body = json.dumps({"query": q, "variables": v or {}}).encode()
    req = urllib.request.Request(BASE, data=body, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return json.loads(r.read())
    except Exception as e: return {"error": str(e)}

# Test 1: slug format variations
print("=== Test slug formats ===")
for slug in ["ben-ufo", "benufo", "Ben UFO", "ben_ufo"]:
    r = gql('{ artist(slug:"'+slug+'") { id name } }')
    d = r.get("data",{}).get("artist")
    print(f"  slug '{slug}': {d}")
    time.sleep(0.3)

# Test 2: get artist ID from eventListings (we already have this working)
print("\n=== Get Ben UFO artist ID from events ===")
r = gql('''{ eventListings(filters:{areas:{eq:20},listingDate:{gte:"2024-01-01",lte:"2026-12-31"}},filterOptions:{},page:1,pageSize:50){
  data{ event{ artists{ id name } } }
}}''')
events = r.get("data",{}).get("eventListings",{}).get("data",[])
artists_found = {}
for el in events:
    for a in (el.get("event",{}).get("artists") or []):
        n = a.get("name","")
        if "ben" in n.lower() or "ufo" in n.lower() or "dixon" in n.lower() or "jayda" in n.lower():
            artists_found[n] = a.get("id")
print(f"  Found: {artists_found}")

# Test 3: try artist with urlSafeName
print("\n=== Try urlSafeName field ===")
r = gql('{ artist(id:"72") { id name urlSafeName contentUrl instagram soundcloud } }')
print(f"  David Morales: {r.get('data',{})}")

# Test 4: relatedArtists full query with more fields
print("\n=== Full relatedArtists for a known ID ===")
# Try Ben UFO - ID from RA is likely findable
# Let's try a few IDs we might know
for artist_id, name in [("6513","Ben UFO"), ("1234","?"), ("5000","?")]:
    r = gql('{ artist(id:"'+artist_id+'") { id name relatedArtists { id name } } }')
    d = r.get("data",{}).get("artist")
    if d and d.get("name"):
        print(f"  ID {artist_id} = {d['name']} → related: {[x['name'] for x in (d.get('relatedArtists') or [])][:5]}")
    time.sleep(0.2)

# Test 5: get artist ID from contentUrl
print("\n=== Get ID from contentUrl via events ===")
r = gql('''{ eventListings(filters:{areas:{eq:20},listingDate:{gte:"2025-01-01",lte:"2026-12-31"}},filterOptions:{},page:1,pageSize:10){
  data{ event{ artists{ id name contentUrl } } }
}}''')
events = r.get("data",{}).get("eventListings",{}).get("data",[])
for el in events[:3]:
    for a in (el.get("event",{}).get("artists") or [])[:2]:
        print(f"  {a.get('name')} → id:{a.get('id')} url:{a.get('contentUrl')}")

print("\nDone.")
