#!/usr/bin/env python3
"""Probe RA event genres field"""
import urllib.request, json, time

BASE = "http://localhost:8000/api/ra/graphql"

def gql(q, v=None):
    body = json.dumps({"query": q, "variables": v or {}}).encode()
    req = urllib.request.Request(BASE, data=body, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return json.loads(r.read())
    except Exception as e: return {"error": str(e)}

print("=== Event genres field ===")
r = gql('''{ eventListings(
  filters:{areas:{eq:20},listingDate:{gte:"2025-01-01",lte:"2026-12-31"}}
  filterOptions:{}
  page:1 pageSize:5
){ data{ event{ id title genres{ id name } startTime artists{id name} } } }}''')
events = r.get("data",{}).get("eventListings",{}).get("data",[])
for el in events[:5]:
    ev = el.get("event",{})
    print(f"\n  {ev.get('title','?')[:50]}")
    print(f"  Genres: {[g.get('name') for g in ev.get('genres') or []]}")
    print(f"  startTime: {ev.get('startTime')}")
    print(f"  Artists: {[a.get('name') for a in (ev.get('artists') or [])[:3]]}")

time.sleep(0.5)

print("\n=== Artist genres/tags field ===")
r = gql('''{ artist(id:"6513") { id name
  genres { id name }
} }''')
print(r.get("data",{}))

time.sleep(0.3)

print("\n=== Genre type introspection ===")
r = gql('{ __type(name:"Genre") { name fields { name type { name } } } }')
print(r.get("data",{}))
