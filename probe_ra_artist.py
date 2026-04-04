#!/usr/bin/env python3
"""Probe RA GraphQL Artist type for related artists field"""
import urllib.request, json, time

BASE = "http://localhost:8000/api/ra/graphql"

def gql(q, v=None):
    body = json.dumps({"query": q, "variables": v or {}}).encode()
    req = urllib.request.Request(BASE, data=body, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return json.loads(r.read())
    except Exception as e: return {"error": str(e)}

print("=== Artist type fields ===")
r = gql('{ __type(name:"Artist") { fields { name type { name kind ofType { name } } } } }')
fields = r.get("data",{}).get("__type",{}).get("fields",[])
for f in fields:
    print(f"  {f['name']}: {f.get('type',{}).get('name') or f.get('type',{}).get('ofType',{}).get('name','?')}")

time.sleep(0.5)

print("\n=== Try artist query with relatedArtists ===")
queries = [
    ("artist by slug", '{ artist(slug:"ben-ufo") { id name relatedArtists { id name } } }'),
    ("artist by id", '{ artist(id:"72") { id name relatedArtists { id name } } }'),
    ("dj query", '{ dj(slug:"ben-ufo") { id name relatedArtists { id name } } }'),
    ("artist name search", '{ artists(name:"Ben UFO") { id name relatedArtists { id name } } }'),
    ("search query", '{ search(query:"Ben UFO") { artists { id name relatedArtists { id name } } } }'),
]
for label, q in queries:
    r = gql(q)
    if r.get("errors"): print(f"  {label}: {r['errors'][0]['message'][:100]}")
    elif r.get("data"): print(f"  {label}: {json.dumps(r['data'])[:200]}")
    time.sleep(0.3)

print("\n=== Check Query type for artist-related fields ===")
r = gql('{ __schema { queryType { fields { name args { name } } } } }')
fields = r.get("data",{}).get("__schema",{}).get("queryType",{}).get("fields",[])
for f in fields:
    if any(k in f['name'].lower() for k in ['artist','dj','similar','related']):
        args = [a['name'] for a in f.get('args',[])]
        print(f"  *** {f['name']}({', '.join(args)})")
