#!/usr/bin/env python3
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

print("=== EventQueryType enum values ===")
r = gql('{ __type(name: "EventQueryType") { name enumValues { name } } }')
vals = r.get("data",{}).get("__type",{}).get("enumValues",[])
print([v["name"] for v in vals])
time.sleep(0.5)

print("\n=== Try promoter events with each type ===")
for t in ["PAST","UPCOMING","ALL","past","upcoming","all"]:
    q = '{ promoter(id: 103095) { name events(type: '+t+') { id title date venue { name } artists { name } } } }'
    r = gql(q)
    if r.get("errors"):
        print(f"  {t}: Error — {r['errors'][0].get('message','?')[:80]}")
    elif r.get("data",{}).get("promoter",{}).get("events"):
        evs = r["data"]["promoter"]["events"]
        print(f"  {t}: ✓ {len(evs)} events — first: {evs[0].get('title','?')[:40] if evs else 'none'}")
    else:
        print(f"  {t}: no data")
    time.sleep(0.3)

print("\n=== Promoter full profile ===")
r = gql('{ promoter(id: 103095) { id name followerCount upcomingEventsCount instagram contentUrl } }')
print(json.dumps(r.get("data",{}), indent=2))
