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

print("=== Try LATEST and PREVIOUS (most promising for recent events) ===")
for t in ["LATEST", "PREVIOUS", "ARCHIVE"]:
    q = '{ promoter(id: 103095) { name events(type: '+t+') { id title date venue { name } artists { name } } } }'
    r = gql(q)
    if r.get("errors"):
        print(f"  {t}: Error — {r['errors'][0].get('message','?')[:120]}")
    elif r.get("data",{}).get("promoter"):
        evs = r["data"]["promoter"].get("events") or []
        if isinstance(evs, list):
            print(f"  {t}: ✓ {len(evs)} events")
            for e in evs[:3]:
                artists = [a["name"] for a in (e.get("artists") or [])]
                print(f"    {e.get('date','?')[:10]} @ {e.get('venue',{}).get('name','?')[:25]} — {e.get('title','?')[:35]} — {', '.join(artists[:2])}")
        else:
            print(f"  {t}: data={json.dumps(evs)[:200]}")
    time.sleep(0.5)

print("\n=== Try LATEST with pagination ===")
# Events might need page/limit args
for t in ["LATEST"]:
    q = '{ promoter(id: 103095) { name events(type: '+t+', limit: 20, page: 1) { id title date venue { name } artists { name } } } }'
    r = gql(q)
    if r.get("errors"):
        print(f"  {t} paginated: {r['errors'][0].get('message','?')[:120]}")
    else:
        evs = r.get("data",{}).get("promoter",{}).get("events") or []
        print(f"  {t} paginated: {len(evs) if isinstance(evs,list) else evs}")
    time.sleep(0.3)

print("\n=== Check Event type fields from promoter ===")
r = gql('{ __type(name: "Event") { fields { name args { name type { name kind ofType { name } } } } } }')
fields = r.get("data",{}).get("__type",{}).get("fields",[])
# Find events-related fields
for f in fields:
    print(f"  {f['name']}")
