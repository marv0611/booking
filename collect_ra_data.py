#!/usr/bin/env python3
"""
Night Pulse — RA Data Collector v2
====================================
Uses correct RA GraphQL query names (discovered via schema introspection).

Queries:
  venues(areaId)    → registered venues per city
  promoters(areaId) → registered promoters per city
  eventListings     → 12-month event count (splits date range to beat 10K cap)

Usage:
  python3 collect_ra_data.py --city berlin         # single city test
  python3 collect_ra_data.py --top 10              # top 10 cities
  python3 collect_ra_data.py --top 10 --deep       # with venue/artist breakdown
  python3 collect_ra_data.py                       # all 78 cities
"""

import json, csv, time, sys, argparse, math
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.2

TODAY = datetime.now()
DATE_12M_AGO = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DATE_TODAY = TODAY.strftime("%Y-%m-%d")

Q_VENUES_LIST = 'query { venues(areaId: %d, limit: 500) { id name } }'
Q_PROMOTERS_LIST = 'query { promoters(areaId: %d, limit: 500) { id name } }'

Q_EVENTS_COUNT = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""

Q_EVENTS_DETAIL = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    data {
      event {
        id title date
        venue { id name }
        artists { id name }
        genres { name }
        interestedCount
      }
    }
    totalResults
  }
}"""


def post_raw(query_str):
    payload = json.dumps({"query": query_str}).encode()
    req = Request(API, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            time.sleep(10)
            return post_raw(query_str)
        return None
    except:
        return None


def post(query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = Request(API, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            time.sleep(10)
            return post(query, variables)
        return None
    except:
        return None


def get_venues(area_id):
    resp = post_raw(Q_VENUES_LIST % area_id)
    if resp and "data" in resp and resp["data"].get("venues"):
        return resp["data"]["venues"]
    return None


def get_promoters(area_id):
    resp = post_raw(Q_PROMOTERS_LIST % area_id)
    if resp and "data" in resp and resp["data"].get("promoters"):
        return resp["data"]["promoters"]
    return None


def get_events_count(area_id, date_from, date_to):
    v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": date_from, "lte": date_to}}, "page": 1, "pageSize": 1}
    resp = post(Q_EVENTS_COUNT, v)
    if resp and "data" in resp:
        return resp["data"]["eventListings"]["totalResults"]
    return None


def get_true_event_count(area_id):
    total = get_events_count(area_id, DATE_12M_AGO, DATE_TODAY)
    if total is None:
        return None
    if total < 10000:
        return total
    print(f"    ↳ Hit 10K cap, splitting into quarters...")
    real = 0
    base = datetime.strptime(DATE_12M_AGO, "%Y-%m-%d")
    for q in range(4):
        qs = (base + timedelta(days=q * 91)).strftime("%Y-%m-%d")
        qe = (base + timedelta(days=(q + 1) * 91 - 1)).strftime("%Y-%m-%d")
        if q == 3:
            qe = DATE_TODAY
        time.sleep(DELAY)
        qc = get_events_count(area_id, qs, qe) or 0
        if qc >= 10000:
            print(f"    ↳ Q{q+1} also capped, splitting months...")
            mc = 0
            qb = datetime.strptime(qs, "%Y-%m-%d")
            for m in range(3):
                ms = (qb + timedelta(days=m * 30)).strftime("%Y-%m-%d")
                me = (qb + timedelta(days=(m + 1) * 30 - 1)).strftime("%Y-%m-%d")
                if m == 2: me = qe
                time.sleep(DELAY)
                mc += get_events_count(area_id, ms, me) or 0
            qc = mc
        real += qc
    return real


def get_event_page(area_id, page=1):
    v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": DATE_12M_AGO, "lte": DATE_TODAY}}, "page": page, "pageSize": 50}
    resp = post(Q_EVENTS_DETAIL, v)
    if resp and "data" in resp:
        ls = resp["data"]["eventListings"]
        return [i["event"] for i in ls.get("data", []) if i.get("event")], ls.get("totalResults", 0)
    return [], 0


def analyze_events(area_id, max_pages=4):
    evts = []
    total = 0
    for p in range(1, max_pages + 1):
        e, total = get_event_page(area_id, p)
        evts.extend(e)
        if len(evts) >= total or not e: break
        time.sleep(DELAY)
    if not evts: return {}
    ratio = total / max(len(evts), 1)
    vm = {}
    for e in evts:
        v = e.get("venue") or {}
        vid = v.get("id")
        if vid:
            vm.setdefault(vid, 0)
            vm[vid] += 1
    vc = [c * ratio for c in vm.values()]
    artists = set()
    for e in evts:
        for a in e.get("artists") or []:
            if a.get("id"): artists.add(a["id"])
    brands = Counter()
    for e in evts:
        t = (e.get("title") or "").strip()
        for sep in [":", " presents ", " w/ ", " x ", " | ", " — ", " at ", " @ "]:
            if sep in t:
                b = t.split(sep)[0].strip()
                if 2 < len(b) < 50: brands[b.lower()] += 1
                break
    genres = Counter()
    for e in evts:
        for g in e.get("genres") or []:
            n = g.get("name", "").strip()
            if n: genres[n] += 1
    att = [e.get("interestedCount", 0) or 0 for e in evts if (e.get("interestedCount") or 0) > 0]
    return {
        "sample_size": len(evts),
        "unique_venues_sample": len(vm),
        "venues_12plus": sum(1 for c in vc if c >= 12),
        "venues_20plus": sum(1 for c in vc if c >= 20),
        "venues_50plus": sum(1 for c in vc if c >= 50),
        "unique_artists_sample": len(artists),
        "brand_promoters_found": len(brands),
        "brands_12plus": sum(1 for c in brands.values() if c * ratio >= 12),
        "brands_20plus": sum(1 for c in brands.values() if c * ratio >= 20),
        "top_genre": genres.most_common(1)[0][0] if genres else "N/A",
        "avg_attendance": round(sum(att) / max(len(att), 1)) if att else 0,
    }


def load_cities(fp="ra_area_ids.json"):
    with open(fp) as f:
        data = json.load(f)
    out = []
    for key, val in data.get("european_cities", {}).items():
        note = val.get("note", "")
        if "legacy" in note or "country-level" in note or "region" in note:
            continue
        out.append({"key": key, "name": key.replace("_", " ").title(), "area_id": val["id"], "country": val["country"], "hist": val.get("events", 0)})
    return sorted(out, key=lambda c: c["hist"], reverse=True)


def main():
    pa = argparse.ArgumentParser()
    pa.add_argument("--top", type=int)
    pa.add_argument("--city", type=str)
    pa.add_argument("--deep", action="store_true")
    pa.add_argument("--ids-file", default="ra_area_ids.json")
    args = pa.parse_args()

    cities = load_cities(args.ids_file)
    if args.city:
        cities = [c for c in cities if c["key"] == args.city]
    elif args.top:
        cities = cities[:args.top]

    if not cities:
        print("No cities found."); return

    # Test
    aid0 = cities[0]["area_id"]
    print(f"Testing on {cities[0]['name']}...")

    print("  venues query...")
    vtest = get_venues(aid0)
    v_ok = vtest is not None
    print(f"  {'✓' if v_ok else '✗'} venues: {len(vtest) if vtest else 'failed'}")
    time.sleep(DELAY)

    print("  promoters query...")
    ptest = get_promoters(aid0)
    p_ok = ptest is not None
    print(f"  {'✓' if p_ok else '✗'} promoters: {len(ptest) if ptest else 'failed'}")
    time.sleep(DELAY)

    n = len(cities)
    calls = n * (1 + int(v_ok) + int(p_ok) + (4 if args.deep else 0))
    print(f"\n{'='*60}")
    print(f"  {n} cities | ~{calls * DELAY / 60:.1f} min")
    print(f"  {DATE_12M_AGO} → {DATE_TODAY}")
    print(f"{'='*60}\n")

    results = []
    for i, c in enumerate(cities, 1):
        aid = c["area_id"]
        print(f"[{i}/{n}] {c['name']}...")
        row = {"city": c["name"], "key": c["key"], "country": c["country"], "area_id": aid}
        row["events_12m"] = get_true_event_count(aid)
        time.sleep(DELAY)
        if v_ok:
            vl = get_venues(aid)
            row["ra_venues"] = len(vl) if vl else 0
            time.sleep(DELAY)
        if p_ok:
            pl = get_promoters(aid)
            row["ra_promoters"] = len(pl) if pl else 0
            time.sleep(DELAY)
        if args.deep:
            row.update(analyze_events(aid))
        ev = row.get("events_12m") or 0
        print(f"    → events={ev:,}  venues={row.get('ra_venues','?')}  promoters={row.get('ra_promoters','?')}")
        results.append(row)

    # Write
    all_keys = []
    for r in results:
        for k in r:
            if k not in all_keys: all_keys.append(k)
    with open("ra_market_data.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(results, key=lambda r: r.get("events_12m") or 0, reverse=True))
    with open("ra_market_data.json", "w") as f:
        json.dump({"collected": datetime.now().isoformat(), "range": [DATE_12M_AGO, DATE_TODAY],
                    "totals": {"events": sum(r.get("events_12m") or 0 for r in results),
                               "venues": sum(r.get("ra_venues") or 0 for r in results) if v_ok else "?",
                               "promoters": sum(r.get("ra_promoters") or 0 for r in results) if p_ok else "?"},
                    "data": results}, f, indent=2, default=str)

    te = sum(r.get("events_12m") or 0 for r in results)
    print(f"\n{'='*60}")
    print(f"  {n} cities | {te:,} events/year")
    if v_ok: print(f"  {sum(r.get('ra_venues',0) for r in results):,} venues")
    if p_ok: print(f"  {sum(r.get('ra_promoters',0) for r in results):,} promoters")
    print(f"{'='*60}")
    print(f"\n✓ ra_market_data.csv + ra_market_data.json")
    print(f"Upload both back to Claude.")

if __name__ == "__main__":
    main()
