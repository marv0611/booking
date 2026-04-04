#!/usr/bin/env python3
"""
Night Pulse — Market Sizing (v4 FINAL)
========================================
Real promoter IDs from RA events. No heuristics. No page cap.

Every event on RA has:
  - venue { id name }     → who hosted it
  - promoters { id name } → who booked it
  - artists { id name }   → who played

A customer = venue or promoter that booked 12+ events in trailing 12 months.
A market = city with 1,000+ events/year.

Usage:
  python3 collect_ra_data.py --city berlin         # ~5 min
  python3 collect_ra_data.py --top 10              # ~60 min
  python3 collect_ra_data.py                       # all qualifying cities
"""

import json, csv, time, argparse
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.2
PAGE_SIZE = 50

TODAY = datetime.now()
DATE_START = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DATE_END = TODAY.strftime("%Y-%m-%d")

Q_EVENTS = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    data {
      event {
        id title date
        venue { id name }
        promoters { id name }
        artists { id name }
      }
    }
    totalResults
  }
}"""

Q_COUNT = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""


def post(query, variables):
    payload = json.dumps({"query": query, "variables": variables}).encode()
    req = Request(API, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            print("      ⚠ Rate limited, waiting 15s...")
            time.sleep(15)
            return post(query, variables)
        return None
    except Exception as e:
        print(f"      ✗ {e}")
        return None


def get_event_count(area_id, date_from=DATE_START, date_to=DATE_END):
    v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": date_from, "lte": date_to}}, "page": 1, "pageSize": 1}
    resp = post(Q_COUNT, v)
    if not resp or "data" not in resp:
        return 0
    total = resp["data"]["eventListings"]["totalResults"]
    if total < 10000:
        return total
    d1 = datetime.strptime(date_from, "%Y-%m-%d")
    d2 = datetime.strptime(date_to, "%Y-%m-%d")
    mid = d1 + (d2 - d1) / 2
    mid_str = mid.strftime("%Y-%m-%d")
    time.sleep(DELAY)
    left = get_event_count(area_id, date_from, mid_str)
    time.sleep(DELAY)
    right = get_event_count(area_id, (mid + timedelta(days=1)).strftime("%Y-%m-%d"), date_to)
    return left + right


def fetch_all_events(area_id, total):
    """Fetch ALL events. No page cap."""
    events = []
    pages = (total // PAGE_SIZE) + 1

    for page in range(1, pages + 1):
        v = {
            "filters": {"areas": {"eq": area_id}, "listingDate": {"gte": DATE_START, "lte": DATE_END}},
            "page": page, "pageSize": PAGE_SIZE,
        }
        resp = post(Q_EVENTS, v)
        if resp and "data" in resp:
            batch = [item["event"] for item in resp["data"]["eventListings"].get("data", []) if item.get("event")]
            events.extend(batch)
            if not batch:
                break
        else:
            break
        if page % 50 == 0:
            print(f"      ... {len(events):,}/{total:,} events")
        time.sleep(DELAY)

    return events


def analyze_city(events):
    # Venues: count events per venue ID
    venue_events = Counter()
    venue_names = {}
    for e in events:
        v = e.get("venue") or {}
        vid = v.get("id")
        if vid:
            venue_events[vid] += 1
            venue_names[vid] = v.get("name", "")

    # Promoters: count events per promoter ID (REAL, from RA data)
    promoter_events = Counter()
    promoter_names = {}
    for e in events:
        for p in e.get("promoters") or []:
            pid = p.get("id")
            if pid:
                promoter_events[pid] += 1
                promoter_names[pid] = p.get("name", "")

    # Artists
    artists = set()
    for e in events:
        for a in e.get("artists") or []:
            if a.get("id"):
                artists.add(a["id"])

    # Competition density
    night_counts = Counter()
    for e in events:
        d = (e.get("date") or "")[:10]
        if d:
            night_counts[d] += 1
    nights = list(night_counts.values())

    # Venue segmentation
    v_total = len(venue_events)
    v_12 = sum(1 for c in venue_events.values() if c >= 12)
    v_20 = sum(1 for c in venue_events.values() if c >= 20)
    v_50 = sum(1 for c in venue_events.values() if c >= 50)

    # Promoter segmentation (REAL IDs, not heuristics)
    p_total = len(promoter_events)
    p_12 = sum(1 for c in promoter_events.values() if c >= 12)
    p_20 = sum(1 for c in promoter_events.values() if c >= 20)
    p_50 = sum(1 for c in promoter_events.values() if c >= 50)

    # Overlap: promoters that are also venues (same entity, e.g. RSO.BERLIN)
    # These shouldn't be double-counted
    venue_name_set = set(n.lower().strip() for n in venue_names.values())
    promoter_name_set = set(n.lower().strip() for n in promoter_names.values())
    overlap_names = venue_name_set & promoter_name_set
    # Count overlapping entities that are 12+ on BOTH sides
    overlap_12 = 0
    for pid, pname in promoter_names.items():
        if pname.lower().strip() in overlap_names:
            if promoter_events[pid] >= 12:
                # Check if corresponding venue is also 12+
                for vid, vname in venue_names.items():
                    if vname.lower().strip() == pname.lower().strip() and venue_events[vid] >= 12:
                        overlap_12 += 1
                        break

    return {
        "events_fetched": len(events),
        "unique_artists": len(artists),
        "avg_events_per_night": round(sum(nights) / max(len(nights), 1), 1) if nights else 0,
        # Venues
        "venues_with_events": v_total,
        "venues_12plus": v_12,
        "venues_20plus": v_20,
        "venues_50plus": v_50,
        # Promoters (REAL from RA)
        "promoters_with_events": p_total,
        "promoters_12plus": p_12,
        "promoters_20plus": p_20,
        "promoters_50plus": p_50,
        # Deduplicated total
        "overlap_12plus": overlap_12,
        "customers_12plus": v_12 + p_12 - overlap_12,
        "customers_20plus": v_20 + p_20 - overlap_12,  # conservative dedup
        # Lists
        "top_venues": [{"id": vid, "name": venue_names.get(vid, ""), "events": c} for vid, c in venue_events.most_common(30)],
        "top_promoters": [{"id": pid, "name": promoter_names.get(pid, ""), "events": c} for pid, c in promoter_events.most_common(30)],
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
    pa = argparse.ArgumentParser(description="Night Pulse — Market Sizing v4")
    pa.add_argument("--top", type=int)
    pa.add_argument("--city", type=str)
    pa.add_argument("--ids-file", default="ra_area_ids.json")
    pa.add_argument("--min-events", type=int, default=1000)
    args = pa.parse_args()

    cities = load_cities(args.ids_file)
    if args.city:
        cities = [c for c in cities if c["key"] == args.city]
    elif args.top:
        cities = cities[:args.top]

    if not cities:
        print("No cities found.")
        return

    print(f"Testing proxy...")
    test = get_event_count(cities[0]["area_id"])
    if not test:
        print("✗ Cannot reach proxy. Run: python3 server.py")
        return
    print(f"✓ {cities[0]['name']}: {test:,} events/year")

    print(f"\n{'='*65}")
    print(f"  NIGHT PULSE — REAL MARKET SIZE")
    print(f"  {len(cities)} cities | {DATE_START} → {DATE_END}")
    print(f"  Using REAL promoter IDs from RA (not title heuristics)")
    print(f"  Fetching ALL events (no page cap)")
    print(f"{'='*65}\n")

    results = []
    for i, city in enumerate(cities, 1):
        aid = city["area_id"]
        name = city["name"]
        print(f"[{i}/{len(cities)}] {name}...")

        total = get_event_count(aid)
        print(f"    {total:,} events")

        if total < args.min_events:
            print(f"    ⊘ Below threshold")
            results.append({"city": name, "key": city["key"], "country": city["country"], "events_12m": total, "is_market": False})
            continue

        events = fetch_all_events(aid, total)
        print(f"    {len(events):,} fetched")
        analysis = analyze_city(events)

        row = {"city": name, "key": city["key"], "country": city["country"], "area_id": aid, "events_12m": total, "is_market": True}
        row.update(analysis)

        v = analysis["venues_12plus"]
        p = analysis["promoters_12plus"]
        o = analysis["overlap_12plus"]
        c = analysis["customers_12plus"]
        print(f"    → {v} venues + {p} promoters - {o} overlap = {c} customers (12+/yr)")
        print(f"    → {analysis['avg_events_per_night']} events/night")
        results.append(row)

    market = [r for r in results if r.get("is_market")]

    # CSV
    csv_keys = ["city", "country", "events_12m", "avg_events_per_night",
                "venues_with_events", "venues_12plus", "venues_20plus", "venues_50plus",
                "promoters_with_events", "promoters_12plus", "promoters_20plus", "promoters_50plus",
                "overlap_12plus", "customers_12plus", "customers_20plus", "unique_artists"]
    with open("night_pulse_tam.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=csv_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(market, key=lambda r: r.get("events_12m", 0), reverse=True))
    print(f"\n✓ night_pulse_tam.csv")

    # JSON
    total_ev = sum(r.get("events_12m", 0) for r in market)
    total_v12 = sum(r.get("venues_12plus", 0) for r in market)
    total_p12 = sum(r.get("promoters_12plus", 0) for r in market)
    total_o12 = sum(r.get("overlap_12plus", 0) for r in market)
    total_c12 = total_v12 + total_p12 - total_o12
    total_c20 = sum(r.get("customers_20plus", 0) for r in market)

    with open("night_pulse_tam.json", "w") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "range": [DATE_START, DATE_END],
            "method": "Real promoter IDs from RA GraphQL event.promoters field. Real venue IDs. Full event fetch, no sampling.",
            "definition": {
                "customer": "Venue or promoter with 12+ events in trailing 12 months, deduplicated where same entity appears as both",
                "market": f"City with {args.min_events}+ events/year",
            },
            "summary": {
                "cities": len(market),
                "total_events": total_ev,
                "venues_12plus": total_v12,
                "promoters_12plus": total_p12,
                "overlap_12plus": total_o12,
                "customers_12plus": total_c12,
                "customers_20plus": total_c20,
            },
            "cities": results,
        }, f, indent=2, default=str)
    print(f"✓ night_pulse_tam.json")

    print(f"\n{'='*65}")
    print(f"  YOUR REAL MARKET")
    print(f"{'='*65}")
    print(f"  Cities:                  {len(market)}")
    print(f"  Events/year:             {total_ev:,}")
    print(f"  Active venues (12+/yr):  {total_v12}")
    print(f"  Active promoters (12+/yr): {total_p12}")
    print(f"  Overlap (same entity):   -{total_o12}")
    print(f"  ─────────────────────────────────────")
    print(f"  CUSTOMERS (12+/yr):      {total_c12}")
    print(f"  POWER (20+/yr):          {total_c20}")
    print(f"{'='*65}")

    for r in sorted(market, key=lambda x: x.get("customers_12plus", 0), reverse=True):
        v = r.get("venues_12plus", 0)
        p = r.get("promoters_12plus", 0)
        c = r.get("customers_12plus", 0)
        print(f"  {r['city']:<20} {r['events_12m']:>6,} events  {v:>3}v + {p:>3}p = {c:>4} customers  {r.get('avg_events_per_night',0):>5}/night")

    print(f"\nUpload night_pulse_tam.csv + night_pulse_tam.json to Claude.")


if __name__ == "__main__":
    main()
