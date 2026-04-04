#!/usr/bin/env python3
"""
Night Pulse — RA Data Collector
=================================
Queries RA's GraphQL API (via server.py proxy) for REAL venue counts,
promoter counts, and 12-month event totals across all European cities.

This gives you the actual TAM — not estimates, not models.

Prerequisites:
  1. server.py running: python3 server.py
  2. ra_area_ids.json in same directory

Usage:
  python3 collect_ra_data.py                    # all cities
  python3 collect_ra_data.py --top 10           # top 10 by event count
  python3 collect_ra_data.py --city berlin       # single city test

Output:
  ra_market_data.csv    — one row per city, real counts
  ra_market_data.json   — full structured data
"""

import json, csv, time, sys, argparse, math
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.2  # seconds between requests (RA rate limit)

TODAY = datetime.now()
DATE_12M_AGO = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DATE_TODAY = TODAY.strftime("%Y-%m-%d")

# ─── GraphQL Queries ───────────────────────────────────────────

# 1. Event count for trailing 12 months (just page 1, we only need totalResults)
Q_EVENTS = """
query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""

# 2. Event listings with full detail (for deeper analysis of a city)
Q_EVENTS_DETAIL = """
query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
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

# 3. Club/venue listings by area
Q_CLUBS = """
query GET_CLUB_LISTINGS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  clubListings(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""

# 4. Promoter listings by area — try different query names
Q_PROMOTERS_V1 = """
query GET_PROMOTER_LISTINGS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  promoterListings(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""

Q_PROMOTERS_V2 = """
query GET_PROMOTERS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  promoters(filters: $filters, page: $page, pageSize: $pageSize) {
    totalResults
  }
}"""

# 5. Introspection — discover what queries are available
Q_INTROSPECT = """
query { __schema { queryType { fields { name description args { name } } } } }
"""


def post(query, variables=None):
    """Send a GraphQL query to the proxy. Returns parsed JSON or None."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = Request(API, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            print("    ⚠ Rate limited, waiting 10s...")
            time.sleep(10)
            return post(query, variables)  # retry once
        body = e.read().decode(errors="replace")[:200]
        print(f"    ✗ HTTP {e.code}: {body}")
        return None
    except Exception as e:
        print(f"    ✗ Error: {e}")
        return None


def get_total_events_12m(area_id):
    """Get total event count for trailing 12 months."""
    vars = {
        "filters": {
            "areas": {"eq": area_id},
            "listingDate": {"gte": DATE_12M_AGO, "lte": DATE_TODAY},
        },
        "page": 1, "pageSize": 1,
    }
    resp = post(Q_EVENTS, vars)
    if resp and "data" in resp:
        return resp["data"]["eventListings"]["totalResults"]
    return None


def get_clubs_total(area_id):
    """Get total registered clubs/venues for an area."""
    vars = {
        "filters": {"areas": {"eq": area_id}},
        "page": 1, "pageSize": 1,
    }
    resp = post(Q_CLUBS, vars)
    if resp and "data" in resp and resp["data"].get("clubListings"):
        return resp["data"]["clubListings"]["totalResults"]
    return None


def get_promoters_total(area_id):
    """Get total registered promoters for an area. Tries multiple query variants."""
    vars = {
        "filters": {"areas": {"eq": area_id}},
        "page": 1, "pageSize": 1,
    }
    # Try variant 1
    resp = post(Q_PROMOTERS_V1, vars)
    if resp and "data" in resp and resp["data"].get("promoterListings"):
        return resp["data"]["promoterListings"]["totalResults"], "promoterListings"

    # Try variant 2
    resp = post(Q_PROMOTERS_V2, vars)
    if resp and "data" in resp and resp["data"].get("promoters"):
        return resp["data"]["promoters"]["totalResults"], "promoters"

    return None, None


def get_event_page(area_id, page=1, page_size=50):
    """Fetch a single page of events with full detail."""
    vars = {
        "filters": {
            "areas": {"eq": area_id},
            "listingDate": {"gte": DATE_12M_AGO, "lte": DATE_TODAY},
        },
        "page": page, "pageSize": page_size,
    }
    resp = post(Q_EVENTS_DETAIL, vars)
    if resp and "data" in resp:
        listings = resp["data"]["eventListings"]
        events = [item["event"] for item in listings.get("data", []) if item.get("event")]
        return events, listings.get("totalResults", 0)
    return [], 0


def analyze_event_sample(area_id, max_pages=5):
    """Fetch a sample of events and extract venue/promoter/artist counts."""
    all_events = []
    total = 0

    for page in range(1, max_pages + 1):
        events, total = get_event_page(area_id, page)
        all_events.extend(events)
        if len(all_events) >= total or not events:
            break
        time.sleep(DELAY)

    if not all_events:
        return {}

    # Unique venues
    venues = {}
    for e in all_events:
        v = e.get("venue") or {}
        vid = v.get("id")
        if vid:
            venues.setdefault(vid, {"name": v.get("name", ""), "count": 0})
            venues[vid]["count"] += 1

    # Venue frequency tiers (extrapolate from sample to full year)
    sample_ratio = total / max(len(all_events), 1)
    venue_counts = [v["count"] * sample_ratio for v in venues.values()]
    venues_12plus = sum(1 for c in venue_counts if c >= 12)
    venues_20plus = sum(1 for c in venue_counts if c >= 20)
    venues_50plus = sum(1 for c in venue_counts if c >= 50)

    # Unique artists
    artists = set()
    for e in all_events:
        for a in e.get("artists") or []:
            if a.get("id"):
                artists.add(a["id"])

    # Promoter brands (title heuristic)
    brands = set()
    for e in all_events:
        title = (e.get("title") or "").strip()
        for sep in [":", " presents ", " w/ ", " x ", " | ", " — ", " at ", " @ "]:
            if sep in title:
                brand = title.split(sep)[0].strip()
                if 2 < len(brand) < 50:
                    brands.add(brand.lower())
                break

    # Genre distribution
    from collections import Counter
    genres = Counter()
    for e in all_events:
        for g in e.get("genres") or []:
            n = g.get("name", "").strip()
            if n:
                genres[n] += 1
    top_genre = genres.most_common(1)[0][0] if genres else "N/A"

    return {
        "sample_events": len(all_events),
        "unique_venues_in_sample": len(venues),
        "est_venues_12plus": venues_12plus,
        "est_venues_20plus": venues_20plus,
        "est_venues_50plus": venues_50plus,
        "unique_artists_in_sample": len(artists),
        "heuristic_brands": len(brands),
        "top_genre": top_genre,
    }


def discover_schema():
    """Run introspection to discover available queries."""
    print("Discovering RA GraphQL schema...")
    resp = post(Q_INTROSPECT)
    if resp and "data" in resp:
        fields = resp["data"]["__schema"]["queryType"]["fields"]
        print(f"  Found {len(fields)} query types:")
        for f in fields:
            args = ", ".join(a["name"] for a in f.get("args", []))
            print(f"    • {f['name']}({args})")
        return fields
    elif resp and "errors" in resp:
        print(f"  Introspection blocked: {resp['errors'][0].get('message', '')[:100]}")
    else:
        print("  Introspection failed — no response")
    return None


def load_cities(filepath="ra_area_ids.json"):
    """Load city definitions, filtering out legacy/region entries."""
    with open(filepath) as f:
        data = json.load(f)

    cities = []
    for key, val in data.get("european_cities", {}).items():
        note = val.get("note", "")
        if "legacy" in note or "country-level" in note or "region" in note:
            continue
        cities.append({
            "key": key,
            "name": key.replace("_", " ").title(),
            "area_id": val["id"],
            "country": val["country"],
            "historical_events": val.get("events", 0),
        })

    return sorted(cities, key=lambda c: c["historical_events"], reverse=True)


def main():
    parser = argparse.ArgumentParser(description="Night Pulse — RA Data Collector")
    parser.add_argument("--top", type=int, help="Only process top N cities by event count")
    parser.add_argument("--city", type=str, help="Single city key to test (e.g., 'berlin')")
    parser.add_argument("--deep", action="store_true", help="Also fetch event samples for venue/artist analysis")
    parser.add_argument("--schema", action="store_true", help="Discover RA GraphQL schema and exit")
    parser.add_argument("--ids-file", default="ra_area_ids.json", help="Path to area IDs file")
    args = parser.parse_args()

    # ── Schema discovery ──
    if args.schema:
        discover_schema()
        return

    # ── Load cities ──
    cities = load_cities(args.ids_file)
    print(f"Loaded {len(cities)} cities from {args.ids_file}")

    if args.city:
        cities = [c for c in cities if c["key"] == args.city]
        if not cities:
            print(f"City '{args.city}' not found")
            return
    elif args.top:
        cities = cities[:args.top]
        print(f"Processing top {len(cities)} cities")

    # ── Step 1: Test connectivity ──
    print(f"\nTesting proxy at {API}...")
    test = get_total_events_12m(cities[0]["area_id"])
    if test is None:
        print("✗ Cannot reach proxy. Is server.py running?")
        print("  Start it with: python3 server.py")
        return
    print(f"✓ Proxy working. {cities[0]['name']}: {test:,} events in trailing 12m")

    # ── Step 2: Test club/promoter queries ──
    print("\nTesting venue query (clubListings)...")
    clubs_test = get_clubs_total(cities[0]["area_id"])
    clubs_available = clubs_test is not None
    if clubs_available:
        print(f"✓ Club query works. {cities[0]['name']}: {clubs_test:,} registered venues")
    else:
        print("✗ clubListings query not available — will estimate from events")

    time.sleep(DELAY)

    print("Testing promoter query...")
    promo_test, promo_query = get_promoters_total(cities[0]["area_id"])
    promos_available = promo_test is not None
    if promos_available:
        print(f"✓ Promoter query works ({promo_query}). {cities[0]['name']}: {promo_test:,} registered promoters")
    else:
        print("✗ Promoter query not available — will estimate from events")

    time.sleep(DELAY)

    # ── Step 3: Collect data for all cities ──
    print(f"\n{'='*70}")
    print(f"  Collecting data for {len(cities)} cities")
    print(f"  Date range: {DATE_12M_AGO} → {DATE_TODAY}")
    est_time = len(cities) * DELAY * (1 + int(clubs_available) + int(promos_available) + (5 if args.deep else 0))
    print(f"  Estimated time: ~{est_time/60:.1f} minutes")
    print(f"{'='*70}\n")

    results = []

    for i, city in enumerate(cities, 1):
        aid = city["area_id"]
        name = city["name"]
        print(f"[{i}/{len(cities)}] {name} (area_id={aid})...")

        row = {
            "city": name,
            "key": city["key"],
            "country": city["country"],
            "area_id": aid,
            "events_12m": None,
            "ra_registered_venues": None,
            "ra_registered_promoters": None,
        }

        # Events (12 months)
        row["events_12m"] = get_total_events_12m(aid)
        time.sleep(DELAY)

        # Venues
        if clubs_available:
            row["ra_registered_venues"] = get_clubs_total(aid)
            time.sleep(DELAY)

        # Promoters
        if promos_available:
            count, _ = get_promoters_total(aid)
            row["ra_registered_promoters"] = count
            time.sleep(DELAY)

        # Deep analysis (event sample)
        if args.deep:
            print(f"    Fetching event sample...")
            sample = analyze_event_sample(aid, max_pages=4)
            row.update(sample)

        ev = row["events_12m"] or 0
        vn = row["ra_registered_venues"]
        pr = row["ra_registered_promoters"]
        print(f"    events={ev:,}  venues={vn or '?'}  promoters={pr or '?'}")

        results.append(row)

    # ── Step 4: Write outputs ──
    # CSV
    csv_path = "ra_market_data.csv"
    if results:
        keys = list(results[0].keys())
        # Add any extra keys from deep analysis
        for r in results:
            for k in r.keys():
                if k not in keys:
                    keys.append(k)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(sorted(results, key=lambda r: r.get("events_12m") or 0, reverse=True))
        print(f"\n✓ CSV written → {csv_path}")

    # JSON
    json_path = "ra_market_data.json"
    summary = {
        "collected_at": datetime.now().isoformat(),
        "date_range": {"start": DATE_12M_AGO, "end": DATE_TODAY},
        "cities_queried": len(results),
        "data_sources": {
            "events_12m": "RA GraphQL eventListings (trailing 12 months)",
            "ra_registered_venues": "RA GraphQL clubListings" if clubs_available else "NOT AVAILABLE",
            "ra_registered_promoters": f"RA GraphQL {promo_query}" if promos_available else "NOT AVAILABLE",
        },
        "totals": {
            "total_events_12m": sum(r.get("events_12m") or 0 for r in results),
            "total_registered_venues": sum(r.get("ra_registered_venues") or 0 for r in results) if clubs_available else "N/A",
            "total_registered_promoters": sum(r.get("ra_registered_promoters") or 0 for r in results) if promos_available else "N/A",
        },
        "cities": results,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"✓ JSON written → {json_path}")

    # ── Summary ──
    total_ev = sum(r.get("events_12m") or 0 for r in results)
    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  Cities queried:       {len(results)}")
    print(f"  Total events (12m):   {total_ev:,}")
    if clubs_available:
        total_v = sum(r.get("ra_registered_venues") or 0 for r in results)
        print(f"  Registered venues:    {total_v:,}")
    if promos_available:
        total_p = sum(r.get("ra_registered_promoters") or 0 for r in results)
        print(f"  Registered promoters: {total_p:,}")
    print(f"{'='*70}")

    print(f"\nNext steps:")
    print(f"  1. Upload ra_market_data.csv and ra_market_data.json back to Claude")
    print(f"  2. I'll build your investor-ready TAM/SAM/SOM from real data")
    if not clubs_available or not promos_available:
        print(f"  3. Run: python3 collect_ra_data.py --schema")
        print(f"     to discover what other RA queries are available")


if __name__ == "__main__":
    main()
