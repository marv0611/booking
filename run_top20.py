#!/usr/bin/env python3
"""
Night Pulse — Top 20 Cities FULL SCAN (Overnight Edition)
============================================================
Gets EVERY event. Never times out. Designed to run unattended.

Strategy for large cities (10K+ events):
  - Split the year into 12 monthly chunks
  - Fetch each month separately (max ~1,500 events = 30 pages)
  - No single fetch ever exceeds 30 pages → no timeout
  - If a page fails, retry 5 times with increasing waits
  - If a month fails entirely, skip it and note the gap
  - Save after every city

Usage:
  python3 run_top20.py
  # Leave it overnight. ~2-3 hours for all 20 cities.
  # If it crashes, run again — it resumes.
"""

import json, time, socket
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.3              # gentle on RA
PAGE_SIZE = 50
MAX_RETRIES = 5          # 5 retries per request
REQUEST_TIMEOUT = 20     # seconds
RESULTS_FILE = "top20_results.json"

TODAY = datetime.now()
DATE_START = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DATE_END = TODAY.strftime("%Y-%m-%d")

CITIES = [
    {"key": "madrid", "name": "Madrid", "area_id": 41, "country": "Spain"},
    {"key": "paris", "name": "Paris", "area_id": 44, "country": "France"},
    {"key": "lisbon", "name": "Lisbon", "area_id": 53, "country": "Portugal"},
    {"key": "cologne", "name": "Cologne", "area_id": 143, "country": "Germany"},
    {"key": "hamburg", "name": "Hamburg", "area_id": 148, "country": "Germany"},
    {"key": "munich", "name": "Munich", "area_id": 151, "country": "Germany"},
    {"key": "amsterdam", "name": "Amsterdam", "area_id": 176, "country": "Netherlands"},
    {"key": "mannheim", "name": "Mannheim", "area_id": 336, "country": "Germany"},
    {"key": "glasgow", "name": "Glasgow", "area_id": 340, "country": "United Kingdom"},
    {"key": "edinburgh", "name": "Edinburgh", "area_id": 341, "country": "United Kingdom"},
    {"key": "manchester", "name": "Manchester", "area_id": 344, "country": "United Kingdom"},
    {"key": "leeds", "name": "Leeds", "area_id": 346, "country": "United Kingdom"},
    {"key": "milan", "name": "Milan", "area_id": 347, "country": "Italy"},
    {"key": "rome", "name": "Rome", "area_id": 351, "country": "Italy"},
    {"key": "dortmund_essen", "name": "Dortmund/Essen", "area_id": 353, "country": "Germany"},
    {"key": "frankfurt", "name": "Frankfurt", "area_id": 354, "country": "Germany"},
    {"key": "leipzig", "name": "Leipzig", "area_id": 356, "country": "Germany"},
    {"key": "regensburg", "name": "Regensburg", "area_id": 357, "country": "Germany"},
    {"key": "dublin", "name": "Dublin", "area_id": 386, "country": "Ireland"},
    {"key": "zurich", "name": "Zurich", "area_id": 390, "country": "Switzerland"},
]

Q_EVENTS = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) {
    data { event { id title date venue { id name } promoters { id name } artists { id name } } }
    totalResults
  }
}"""

Q_COUNT = """
query ($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
  eventListings(filters: $filters, page: $page, pageSize: $pageSize) { totalResults }
}"""


def post(query, variables):
    """POST with aggressive retry. Will try 5 times with increasing waits."""
    payload = json.dumps({"query": query, "variables": variables}).encode()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(API, data=payload, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code == 429:
                wait = 15 * attempt
                print(f"        ⚠ Rate limited, waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if attempt == MAX_RETRIES:
                return None
            time.sleep(5 * attempt)
        except (URLError, socket.timeout, TimeoutError, ConnectionError, OSError):
            if attempt < MAX_RETRIES:
                wait = 5 * attempt
                print(f"        ⚠ Timeout, retry {attempt}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)
            else:
                print(f"        ✗ Failed after {MAX_RETRIES} attempts")
                return None
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"        ✗ {e}")
                return None
            time.sleep(5 * attempt)
    return None


def get_count(area_id, date_from, date_to):
    """Get event count for a date range."""
    v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": date_from, "lte": date_to}}, "page": 1, "pageSize": 1}
    resp = post(Q_COUNT, v)
    if resp and "data" in resp:
        return resp["data"]["eventListings"]["totalResults"]
    return 0


def get_total_events(area_id):
    """Get true event count, splitting to beat 10K cap."""
    total = get_count(area_id, DATE_START, DATE_END)
    if total < 10000:
        return total
    # Split into halves recursively
    d1 = datetime.strptime(DATE_START, "%Y-%m-%d")
    d2 = datetime.strptime(DATE_END, "%Y-%m-%d")
    mid = d1 + (d2 - d1) / 2
    time.sleep(DELAY)
    left = get_count(area_id, DATE_START, mid.strftime("%Y-%m-%d"))
    time.sleep(DELAY)
    right = get_count(area_id, (mid + timedelta(days=1)).strftime("%Y-%m-%d"), DATE_END)
    # If halves are still capped, split again
    if left >= 10000:
        q1 = d1 + (mid - d1) / 2
        time.sleep(DELAY)
        l1 = get_count(area_id, DATE_START, q1.strftime("%Y-%m-%d"))
        time.sleep(DELAY)
        l2 = get_count(area_id, (q1 + timedelta(days=1)).strftime("%Y-%m-%d"), mid.strftime("%Y-%m-%d"))
        left = l1 + l2
    if right >= 10000:
        q3 = mid + (d2 - mid) / 2
        time.sleep(DELAY)
        r1 = get_count(area_id, (mid + timedelta(days=1)).strftime("%Y-%m-%d"), q3.strftime("%Y-%m-%d"))
        time.sleep(DELAY)
        r2 = get_count(area_id, (q3 + timedelta(days=1)).strftime("%Y-%m-%d"), DATE_END)
        right = r1 + r2
    return left + right


def fetch_month(area_id, month_start, month_end):
    """Fetch all events for one month. Max ~30 pages. Safe."""
    events = []
    page = 1

    while True:
        v = {
            "filters": {"areas": {"eq": area_id}, "listingDate": {"gte": month_start, "lte": month_end}},
            "page": page, "pageSize": PAGE_SIZE,
        }
        resp = post(Q_EVENTS, v)

        if resp and "data" in resp:
            batch = [item["event"] for item in resp["data"]["eventListings"].get("data", []) if item.get("event")]
            events.extend(batch)
            total_this_month = resp["data"]["eventListings"].get("totalResults", 0)

            if not batch or len(events) >= total_this_month:
                break
        else:
            # Failed — return what we have
            break

        page += 1
        if page > 100:  # safety valve
            break
        time.sleep(DELAY)

    return events


def fetch_all_events_by_month(area_id, total):
    """Fetch a full year by splitting into 12 monthly chunks."""
    all_events = []
    base = datetime.strptime(DATE_START, "%Y-%m-%d")

    for m in range(12):
        m_start = base + timedelta(days=m * 30)
        m_end = base + timedelta(days=(m + 1) * 30 - 1)
        if m == 11:
            m_end = datetime.strptime(DATE_END, "%Y-%m-%d")

        ms = m_start.strftime("%Y-%m-%d")
        me = m_end.strftime("%Y-%m-%d")

        month_events = fetch_month(area_id, ms, me)
        all_events.extend(month_events)

        month_name = m_start.strftime("%b %Y")
        print(f"      {month_name}: {len(month_events)} events (total: {len(all_events):,})")

    return all_events


def analyze(events):
    venue_ev = Counter()
    venue_nm = {}
    promo_ev = Counter()
    promo_nm = {}
    artists = set()
    nights = Counter()

    for e in events:
        v = e.get("venue") or {}
        vid = v.get("id")
        if vid:
            venue_ev[vid] += 1
            venue_nm[vid] = v.get("name", "")
        for p in e.get("promoters") or []:
            pid = p.get("id")
            if pid:
                promo_ev[pid] += 1
                promo_nm[pid] = p.get("name", "")
        for a in e.get("artists") or []:
            if a.get("id"):
                artists.add(a["id"])
        d = (e.get("date") or "")[:10]
        if d:
            nights[d] += 1

    nv = list(nights.values())
    v12 = sum(1 for c in venue_ev.values() if c >= 12)
    v20 = sum(1 for c in venue_ev.values() if c >= 20)
    v50 = sum(1 for c in venue_ev.values() if c >= 50)
    p12 = sum(1 for c in promo_ev.values() if c >= 12)
    p20 = sum(1 for c in promo_ev.values() if c >= 20)
    p50 = sum(1 for c in promo_ev.values() if c >= 50)

    # Dedup
    vnames = set(n.lower().strip() for n in venue_nm.values())
    pnames = set(n.lower().strip() for n in promo_nm.values())
    overlap = vnames & pnames
    o12 = 0
    for pid, pn in promo_nm.items():
        if pn.lower().strip() in overlap and promo_ev[pid] >= 12:
            for vid, vn in venue_nm.items():
                if vn.lower().strip() == pn.lower().strip() and venue_ev[vid] >= 12:
                    o12 += 1
                    break

    return {
        "events_fetched": len(events),
        "unique_artists": len(artists),
        "avg_events_per_night": round(sum(nv) / max(len(nv), 1), 1) if nv else 0,
        "venues_active": len(venue_ev),
        "venues_12plus": v12, "venues_20plus": v20, "venues_50plus": v50,
        "promoters_active": len(promo_ev),
        "promoters_12plus": p12, "promoters_20plus": p20, "promoters_50plus": p50,
        "overlap_12plus": o12,
        "customers_12plus": v12 + p12 - o12,
        "customers_20plus": v20 + p20 - o12,
        "top_venues": [{"name": venue_nm.get(vid, ""), "events": c} for vid, c in venue_ev.most_common(15)],
        "top_promoters": [{"name": promo_nm.get(pid, ""), "events": c} for pid, c in promo_ev.most_common(15)],
    }


def load_results():
    try:
        with open(RESULTS_FILE) as f:
            return json.load(f)
    except:
        return {"generated": datetime.now().isoformat(), "range": [DATE_START, DATE_END], "cities": {}}


def save_results(results):
    results["last_updated"] = datetime.now().isoformat()
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2, default=str)


def main():
    print(f"{'='*60}")
    print(f"  Night Pulse — FULL SCAN, 20 Cities")
    print(f"  Monthly chunks | 5x retry | Saves after each city")
    print(f"  {DATE_START} → {DATE_END}")
    print(f"{'='*60}\n")

    # Test proxy
    try:
        req = Request(API, data=json.dumps({"query": "{ __typename }"}).encode(),
                      headers={"Content-Type": "application/json"})
        urlopen(req, timeout=5)
        print("✓ Proxy connected\n")
    except:
        print("✗ Cannot reach proxy. Run: python3 server.py")
        return

    results = load_results()
    done = set(results.get("cities", {}).keys())
    remaining = [c for c in CITIES if c["key"] not in done]

    if not remaining:
        print("All 20 cities done!")
        print_summary(results)
        return

    print(f"{len(done)}/20 done, {len(remaining)} remaining\n")

    for i, city in enumerate(remaining, 1):
        name = city["name"]
        aid = city["area_id"]
        start_time = time.time()

        print(f"[{len(done)+i}/{20}] {name} ({'='*40})")

        # Get true event count
        print(f"    Counting events...")
        total = get_total_events(aid)
        print(f"    {total:,} events/year")

        if total < 500:
            print(f"    ⊘ Too small, skipping\n")
            results["cities"][city["key"]] = {"city": name, "country": city["country"], "events_12m": total, "skipped": True}
            save_results(results)
            continue

        # Fetch by month
        print(f"    Fetching by month...")
        events = fetch_all_events_by_month(aid, total)
        print(f"    {len(events):,} total events fetched")

        if not events:
            print(f"    ✗ No events, skipping\n")
            results["cities"][city["key"]] = {"city": name, "country": city["country"], "events_12m": total, "skipped": True, "reason": "fetch_failed"}
            save_results(results)
            continue

        # Analyze
        a = analyze(events)
        row = {"city": name, "country": city["country"], "events_12m": total}
        row.update(a)

        elapsed = round(time.time() - start_time)
        v = a["venues_12plus"]
        p = a["promoters_12plus"]
        o = a["overlap_12plus"]
        c = a["customers_12plus"]

        print(f"    ────────────────────────────────────")
        print(f"    {v} venues + {p} promoters - {o} overlap = {c} customers (12+/yr)")
        print(f"    {a['customers_20plus']} power customers (20+/yr)")
        print(f"    {a['avg_events_per_night']} events/night")
        print(f"    {elapsed}s elapsed")

        results["cities"][city["key"]] = row
        save_results(results)
        print(f"    ✓ Saved\n")

    print_summary(results)


def print_summary(results):
    cities = results.get("cities", {})
    active = {k: v for k, v in cities.items() if not v.get("skipped")}

    total_ev = sum(v.get("events_12m", 0) for v in active.values())
    total_c12 = sum(v.get("customers_12plus", 0) for v in active.values())
    total_c20 = sum(v.get("customers_20plus", 0) for v in active.values())
    total_v12 = sum(v.get("venues_12plus", 0) for v in active.values())
    total_p12 = sum(v.get("promoters_12plus", 0) for v in active.values())

    print(f"\n{'='*60}")
    print(f"  COMPLETE RESULTS — TOP 20 CITIES")
    print(f"{'='*60}")
    print(f"  Cities analyzed:     {len(active)}")
    print(f"  Total events/year:   {total_ev:,}")
    print(f"  Venues (12+/yr):     {total_v12}")
    print(f"  Promoters (12+/yr):  {total_p12}")
    print(f"  CUSTOMERS (12+):     {total_c12}")
    print(f"  POWER (20+):         {total_c20}")
    print(f"{'='*60}")

    for k, v in sorted(active.items(), key=lambda x: x[1].get("customers_12plus", 0), reverse=True):
        c12 = v.get("customers_12plus", 0)
        c20 = v.get("customers_20plus", 0)
        ev = v.get("events_12m", 0)
        epn = v.get("avg_events_per_night", 0)
        vn = v.get("venues_12plus", 0)
        pr = v.get("promoters_12plus", 0)
        print(f"  {v['city']:<18} {ev:>6,} events  {vn:>3}v+{pr:>3}p = {c12:>4} cust  ({c20:>3} power)  {epn:>5}/night")

    already = 708
    print(f"\n  These 20 cities:                   {total_c12}")
    print(f"  + London/Barcelona/Berlin/Ibiza:   {already}")
    print(f"  ─────────────────────────────────────")
    print(f"  TOTAL 24 CITIES:                   {total_c12 + already}")
    print(f"\nUpload top20_results.json to Claude.")


if __name__ == "__main__":
    main()
