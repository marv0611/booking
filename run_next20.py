#!/usr/bin/env python3
"""
Night Pulse — NEXT 20 Cities (Batch 2)
========================================
Same bulletproof approach: monthly chunks, 5x retry, saves after each city.
Results go to next20_results.json (won't overwrite top20_results.json).

Usage:
  python3 run_next20.py
  # If it crashes, run again — it resumes.
"""

import json, time, socket
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.3
PAGE_SIZE = 50
MAX_RETRIES = 5
REQUEST_TIMEOUT = 20
RESULTS_FILE = "next20_results.json"

TODAY = datetime.now()
DATE_START = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DATE_END = TODAY.strftime("%Y-%m-%d")

CITIES = [
    {"key": "copenhagen", "name": "Copenhagen", "area_id": 402, "country": "Denmark"},
    {"key": "brussels", "name": "Brussels", "area_id": 405, "country": "Belgium"},
    {"key": "bristol", "name": "Bristol", "area_id": 446, "country": "United Kingdom"},
    {"key": "budapest", "name": "Budapest", "area_id": 449, "country": "Hungary"},
    {"key": "vienna", "name": "Vienna", "area_id": 450, "country": "Austria"},
    {"key": "prague", "name": "Prague", "area_id": 451, "country": "Czech Republic"},
    {"key": "lyon", "name": "Lyon", "area_id": 337, "country": "France"},
    {"key": "tbilisi", "name": "Tbilisi", "area_id": 376, "country": "Georgia"},
    {"key": "stuttgart", "name": "Stuttgart", "area_id": 152, "country": "Germany"},
    {"key": "sofia", "name": "Sofia", "area_id": 95, "country": "Bulgaria"},
    {"key": "warsaw", "name": "Warsaw", "area_id": 454, "country": "Poland"},
    {"key": "liverpool", "name": "Liverpool", "area_id": 343, "country": "United Kingdom"},
    {"key": "stockholm", "name": "Stockholm", "area_id": 396, "country": "Sweden"},
    {"key": "rotterdam", "name": "Rotterdam", "area_id": 174, "country": "Netherlands"},
    {"key": "porto", "name": "Porto", "area_id": 364, "country": "Portugal"},
    {"key": "geneva", "name": "Geneva", "area_id": 392, "country": "Switzerland"},
    {"key": "oslo", "name": "Oslo", "area_id": 408, "country": "Norway"},
    {"key": "basel", "name": "Basel", "area_id": 391, "country": "Switzerland"},
    {"key": "helsinki", "name": "Helsinki", "area_id": 407, "country": "Finland"},
    {"key": "naples", "name": "Naples", "area_id": 406, "country": "Italy"},
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
    payload = json.dumps({"query": query, "variables": variables}).encode()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(API, data=payload, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code == 429:
                wait = 15 * attempt
                print(f"        ⚠ Rate limited, waiting {wait}s")
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
    v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": date_from, "lte": date_to}}, "page": 1, "pageSize": 1}
    resp = post(Q_COUNT, v)
    if resp and "data" in resp:
        return resp["data"]["eventListings"]["totalResults"]
    return 0


def get_total_events(area_id):
    total = get_count(area_id, DATE_START, DATE_END)
    if total < 10000:
        return total
    d1 = datetime.strptime(DATE_START, "%Y-%m-%d")
    d2 = datetime.strptime(DATE_END, "%Y-%m-%d")
    mid = d1 + (d2 - d1) / 2
    time.sleep(DELAY)
    left = get_count(area_id, DATE_START, mid.strftime("%Y-%m-%d"))
    time.sleep(DELAY)
    right = get_count(area_id, (mid + timedelta(days=1)).strftime("%Y-%m-%d"), DATE_END)
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
    events = []
    page = 1
    while True:
        v = {"filters": {"areas": {"eq": area_id}, "listingDate": {"gte": month_start, "lte": month_end}}, "page": page, "pageSize": PAGE_SIZE}
        resp = post(Q_EVENTS, v)
        if resp and "data" in resp:
            batch = [item["event"] for item in resp["data"]["eventListings"].get("data", []) if item.get("event")]
            events.extend(batch)
            total = resp["data"]["eventListings"].get("totalResults", 0)
            if not batch or len(events) >= total:
                break
        else:
            break
        page += 1
        if page > 100:
            break
        time.sleep(DELAY)
    return events


def fetch_all_by_month(area_id, total):
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
        mn = m_start.strftime("%b %Y")
        print(f"      {mn}: {len(month_events)} events (total: {len(all_events):,})")
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
    print(f"  Night Pulse — NEXT 20 Cities")
    print(f"  Monthly chunks | 5x retry | Saves after each city")
    print(f"  {DATE_START} → {DATE_END}")
    print(f"{'='*60}\n")

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
        print(f"    Counting events...")
        total = get_total_events(aid)
        print(f"    {total:,} events/year")

        if total < 500:
            print(f"    ⊘ Too small, skipping\n")
            results["cities"][city["key"]] = {"city": name, "country": city["country"], "events_12m": total, "skipped": True}
            save_results(results)
            continue

        print(f"    Fetching by month...")
        events = fetch_all_by_month(aid, total)
        print(f"    {len(events):,} total events fetched")

        if not events:
            print(f"    ✗ No events, skipping\n")
            results["cities"][city["key"]] = {"city": name, "country": city["country"], "events_12m": total, "skipped": True, "reason": "fetch_failed"}
            save_results(results)
            continue

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
    print(f"  NEXT 20 RESULTS")
    print(f"{'='*60}")
    print(f"  Cities:              {len(active)}")
    print(f"  Events/year:         {total_ev:,}")
    print(f"  Venues (12+):        {total_v12}")
    print(f"  Promoters (12+):     {total_p12}")
    print(f"  CUSTOMERS (12+):     {total_c12}")
    print(f"  POWER (20+):         {total_c20}")
    print(f"{'='*60}")

    for k, v in sorted(active.items(), key=lambda x: x[1].get("customers_12plus", 0), reverse=True):
        c12 = v.get("customers_12plus", 0)
        ev = v.get("events_12m", 0)
        vn = v.get("venues_12plus", 0)
        pr = v.get("promoters_12plus", 0)
        epn = v.get("avg_events_per_night", 0)
        print(f"  {v['city']:<18} {ev:>6,} events  {vn:>3}v+{pr:>3}p = {c12:>4} cust  {epn:>5}/night")

    # Combined with previous batches
    prev = 1905  # from top 24
    print(f"\n  These 20 cities:                     {total_c12}")
    print(f"  + Previous 24 cities:                {prev}")
    print(f"  ─────────────────────────────────────")
    print(f"  TOTAL 44 CITIES:                     {total_c12 + prev}")
    print(f"\nUpload next20_results.json to Claude.")


if __name__ == "__main__":
    main()
