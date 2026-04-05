#!/usr/bin/env python3
"""
Night Pulse — Barcelona Target List
======================================
1. Fetch all Barcelona events (12 months)
2. Find venues/promoters with 12+ events
3. Look up follower count for their artists
4. Filter: who books 12+ artists with 100+ RA followers?
   Those are your real customers.

Usage:
  python3 run_barcelona.py
  # ~1 hour (events + artist lookups)
"""

import json, time, socket, csv
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API = "http://localhost:8000/api/ra/graphql"
DELAY = 1.3
PAGE_SIZE = 50
MAX_RETRIES = 5
REQUEST_TIMEOUT = 20
AREA_ID = 20  # Barcelona

TODAY = datetime.now()
DS = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
DE = TODAY.strftime("%Y-%m-%d")

Q_EVENTS = """
query ($f: FilterInputDtoInput, $p: Int, $ps: Int) {
  eventListings(filters: $f, page: $p, pageSize: $ps) {
    data { event { id title date
      venue { id name }
      promoters { id name }
      artists { id name }
    } } totalResults
  }
}"""


def post(query, variables=None):
    payload = json.dumps({"query": query,
                          "variables": variables or {}}).encode()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(API, data=payload,
                          headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code == 429:
                time.sleep(15 * attempt)
                continue
            if attempt == MAX_RETRIES:
                return None
            time.sleep(5 * attempt)
        except (URLError, socket.timeout, TimeoutError,
                ConnectionError, OSError):
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
            else:
                return None
        except:
            if attempt == MAX_RETRIES:
                return None
            time.sleep(5 * attempt)
    return None


def fetch_month(ms, me):
    events = []
    page = 1
    while True:
        v = {"f": {"areas": {"eq": AREA_ID},
             "listingDate": {"gte": ms, "lte": me}},
             "p": page, "ps": PAGE_SIZE}
        resp = post(Q_EVENTS, v)
        if resp and "data" in resp:
            batch = [i["event"]
                     for i in resp["data"]["eventListings"].get("data", [])
                     if i.get("event")]
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


def get_artist_followers(artist_id):
    q = '{ artist(id: ' + str(artist_id) + ') { id name followerCount } }'
    resp = post(q)
    if resp and "data" in resp and resp["data"].get("artist"):
        a = resp["data"]["artist"]
        return a.get("followerCount", 0), a.get("name", "")
    return 0, ""


def main():
    print(f"{'='*60}")
    print(f"  BARCELONA — TARGET LIST")
    print(f"  {DS} → {DE}")
    print(f"{'='*60}\n")

    # Test proxy
    try:
        req = Request(API,
                      data=json.dumps({"query": "{ __typename }"}).encode(),
                      headers={"Content-Type": "application/json"})
        urlopen(req, timeout=5)
        print("✓ Proxy connected\n")
    except:
        print("✗ Cannot reach proxy. Run: python3 server.py")
        return

    # Phase 1: Fetch all events
    print("PHASE 1: Fetching all Barcelona events")
    print("-" * 40)
    base = datetime.strptime(DS, "%Y-%m-%d")
    all_events = []
    for m in range(12):
        ms = (base + timedelta(days=m * 30)).strftime("%Y-%m-%d")
        me = (base + timedelta(days=(m + 1) * 30 - 1)).strftime("%Y-%m-%d")
        if m == 11:
            me = DE
        evts = fetch_month(ms, me)
        all_events.extend(evts)
        mn = (base + timedelta(days=m * 30)).strftime("%b %Y")
        print(f"  {mn}: {len(evts)} events "
              f"(total: {len(all_events):,})")

    print(f"\n{len(all_events):,} total events\n")

    # Phase 2: Build venue and promoter profiles
    print("PHASE 2: Building profiles")
    print("-" * 40)

    venues = {}   # vid -> {name, events, artist_ids: set}
    promos = {}   # pid -> {name, events, artist_ids: set}

    for e in all_events:
        v = e.get("venue") or {}
        vid = v.get("id")
        eartists = set()
        for a in e.get("artists") or []:
            if a.get("id"):
                eartists.add(a["id"])

        if vid:
            if vid not in venues:
                venues[vid] = {"name": v.get("name", ""),
                               "events": 0, "artists": set()}
            venues[vid]["events"] += 1
            venues[vid]["artists"].update(eartists)

        for p in e.get("promoters") or []:
            pid = p.get("id")
            if pid:
                if pid not in promos:
                    promos[pid] = {"name": p.get("name", ""),
                                   "events": 0, "artists": set()}
                promos[pid]["events"] += 1
                promos[pid]["artists"].update(eartists)

    # Filter to 12+ events
    v12 = {vid: v for vid, v in venues.items() if v["events"] >= 12}
    p12 = {pid: p for pid, p in promos.items() if p["events"] >= 12}

    print(f"  Venues with 12+ events:   {len(v12)}")
    print(f"  Promoters with 12+ events: {len(p12)}")

    # Collect all unique artist IDs from these entities
    all_artist_ids = set()
    for v in v12.values():
        all_artist_ids.update(v["artists"])
    for p in p12.values():
        all_artist_ids.update(p["artists"])

    print(f"  Unique artists to look up: {len(all_artist_ids):,}")

    # Phase 3: Look up artist follower counts
    print(f"\nPHASE 3: Looking up artist followers")
    print(f"  ~{len(all_artist_ids) * DELAY / 60:.0f} minutes estimated")
    print("-" * 40)

    artist_followers = {}  # aid -> (follower_count, name)
    artist_list = list(all_artist_ids)

    for i, aid in enumerate(artist_list, 1):
        fc, name = get_artist_followers(aid)
        artist_followers[aid] = (fc, name)
        if i % 100 == 0:
            notable = sum(1 for f, _ in artist_followers.values()
                          if f >= 100)
            print(f"  ... {i:,}/{len(artist_list):,} looked up "
                  f"({notable} notable so far)")
        time.sleep(DELAY)

    notable_artists = {aid for aid, (fc, _)
                       in artist_followers.items() if fc >= 100}
    print(f"\n  Total artists looked up:  {len(artist_followers):,}")
    print(f"  Notable (100+ followers): {len(notable_artists):,}")

    # Phase 4: Score and filter
    print(f"\nPHASE 4: Scoring targets")
    print("-" * 40)

    targets = []

    # Score venues
    for vid, v in v12.items():
        notable_booked = v["artists"] & notable_artists
        targets.append({
            "type": "venue",
            "id": vid,
            "name": v["name"],
            "events": v["events"],
            "total_artists": len(v["artists"]),
            "notable_artists": len(notable_booked),
            "is_target": len(notable_booked) >= 12,
        })

    # Score promoters
    for pid, p in p12.items():
        notable_booked = p["artists"] & notable_artists
        targets.append({
            "type": "promoter",
            "id": pid,
            "name": p["name"],
            "events": p["events"],
            "total_artists": len(p["artists"]),
            "notable_artists": len(notable_booked),
            "is_target": len(notable_booked) >= 12,
        })

    # Deduplicate (same name as venue and promoter)
    seen = set()
    deduped = []
    for t in sorted(targets, key=lambda x: x["notable_artists"],
                    reverse=True):
        key = t["name"].lower().strip()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(t)

    real_targets = [t for t in deduped if t["is_target"]]
    not_targets = [t for t in deduped if not t["is_target"]]

    # Output
    print(f"\n{'='*70}")
    print(f"  BARCELONA — YOUR TARGETS")
    print(f"  Filter: 12+ notable artists (100+ RA followers) booked/year")
    print(f"{'='*70}\n")

    print(f"  {'Name':<35} {'Type':<10} {'Events':>7} "
          f"{'Artists':>8} {'Notable':>8}")
    print(f"  {'-'*70}")
    for t in real_targets:
        print(f"  {t['name'][:35]:<35} {t['type']:<10} "
              f"{t['events']:>7} {t['total_artists']:>8} "
              f"{t['notable_artists']:>8}")

    print(f"\n  TOTAL TARGETS: {len(real_targets)}")
    print(f"  (Filtered out {len(not_targets)} local-only operators)")

    # Not targets (for reference)
    print(f"\n  FILTERED OUT (< 12 notable artists):")
    print(f"  {'Name':<35} {'Type':<10} {'Events':>7} "
          f"{'Notable':>8}")
    print(f"  {'-'*60}")
    for t in not_targets[:20]:
        print(f"  {t['name'][:35]:<35} {t['type']:<10} "
              f"{t['events']:>7} {t['notable_artists']:>8}")
    if len(not_targets) > 20:
        print(f"  ... and {len(not_targets) - 20} more")

    # Save CSV
    with open("barcelona_targets.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["name", "type", "events",
                         "total_artists", "notable_artists",
                         "is_target"],
            extrasaction="ignore")
        w.writeheader()
        w.writerows(deduped)
    print(f"\n✓ barcelona_targets.csv")

    # Save JSON
    with open("barcelona_targets.json", "w") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "city": "Barcelona",
            "filter": "12+ artists with 100+ RA followers booked/year",
            "total_events": len(all_events),
            "total_targets": len(real_targets),
            "total_filtered_out": len(not_targets),
            "targets": real_targets,
            "filtered_out": not_targets,
        }, f, indent=2)
    print(f"✓ barcelona_targets.json")

    print(f"\nUpload both files to Claude.")


if __name__ == "__main__":
    main()
