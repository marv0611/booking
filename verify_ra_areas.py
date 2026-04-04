#!/usr/bin/env python3
"""
Run this ONCE to verify RA area IDs for the 9 unverified cities.
Requires: python3, server.py running is NOT needed.

Usage: python3 verify_ra_areas.py
"""
import json
import urllib.request

RA_URL = "https://ra.co/graphql"
RA_HDRS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://ra.co/events",
    "Origin": "https://ra.co",
}

# Query to search for areas by name
AREA_QUERY = """
query AreaSearch($name: String!) {
  areas(name: $name) {
    id
    name
    country { name }
  }
}
"""

# Fallback: try eventListings with area ID to verify it returns results
EVENT_CHECK = """
query Check($area: Int!) {
  eventListings(
    filters: { areas: { eq: $area } }
    pageSize: 1
  ) {
    totalResults
  }
}
"""

CITIES_TO_VERIFY = {
    "Edinburgh": 15,
    "Budapest": 67,
    "Rome": 82,
    "Helsinki": 33,
    "Oslo": 61,
    "Athens": 75,
    "Bucharest": 110,
    "Gothenburg": 69,
    "Nottingham": 24,
}

# Also verify confirmed IDs as sanity check
CONFIRMED = {
    "Barcelona": 20,
    "Berlin": 34,
    "London": 13,
    "Glasgow": 36,
}


def ra_query(query, variables):
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(RA_URL, data=body, headers=RA_HDRS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def check_area_id(area_id):
    """Check if an area ID returns event results."""
    d = ra_query(EVENT_CHECK, {"area": area_id})
    total = d.get("data", {}).get("eventListings", {}).get("totalResults", 0)
    return total


def search_area(name):
    """Search for area by name."""
    d = ra_query(AREA_QUERY, {"name": name})
    areas = d.get("data", {}).get("areas", [])
    return areas


print("=" * 60)
print("RA AREA ID VERIFICATION")
print("=" * 60)

# First try area search
print("\n--- Searching for areas by name ---\n")
for city, guessed_id in {**CONFIRMED, **CITIES_TO_VERIFY}.items():
    areas = search_area(city)
    if areas:
        for a in areas[:3]:
            marker = " ✓ MATCH" if str(a.get("id")) == str(guessed_id) else ""
            country = a.get("country", {}).get("name", "?")
            print(f"  {city}: ID={a['id']} name='{a['name']}' ({country}){marker}")
    else:
        print(f"  {city}: No area search results")

# Then verify each guessed ID returns events
print("\n--- Verifying IDs return events ---\n")
for city, area_id in {**CONFIRMED, **CITIES_TO_VERIFY}.items():
    total = check_area_id(area_id)
    status = "✓" if total > 0 else "✗ NO EVENTS"
    print(f"  {city} (ID {area_id}): {total} events {status}")

print("\n" + "=" * 60)
print("Copy the correct IDs into index.html AREAS constant")
print("=" * 60)
