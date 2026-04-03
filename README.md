# Libro — Booking Intelligence for La Paloma Barcelona

Enter seed artists → discover similar artists → filter out anyone unavailable → rank by Barcelona Spotify demand + Instagram Spain followers.

## Setup

1. Run `verify_exclusions.py` to scrape fresh RA data
2. Run `LibroSetup.command` (double-click) or `python3 server.py`
3. Open http://localhost:8000

## How it works

1. **Seed artists** → Soundcharts finds 50 similar artists per seed
2. **RA BCN filter** → Excludes anyone who played/is announced in Barcelona (7 months back, 3 months forward from your event date)
3. **RA same-day filter** → Excludes anyone playing anywhere in the world on your event date
4. **Soundcharts streaming** → Fetches Barcelona Spotify listeners + Instagram Spain followers
5. **Threshold** → Only shows artists with 300+ BCN Spotify listeners
6. **Rank** → Sorted by BCN Spotify listeners, with IG Spain shown alongside

## Data sources

- **Resident Advisor** — BCN event history + worldwide same-day check (GraphQL API)
- **Soundcharts** — Artist discovery, Spotify streaming, Instagram followers
- **Bandsintown** — Deprecated (API blocked)

## Files

- `libro.html` — The app (served via server.py)
- `server.py` — Local proxy server (Soundcharts + RA GraphQL)
- `verify_exclusions.py` — Scrapes RA and generates verified exclusion list
- `exclusions_verified.json` — Generated exclusion data (run verify_exclusions.py to create)
- `LibroSetup.command` — macOS double-click launcher (scrapes + launches)

## Event: June 26, 2026

- BCN exclusion window: Nov 26, 2025 → Sep 26, 2026
- Same-day worldwide check: Jun 26, 2026
- 3,146 BCN artists excluded
- 545 worldwide same-day artists excluded
