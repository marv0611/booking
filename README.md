# Libro — Booking Intelligence for Live Music

Libro helps promoters and venues make smarter booking decisions. Enter seed artists, discover similar ones, filter out anyone unavailable, and rank by local demand.

## Quick Start

```bash
python3 server.py
# Open http://localhost:8000
```

First visit: complete the 3-step onboarding (venue, taste, constraints). This customizes everything to your market.

## How It Works

### Discovery Pipeline
1. **Seed artists** → Soundcharts finds 50 similar artists per seed
2. **RA city filter** (live) → Excludes anyone who played/is announced in your city (7mo back, 3mo forward)
3. **RA same-day filter** (live) → Excludes anyone playing anywhere in the world on your event date
4. **Soundcharts streaming** → Fetches city-level Spotify listeners
5. **Threshold & rank** → Only shows artists above your minimum, ranked by local demand

### Date Scanner
Pick any date, see every event in your city. Spot competing events before you commit to a date.

### Venue Report
Auto-generated intelligence report based on your onboarding answers + RA event history + Soundcharts data.

## Data Sources

- **Soundcharts** — Artist discovery, Spotify city listeners, Instagram (city/country on paid plan)
- **Resident Advisor** — Live event queries via GraphQL. No pre-scraping needed.

## Architecture

- `server.py` — Python proxy server. Proxies Soundcharts API + RA GraphQL to avoid CORS.
- `index.html` — Single-file app. All logic in vanilla JS. Data persists in localStorage.

All RA queries are live — no JSON files to maintain, no scrapers to run. The app queries RA in real-time for every availability check.

## Supported Cities

Barcelona, Berlin, London, Amsterdam, Paris, Lisbon, Madrid, Milan, New York, Los Angeles, Detroit, Melbourne, Tokyo, São Paulo, Buenos Aires, Tbilisi, Manchester, Ibiza, Brussels, Copenhagen, Stockholm, Warsaw, Prague, Zurich, Bangkok, Seoul.

## Environment Variables (optional)

```bash
export SC_APP_ID="your-soundcharts-app-id"
export SC_API_KEY="your-soundcharts-api-key"
```

If not set, defaults to the built-in free tier credentials.

## License

Private. Not for redistribution.
