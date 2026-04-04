#!/usr/bin/env python3
"""
Libro Server v2 — Live RA queries, Soundcharts proxy, static files.
No pre-scraping. Everything is real-time.

Run:  python3 server.py
Open: http://localhost:8000
"""

import http.server
import json
import os
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

PORT = 8000

# Soundcharts — used ONLY for Spotify city listener counts (X-Ray)
SC_BASE = "https://customer.api.soundcharts.com"
SC_HDRS = {
    "x-app-id": os.environ.get("SC_APP_ID", "NEJNJ-API_CE1B9ACA"),
    "x-api-key": os.environ.get("SC_API_KEY", "b3b12614c22b60d1"),
}

# RA GraphQL
RA_URL = "https://ra.co/graphql"
RA_HDRS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://ra.co/events",
    "Origin": "https://ra.co",
}

# Last.fm — free, no quota, electronic music similarity + tags
LASTFM_BASE = "https://ws.audioscrobbler.com/2.0"
LASTFM_KEY = os.environ.get("LASTFM_KEY", "")  # optional — works without key for public data

# MusicBrainz — free, no key, label + genre data
MB_BASE = "https://musicbrainz.org/ws/2"
MB_HDRS = {
    "User-Agent": "NightPulse/1.0 (booking intelligence)",
    "Accept": "application/json",
}

# Dice.fm — public artist pages (co-billing data)
DICE_BASE = "https://dice.fm"
DICE_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


class LibroHandler(http.server.SimpleHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/soundcharts/"):
            return self._proxy_sc()
        if self.path.startswith("/api/lastfm"):
            return self._proxy_lastfm()
        if self.path.startswith("/api/musicbrainz"):
            return self._proxy_mb()
        if self.path.startswith("/api/dice"):
            return self._proxy_dice()
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/ra/graphql":
            return self._proxy_ra()
        if self.path == "/api/ra/probe":
            return self._probe_ra()
        self.send_error(404)

    def _proxy_sc(self):
        url = SC_BASE + self.path.replace("/api/soundcharts", "", 1)
        try:
            req = urllib.request.Request(url, headers=SC_HDRS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _proxy_ra(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            req = urllib.request.Request(
                RA_URL, data=body, headers=RA_HDRS, method="POST"
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _probe_ra(self):
        """Probe RA GraphQL schema — used once to discover artist/related fields."""
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            req = urllib.request.Request(RA_URL, data=body, headers=RA_HDRS, method="POST")
            with urllib.request.urlopen(req, timeout=20) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _proxy_lastfm(self):
        """
        Last.fm proxy — free, no quota for public data.
        Routes:
          GET /api/lastfm/similar?artist=Ben+UFO&limit=50
          GET /api/lastfm/tags?artist=Ben+UFO
          GET /api/lastfm/search?artist=Ben+UFO&limit=5
        """
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = parsed.path.replace("/api/lastfm/", "")
        artist = urllib.parse.unquote(params.get("artist", [""])[0])
        limit = params.get("limit", ["50"])[0]

        method_map = {
            "similar": "artist.getSimilar",
            "tags":    "artist.getTopTags",
            "search":  "artist.search",
            "info":    "artist.getInfo",
        }
        method = method_map.get(action, "artist.getSimilar")

        qs = urllib.parse.urlencode({
            "method": method,
            "artist": artist,
            "limit": limit,
            "autocorrect": 1,
            "format": "json",
            **({"api_key": LASTFM_KEY} if LASTFM_KEY else {}),
        })
        # Last.fm works without an API key for most read endpoints
        if not LASTFM_KEY:
            qs = urllib.parse.urlencode({
                "method": method,
                "artist": artist,
                "limit": limit,
                "autocorrect": 1,
                "format": "json",
            })

        url = f"{LASTFM_BASE}/?{qs}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "NightPulse/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _proxy_mb(self):
        """
        MusicBrainz proxy — free, no key needed, 1 req/sec enforced by MB.
        Routes:
          GET /api/musicbrainz/artist?name=Ben+UFO   → search + tags + label-rels
          GET /api/musicbrainz/lookup?mbid=xxxx      → full artist lookup
        """
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = parsed.path.replace("/api/musicbrainz/", "")

        if action == "artist":
            name = urllib.parse.unquote(params.get("name", [""])[0])
            qs = urllib.parse.urlencode({
                "query": f'artist:"{name}"',
                "limit": 3,
                "fmt": "json",
            })
            url = f"{MB_BASE}/artist?{qs}"
        elif action == "lookup":
            mbid = params.get("mbid", [""])[0]
            url = f"{MB_BASE}/artist/{mbid}?inc=tags+genres+label-rels+artist-rels&fmt=json"
        else:
            self._send(404, b'{"error":"unknown action"}')
            return

        try:
            req = urllib.request.Request(url, headers=MB_HDRS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _proxy_dice(self):
        """
        Dice.fm proxy — public artist event pages for co-billing data.
        Route: GET /api/dice/artist?slug=ben-ufo
        Fetches the Dice artist page and extracts event + artist data.
        """
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        slug = params.get("slug", [""])[0]
        if not slug:
            self._send(400, b'{"error":"slug required"}')
            return

        # Dice has an internal API used by their web app
        url = f"https://api.dice.fm/api/v1/artists/{slug}/events?page[size]=50"
        hdrs = {**DICE_HDRS, "x-api-key": ""}
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            # Fallback: try the public browse endpoint by artist name
            try:
                name = slug.replace("-", " ")
                url2 = f"https://api.dice.fm/api/v1/events?page[size]=20&filter[query]={urllib.parse.quote(name)}"
                req2 = urllib.request.Request(url2, headers=DICE_HDRS)
                with urllib.request.urlopen(req2, timeout=10) as resp2:
                    self._send(200, resp2.read())
            except Exception as e2:
                self._send(502, json.dumps({"error": str(e2)}).encode())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _send(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if isinstance(body, str):
            body = body.encode()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        path = args[0] if args else ""
        if "/api/" in str(path):
            print("\033[32m[API]\033[0m " + fmt % args)
        else:
            print("\033[90m[SRV]\033[0m " + fmt % args)


if __name__ == "__main__":
    os.chdir(Path(__file__).parent)
    with http.server.HTTPServer(("", PORT), LibroHandler) as httpd:
        print("LIBRO running -> http://localhost:" + str(PORT))
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Shutting down.")
