#!/usr/bin/env python3
"""
Sub Pulse Server v3 — RA proxy, Last.fm, MusicBrainz, Claude AI email drafting.

Run:  ANTHROPIC_API_KEY=sk-ant-... python3 server.py
      or: python3 server.py  (email drafting will be disabled)
Open: http://localhost:8001

"""

import http.server
import json
import os
import time
import threading
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

PORT = int(os.environ.get("PORT", 8000))

# Anthropic Claude API — for smart email drafting
CLAUDE_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_URL = "https://api.anthropic.com/v1/messages"

# Soundcharts
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

# Last.fm
LASTFM_BASE = "https://ws.audioscrobbler.com/2.0"
LASTFM_KEY = os.environ.get("LASTFM_KEY", "9985c4c3971211d3a422c8477c5ec7cd")

# MusicBrainz
MB_BASE = "https://musicbrainz.org/ws/2"
MB_HDRS = {
    "User-Agent": "SubPulse/1.0 (booking intelligence)",
    "Accept": "application/json",
}
_mb_lock = threading.Lock()
_mb_last_call = 0.0
MB_MIN_INTERVAL = 1.1


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
        if self.path.startswith("/api/ra/image"):
            return self._ra_artist_image()
        if self.path == "/api/claude/status":
            return self._claude_status()
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/ra/graphql":
            return self._proxy_ra()
        if self.path == "/api/ra/probe":
            return self._probe_ra()
        if self.path == "/api/claude/email":
            return self._claude_email()
        self.send_error(404)

    # ── Claude AI Email Drafting ──

    def _claude_status(self):
        """Check if Claude API is configured."""
        self._send(200, json.dumps({"enabled": bool(CLAUDE_API_KEY)}).encode())

    def _claude_email(self):
        """Generate a booking email using Claude."""
        if not CLAUDE_API_KEY:
            self._send(200, json.dumps({
                "email": "Hi,\n\nWe'd love to book [artist] for an upcoming event in [city].\n\nCould you share availability and fee range?\n\nBest,\n[Your name]",
                "ai": False
            }).encode())
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        artist = body.get("artist", "")
        venue = body.get("venue", "")
        city = body.get("city", "")
        genre = body.get("genre", "")
        routing = body.get("routing", "")  # e.g., "Madrid Fri Apr 11"
        fee_range = body.get("feeRange", "")
        context = body.get("context", "")  # additional context like "last played 14mo ago"

        prompt = f"""Write a short, professional booking inquiry email from a promoter to an artist's agent.

Details:
- Artist: {artist}
- Promoter/Venue: {venue}
- City: {city}
- Genre context: {genre}
{f'- Routing opportunity: The artist is playing {routing}. Mention this and suggest adding our date with shared travel costs.' if routing else ''}
{f'- Budget range: {fee_range}' if fee_range else ''}
{f'- Additional context: {context}' if context else ''}

Rules:
- Keep it under 100 words
- Be warm but professional — this is underground electronic music, not corporate
- If there's a routing opportunity, lead with it — it's the strongest hook
- Include the fee range naturally if provided
- End with the promoter name placeholder [Your name] and venue name
- No subject line — just the email body
- No fluff, no "I hope this email finds you well"
- Sound like a real promoter, not a template"""

        try:
            req_body = json.dumps({
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            }).encode()

            req = urllib.request.Request(CLAUDE_URL, data=req_body, headers={
                "Content-Type": "application/json",
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
            }, method="POST")

            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
                email_text = result.get("content", [{}])[0].get("text", "")
                self._send(200, json.dumps({"email": email_text, "ai": True}).encode())

        except Exception as e:
            print(f"\033[31m[CLAUDE ERROR]\033[0m {e}")
            # Fallback to template
            fallback = f"Hi,\n\nWe're {venue} in {city}. We'd love to book {artist} for an upcoming event."
            if routing:
                fallback += f"\n\nWe noticed they're playing {routing} — would it be possible to add our date and split travel?"
            if fee_range:
                fallback += f"\n\nWe typically work in the {fee_range} range."
            fallback += "\n\nBest,\n[Your name]"
            self._send(200, json.dumps({"email": fallback, "ai": False}).encode())

    # ── RA ──

    def _ra_artist_image(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        slug = params.get("slug", [""])[0].strip()
        if not slug:
            self._send(400, b'{"error":"missing slug"}')
            return
        url = f"https://ra.co/dj/{slug}"
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode("utf-8", errors="ignore")
            import re
            m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html)
            if not m:
                m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html)
            image = m.group(1) if m else ""
            if image and ("default" in image.lower() or "placeholder" in image.lower()):
                image = ""
            self._send(200, json.dumps({"image": image}).encode())
        except Exception:
            self._send(200, b'{"image":""}')

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
            req = urllib.request.Request(RA_URL, data=body, headers=RA_HDRS, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _probe_ra(self):
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
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = parsed.path.replace("/api/lastfm/", "")
        artist = urllib.parse.unquote(params.get("artist", [""])[0])
        limit = params.get("limit", ["50"])[0]
        method_map = {
            "similar": "artist.getSimilar",
            "tags": "artist.getTopTags",
            "search": "artist.search",
            "info": "artist.getInfo",
        }
        method = method_map.get(action, "artist.getSimilar")
        qs = urllib.parse.urlencode({
            "method": method, "artist": artist, "limit": limit,
            "autocorrect": 1, "api_key": LASTFM_KEY, "format": "json",
        })
        url = f"{LASTFM_BASE}/?{qs}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "SubPulse/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}).encode())

    def _proxy_mb(self):
        global _mb_last_call
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = parsed.path.replace("/api/musicbrainz/", "")
        if action == "artist":
            name = urllib.parse.unquote(params.get("name", [""])[0])
            qs = urllib.parse.urlencode({"query": f'artist:"{name}"', "limit": 3, "fmt": "json"})
            url = f"{MB_BASE}/artist?{qs}"
        elif action == "lookup":
            mbid = params.get("mbid", [""])[0]
            url = f"{MB_BASE}/artist/{mbid}?inc=tags+genres+label-rels+artist-rels&fmt=json"
        else:
            self._send(404, b'{"error":"unknown action"}')
            return
        with _mb_lock:
            now = time.time()
            wait = MB_MIN_INTERVAL - (now - _mb_last_call)
            if wait > 0:
                time.sleep(wait)
            _mb_last_call = time.time()
        try:
            req = urllib.request.Request(url, headers=MB_HDRS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._send(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
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
        if "/api/claude" in str(path):
            print("\033[35m[AI]\033[0m " + fmt % args)
        elif "/api/" in str(path):
            print("\033[32m[API]\033[0m " + fmt % args)
        else:
            print("\033[90m[SRV]\033[0m " + fmt % args)


if __name__ == "__main__":
    os.chdir(Path(__file__).parent)
    if CLAUDE_API_KEY:
        print(f"✓ Claude AI email drafting enabled")
    else:
        print(f"⚠ No ANTHROPIC_API_KEY — email drafting will use templates")
    with http.server.HTTPServer(("", PORT), LibroHandler) as httpd:
        print(f"SUB PULSE running -> http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Shutting down.")
