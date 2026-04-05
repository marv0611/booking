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

PORT = int(os.environ.get("PORT", 8001))

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

# Beatport v4 API — genre enrichment (the genre authority for electronic music)
BP_BASE = "https://api.beatport.com/v4"
BP_TOKEN_URL = "https://api.beatport.com/v4/auth/o/token/"
BP_CLIENT_ID = "0GIvkCltVIuPkkwSJHp6NDb3s0potTjLBQr388Dd"
BP_USERNAME = os.environ.get("BP_USERNAME", "dropbcn")
BP_PASSWORD = os.environ.get("BP_PASSWORD", "Dropbarcelona1!")
_bp_token = None
_bp_token_expires = 0
_bp_lock = threading.Lock()


class _CodeCaptureHandler(urllib.request.HTTPRedirectHandler):
    """Follow all redirects, but capture the authorization code when it appears."""
    captured_code = None
    
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        import re as _re
        m = _re.search(r'[?&]code=([^&]+)', newurl)
        if m:
            _CodeCaptureHandler.captured_code = m.group(1)
            return None  # stop following
        return super().redirect_request(req, fp, code, msg, headers, newurl)


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
        if self.path.startswith("/api/beatport/"):
            return self._proxy_beatport()
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

    # ── Beatport v4 API ──

    def _bp_get_token(self):
        """Get Beatport token using the exact beets-beatport4 auth flow:
        1. POST /auth/login/ with JSON credentials → session cookies
        2. GET /auth/o/authorize/ with allow_redirects=False → code in Location header
        3. POST /auth/o/token/ with code → access token
        """
        global _bp_token, _bp_token_expires
        with _bp_lock:
            if _bp_token and time.time() < _bp_token_expires - 60:
                return _bp_token

            import http.cookiejar
            
            redirect_uri = f"{BP_BASE}/auth/o/post-message/"
            cj = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

            try:
                # Step 1: POST /auth/login/ with JSON body
                print(f"\033[36m[BP]\033[0m Step 1: logging in...")
                login_body = json.dumps({"username": BP_USERNAME, "password": BP_PASSWORD}).encode()
                req1 = urllib.request.Request(
                    f"{BP_BASE}/auth/login/",
                    data=login_body,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                    },
                )
                resp1 = opener.open(req1, timeout=15)
                data1 = json.loads(resp1.read().decode())
                resp1.close()
                if "username" not in data1:
                    print(f"\033[31m[BP]\033[0m Login failed: {json.dumps(data1)[:200]}")
                    return None
                print(f"\033[36m[BP]\033[0m Step 1: logged in as {data1.get('username')}")

                # Step 2: GET /auth/o/authorize/ — DON'T follow redirect, capture code from Location
                print(f"\033[36m[BP]\033[0m Step 2: fetching auth code...")
                auth_params = urllib.parse.urlencode({
                    "response_type": "code",
                    "client_id": BP_CLIENT_ID,
                    "redirect_uri": redirect_uri,
                })
                # Use raw http.client to avoid urllib's automatic redirect following
                import http.client as _hc
                conn = _hc.HTTPSConnection("api.beatport.com", timeout=15)
                cookie_hdr = "; ".join(f"{c.name}={c.value}" for c in cj)
                conn.request("GET", f"/v4/auth/o/authorize/?{auth_params}", headers={
                    "User-Agent": "Mozilla/5.0",
                    "Cookie": cookie_hdr,
                })
                resp2 = conn.getresponse()
                location = resp2.getheader("Location", "")
                resp2.read()
                conn.close()
                print(f"\033[36m[BP]\033[0m Step 2: status={resp2.status} Location={location[:120]}")

                code = None
                import re as _re
                m = _re.search(r'code=([^&]+)', location)
                if m:
                    code = m.group(1)
                if not code:
                    print(f"\033[31m[BP]\033[0m No auth code in redirect")
                    return None
                print(f"\033[36m[BP]\033[0m Step 2: got auth code: {code[:20]}...")

                # Step 3: POST /auth/o/token/ — exchange code for token
                print(f"\033[36m[BP]\033[0m Step 3: exchanging code for token...")
                token_params = urllib.parse.urlencode({
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                    "client_id": BP_CLIENT_ID,
                })
                token_url = f"{BP_TOKEN_URL}?{token_params}"
                req3 = urllib.request.Request(token_url, data=b"", headers={
                    "User-Agent": "SubPulse/1.0",
                    "Accept": "application/json",
                }, method="POST")
                resp3 = opener.open(req3, timeout=15)
                td = json.loads(resp3.read().decode())
                resp3.close()
                _bp_token = td.get("access_token", "")
                _bp_token_expires = time.time() + td.get("expires_in", 3600)
                print(f"\033[36m[BP]\033[0m ✓ Token obtained, expires in {td.get('expires_in', '?')}s")
                return _bp_token

            except Exception as e:
                print(f"\033[31m[BP AUTH ERROR]\033[0m {e}")
                import traceback
                traceback.print_exc()
                return None

    def _bp_api_call(self, path):
        """Make an authenticated GET to Beatport v4 catalog API."""
        token = self._bp_get_token()
        if not token:
            return None
        url = f"{BP_BASE}{path}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "SubPulse/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())

    def _proxy_beatport(self):
        """Handle /api/beatport/* requests.

        Routes:
          /api/beatport/search?q=NAME     → search artists, return {artists: [{id, name}]}
          /api/beatport/artist/ID/genres   → get artist tracks, aggregate genres
        """
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path = parsed.path.replace("/api/beatport", "")

        try:
            if path == "/search":
                q = urllib.parse.unquote(params.get("q", [""])[0])
                if not q:
                    self._send(400, b'{"error":"missing q param"}')
                    return
                data = self._bp_api_call(f"/catalog/search/?q={urllib.parse.quote(q)}&type=artists&per_page=5")
                if data is None:
                    self._send(401, b'{"error":"beatport auth failed"}')
                    return
                # Search returns {artists: [...], releases: [...], tracks: [...]}
                results = data.get("artists", data.get("results", []))
                if not isinstance(results, list):
                    results = []
                artists = []
                for a in results:
                    artists.append({
                        "id": a.get("id"),
                        "name": a.get("name", ""),
                        "slug": a.get("slug", ""),
                        "image": (a.get("image", {}) or {}).get("uri", "") if isinstance(a.get("image"), dict) else "",
                    })
                self._send(200, json.dumps({"artists": artists}).encode())

            elif "/artist/" in path and path.endswith("/genres"):
                parts = path.strip("/").split("/")
                if len(parts) < 3:
                    self._send(400, b'{"error":"invalid path"}')
                    return
                artist_id = parts[1]
                data = self._bp_api_call(f"/catalog/tracks/?artist_id={artist_id}&per_page=100")
                if data is None:
                    self._send(401, b'{"error":"beatport auth failed"}')
                    return
                tracks = data.get("results", data.get("data", []))
                if isinstance(data, list):
                    tracks = data
                genres = {}
                subgenres = {}
                for t in (tracks if isinstance(tracks, list) else []):
                    g = t.get("genre") or {}
                    sg = t.get("sub_genre") or {}
                    if isinstance(g, dict) and g.get("name"):
                        genres[g["name"]] = genres.get(g["name"], 0) + 1
                    elif isinstance(g, list):
                        for gi in g:
                            gn = gi.get("name", str(gi)) if isinstance(gi, dict) else str(gi)
                            genres[gn] = genres.get(gn, 0) + 1
                    if isinstance(sg, dict) and sg.get("name"):
                        subgenres[sg["name"]] = subgenres.get(sg["name"], 0) + 1
                    elif isinstance(sg, list):
                        for si in sg:
                            sn = si.get("name", str(si)) if isinstance(si, dict) else str(si)
                            subgenres[sn] = subgenres.get(sn, 0) + 1
                self._send(200, json.dumps({
                    "genres": genres,
                    "subgenres": subgenres,
                    "tracks": len(tracks) if isinstance(tracks, list) else 0,
                }).encode())

            else:
                self._send(404, json.dumps({"error": "unknown beatport action"}).encode())

        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"\033[31m[BP ERROR]\033[0m {e.code}: {body[:200]}")
            self._send(e.code, json.dumps({"error": f"beatport {e.code}"}).encode())
        except Exception as e:
            print(f"\033[31m[BP ERROR]\033[0m {e}")
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
    if BP_USERNAME and BP_PASSWORD:
        print(f"✓ Beatport v4 genre enrichment enabled (user: {BP_USERNAME})")
    else:
        print(f"⚠ No Beatport credentials — genre enrichment disabled")
    http.server.HTTPServer.allow_reuse_address = True
    with http.server.HTTPServer(("", PORT), LibroHandler) as httpd:
        print(f"SUB PULSE running -> http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Shutting down.")
