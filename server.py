#!/usr/bin/env python3
"""
Libro Server — serves libro.html + proxies API calls to Soundcharts & Bandsintown.
Run:  python3 server.py
Then: open http://localhost:8000
"""

import http.server
import json
import os
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

PORT = 8000
SOUNDCHARTS_BASE = "https://customer.api.soundcharts.com"
SOUNDCHARTS_HEADERS = {
    "x-app-id": "DROP-API_A71FB164",
    "x-api-key": "8f73f2f557f4e693",
}
BANDSINTOWN_APP_ID = "libro"


class LibroHandler(http.server.SimpleHTTPRequestHandler):
    """Serve static files + proxy API requests."""

    def do_GET(self):
        # --- Soundcharts proxy ---
        if self.path.startswith("/api/soundcharts/"):
            self._proxy_soundcharts()
            return

        # --- Default: / serves libro.html ---
        if self.path == "/":
            self.path = "/libro.html"

        return super().do_GET()

    def do_POST(self):
        # --- RA GraphQL proxy ---
        if self.path == "/api/ra/graphql":
            self._proxy_ra()
            return
        self.send_error(404)

    # ── RA GraphQL ─────────────────────────────────────────────
    def _proxy_ra(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            req = urllib.request.Request(
                "https://ra.co/graphql",
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Referer": "https://ra.co/events",
                    "Origin": "https://ra.co",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._send_json(200, resp.read())
        except urllib.error.HTTPError as e:
            self._send_json(e.code, e.read())
        except Exception as e:
            self._send_json(502, json.dumps({"error": str(e)}).encode())

    # ── Soundcharts ──────────────────────────────────────────────
    def _proxy_soundcharts(self):
        # Strip our prefix, forward the rest
        api_path = self.path.replace("/api/soundcharts", "", 1)
        url = SOUNDCHARTS_BASE + api_path
        try:
            req = urllib.request.Request(url, headers=SOUNDCHARTS_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read()
                self._send_json(200, body)
        except urllib.error.HTTPError as e:
            body = e.read()
            self._send_json(e.code, body)
        except Exception as e:
            self._send_json(502, json.dumps({"error": str(e)}).encode())

    # ── Helpers ──────────────────────────────────────────────────
    def _send_json(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if isinstance(body, str):
            body = body.encode()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Colour API calls green, static grey
        path = args[0] if args else ""
        if "/api/" in str(path):
            print(f"\033[32m[API]\033[0m {format % args}")
        else:
            print(f"\033[90m[SRV]\033[0m {format % args}")


if __name__ == "__main__":
    os.chdir(Path(__file__).parent)
    with http.server.HTTPServer(("", PORT), LibroHandler) as httpd:
        print(f"\n  ╔══════════════════════════════════════╗")
        print(f"  ║  LIBRO running → http://localhost:{PORT}  ║")
        print(f"  ╚══════════════════════════════════════╝\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n  Shutting down.")
