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
from pathlib import Path

PORT = 8000

# Soundcharts credentials (upgrade to paid for Instagram city/country data)
SC_BASE = "https://customer.api.soundcharts.com"
SC_HDRS = {
    "x-app-id": os.environ.get("SC_APP_ID", "DROP-API2_EB90B74F"),
    "x-api-key": os.environ.get("SC_API_KEY", "522cf4373a70c456"),
}

# RA GraphQL
RA_URL = "https://ra.co/graphql"
RA_HDRS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://ra.co/events",
    "Origin": "https://ra.co",
}


class LibroHandler(http.server.SimpleHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/api/soundcharts/"):
            return self._proxy_sc()
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/ra/graphql":
            return self._proxy_ra()
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
