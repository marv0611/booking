#!/usr/bin/env python3
"""
Beatport v4 API Probe — run locally to test auth + endpoints.

Usage: python3 probe_beatport.py

This script:
1. Scrapes the client_id from Beatport's Swagger docs page
2. Authenticates using authorization_code grant with username/password
3. Searches for an artist ("Kerri Chandler")
4. Gets artist detail + tracks with genre info
5. Prints all response structures for wiring into server.py

Credentials: dropbcn / Dropbarcelona1!
"""

import json
import re
import sys
import urllib.request
import urllib.parse
import urllib.error

BP_DOCS = "https://api.beatport.com/v4/docs/"
BP_TOKEN = "https://api.beatport.com/v4/auth/o/token/"
BP_API = "https://api.beatport.com/v4"
USERNAME = "dropbcn"
PASSWORD = "Dropbarcelona1!"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

def log(msg):
    print(f"\033[33m[BP]\033[0m {msg}")

def fetch(url, headers=None, data=None, method=None):
    """Simple fetch with error reporting."""
    h = {**HEADERS, **(headers or {})}
    req = urllib.request.Request(url, headers=h, method=method)
    if data:
        if isinstance(data, dict):
            data = urllib.parse.urlencode(data).encode()
        elif isinstance(data, str):
            data = data.encode()
        req.data = data
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode()
            log(f"  {resp.status} {url[:80]}")
            return resp.status, body, dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log(f"  HTTP {e.code} {url[:80]}")
        log(f"  Response: {body[:500]}")
        return e.code, body, {}
    except Exception as e:
        log(f"  ERROR: {e}")
        return 0, str(e), {}


def step1_scrape_client_id():
    """Scrape client_id from Beatport docs page (Swagger UI)."""
    log("STEP 1: Scraping client_id from Beatport docs...")
    code, body, _ = fetch(BP_DOCS, headers={"Accept": "text/html"})
    if code != 200:
        log(f"  Failed to load docs page: {code}")
        return None
    
    # Try various patterns for client_id in the page source
    patterns = [
        r'client_id["\']?\s*[:=]\s*["\']([a-zA-Z0-9_-]+)["\']',
        r'clientId["\']?\s*[:=]\s*["\']([a-zA-Z0-9_-]+)["\']',
        r'"client_id":"([^"]+)"',
        r'initOAuth\(\{.*?clientId:\s*["\']([^"\']+)["\']',
        r'oauth2.*?client_id.*?["\']([a-zA-Z0-9_-]+)["\']',
    ]
    for pat in patterns:
        m = re.search(pat, body, re.IGNORECASE | re.DOTALL)
        if m:
            cid = m.group(1)
            log(f"  ✓ Found client_id: {cid}")
            return cid
    
    # Dump a section of the page for debugging
    log("  ✗ Could not find client_id in page source")
    log(f"  Page length: {len(body)} chars")
    # Look for oauth-related strings
    for line in body.split('\n'):
        ll = line.lower()
        if 'oauth' in ll or 'client' in ll or 'token' in ll:
            log(f"  HINT: {line.strip()[:200]}")
    return None


def step2_authenticate(client_id):
    """Authenticate using authorization_code flow with username/password."""
    log("STEP 2: Authenticating with Beatport...")
    
    # Method A: Try direct password grant (Resource Owner Password Credentials)
    log("  Trying password grant...")
    data = {
        "grant_type": "password",
        "username": USERNAME,
        "password": PASSWORD,
    }
    if client_id:
        data["client_id"] = client_id
    
    code, body, _ = fetch(BP_TOKEN, data=data, headers={
        "Content-Type": "application/x-www-form-urlencoded",
    })
    if code == 200:
        try:
            token_data = json.loads(body)
            log(f"  ✓ Token obtained! Type: {token_data.get('token_type')}, expires: {token_data.get('expires_in')}")
            log(f"  Token keys: {list(token_data.keys())}")
            return token_data.get("access_token")
        except:
            pass
    
    # Method B: Try with client_credentials grant
    log("  Trying client_credentials grant...")
    data2 = {
        "grant_type": "client_credentials",
    }
    if client_id:
        data2["client_id"] = client_id
    code, body, _ = fetch(BP_TOKEN, data=data2, headers={
        "Content-Type": "application/x-www-form-urlencoded",
    })
    if code == 200:
        try:
            token_data = json.loads(body)
            log(f"  ✓ Token obtained via client_credentials!")
            return token_data.get("access_token")
        except:
            pass

    # Method C: Try the authorization_code flow (beets-beatport4 approach)
    log("  Trying authorization_code flow...")
    # Step C1: Get authorization code by posting login form
    auth_url = f"{BP_API}/auth/o/authorize/"
    if client_id:
        auth_params = urllib.parse.urlencode({
            "client_id": client_id,
            "response_type": "code",
        })
        auth_full = f"{auth_url}?{auth_params}"
        login_data = {
            "username": USERNAME,
            "password": PASSWORD,
            "client_id": client_id,
            "response_type": "code",
            "allow": "Authorize",
        }
        code3, body3, headers3 = fetch(auth_full, data=login_data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
        })
        log(f"  Auth response: {code3}")
        if body3:
            log(f"  Body (first 500): {body3[:500]}")
        # Check for redirect with code
        for k, v in headers3.items():
            if 'location' in k.lower():
                log(f"  Redirect: {v}")
                # Extract code from redirect URL
                m = re.search(r'code=([^&]+)', v)
                if m:
                    auth_code = m.group(1)
                    log(f"  ✓ Got authorization code: {auth_code[:20]}...")
                    # Exchange code for token
                    token_data4 = {
                        "grant_type": "authorization_code",
                        "code": auth_code,
                        "client_id": client_id,
                    }
                    code4, body4, _ = fetch(BP_TOKEN, data=token_data4, headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                    })
                    if code4 == 200:
                        td = json.loads(body4)
                        log(f"  ✓ Token obtained via auth code!")
                        return td.get("access_token")
    
    log("  ✗ All authentication methods failed")
    log("  You may need to manually get a token from the Beatport docs page")
    log("  Instructions:")
    log("    1. Open https://api.beatport.com/v4/docs/ in your browser")
    log("    2. Open DevTools > Network tab")
    log("    3. Log in with your Beatport account")  
    log("    4. Find the request to /v4/auth/o/token/")
    log("    5. Copy the access_token from the response")
    log("    6. Re-run this script with: python3 probe_beatport.py TOKEN_HERE")
    return None


def step3_test_endpoints(token):
    """Test catalog endpoints with the token."""
    log("STEP 3: Testing API endpoints...")
    auth_headers = {"Authorization": f"Bearer {token}"}
    
    # 3a: Search artists
    log("  3a: Artist search for 'Kerri Chandler'...")
    url = f"{BP_API}/catalog/artists/?q=Kerri+Chandler&per_page=3"
    code, body, _ = fetch(url, headers=auth_headers)
    if code == 200:
        data = json.loads(body)
        log(f"  ✓ Artist search response keys: {list(data.keys())}")
        results = data.get("results", data.get("data", []))
        if isinstance(data, list):
            results = data
        log(f"  Results count: {len(results) if isinstance(results, list) else 'N/A'}")
        if results and isinstance(results, list) and len(results) > 0:
            artist = results[0]
            log(f"  First artist keys: {list(artist.keys())}")
            log(f"  First artist: {json.dumps(artist, indent=2)[:1000]}")
            artist_id = artist.get("id")
            
            # 3b: Artist detail
            if artist_id:
                log(f"\n  3b: Artist detail for ID {artist_id}...")
                url2 = f"{BP_API}/catalog/artists/{artist_id}/"
                code2, body2, _ = fetch(url2, headers=auth_headers)
                if code2 == 200:
                    detail = json.loads(body2)
                    log(f"  ✓ Artist detail keys: {list(detail.keys())}")
                    log(f"  Detail: {json.dumps(detail, indent=2)[:1500]}")
                
                # 3c: Artist tracks (this is where genres live)
                log(f"\n  3c: Artist tracks for ID {artist_id}...")
                url3 = f"{BP_API}/catalog/tracks/?artist_id={artist_id}&per_page=10"
                code3, body3, _ = fetch(url3, headers=auth_headers)
                if code3 == 200:
                    tracks = json.loads(body3)
                    log(f"  ✓ Tracks response keys: {list(tracks.keys())}")
                    tlist = tracks.get("results", tracks.get("data", []))
                    if tlist and len(tlist) > 0:
                        log(f"  First track keys: {list(tlist[0].keys())}")
                        log(f"  First track: {json.dumps(tlist[0], indent=2)[:1500]}")
                        # Extract genre info
                        genre = tlist[0].get("genre", tlist[0].get("genres", {}))
                        sub_genre = tlist[0].get("sub_genre", tlist[0].get("sub_genres", {}))
                        log(f"\n  GENRE DATA: {json.dumps(genre, indent=2)}")
                        log(f"  SUB_GENRE DATA: {json.dumps(sub_genre, indent=2)}")
                        
                        # Aggregate genres across all tracks
                        genre_counts = {}
                        subgenre_counts = {}
                        for t in tlist:
                            g = t.get("genre", t.get("genres", {}))
                            sg = t.get("sub_genre", t.get("sub_genres", {}))
                            if isinstance(g, dict) and g.get("name"):
                                genre_counts[g["name"]] = genre_counts.get(g["name"], 0) + 1
                            elif isinstance(g, list):
                                for gi in g:
                                    gn = gi.get("name", str(gi)) if isinstance(gi, dict) else str(gi)
                                    genre_counts[gn] = genre_counts.get(gn, 0) + 1
                            if isinstance(sg, dict) and sg.get("name"):
                                subgenre_counts[sg["name"]] = subgenre_counts.get(sg["name"], 0) + 1
                            elif isinstance(sg, list):
                                for si in sg:
                                    sn = si.get("name", str(si)) if isinstance(si, dict) else str(si)
                                    subgenre_counts[sn] = subgenre_counts.get(sn, 0) + 1
                        log(f"\n  GENRE BREAKDOWN (from {len(tlist)} tracks):")
                        log(f"  Genres: {json.dumps(genre_counts, indent=2)}")
                        log(f"  Sub-genres: {json.dumps(subgenre_counts, indent=2)}")
    
    # 3d: Genres list
    log("\n  3d: All genres list...")
    url4 = f"{BP_API}/catalog/genres/"
    code4, body4, _ = fetch(url4, headers=auth_headers)
    if code4 == 200:
        genres = json.loads(body4)
        log(f"  ✓ Genres response keys: {list(genres.keys())}")
        glist = genres.get("results", genres.get("data", []))
        if glist:
            log(f"  Total genres: {len(glist)}")
            for g in glist[:15]:
                log(f"    {g.get('id', '?')}: {g.get('name', g)}")


def main():
    # Allow manual token as argument
    if len(sys.argv) > 1:
        token = sys.argv[1]
        log(f"Using provided token: {token[:20]}...")
        step3_test_endpoints(token)
        return
    
    client_id = step1_scrape_client_id()
    token = step2_authenticate(client_id)
    if token:
        step3_test_endpoints(token)
    else:
        log("\n═══ MANUAL TOKEN FALLBACK ═══")
        log("If auto-auth failed, get a token manually:")
        log("  1. Open https://api.beatport.com/v4/docs/ in Chrome")
        log("  2. Open DevTools → Network tab → check 'Preserve log'")
        log("  3. Click 'Authorize' on the docs page, log in with: dropbcn / Dropbarcelona1!")
        log("  4. Look for a request to /v4/auth/o/token/ in the Network tab")
        log("  5. Copy the access_token from the response body")
        log("  6. Run: python3 probe_beatport.py YOUR_TOKEN_HERE")


if __name__ == "__main__":
    main()
