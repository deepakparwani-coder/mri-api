"""
geo_resolver.py  (v2) — Google Maps pin -> graph location resolution for MRI.

WHY v2
------
v1 failed on every `maps.app.goo.gl/...` link, which is the format the Google
Maps "Share" button produces — i.e. the only format a user ever actually pastes.
Three separate reasons, all of which had to be fixed:

  1. It expanded the link with a HEAD request and then read only `r.geturl()`.
     Google answers non-browser clients with a 200 HTML interstitial, not a 302,
     so `geturl()` returned the goo.gl URL unchanged and no pattern matched.

  2. Even on a successful redirect, share links resolve to
     `/maps/place/<name>/data=!4m...` which frequently carries NO `!3d/!4d`
     pair. v1 only looked at the URL, never the page body — where the
     coordinates are always present, in several forms.

  3. The User-Agent was `MRI-geo/1.0`. Google serves bot UAs a different,
     coordinate-free page.

v2 reads the response body and mines it in priority order, falls back to
forward-geocoding the place name, and — critically — never returns a bare null.
Every path records `resolution_path` and, on failure, `failure_reason`, so a
miss is diagnosable instead of silently degrading into "geo-resolver
limitation".

Drop-in replacement: same public API as v1.
    from geo_resolver import extract_pin, resolve_pin
"""
import re
import json
import difflib
import urllib.error
import urllib.parse
import urllib.request

# A real browser UA. Google serves coordinate-free HTML to obvious bots.
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
_HDRS = {"User-Agent": _UA,
         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
         "Accept-Language": "en-GB,en;q=0.9"}

MAPS_URL_RE = re.compile(
    r'https?://(?:www\.)?(?:google\.[a-z.]+/maps|maps\.google\.[a-z.]+|'
    r'maps\.app\.goo\.gl|goo\.gl/maps)[^\s<>"\']*', re.IGNORECASE)

# ── Coordinate patterns ─────────────────────────────────────────────────────
# ORDER MATTERS AND THE ORDER IS NOT OBVIOUS.
#
# The share link https://maps.app.goo.gl/RHAhAyRmfEjLZXNJ7 resolves to
#   https://www.google.com/maps/search/18.584477,+73.736395?entry=tts&...
# i.e. the true pin sits in the URL PATH. The page body of that same response
# also contains an og:image staticmap pointing at 19.117286,72.859648 — a
# generic Google image of Andheri East, Mumbai, 110 km away. Trusting the
# staticmap first (as an earlier version of this file did) silently produced a
# Mumbai answer for a Pune plot.
#
# Rule: coordinates in the FINAL URL are authoritative. Body patterns are a
# fallback only, and the staticmap goes last among them because it is the one
# that can be a decoy.
_URL_COORD_PATTERNS = [
    (re.compile(r'/maps/search/(-?\d{1,2}\.\d+),\s*\+?\s*(-?\d{1,3}\.\d+)'), 'path_search'),
    (re.compile(r'/maps/place/(-?\d{1,2}\.\d+),\s*\+?\s*(-?\d{1,3}\.\d+)'), 'path_place'),
    (re.compile(r'/maps/dir/[^/]*/(-?\d{1,2}\.\d+),\s*\+?\s*(-?\d{1,3}\.\d+)'), 'path_dir'),
    (re.compile(r'!3d(-?\d{1,2}\.\d+)!4d(-?\d{1,3}\.\d+)'), 'pin_3d4d'),
    (re.compile(r'[?&]q=(-?\d{1,2}\.\d+),\s*\+?\s*(-?\d{1,3}\.\d+)'), 'query_q'),
    (re.compile(r'[?&]ll=(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)'), 'query_ll'),
    (re.compile(r'[?&]destination=(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)'), 'query_destination'),
    (re.compile(r'[?&]center=(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)'), 'query_center'),
    (re.compile(r'/@(-?\d{1,2}\.\d+),(-?\d{1,3}\.\d+)'), 'viewport_at'),
]
_BODY_COORD_PATTERNS = [
    (re.compile(r'!3d(-?\d{1,2}\.\d+)!4d(-?\d{1,3}\.\d+)'), 'body_pin_3d4d'),
    (re.compile(r'"latitude"\s*:\s*(-?\d{1,2}\.\d+)\s*,\s*"longitude"\s*:\s*(-?\d{1,3}\.\d+)'), 'body_json_latlng'),
    (re.compile(r'staticmap[^"\'\s]*?center=(-?\d{1,2}\.\d+)(?:%2C|,)\s*(-?\d{1,3}\.\d+)'), 'body_og_staticmap'),
]
_COORD_PATTERNS = _URL_COORD_PATTERNS          # back-compat for callers/tests
_APP_INIT_RE = re.compile(r'\[\s*null\s*,\s*null\s*,\s*(-?\d{1,3}\.\d{4,})\s*,\s*(-?\d{1,2}\.\d{4,})\s*\]')
_META_REFRESH_RE = re.compile(r'http-equiv=["\']?refresh["\']?[^>]*url=([^"\'>\s]+)', re.I)
_OG_TITLE_RE = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', re.I)
_TITLE_RE = re.compile(r'<title[^>]*>([^<]+)</title>', re.I)

RAW_COORDS_RE = re.compile(r'(?<![\d.])(-?\d{1,2}\.\d{3,}),\s*(-?\d{1,3}\.\d{3,})(?![\d.])')


def _india_sane(lat, lng):
    return 6.0 <= lat <= 37.5 and 68.0 <= lng <= 97.5


def _scan_for_coords(text, patterns=None):
    """Return (lat, lng, pattern_name) from any supported encoding, or None."""
    if not text:
        return None
    for pat, name in (patterns or _URL_COORD_PATTERNS):
        for m in pat.finditer(text):
            try:
                lat, lng = float(m.group(1)), float(m.group(2))
            except (TypeError, ValueError):
                continue
            if _india_sane(lat, lng):
                return lat, lng, name
    return None


def _scan_body(body):
    if not body:
        return None
    hit = _scan_for_coords(body, _BODY_COORD_PATTERNS)
    if hit:
        return hit
    for m in _APP_INIT_RE.finditer(body):          # note: lng stored before lat
        try:
            lng, lat = float(m.group(1)), float(m.group(2))
        except (TypeError, ValueError):
            continue
        if _india_sane(lat, lng):
            return lat, lng, 'body_app_init_state'
    return None


def _km(lat1, lng1, lat2, lng2):
    import math
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def fetch_url(url, timeout=10, max_redirects=5):
    """GET with a browser UA, following redirects AND html meta-refresh.
    Returns (final_url, body_text). Never raises."""
    body = ""
    for _ in range(max_redirects):
        try:
            req = urllib.request.Request(url, headers=_HDRS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                final = r.geturl()
                raw = r.read(600_000)          # cap: coords appear early
            body = raw.decode("utf-8", errors="replace")
        except Exception:
            return url, body
        m = _META_REFRESH_RE.search(body[:4000])
        if m:
            nxt = urllib.parse.urljoin(final, m.group(1).strip())
            if nxt != url:
                url = nxt
                continue
        return final, body
    return url, body


def expand_short_url(url, timeout=10):
    """Kept for API compatibility with v1."""
    return fetch_url(url, timeout=timeout)[0]


def _place_from(decoded_url, body):
    m = re.search(r'/maps/place/([^/@?]+)', decoded_url)
    if m:
        return urllib.parse.unquote(m.group(1)).replace('+', ' ')
    for pat in (_OG_TITLE_RE, _TITLE_RE):
        m = pat.search(body or "")
        if m:
            t = m.group(1).strip()
            t = re.sub(r'\s*[-–—]\s*Google\s*Maps\s*$', '', t, flags=re.I)
            if t and t.lower() not in ('google maps', 'maps'):
                return t
    return None


def extract_pin(text):
    """Return {'lat','lng','source_url','resolution_path',...} or None."""
    if not text:
        return None

    m = MAPS_URL_RE.search(text)
    if m:
        url = m.group(0).rstrip('.,;)]}')
        path = []

        # try the raw URL first — long-form links already carry coordinates
        hit = _scan_for_coords(urllib.parse.unquote(url))
        if hit:
            return {"lat": hit[0], "lng": hit[1], "source_url": url,
                    "resolution_path": "url:" + hit[2]}

        final_url, body = fetch_url(url)
        path.append("fetched")
        decoded = urllib.parse.unquote(final_url)

        url_hit = _scan_for_coords(decoded)
        body_hit = _scan_body(body)

        # URL wins. If the body disagrees materially, say so rather than
        # silently picking one - a 110 km disagreement is what a decoy
        # staticmap looks like.
        if url_hit:
            out = {"lat": url_hit[0], "lng": url_hit[1], "source_url": final_url,
                   "resolution_path": "+".join(path) + ":" + url_hit[2]}
            if body_hit:
                d = _km(url_hit[0], url_hit[1], body_hit[0], body_hit[1])
                if d > 25:
                    out["coord_conflict_km"] = round(d, 1)
                    out["coord_conflict_note"] = (
                        f"page body also contained {body_hit[0]},{body_hit[1]} "
                        f"({body_hit[2]}) {d:.0f} km away - ignored, URL is authoritative")
            return out
        if body_hit:
            return {"lat": body_hit[0], "lng": body_hit[1], "source_url": final_url,
                    "resolution_path": "+".join(path) + ":" + body_hit[2],
                    "confidence_note": "from page body, not the URL - verify against the place name"}

        place = _place_from(decoded, body)
        return {"lat": None, "lng": None, "source_url": final_url,
                "place_hint": place,
                "resolution_path": "+".join(path) + ":place_name" if place else "+".join(path),
                "failure_reason": (
                    "no coordinates in redirect target or page body"
                    + ("" if place else "; no place name either — link may be "
                                        "expired, private, or the fetch was blocked"))}

    rm = RAW_COORDS_RE.search(text)
    if rm:
        lat, lng = float(rm.group(1)), float(rm.group(2))
        if _india_sane(lat, lng):
            return {"lat": lat, "lng": lng, "source_url": None,
                    "resolution_path": "raw_coords_in_text"}
    return None


# ── Geocoding (coordinates only — no proprietary data leaves the server) ─────
NOMINATIM_REVERSE = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_SEARCH = "https://nominatim.openstreetmap.org/search"
_GEO_HDRS = {"User-Agent": "MRI-feasibility/2.0 (contact: admin@liasesforas.com)"}


def reverse_geocode(lat, lng, timeout=8):
    q = urllib.parse.urlencode({"lat": lat, "lon": lng, "format": "jsonv2",
                                "zoom": 16, "addressdetails": 1})
    req = urllib.request.Request(f"{NOMINATIM_REVERSE}?{q}", headers=_GEO_HDRS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode())
    return _addr_out(data)


def forward_geocode(place, timeout=8):
    """Place name -> coords. Used when a share link yields a name but no pin."""
    q = urllib.parse.urlencode({"q": place, "format": "jsonv2", "limit": 1,
                                "addressdetails": 1, "countrycodes": "in"})
    req = urllib.request.Request(f"{NOMINATIM_SEARCH}?{q}", headers=_GEO_HDRS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        arr = json.loads(r.read().decode())
    if not arr:
        return None
    d = arr[0]
    out = _addr_out(d)
    out["lat"], out["lng"] = float(d["lat"]), float(d["lon"])
    return out


def _addr_out(data):
    a = data.get("address", {}) or {}
    return {
        # ordered candidates — matching tries each in turn, not just the first
        "locality": (a.get("suburb") or a.get("neighbourhood") or a.get("village")
                     or a.get("town") or a.get("city_district") or a.get("hamlet")),
        "locality_candidates": [v for v in (
            a.get("suburb"), a.get("neighbourhood"), a.get("village"), a.get("town"),
            a.get("city_district"), a.get("hamlet"), a.get("quarter"),
            a.get("residential"), a.get("county"), a.get("municipality")) if v],
        "city": a.get("city") or a.get("town") or a.get("county"),
        "district": a.get("state_district"),
        "state": a.get("state"),
        "display_name": data.get("display_name"),
    }


# ── Micromarket matching ────────────────────────────────────────────────────
MM_ALIASES = {
    "kolkata": {"tollygunge": "Tollygunj", "behala": "Behala",
                "em bypass": "E M Bypass",
                "eastern metropolitan bypass": "E M Bypass",
                "rajarhat": "Rajarhat", "new town": "New Town"},
    "gurugram": {"gurgaon": "Gurugram"},
    # v1 had no entry for either new city, so a correct geocode still missed.
    "hinjewadi": {"hinjawadi": "Hinjewadi", "hinjwadi": "Hinjewadi",
                  "hinjewadi phase 1": "Hinjewadi", "hinjewadi phase 2": "Hinjewadi",
                  "hinjewadi phase 3": "Hinjewadi", "rajiv gandhi infotech park": "Hinjewadi",
                  "maan": "Hinjewadi", "marunji": "Hinjewadi", "nere": "Hinjewadi",
                  "jambe": "Hinjewadi", "chakan midc": "Chakan"},
    "whitefield": {"whitefeild": "WHITEFEILD", "whitefield": "WHITEFEILD",
                   "varthur": "VARTHUR LAKE", "gunjur": "GUNJUR",
                   "kadugodi": "KADUGODI", "hoodi": "HOODI",
                   "brookefield": "BROOKEFIELD", "itpl": "WHITEFEILD RD- KIADB AREA"},
}


def _norm(s):
    return re.sub(r'[^a-z0-9 ]', ' ', (s or '').lower()).strip()


def match_micromarket(locality, known_micromarkets, city=None, cutoff=0.62,
                      candidates=None):
    """Exact -> alias -> containment -> token overlap -> fuzzy.
    `candidates` lets the caller pass every address field, not just one."""
    if not known_micromarkets:
        return None, None
    tries = [t for t in ([locality] + list(candidates or [])) if t]
    if not tries:
        return None, None
    alias_tbl = MM_ALIASES.get(_norm(city), {})
    known_norm = {_norm(m): m for m in known_micromarkets}

    for raw in tries:
        loc = _norm(raw)
        if loc in known_norm:
            return known_norm[loc], "exact"
        if loc in alias_tbl and alias_tbl[loc] in known_micromarkets:
            return alias_tbl[loc], "alias"
    for raw in tries:                                   # substring, both ways
        loc = _norm(raw)
        for kn, orig in known_norm.items():
            if kn and (kn in loc or loc in kn):
                return orig, "contains"
    for raw in tries:                                   # shared significant token
        toks = {t for t in _norm(raw).split() if len(t) > 3}
        for kn, orig in known_norm.items():
            if toks & {t for t in kn.split() if len(t) > 3}:
                return orig, "token"
    for raw in tries:
        hit = difflib.get_close_matches(_norm(raw), list(known_norm), n=1, cutoff=cutoff)
        if hit:
            return known_norm[hit[0]], "fuzzy"
    return None, None


def resolve_pin(pin, known_micromarkets=None, city=None):
    """Full resolution. Never raises. Always explains itself."""
    out = dict(pin)
    out.setdefault("resolution_path", "unknown")

    if pin.get("lat") is None:
        place = pin.get("place_hint")
        if place:
            try:
                fg = forward_geocode(place)
                if fg:
                    out.update(fg)
                    out["resolution_path"] += "+forward_geocode"
                else:
                    out["failure_reason"] = f"place name '{place}' not found by geocoder"
            except Exception as e:
                out["failure_reason"] = f"forward geocode failed: {e}"
        if out.get("lat") is None:
            out.setdefault("locality", place)
            out.setdefault("city", None)
            out.setdefault("state", None)
    else:
        try:
            out.update(reverse_geocode(pin["lat"], pin["lng"]))
            out["resolution_path"] += "+reverse_geocode"
        except Exception as e:
            out.update(locality=None, city=None, state=None)
            out["failure_reason"] = f"reverse geocode failed: {e}"

    mm, conf = match_micromarket(out.get("locality"), known_micromarkets or [],
                                 city, candidates=out.get("locality_candidates"))
    out["matched_micromarket"], out["match_confidence"] = mm, conf
    if mm is None and out.get("locality"):
        out.setdefault("failure_reason",
                       f"geocoded to '{out['locality']}' but no micromarket in "
                       f"{city} matched it")

    st = (out.get("state") or "").lower()
    out["regulatory_regime"] = ("UDCPR" if "maharashtra" in st
                                else "WBHIRA_KMC" if "west bengal" in st
                                else "KARNATAKA_BBMP" if "karnataka" in st
                                else "UNKNOWN")
    return out


CATCHMENT_CYPHER = """
MATCH (mm:MicroMarket)-[:LOCATED_IN|BELONGS_TO*0..1]-(:City {name:$city})
WHERE mm.location IS NOT NULL
WITH mm, point.distance(mm.location,
     point({latitude:$lat, longitude:$lng})) / 1000.0 AS km
WHERE km <= $radius_km
RETURN mm.name AS micromarket, round(km,1) AS distance_km
ORDER BY km LIMIT 10
"""
