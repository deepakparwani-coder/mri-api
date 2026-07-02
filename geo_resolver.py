"""
geo_resolver.py — Google Maps pin → graph location resolution for MRI.

Pipeline: detect Maps URL / raw coords in chat -> expand short links ->
extract lat/lng -> reverse geocode (Nominatim, coords only — no LF data
leaves the server) -> match to MicroMarket nodes (exact -> alias -> fuzzy)
-> optional true spatial catchment via Neo4j point.distance() if micromarket
nodes carry lat/lng (see enrich_micromarkets.py).

Drop-in for app.py:
    from geo_resolver import extract_pin, resolve_pin
    pin = extract_pin(user_query)
    if pin:
        geo = resolve_pin(pin)   # {'lat','lng','locality','city','state','matched_micromarket',...}
"""
import re
import difflib
import urllib.request
import urllib.parse
import json

# ── 1. DETECTION & COORDINATE EXTRACTION ────────────────────────────────
MAPS_URL_RE = re.compile(
    r'https?://(?:www\.)?(?:google\.[a-z.]+/maps|maps\.google\.[a-z.]+|'
    r'maps\.app\.goo\.gl|goo\.gl/maps)[^\s]*', re.IGNORECASE)

# coordinate patterns inside expanded Google Maps URLs, most specific first
_COORD_PATTERNS = [
    re.compile(r'[?&]q=(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)'),        # ?q=lat,lng
    re.compile(r'!3d(-?\d{1,2}\.\d+)!4d(-?\d{1,3}\.\d+)'),            # !3d..!4d.. (pin, most precise)
    re.compile(r'/@(-?\d{1,2}\.\d+),(-?\d{1,3}\.\d+)'),               # /@lat,lng,zoom (viewport)
    re.compile(r'[?&]ll=(-?\d{1,2}\.\d+),(-?\d{1,3}\.\d+)'),          # ?ll=lat,lng
]
# raw "22.5726, 88.3639" pasted directly in chat (India bounding box sanity)
RAW_COORDS_RE = re.compile(r'(?<![\d.])(-?\d{1,2}\.\d{3,}),\s*(-?\d{1,3}\.\d{3,})(?![\d.])')

def _india_sane(lat, lng):
    return 6.0 <= lat <= 37.5 and 68.0 <= lng <= 97.5

def expand_short_url(url, timeout=6):
    """Follow redirects on maps.app.goo.gl / goo.gl links. Returns final URL."""
    try:
        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": "MRI-geo/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.geturl()
    except Exception:
        try:  # some servers reject HEAD
            req = urllib.request.Request(url, headers={"User-Agent": "MRI-geo/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.geturl()
        except Exception:
            return url

def extract_pin(text):
    """Return {'lat','lng','source_url'} or None. Pure parsing; network only
    to expand short links."""
    if not text:
        return None
    m = MAPS_URL_RE.search(text)
    if m:
        url = m.group(0).rstrip('.,;)')
        if 'goo.gl' in url:
            url = expand_short_url(url)
        decoded = urllib.parse.unquote(url)
        for pat in _COORD_PATTERNS:
            cm = pat.search(decoded)
            if cm:
                lat, lng = float(cm.group(1)), float(cm.group(2))
                if _india_sane(lat, lng):
                    return {"lat": lat, "lng": lng, "source_url": url}
        # URL but no coords (pure place-name link): return URL so caller can
        # still use the place name path
        return {"lat": None, "lng": None, "source_url": url,
                "place_hint": _place_from_url(decoded)}
    rm = RAW_COORDS_RE.search(text)
    if rm:
        lat, lng = float(rm.group(1)), float(rm.group(2))
        if _india_sane(lat, lng):
            return {"lat": lat, "lng": lng, "source_url": None}
    return None

def _place_from_url(decoded_url):
    m = re.search(r'/maps/place/([^/@]+)', decoded_url)
    return m.group(1).replace('+', ' ') if m else None

# ── 2. REVERSE GEOCODING (coords only — no proprietary data in request) ──
NOMINATIM = "https://nominatim.openstreetmap.org/reverse"

def reverse_geocode(lat, lng, timeout=8):
    """lat/lng -> locality dict via OSM Nominatim (free, keyless).
    Swap for Google Geocoding API by changing this one function."""
    q = urllib.parse.urlencode({"lat": lat, "lon": lng, "format": "jsonv2",
                                "zoom": 16, "addressdetails": 1})
    req = urllib.request.Request(f"{NOMINATIM}?{q}",
                                 headers={"User-Agent": "MRI-geo/1.0 (feasibility)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode())
    a = data.get("address", {})
    return {
        "locality": a.get("suburb") or a.get("neighbourhood") or a.get("village")
                    or a.get("town") or a.get("city_district"),
        "city": a.get("city") or a.get("town") or a.get("county"),
        "district": a.get("state_district"),
        "state": a.get("state"),
        "display_name": data.get("display_name"),
    }

# ── 3. MICROMARKET MATCHING (exact -> alias -> fuzzy) ───────────────────
# City-level alias seeds; extend as demos surface variants.
MM_ALIASES = {
    "kolkata": {"tollygunge": "Tollygunj", "behala": "Behala",
                "em bypass": "E M Bypass", "eastern metropolitan bypass": "E M Bypass",
                "rajarhat": "Rajarhat", "new town": "New Town"},
    "gurugram": {"gurgaon": "Gurugram"},
}

def match_micromarket(locality, known_micromarkets, city=None, cutoff=0.72):
    """Match a geocoded locality to graph MicroMarket names.
    Returns (matched_name, confidence: 'exact'|'alias'|'fuzzy'|None)."""
    if not locality or not known_micromarkets:
        return None, None
    loc = locality.strip().lower()
    for mm in known_micromarkets:                      # exact (case-insensitive)
        if mm.strip().lower() == loc:
            return mm, "exact"
    alias = MM_ALIASES.get((city or "").lower(), {}).get(loc)
    if alias and alias in known_micromarkets:
        return alias, "alias"
    hit = difflib.get_close_matches(loc, [m.lower() for m in known_micromarkets],
                                    n=1, cutoff=cutoff)
    if hit:
        for mm in known_micromarkets:
            if mm.lower() == hit[0]:
                return mm, "fuzzy"
    return None, None

def resolve_pin(pin, known_micromarkets=None, city=None):
    """Full resolution. Never raises; degrades gracefully so the feasibility
    flow can proceed with whatever resolved."""
    out = dict(pin)
    if pin.get("lat") is None:
        out.update(locality=pin.get("place_hint"), city=None, state=None)
    else:
        try:
            out.update(reverse_geocode(pin["lat"], pin["lng"]))
        except Exception as e:
            out.update(locality=None, city=None, state=None,
                       geocode_error=str(e))
    mm, conf = match_micromarket(out.get("locality"), known_micromarkets or [], city)
    out["matched_micromarket"], out["match_confidence"] = mm, conf
    # regulatory routing hint for the UDCPR scope guard
    st = (out.get("state") or "").lower()
    out["regulatory_regime"] = ("UDCPR" if "maharashtra" in st
                                else "WBHIRA_KMC" if "west bengal" in st
                                else "UNKNOWN")
    return out

# ── 4. SPATIAL CATCHMENT CYPHER (requires lat/lng enrichment on nodes) ──
CATCHMENT_CYPHER = """
// pin_catchment: true spatial query once MicroMarket nodes carry location
MATCH (mm:MicroMarket)-[:LOCATED_IN|BELONGS_TO*0..1]-(:City {name:$city})
WHERE mm.location IS NOT NULL
WITH mm, point.distance(mm.location,
     point({latitude:$lat, longitude:$lng})) / 1000.0 AS km
WHERE km <= $radius_km
RETURN mm.name AS micromarket, round(km,1) AS distance_km
ORDER BY km LIMIT 10
"""
