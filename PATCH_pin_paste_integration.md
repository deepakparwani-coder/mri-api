# PATCH — Google Maps Pin-Paste → Feasibility Flow
**Files touched:** `app.py` (2 edits), `cypher_queries.py` (1 addition), repo root (+`geo_resolver.py`)
**Prereq:** `geo_resolver.py` committed to repo root. Spatial catchment (Step 3) additionally needs `enrich_micromarkets.py` to have been run once per city.
**Rollback:** every block is delimited by `# ── PIN-PASTE` comments — delete blocks to revert.

---

## Step 1 — `app.py`: import + micromarket cache helper

**1a.** Near the other imports at the top:

```python
# ── PIN-PASTE: geo resolution ──
from geo_resolver import extract_pin, resolve_pin
```

**1b.** Anywhere after `run_query` is defined (e.g. directly below it):

```python
# ── PIN-PASTE: per-city micromarket name cache (for locality matching) ──
_MM_CACHE = {}
def get_micromarket_names(city):
    if city not in _MM_CACHE:
        try:
            with get_driver().session() as s:
                recs = s.run(
                    "MATCH (mm:MicroMarket)<-[*0..1]-(c:City {name:$city}) "
                    "RETURN DISTINCT mm.name AS name", city=city).data()
            _MM_CACHE[city] = [r["name"] for r in recs]
        except Exception:
            _MM_CACHE[city] = []
    return _MM_CACHE[city]
```

---

## Step 2 — `app.py`: pin block inside `handle_query`

Insert **immediately after** `data_results = classify_intent(user_query, city)` (inside the same `try` is fine, or just after the except block):

```python
    # ── PIN-PASTE: detect Google Maps link / raw coordinates ──
    pin = extract_pin(user_query)
    geo = None
    if pin:
        geo = resolve_pin(pin, known_micromarkets=get_micromarket_names(city), city=city)
        print(f"  📍 [PIN] {geo.get('lat')},{geo.get('lng')} -> locality={geo.get('locality')!r} "
              f"mm={geo.get('matched_micromarket')!r} regime={geo.get('regulatory_regime')}")
        # 2a. spatial catchment (works once enrich_micromarkets.py has run)
        if geo.get("lat") is not None:
            data_results.append(run_query("pin_catchment",
                city=city, lat=geo["lat"], lng=geo["lng"], radius_km=5.0))
        # 2b. matched micromarket -> pull its detail like any location query
        if geo.get("matched_micromarket"):
            data_results.append(run_query("micromarket_detail",
                city=city, location=geo["matched_micromarket"]))
        # 2c. inject resolution facts as a data block so Claude cites, not guesses
        data_results.append({
            "query": "pin_resolution",
            "description": ("Resolved from the Google Maps pin the user pasted. "
                            "locality/city/state come from reverse geocoding the pin "
                            "coordinates (OpenStreetMap). regulatory_regime tells you "
                            "which framework applies: UDCPR (Maharashtra) or WBHIRA_KMC "
                            "(West Bengal). Treat as location context, not LF market data."),
            "params": {}, "row_count": 1,
            "data": [{k: geo.get(k) for k in
                      ("lat","lng","locality","city","state",
                       "matched_micromarket","match_confidence","regulatory_regime")}],
            "source": "GeoResolver_pin",
        })
```

Note: the existing `needs_web` feasibility shortcut already fires on maps links, so web
mode for macro/location context is unchanged — this patch adds the *deterministic*
resolution path so coordinates and regime never depend on web search.

---

## Step 3 — `cypher_queries.py`: add the catchment query

Add to the `QUERIES` dict:

```python
    "pin_catchment": {
        "description": (
            "Micromarkets within radius_km of the map pin the user pasted, with "
            "distances in km. Computed geometrically from geocoded micromarket "
            "coordinates (point.distance). Use this to define the catchment for "
            "feasibility analysis. If zero rows, geocoding enrichment has not been "
            "run for this city — say the spatial catchment is unavailable and fall "
            "back to the matched micromarket only. Distances are point-to-point."
        ),
        "cypher": """
            MATCH (mm:MicroMarket)<-[*0..1]-(c:City {name:$city})
            WHERE mm.location IS NOT NULL
            WITH DISTINCT mm,
                 point.distance(mm.location,
                     point({latitude:$lat, longitude:$lng})) / 1000.0 AS km
            WHERE km <= $radius_km
            RETURN mm.name AS micromarket, round(km, 1) AS distance_km
            ORDER BY km
            LIMIT 12
        """,
    },
```

---

## Step 4 — one-time per city (separate from deploy)

```
python enrich_micromarkets.py --city Gurugram --dry-run   # inspect matches first
python enrich_micromarkets.py --city Gurugram
python enrich_micromarkets.py --city Hinjewadi
# Kolkata: run after the Layer 1-3 rebuild
```
Failures are listed at the end — paste their coordinates into `MANUAL_COORDS` in the
script and re-run (already-set nodes are skipped, so re-runs are cheap).

---

## Smoke tests (after deploy)

| Paste into chat | Expect |
|---|---|
| `https://www.google.com/maps/@28.4595,77.0266,14z feasibility?` | resolves Gurugram locality, pin_catchment rows, UDCPR **not** claimed (Haryana → regime UNKNOWN, bot must not apply UDCPR) |
| `Plot at 18.5913, 73.7389 — what can I build?` | Hinjewadi catchment + regime UDCPR → FSI flow fires |
| `Check 22.4521, 88.3038` (Joka) | regime WBHIRA_KMC; market data flows; no Maharashtra FSI numbers |
| `price is 22.5 lakhs, area 88.4 sqm` | **no** pin detection (guard against false fire) |

The Haryana case is worth watching: `regulatory_regime` returns `UNKNOWN` there by
design — Gurugram building rules aren't in any trusted module yet, so the bot should
give market/catchment analysis and explicitly say regulatory FSI analysis is
available for Maharashtra only.
