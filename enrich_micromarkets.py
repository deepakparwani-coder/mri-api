"""
enrich_micromarkets.py — ONE-TIME geocoding enrichment for MRI Neo4j graph.

Writes mm.location = point({latitude, longitude}) onto every MicroMarket node
so pin-paste catchment queries (point.distance) work. Also stamps
mm.geocoded_name and mm.geocode_confidence for auditability.

PRIVACY: only micromarket NAMES + city go to the geocoder (public place
names — zero LF market data). Run from your machine or Railway shell:

    export NEO4J_PASSWORD=...
    python enrich_micromarkets.py --city Gurugram --dry-run   # preview
    python enrich_micromarkets.py --city Gurugram             # write
    python enrich_micromarkets.py --city Hinjewadi
    (re-run safe: nodes with mm.location already set are skipped)

Respects Nominatim's 1 request/second policy. ~48 micromarkets ≈ 1 minute.
"""
import os, re, sys, time, json, argparse
import urllib.request, urllib.parse
from neo4j import GraphDatabase

NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j+s://c26f3089.databases.neo4j.io')
NEO4J_USER = os.environ.get('NEO4J_USER', 'c26f3089')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
NOMINATIM = "https://nominatim.openstreetmap.org/search"

# ── Known-hard names: LF label -> geocodable query (extend as failures surface)
MANUAL_QUERY = {
    "E M Bypass": "Eastern Metropolitan Bypass, Kolkata",
    "Tollygunj": "Tollygunge, Kolkata",
    "Dwarka Expressway": "Dwarka Expressway, Gurugram",
}
# Names that geocoders can't resolve — paste coords manually after a failed run
MANUAL_COORDS = {
    # "Sector - 99A": (28.4321, 76.9876),
}
# City context appended to every query (LF city -> geocoder region)
CITY_CONTEXT = {
    "Gurugram": "Gurugram, Haryana, India",
    "Hinjewadi": "Pune, Maharashtra, India",
    "Kolkata": "Kolkata, West Bengal, India",
}

def normalize(name, city):
    """LF naming quirks -> geocodable string. 'Sector - 104' -> 'Sector 104'."""
    if name in MANUAL_QUERY:
        return MANUAL_QUERY[name]
    n = re.sub(r'\s*-\s*', ' ', name).strip()          # 'Sector - 104' -> 'Sector 104'
    n = re.sub(r'\s+', ' ', n)
    return f"{n}, {CITY_CONTEXT.get(city, city + ', India')}"

def geocode(query, retries=2):
    q = urllib.parse.urlencode({"q": query, "format": "jsonv2", "limit": 1,
                                "countrycodes": "in"})
    req = urllib.request.Request(f"{NOMINATIM}?{q}",
                                 headers={"User-Agent": "MRI-enrich/1.0"})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                hits = json.loads(r.read().decode())
            if hits:
                h = hits[0]
                return float(h["lat"]), float(h["lon"]), h.get("display_name", "")
            return None
        except Exception as e:
            if attempt == retries:
                print(f"    ! geocode error: {e}")
                return None
            time.sleep(2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-geocode even if mm.location exists")
    args = ap.parse_args()

    if not NEO4J_PASSWORD:
        sys.exit("Set NEO4J_PASSWORD env var first.")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as s:
        skip = "" if args.force else "AND mm.location IS NULL"
        rows = s.run(f"""
            MATCH (mm:MicroMarket)<-[:HAS_MICROMARKET|LOCATED_IN|BELONGS_TO*0..1]-(c:City {{name:$city}})
            WHERE true {skip}
            RETURN DISTINCT mm.name AS name ORDER BY name
        """, city=args.city).data()
        # Fallback if micromarkets link differently in your schema:
        if not rows:
            rows = s.run("""
                MATCH (mm:MicroMarket) WHERE mm.city = $city
                AND ($force OR mm.location IS NULL)
                RETURN mm.name AS name ORDER BY name
            """, city=args.city, force=args.force).data()

        print(f"{len(rows)} micromarkets to geocode for {args.city}"
              f"{' (DRY RUN)' if args.dry_run else ''}\n")
        ok, failed = 0, []
        for r in rows:
            name = r["name"]
            if name in MANUAL_COORDS:
                lat, lng, disp, conf = *MANUAL_COORDS[name], "manual", "manual"
            else:
                hit = geocode(normalize(name, args.city))
                time.sleep(1.1)                        # Nominatim rate policy
                if not hit:
                    print(f"  ✗ {name}  -> NO MATCH (add to MANUAL_COORDS)")
                    failed.append(name)
                    continue
                lat, lng, disp = hit
                conf = "geocoded"
            print(f"  ✓ {name:30s} -> {lat:.5f},{lng:.5f}  {disp[:50]}")
            if not args.dry_run:
                s.run("""
                    MATCH (mm:MicroMarket {name:$name})
                    SET mm.location = point({latitude:$lat, longitude:$lng}),
                        mm.geocoded_name = $disp, mm.geocode_confidence = $conf
                """, name=name, lat=lat, lng=lng, disp=disp, conf=conf)
            ok += 1
        print(f"\nDone: {ok} set, {len(failed)} failed.")
        if failed:
            print("Add failures to MANUAL_COORDS and re-run:", failed)
    driver.close()

if __name__ == "__main__":
    main()
