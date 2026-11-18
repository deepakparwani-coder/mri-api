"""
load_poi_city.py - load an OpenStreetMap points-of-interest layer for one city.

The site scorecard scores public transit, employment, social infrastructure
and education from distances to these points. Without the layer those four
parameters are NOT SCORED (never guessed). With it, every score has a named
place and a measured distance behind it, from our own database, with no web
search at report time.

Writes (MERGE, idempotent):
    (:POI {city_name, osm_type, osm_id, category, name, status, lat, lng,
           location, source: 'OpenStreetMap', fetched})
categories: metro_station, rail_station, it_park, hospital, mall, supermarket,
            school, college, university

Usage (on a machine with normal internet access):
    python load_poi_city.py --city "Hinjewadi Nov 26"              # bbox from the city's projects
    python load_poi_city.py --city "Kolkata" --bbox 22.40,88.25,22.70,88.55
    python load_poi_city.py --city "Hinjewadi Nov 26" --save poi.json --dry-run
    python load_poi_city.py --city "Hinjewadi Nov 26" --file poi.json   # load a saved fetch
Credentials from the environment only: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
"""
import argparse, datetime, json, os, re, sys, time, urllib.parse, urllib.request

MIRRORS = ["https://overpass-api.de/api/interpreter",
           "https://overpass.kumi.systems/api/interpreter",
           "https://maps.mail.ru/osm/tools/overpass/api/interpreter"]

IT_PARK_RE = re.compile(r"infotech|info ?tech|tech ?park|it ?park|\bsez\b|business ?park|cyber ?(city|park)|"
                        r"software ?park|knowledge ?park|technology ?park|\bit ?(hub|city|campus)\b|"
                        r"\b(phase|ph)\s*(i{1,3}|[123])\b.*(park|hinjewadi)|tech ?(city|hub|centre|center)", re.I)
METRO_RE = re.compile(r"metro|subway|mrts|line\s*\d", re.I)


def overpass_query(bbox):
    s, w, n, e = bbox
    b = f"({s},{w},{n},{e})"
    return f"""[out:json][timeout:180];
(
  nwr["railway"~"^(station|halt)$"]{b};
  node["construction:railway"~"station|halt"]{b};
  nwr["railway"="construction"]["construction"~"station|halt"]{b};
  nwr["amenity"~"^(hospital|school|college|university)$"]{b};
  nwr["shop"~"^(mall|department_store|supermarket)$"]{b};
  nwr["landuse"~"^(commercial|industrial)$"]["name"]{b};
  nwr["office"~"^(it|company)$"]["name"]{b};
);
out center tags;"""


def fetch(bbox, tries=6):
    data = urllib.parse.urlencode({"data": overpass_query(bbox)}).encode()
    wait = 10
    last = None
    for attempt in range(tries):
        url = MIRRORS[attempt % len(MIRRORS)]
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": "MRI-POI-loader/1.0"})
            with urllib.request.urlopen(req, timeout=200) as r:
                body = r.read().decode("utf-8")
            doc = json.loads(body)
            if "elements" in doc:
                print(f"  fetched {len(doc['elements'])} elements from {url}")
                return doc
            last = body[:200]
        except Exception as ex:
            last = f"{ex.__class__.__name__}: {ex}"
        print(f"  {url} not available ({str(last)[:90]}); retrying in {wait}s")
        time.sleep(wait)
        wait = min(wait * 2, 120)
    raise SystemExit(f"Overpass unavailable after {tries} attempts: {last}")


def classify(el):
    t = el.get("tags", {}) or {}
    name = t.get("name") or t.get("name:en")
    lat = el.get("lat") or (el.get("center") or {}).get("lat")
    lng = el.get("lon") or (el.get("center") or {}).get("lon")
    if lat is None or lng is None:
        return None
    blob = " ".join(str(t.get(k, "")) for k in ("name", "network", "operator", "station", "line", "subway"))
    cat, status = None, None
    if t.get("railway") in ("station", "halt"):
        cat = "metro_station" if (t.get("station") == "subway" or METRO_RE.search(blob)) else "rail_station"
        status = "operating"
    elif "construction:railway" in t or (t.get("railway") == "construction"):
        cat, status = ("metro_station" if METRO_RE.search(blob) or "metro" in str(t).lower() else "rail_station"), "construction"
    elif t.get("amenity") == "hospital":
        cat = "hospital"
    elif t.get("amenity") in ("school",):
        cat = "school"
    elif t.get("amenity") == "college":
        cat = "college"
    elif t.get("amenity") == "university":
        cat = "university"
    elif t.get("shop") in ("mall", "department_store"):
        cat = "mall"
    elif t.get("shop") == "supermarket":
        cat = "supermarket"
    elif name and (t.get("landuse") in ("commercial", "industrial") or t.get("office") in ("it", "company")):
        if IT_PARK_RE.search(name) or t.get("office") == "it" and IT_PARK_RE.search(name):
            cat = "it_park"
    if not cat:
        return None
    return dict(osm_type=el.get("type"), osm_id=str(el.get("id")), category=cat, name=name,
                status=status, lat=float(lat), lng=float(lng))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--bbox", help="south,west,north,east (default: the city's projects + 6 km)")
    ap.add_argument("--file", help="load a previously saved fetch instead of calling Overpass")
    ap.add_argument("--save", help="write the classified POIs to this JSON file")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass
    try:
        from dotenv import load_dotenv; load_dotenv()
    except ImportError:
        pass
    drv = None
    uri, user, pwd = os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")

    if a.file:
        doc = json.load(open(a.file, encoding="utf-8"))
        pois = doc["pois"] if "pois" in doc else [p for p in map(classify, doc.get("elements", [])) if p]
    else:
        if a.bbox:
            bbox = tuple(float(x) for x in a.bbox.split(","))
        else:
            if not all([uri, user, pwd]):
                print("Missing NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD (needed to read the city's extent)"); return 1
            from neo4j import GraphDatabase
            drv = GraphDatabase.driver(uri, auth=(user, pwd))
            with drv.session() as s:
                r = s.run("""MATCH (p:Project {city_name:$c}) WHERE p.lat IS NOT NULL
                             RETURN min(p.lat) AS s, min(p.lng) AS w, max(p.lat) AS n, max(p.lng) AS e,
                                    count(p) AS k""", c=a.city).single()
            if not r or not r["k"]:
                print(f"No project coordinates for {a.city!r}; pass --bbox south,west,north,east"); return 2
            pad = 0.055   # ~6 km
            bbox = (round(r["s"] - pad, 4), round(r["w"] - pad, 4), round(r["n"] + pad, 4), round(r["e"] + pad, 4))
        print(f"{a.city}: fetching OpenStreetMap POIs in bbox {bbox}")
        pois = [p for p in map(classify, fetch(bbox).get("elements", [])) if p]

    # de-duplicate: same category + name within ~150 m (a station mapped as node and area)
    seen, out = [], []
    for p in sorted(pois, key=lambda x: (x["category"], x["name"] or "", x["osm_type"] != "node")):
        dup = any(q["category"] == p["category"] and (q["name"] or "") == (p["name"] or "") and p["name"]
                  and abs(q["lat"] - p["lat"]) < 0.0014 and abs(q["lng"] - p["lng"]) < 0.0014 for q in seen)
        if not dup:
            seen.append(p); out.append(p)
    from collections import Counter
    print("Classified:", dict(Counter(p["category"] for p in out)))
    if a.save:
        json.dump({"city": a.city, "source": "OpenStreetMap via Overpass API (ODbL)",
                   "fetched": datetime.date.today().isoformat(), "pois": out},
                  open(a.save, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
        print(f"Saved {len(out)} POIs to {a.save}")
    if a.dry_run:
        return 0
    if not all([uri, user, pwd]):
        print("Missing NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD"); return 1
    if drv is None:
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver(uri, auth=(user, pwd))
    with drv.session() as s:
        try:
            s.run("CREATE CONSTRAINT poi_key IF NOT EXISTS FOR (x:POI) "
                  "REQUIRE (x.city_name, x.osm_type, x.osm_id) IS UNIQUE").consume()
        except Exception as ex:
            print(f"  note: constraint not created ({ex.__class__.__name__}); continuing")
        n = s.run("""UNWIND $rows AS r
                     MERGE (x:POI {city_name: $c, osm_type: r.osm_type, osm_id: r.osm_id})
                     SET x.category = r.category, x.name = r.name, x.status = r.status,
                         x.lat = r.lat, x.lng = r.lng,
                         x.location = point({latitude: r.lat, longitude: r.lng}),
                         x.source = 'OpenStreetMap', x.fetched = $d
                     RETURN count(x) AS n""", rows=out, c=a.city, d=datetime.date.today().isoformat()).single()["n"]
        got = s.run("MATCH (x:POI {city_name:$c}) RETURN x.category AS c, count(*) AS n ORDER BY n DESC",
                    c=a.city).data()
        print(f"Loaded {n} POIs for {a.city}: {got}")
    drv.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
