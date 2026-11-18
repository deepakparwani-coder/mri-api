"""
load_geo_city.py — give a city's projects and micromarkets real coordinates in Neo4j.

pin_projects and pin_catchment (cypher_queries.py) only return rows where
p.location / mm.location is set. Without this step a dropped pin answers
"spatial ranking unavailable" and the report falls back to unrelated projects.

Writes, for the city named in the file:
    (:Project {name, city_name})   SET location = point(lat,lng), lat, lng, builder_name
    (:MicroMarket {name, city_name}) SET location = centroid of its projects

Coordinates are LF's own per-project lat/lng (RESSEX), not geocoded names.

Usage:
    python load_geo_city.py --file HJN26_GEO.json [--dry-run]
Credentials from the environment only: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
"""
import argparse, json, os, sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--file', required=True)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    doc = json.load(open(a.file, encoding='utf-8'))
    city, projects, mms = doc['city'], doc['projects'], doc['micromarkets']
    print(f"{city}: {len(projects)} projects, {len(mms)} micromarkets with coordinates")
    if a.dry_run:
        return 0

    from neo4j import GraphDatabase
    try:
        from dotenv import load_dotenv; load_dotenv()
    except ImportError:
        pass
    uri, user, pwd = os.getenv('NEO4J_URI'), os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')
    if not all([uri, user, pwd]):
        print('Missing NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD'); return 1
    drv = GraphDatabase.driver(uri, auth=(user, pwd))
    with drv.session() as s:
        n = s.run('''UNWIND $rows AS r
                     MATCH (p:Project {name: r.name, city_name: $c})
                     SET p.location = point({latitude: r.lat, longitude: r.lng}),
                         p.lat = r.lat, p.lng = r.lng,
                         p.builder_name = coalesce(p.builder_name, r.builder)
                     RETURN count(p) AS n''', rows=projects, c=city).single()['n']
        m = s.run('''UNWIND $rows AS r
                     MATCH (mm:MicroMarket {name: r.name, city_name: $c})
                     SET mm.location = point({latitude: r.lat, longitude: r.lng}),
                         mm.lat = r.lat, mm.lng = r.lng
                     RETURN count(mm) AS n''', rows=mms, c=city).single()['n']
        print(f"Projects located: {n}/{len(projects)}   Micromarkets located: {m}/{len(mms)}")
        test = s.run('''MATCH (p:Project {city_name: $c}) WHERE p.location IS NOT NULL
                        WITH p, point.distance(p.location, point({latitude: $lat, longitude: $lng}))/1000.0 AS km
                        RETURN p.name AS project, round(km*10)/10 AS km ORDER BY km LIMIT 3''',
                     c=city, lat=projects[0]['lat'], lng=projects[0]['lng']).data()
        print('Nearest-project check:', test)
    drv.close()
    return 0 if n == len(projects) and m == len(mms) else 3


if __name__ == '__main__':
    sys.exit(main())
