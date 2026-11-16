"""
load_igr_city.py — load registry (IGR) buyer segments for one city into Neo4j.

Writes the shape cypher_queries.py reads for the buyer_* queries:
    (:City {name})-[:BUYER_PROFILE]->(:BuyerSegment {dimension, value, count, city_name})

Dimension names are exactly the `WHERE bs.dimension = '...'` values in cypher_queries.py:
age, gender, locality, state, religion, language, district, pincode, category.

Idempotent: deletes this city's existing BuyerSegment nodes first, then recreates them.
Run AFTER build_graph_v3_gurugram.py has created the City node.

Usage:
    python load_igr_city.py --file HJN26_IGR.json
    python load_igr_city.py --file HJN26_IGR.json --dry-run

Credentials from the environment only: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
"""
import argparse, json, os, sys
from collections import Counter

EXPECTED = {'age', 'gender', 'locality', 'state', 'religion', 'language', 'district', 'pincode', 'category'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--file', required=True)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()

    doc = json.load(open(a.file, encoding='utf-8'))
    city, segs = doc['city'], doc['segments']
    dims = Counter(s['dimension'] for s in segs)
    print(f"{city}: {len(segs)} segments, window={doc.get('window')}, registrations={doc.get('registrations')}")
    for d in sorted(dims):
        print(f"  {d:10s} {dims[d]:5d}")
    missing = EXPECTED - set(dims)
    if missing:
        print(f"WARNING: no segments for {sorted(missing)} — those buyer queries will return zero rows")
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
        if not s.run('MATCH (c:City {name:$c}) RETURN count(c) AS n', c=city).single()['n']:
            print(f'City {city!r} not found — run build_graph_v3_gurugram.py first'); return 2
        gone = s.run('MATCH (c:City {name:$c})-[:BUYER_PROFILE]->(bs:BuyerSegment) '
                     'DETACH DELETE bs RETURN count(*) AS n', c=city).single()['n']
        print(f'Removed {gone} existing segments for {city}')
        for i in range(0, len(segs), 2000):
            s.run('''UNWIND $rows AS r
                     MATCH (c:City {name:$c})
                     CREATE (bs:BuyerSegment {dimension: r.dimension, value: r.value,
                                              count: r.count, city_name: $c})
                     CREATE (c)-[:BUYER_PROFILE]->(bs)''', rows=segs[i:i + 2000], c=city)
        got = s.run('MATCH (c:City {name:$c})-[:BUYER_PROFILE]->(bs) RETURN bs.dimension AS d, count(*) AS n',
                    c=city).data()
        got = {r['d']: r['n'] for r in got}
        ok = got == dict(dims)
        print('Verified' if ok else f'MISMATCH loaded={got}')
        top = s.run("MATCH (c:City {name:$c})-[:BUYER_PROFILE]->(bs:BuyerSegment) WHERE bs.dimension='state' "
                    "RETURN bs.value AS v, bs.count AS n ORDER BY n DESC LIMIT 3", c=city).data()
        print('Top buyer states:', top)
    drv.close()
    return 0 if ok else 3


if __name__ == '__main__':
    sys.exit(main())
