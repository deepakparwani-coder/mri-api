"""
load_typology_city.py - load LF per-project flat typology for one city into Neo4j.

The product-mix benchmark needs, for every competing project, what it offers
by configuration (supply, sold, sizes, rates, tickets). The market workbooks the
graph was built from only carry city-level flat-type totals, so a mix
recommendation could not be benchmarked against named competitors. This adds:

    (:Project {name, city_name})-[:HAS_TYPOLOGY]->(:FlatTypology {
        project_name, city_name, flat, supply_units, sold_units, sold_pct,
        saleable_sizes, carpet_sizes, min/max_saleable, min/max_carpet,
        avg_rate, wtavg_rate, absorption_rate, min/max_cost_lacs, source })

and on each Project: unsold_units, launch_month_year, possession_month_year,
cagr_1yr_pct, cagr_3yr_pct, cagr_5yr_pct, lf_segment.

Idempotent: deletes the city's FlatTypology nodes, then recreates them.
Run AFTER build_graph_v3_gurugram.py has created the projects.

Usage:
    python load_typology_city.py --file HJN26_TYPOLOGY.json [--dry-run]
Credentials from the environment only: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
"""
import argparse, json, os, sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass
    doc = json.load(open(a.file, encoding="utf-8"))
    city, projects, typ = doc["city"], doc["projects"], doc["typology"]
    print(f"{city}: {len(projects)} projects, {len(typ)} typology rows")
    if a.dry_run:
        return 0

    from neo4j import GraphDatabase
    try:
        from dotenv import load_dotenv; load_dotenv()
    except ImportError:
        pass
    uri, user, pwd = os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")
    if not all([uri, user, pwd]):
        print("Missing NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD"); return 1
    drv = GraphDatabase.driver(uri, auth=(user, pwd))
    with drv.session() as s:
        n_city = s.run("MATCH (p:Project {city_name:$c}) RETURN count(p) AS n", c=city).single()["n"]
        if not n_city:
            print(f"No projects for {city!r} - run build_graph_v3_gurugram.py first"); return 2
        gone = s.run("MATCH (t:FlatTypology {city_name:$c}) DETACH DELETE t RETURN count(*) AS n",
                     c=city).single()["n"]
        print(f"Removed {gone} existing typology rows for {city}")
        upd = s.run("""
            UNWIND $rows AS r
            MATCH (p:Project {name: r.name, city_name: $c})
            SET p.unsold_units = r.unsold_units,
                p.launch_month_year = r.launch,
                p.possession_month_year = r.possession,
                p.cagr_1yr_pct = r.cagr_1yr, p.cagr_3yr_pct = r.cagr_3yr, p.cagr_5yr_pct = r.cagr_5yr,
                p.lf_segment = r.segment
            RETURN count(p) AS n""", rows=projects, c=city).single()["n"]
        made = s.run("""
            UNWIND $rows AS r
            MATCH (p:Project {name: r.project, city_name: $c})
            CREATE (t:FlatTypology {project_name: r.project, city_name: $c, flat: r.flat,
                    supply_units: r.supply_units, sold_units: r.sold_units, sold_pct: r.sold_pct,
                    saleable_sizes: toString(r.saleable_sizes), carpet_sizes: toString(r.carpet_sizes),
                    min_saleable: r.min_saleable, max_saleable: r.max_saleable,
                    min_carpet: r.min_carpet, max_carpet: r.max_carpet,
                    avg_rate: r.avg_rate, wtavg_rate: r.wtavg_rate, absorption_rate: r.absorption_rate,
                    min_cost_lacs: r.min_cost_lacs, max_cost_lacs: r.max_cost_lacs, source: $src})
            CREATE (p)-[:HAS_TYPOLOGY]->(t)
            RETURN count(t) AS n""", rows=typ, c=city, src=doc.get("source", "LF")).single()["n"]
        print(f"Projects updated: {upd}/{len(projects)}   Typology rows created: {made}/{len(typ)}")
        miss = sorted({r["project"] for r in typ} - {r["name"] for r in s.run(
            "MATCH (p:Project {city_name:$c}) RETURN p.name AS name", c=city).data()})
        if miss:
            print("Not matched to a Project node (name differs):", miss)
        chk = s.run("""MATCH (p:Project {city_name:$c})-[:HAS_TYPOLOGY]->(t)
                       RETURN t.flat AS flat, count(*) AS projects, sum(t.supply_units) AS supply
                       ORDER BY supply DESC LIMIT 5""", c=city).data()
        print("Top configurations:", chk)
    drv.close()
    ok = upd == len(projects) and made == len(typ)
    print("Verified" if ok else "INCOMPLETE - see unmatched names above")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
