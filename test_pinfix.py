#!/usr/bin/env python3
"""
test_pinfix.py — the two Whitefield demo objections, tested against shipped code.

Objection 1: benchmarked projects were nowhere near the pin.
Objection 2: FAR 2.5 assumed; the plot is sanctioned at 3.0.

Neither app.py nor cypher_queries.py is importable here (Neo4j, Anthropic, the
KB), so the pieces under test are lifted out of the patched files and executed.
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).parent
APP = (HERE / "app.py").read_text(encoding="utf-8")
CQ = (HERE / "cypher_queries.py").read_text(encoding="utf-8")

FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


def main() -> int:
    print("\n1. THE QUERY THAT DID NOT EXIST NOW EXISTS")
    check("pin_projects is defined", '"pin_projects"' in CQ)
    body = CQ[CQ.index('"pin_projects"'):CQ.index('"pin_catchment"')]
    check("it matches Project nodes, not MicroMarket",
          "(p:Project" in body and "MicroMarket" not in body)
    check("it computes real distance", "point.distance(p.location" in body)
    check("it returns a distance column", "AS distance_km" in body)
    check("it orders by distance", "ORDER BY km" in body)
    check("it filters to the radius", "km <= $radius_km" in body)
    check("it is scoped to the city", "city_name: $city" in body)
    check("its description forbids substitution",
          "Do NOT substitute a city-wide sales ranking" in body)

    print("\n2. IT IS ACTUALLY FIRED ON A PIN")
    pin_block = APP[APP.index('if geo.get("lat") is not None:'):
                    APP.index('"query": "pin_resolution"')]
    check("run_query('pin_projects') is called", 'run_query("pin_projects"' in pin_block)
    check("the pin's own coordinates are passed",
          'lat=geo["lat"]' in pin_block and 'lng=geo["lng"]' in pin_block)
    check("a thin 5 km result widens to 10 km",
          "radius_km=10.0" in pin_block and "< 5" in pin_block)
    check("the widening is disclosed to the model",
          "radius widened to 10 km" in pin_block)
    check("pin_catchment is still fired too", 'run_query("pin_catchment"' in pin_block)

    print("\n3. PROXIMITY CLAIMS ARE CONSTRAINED")
    check("rule present", "PROXIMITY CLAIMS MUST COME FROM PROXIMITY DATA" in APP)
    check("distance column is mandatory", "must show the" in APP and "distance_km" in APP)
    check("city-wide rankings are named as the trap",
          "top_projects_by_sales" in APP and "Never describe" in APP)
    check("the empty case has a required wording",
          "spatial ranking unavailable" in APP)
    check("the radius must be stated", "State the radius you used" in APP)

    print("\n4. THE CALCULATOR'S ABSTENTION IS NOW LOUD")
    check("a block is returned instead of None",
          "COMPUTED FEASIBILITY: NOT RUN" in APP)
    i = APP.index("COMPUTED FEASIBILITY: NOT RUN")
    seg = APP[i - 400:i + 1400]
    check("it lists the missing inputs", "these inputs are " in seg)
    check("it forbids supplying a figure anyway", "You must " in seg and "NOT supply one" in seg)
    check("it names the four things that were invented",
          all(w in seg for w in ("FSI", "efficiency", "construction cost", "collection")))
    check("it forbids the 'DERIVED' label on an assumption",
          "label an assumed figure" in seg)
    check("the diagnostic says ABSTAINED", 'ABSTAINED (block states the gap)' in APP)

    print("\n5. NO PROSE ARITHMETIC, AND FSI IS NEVER ASSUMED")
    check("prose arithmetic banned", "NO PROSE ARITHMETIC. NONE." in APP)
    check("nothing is produced when the block says NOT RUN",
          "produce no P&L" in APP)
    check("FSI/FAR rule present", "FSI / FAR IS NEVER ASSUMED" in APP)
    check("the band replaces a single guess", "2.0 / 2.5 / 3.0 / 3.25" in APP)
    check("the cost of the error is quoted", "164 Cr" in APP)
    check("authority, not state", "set per authority" in APP)
    check("the authorities are named",
          all(a in APP for a in ("GBA/BBMP", "BDA", "BMRDA", "PMRDA")))

    print("\n6. NOTHING EARLIER WAS CLOBBERED")
    for rule, label in (
            ("NEVER CONVERT A PRICE", "carpet basis discipline"),
            ("THE SITE SCORE IS ARITHMETIC", "scorecard rule"),
            ("ONE VERDICT ONLY", "one-verdict rule"),
            ("_quarter_sort_key", "latest-quarter price resolver"),
            ("_wants_carpet_basis", "carpet routing"),
            ("/api/query/async", "async generation"),
            ("2026-08-28-consistency", "build marker")):
        check(f"{label} still present", rule in APP)

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
