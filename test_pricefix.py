#!/usr/bin/env python3
"""
test_pricefix.py — prove the stale-price bug is dead, using the real shipped code.

app.py cannot be imported here (Neo4j, Anthropic, the KB), so the resolver's
source is lifted straight out of the patched app.py and executed. This tests the
code that will actually run, not a copy of it that might drift.

The fixture is the real shape of `price_trend_saleable` for Hinjewadi: quarters
ascending, oldest absorption Rs.5,239, latest (Q1 26-27) Rs.8,455 - the two
numbers that appeared on opposite sides of the contradiction in the 20 August
report.
"""
import re
import sys
from pathlib import Path

APP = Path(__file__).parent / "app.py"


def load_resolver():
    src = APP.read_text(encoding="utf-8")
    start = src.find("def _quarter_sort_key")
    end = src.find("def build_feasibility_block")
    assert start != -1 and end != -1, "resolver not found in app.py"
    ns = {"re": re}
    exec(compile(src[start:end], "app.py:resolver", "exec"), ns)
    return ns


# ── fixtures ───────────────────────────────────────────────────────────────
PRICE_TREND = {
    "query": "price_trend_saleable",
    "data": [
        {"quarter": "Q1 22-23", "wt_avg_price": 5310, "absorption_price": 5239},
        {"quarter": "Q2 22-23", "wt_avg_price": 5480, "absorption_price": 5402},
        {"quarter": "Q1 23-24", "wt_avg_price": 6020, "absorption_price": 5950},
        {"quarter": "Q1 24-25", "wt_avg_price": 6890, "absorption_price": 6810},
        {"quarter": "Q1 25-26", "wt_avg_price": 7740, "absorption_price": 7690},
        {"quarter": "Q4 25-26", "wt_avg_price": 8320, "absorption_price": 8210},
        {"quarter": "Q1 26-27", "wt_avg_price": 8571, "absorption_price": 8455},
    ],
}
CARPET_TREND = {
    "query": "price_trend_carpet",
    "data": [
        {"quarter": "Q1 22-23", "wt_avg_price": 7100, "absorption_price": 7020},
        {"quarter": "Q1 26-27", "wt_avg_price": 11420, "absorption_price": 11280},
    ],
}
VELOCITY = {
    "query": "velocity_trend",
    "data": [
        {"quarter": "Q1 22-23", "velocity": 1.21},
        {"quarter": "Q1 25-26", "velocity": 2.94},
        {"quarter": "Q1 26-27", "velocity": 3.76},
    ],
}
# A project-level result that the OLD blind scanner would happily price off.
NOISE = {
    "query": "top_projects",
    "data": [
        {"project": "41 Zoy", "absorption_price": 4100, "saleable_price": 4300},
        {"project": "Megapolis", "absorption_price": 9900, "price": 10100},
    ],
}
ALL = [NOISE, PRICE_TREND, VELOCITY, CARPET_TREND]


def old_resolver(data_results):
    """The shipped-and-broken version, for the before/after comparison."""
    absorption = wt_avg = velocity = None
    for r in data_results or []:
        for row in (r.get("data") or []):
            for k, v in row.items():
                lk = str(k).lower()
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if 1000 < fv < 100000:
                    if absorption is None and "absorption" in lk:
                        absorption = fv
                    elif wt_avg is None and ("wt_avg" in lk or "price_psf" in lk
                                             or lk in ("price", "wt_avg_price", "saleable_price")):
                        wt_avg = fv
                if velocity is None and "velocit" in lk and 0 < fv < 50:
                    velocity = fv
    return absorption or wt_avg, velocity


FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


def main() -> int:
    ns = load_resolver()
    resolve = ns["_lf_price_and_velocity"]
    qkey = ns["_quarter_sort_key"]

    print("\n1. THE BUG, REPRODUCED")
    # Exactly what the 20 August report was priced off: the market series alone,
    # no noise, oldest row wins.
    old_clean, old_vel_clean = old_resolver([PRICE_TREND, VELOCITY])
    check("old resolver returns the OLDEST quarter's price", old_clean == 5239,
          f"Rs.{old_clean:,.0f} - the figure the report complained about")
    check("old resolver returns the OLDEST velocity", old_vel_clean == 1.21,
          f"{old_vel_clean}%/month instead of 3.76%")
    old_price, _ = old_resolver(ALL)
    check("old resolver could even price off a single project", old_price == 4100,
          f"Rs.{old_price:,.0f} came from a project row, not the market series")

    print("\n2. THE FIX")
    price, vel, basis, qtr, detail = resolve(ALL)
    check("price is the latest quarter", price == 8455, f"Rs.{price:,.0f}")
    check("basis is absorption, not asking", basis == "absorption", str(basis))
    check("quarter is reported", qtr == "Q1 26-27", str(qtr))
    check("velocity is the latest quarter", vel == 3.76, f"{vel}%/month")
    check("provenance is recorded", "price_trend_saleable" in detail, detail)
    check("project rows cannot set the price", price not in (4100, 9900))

    print("\n3. QUARTER ORDERING")
    check("FY quarter parses", qkey("Q1 26-27") == (2026, 1), str(qkey("Q1 26-27")))
    check("later FY sorts later", qkey("Q1 26-27") > qkey("Q4 25-26"))
    check("later quarter within FY sorts later", qkey("Q4 25-26") > qkey("Q1 25-26"))
    check("junk label rejected", qkey("not a quarter") is None)
    shuffled = {"query": "price_trend_saleable", "data": list(reversed(PRICE_TREND["data"]))}
    p2, _, _, q2, _ = resolve([shuffled])
    check("row order does not matter", p2 == 8455 and q2 == "Q1 26-27",
          f"Rs.{p2:,.0f} @ {q2} from a reversed series")

    print("\n4. CARPET BASIS")
    pc, _, _, qc, _ = resolve(ALL, want_carpet=True)
    check("carpet mode uses the carpet series", pc == 11280, f"Rs.{pc:,.0f} @ {qc}")

    print("\n5. ABSTAIN RATHER THAN GUESS")
    conflicting = [PRICE_TREND, {"query": "micromarket_price_trend",
                                 "data": [{"quarter": "Q1 26-27", "absorption_price": 2100}]}]
    p3, _, _, _, d3 = resolve(conflicting)
    check("wildly divergent candidates -> no block", p3 is None, d3)
    p4, _, _, _, d4 = resolve([])
    check("no price series -> no block", p4 is None, d4)
    p5, _, _, _, _ = resolve([{"query": "price_trend_saleable", "data": []}])
    check("empty series -> no block", p5 is None)

    print("\n6. UNDATED ROWS STILL WORK")
    undated = {"query": "price_trend_saleable",
               "data": [{"absorption_price": 7000}, {"absorption_price": 8455}]}
    p6, _, _, q6, d6 = resolve([undated])
    check("falls back to row order", p6 == 8455, f"Rs.{p6:,.0f}")
    check("and says so", "no quarter column" in d6, d6)

    print("\n7. NEGATIVE LAND COST READS AS WORDS")
    fsrc = (Path(__file__).parent / "feasibility.py").read_text(encoding="utf-8")
    check("negative branch present", "no land price makes" in fsrc)
    check("positive branch preserved", 'Maximum viable land cost at' in fsrc)

    print("\n8. AREA CHAIN RECONCILES WITH ITSELF")
    # Found while checking the corrected P&L: the cash-flow loop reused the name
    # `net`, which at the top of compute() is the net PLOT AREA. The block was
    # reporting a 5-acre plot as "25 sq.ft" - the last year's net cash flow.
    # Every derived area is now checked against the step it came from.
    sys.path.insert(0, str(Path(__file__).parent))
    from feasibility import parse_feasibility_inputs, compute_with_launch_plan
    fi = parse_feasibility_inputs(
        "5 acre plot, cost of acquisition 25 cr, construction 3000 psf, 3 year delivery")
    fi.price_psf, fi.monthly_velocity_pct = 8455, 3.76
    res = compute_with_launch_plan(fi)
    ar, ins = res["areas"], res["inputs"]
    check("gross = 5 acres", ar["gross_sqft"] == 217800, f"{ar['gross_sqft']:,}")
    check("net = gross x (1 - deductions)",
          abs(ar["net_sqft"] - ar["gross_sqft"] * (1 - ins["deduction_pct"] / 100)) < 1,
          f"{ar['net_sqft']:,}")
    check("bua = net x FSI",
          abs(ar["bua_sqft"] - ar["net_sqft"] * ins["fsi"]) < 1, f"{ar['bua_sqft']:,}")
    check("saleable = bua x efficiency",
          abs(ar["saleable_sqft"] - ar["bua_sqft"] * ins["efficiency_pct"] / 100) < 1,
          f"{ar['saleable_sqft']:,}")
    check("carpet = saleable x carpet factor",
          abs(ar["carpet_sqft"] - ar["saleable_sqft"] * ins["carpet_factor"]) < 1,
          f"{ar['carpet_sqft']:,}")
    check("revenue = saleable x price",
          abs(res["revenue_cr"] - ar["saleable_sqft"] * 8455 / 1e7) < 0.02,
          f"Rs.{res['revenue_cr']} Cr")
    check("verdict at the correct price is positive", res["margin_on_revenue_pct"] > 0,
          f"margin {res['margin_on_revenue_pct']}%, IRR {res['irr_pct']}%")

    print("\n" + "=" * 62)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    print(f"\nBefore: Rs.{old_price:,.0f} PSF (oldest quarter on file)")
    print(f"After : Rs.{price:,.0f} PSF ({qtr}, {basis})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
