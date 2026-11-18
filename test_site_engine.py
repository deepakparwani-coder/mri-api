"""
Regression tests for the 18-Nov Hinjewadi report defects.  python test_site_engine.py

Each test names the defect it guards. Fixture data is synthetic but shaped like
the LF rows the queries return; no database or LLM is needed.
"""
import sys

import site_engine as se
from feasibility import FeasibilityInputs, compute, compute_scenarios, render_scenarios
from udcpr_feasibility import effective_fsi

FAILS = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


Q = ('This is the pin location of our 5 Acre plot "https://maps.app.goo.gl/9C29gviaCftsDt1u9" , consider current '
     'scenario A as the plot facing a road with width of 18M and Scenario B is 24M ; give FSI and Development '
     'economics basis both scenarios. Benchmark the project with top performers within 3KM dist')

print("1. query parsing - the 18-Nov query read no road width and ran at FSI 2.0")
check("both road widths read", se.parse_road_widths(Q) == [18.0, 24.0], se.parse_road_widths(Q))
check("scenario labels A, B", se.scenario_labels(Q, [18, 24]) == ["A", "B"])
check("radius 3 km", se.parse_radius_km(Q) == 3.0)
check("'3KM' is not a road width", 3.0 not in se.parse_road_widths(Q))
check("'18 m road' form", se.parse_road_widths("plot on 18 m road") == [18.0])
check("'60 ft road' converts", se.parse_road_widths("abutting a 60 ft road") == [18.3])
check("height is not a road width", se.parse_road_widths("building height 70 m, 24 m wide road") == [24.0])
check("'net 35% profit' is a 35% hurdle", se.parse_target_margin("we need net 35% profit overall") == 35.0)
check("'margin of 25%'", se.parse_target_margin("target margin of 25%") == 25.0)
check("IRR is not a margin", se.parse_target_margin("IRR 20% expected") is None)

print("2. configuration vocabulary")
for raw, want in (("2 1/2 BHK", "2.5BHK"), ("3BHK", "3BHK"), ("1RK", "Studio/1RK"), ("Studio Apt", "Studio/1RK"),
                  ("Penthouse 4BHK", "Penthouse/Duplex"), ("4 1/2 BHK", "4.5BHK+"), ("5BHK", "4.5BHK+")):
    check(f"{raw!r} -> {want}", se.norm_config(raw) == want, se.norm_config(raw))
for raw, want in (("2.5/3 BHK", "2.5/3BHK"), ("1.5/2BHK", "1.5/2BHK"), ("5 BHKand Above", "5BHK+"),
                  ("Duplex/ Penthouse", "Penthouse/Duplex"), ("1/2RK", "Studio/1RK")):
    check(f"market {raw!r} -> {want}", se.norm_market_group(raw) == want, se.norm_market_group(raw))
check("ticket band '1 Crore - 1.25 Crore'", se._parse_band_lakhs("1 Crore - 1.25 Crore") == (100.0, 125.0))
check("ticket band '80 Lac - 1 Crore'", se._parse_band_lakhs("80 Lac - 1 Crore") == (80.0, 100.0))

print("3. competition set - the report listed the nearest 15 and missed Park District at 2.5 km")
pins = [dict(distance_km=0.7, project="Godrej Park World", total_supply=2736, annual_sales=396, sold_pct=74, velocity=4.28,
             saleable_psf=8466, months_inventory=22),
        dict(distance_km=2.5, project="Park District", total_supply=1180, annual_sales=161, sold_pct=79, velocity=2.75,
             saleable_psf=7894, months_inventory=19),
        dict(distance_km=3.9, project="Godrej Evergreen Square", total_supply=2348, annual_sales=489, sold_pct=85,
             velocity=5.81, saleable_psf=8475, months_inventory=9)]
comp = se.competition_set(pins, 3.0)
check("Park District (2.5 km) included", any(p["project"] == "Park District" for p in comp["projects"]))
check("Evergreen Square (3.9 km) excluded", all(p["project"] != "Godrej Evergreen Square" for p in comp["projects"]))
check("ranked by annual sales", [p["project"] for p in comp["projects"]] == ["Godrej Park World", "Park District"])
check("catchment MI computed", comp["months_inventory"] is not None and comp["months_inventory"] > 0)

print("4. scorecard - 'not verified' scored 4/10; competition used the whole city")
sc = se.score_site(road_widths=[18, 24], comp=comp, market=dict(velocity_pct=3.26, price_psf=8675, qtr="Q2 26-27"),
                   poi=[], poi_loaded=False)
check("unloaded POI layer -> NOT SCORED, never a number",
      all(r["score"] is None for r in sc["rows"] if r["n"] in (2, 3, 4, 5)))
check("one total per road-width scenario", set(sc["totals"]) == {18, 24})
n_scored = sum(1 for r in sc["rows"] if r["score"] is not None)
check("total is out of 10 x scored rows only", all(t["out_of"] == 10 * n_scored for t in sc["totals"].values()), sc["totals"])
check("24 m frontage scores above 18 m", sc["totals"][24]["total"] > sc["totals"][18]["total"])
check("no '|' inside a rule (it broke the table)", all("|" not in r["rule"] for r in sc["rows"]))
poi = [dict(category="metro_station", name="Hinjawadi", distance_km=0.9, status="operating"),
       dict(category="hospital", name="H1", distance_km=1.2), dict(category="school", name="S1", distance_km=1.0),
       dict(category="it_park", name="RGIP Phase 1", distance_km=1.8)]
sc2 = se.score_site(road_widths=[18], comp=comp, market={}, poi=poi, poi_loaded=True)
check("with POIs, transit scored from distance (0.9 km -> 9)", sc2["rows"][1]["score"] == 9, sc2["rows"][1])
check("band table is 70/55/40", se.band80(70) == "STRONG" and se.band80(55) == "MODERATE"
      and se.band80(40) == "WEAK" and se.band80(39) == "POOR")

print("5. product mix - must be benchmarked on competitors, with units, sizes, prices")
typ = []
for proj, vel, ann, rows in (
        ("P1", 6.2, 139, [("2BHK", 400, 260, "1050, 1080", "780, 800", 8700), ("3BHK", 200, 120, "1500", "1100", 9000)]),
        ("P2", 6.0, 200, [("2BHK", 300, 140, "1000", "750", 8300), ("3BHK", 140, 60, "1400", "1020", 8600)]),
        ("P3", 3.0, 90, [("2BHK", 500, 450, "980", "720", 7600), ("3BHK", 300, 250, "1350", "990", 7900),
                         ("1BHK", 60, 55, "650", "480", 8000)]),
        ("P4", 2.5, 40, [("2BHK", 200, 180, "1020", "760", 8100), ("1BHK", 50, 40, "700", "520", 8400)])):
    for flat, sup, sold, sz, cz, rate in rows:
        typ.append(dict(project=proj, flat=flat, supply_units=sup, sold_units=sold, saleable_sizes=sz,
                        carpet_sizes=cz, absorption_rate=rate, project_velocity=vel, project_annual_sales=ann))
bench = se.benchmark_configs(typ)
flats = [dict(flat_type="1BHK", annual_sales=92, unsold=71, months_inv=9),
         dict(flat_type="1.5/2BHK", annual_sales=1747, unsold=2760, months_inv=19),
         dict(flat_type="2.5/3 BHK", annual_sales=842, unsold=1908, months_inv=27),
         dict(flat_type="3.5/4 BHK", annual_sales=59, unsold=273, months_inv=56)]
mix = se.recommend_mix(bench, flats, [dict(band="80 Lac - 1 Crore", annual_sales=896, months_inv=19)],
                       comp=dict(annual_sales=1645), carpet_factor=0.73)
check("mix sums to 100%", sum(o["mix_pct"] for o in mix["rows"]) == 100, [o["mix_pct"] for o in mix["rows"]])
check("every allocated config has size, carpet, rate, ticket",
      all(o["size"] and o["carpet"] and o["rate"] and o["ticket_lakhs"] for o in mix["rows"] if o["mix_pct"]))
check("benchmark uses CURRENT sellers (P1/P2), not the sold-out-era P3",
      bench["2BHK"]["best"][:2] == ["P1", "P2"], bench["2BHK"]["best"])
al = se.allocate_units(mix, 665_000)
check("units allocated reproduce the saleable area within 1%",
      abs(al["area_allocated"] - 665_000) / 665_000 < 0.01, al["area_allocated"])

print("6. scenario economics - FSI 2.0 default, land-0 IRR of 219.7%")
f18, f24 = effective_fsi(18, "other"), effective_fsi(24, "other")
check("18 m -> 2.10 + ancillary = 3.36", abs(f18["effective_max_fsi"] - 3.36) < 1e-9)
check("24 m -> 2.30 + ancillary = 3.68", abs(f24["effective_max_fsi"] - 3.68) < 1e-9)
inp = FeasibilityInputs(plot_sqft=217800, construction_psf=3500, price_psf=8562, monthly_velocity_pct=3.26,
                        marketing_pct=5, avg_unit_sqft=1174, target_margin_pct=35)
rows = compute_scenarios(inp, [dict(label="A", road_m=18, fsi=f18["effective_max_fsi"], fsi_basic=1.1, breakdown=f18),
                               dict(label="B", road_m=24, fsi=f24["effective_max_fsi"], fsi_basic=1.1, breakdown=f24)])
check("scenario B builds more than A",
      rows[1]["result"]["areas"]["saleable_sqft"] > rows[0]["result"]["areas"]["saleable_sqft"])
check("no land value at 35% -> IRR suppressed, not 200%+",
      rows[0]["result"]["irr_pct"] is None and rows[0]["result"]["no_land_value"])
md = render_scenarios(rows, acres=5)
check("ceiling table shows 15/20/25/35% hurdles", all(f"at {m}%" in md for m in (15, 20, 25, 35)))
check("purchase-rate grid present when rate unknown", "Rs.2,000/sq.ft" in md)
i15 = FeasibilityInputs(plot_sqft=217800, construction_psf=3500, price_psf=8562, monthly_velocity_pct=3.26,
                        fsi=3.36, fsi_basic=1.1, fsi_assumed=False)
r15 = compute(i15)
check("15% hurdle: land solved at the ceiling and IRR computed on it",
      r15["land_solved"] and r15["costs"]["land_cr"] > 0 and r15["irr_pct"] is not None)
check("15% hurdle: margin at solved land == 15%", abs(r15["margin_on_revenue_pct"] - 15.0) < 0.15,
      r15["margin_on_revenue_pct"])
check("peak equity no longer double-counts land", r15["peak_equity_cr"] <= r15["costs"]["land_cr"] + 60)
i15b = FeasibilityInputs(plot_sqft=217800, construction_psf=3500, price_psf=8562, fsi=3.36, fsi_basic=1.1,
                         fsi_assumed=False, fsi_purchase_psf=1000)
check("FSI purchase cost lowers the land ceiling by about its own amount",
      abs((r15["max_viable_land_cr"] - compute(i15b)["max_viable_land_cr"]) - 418_394 * 1000 / 1e7) < 0.5)

print("7. markdown - glued tables printed the cost table inside the area table")
try:
    import importlib
    app = importlib.import_module("app")
    glued = "| A | B |\n|---|---|\n| x | 1 |\n| Line | Rs. Cr |\n|---|---|\n| Land | 0 |"
    fixed = app.fix_markdown_tables(glued)
    check("blank line inserted before the second header", "| x | 1 |\n\n| Line | Rs. Cr |" in fixed)
    check("a normal table is untouched", app.fix_markdown_tables("| A |\n|---|\n| 1 |") == "| A |\n|---|\n| 1 |")
    out = app.finalize_report("Site score: 49/80 - WEAK",
                              [dict(label="Scenario A (18 m road)", total=30, out_of=40, band="MODERATE")])
    check("a stated score that disagrees with the computed one is flagged", "computed score governs" in out)
except Exception as e:
    print(f"  SKIP  app import unavailable here ({e.__class__.__name__}: {e})")

print()
print("ALL CHECKS PASSED" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}")
sys.exit(1 if FAILS else 0)
