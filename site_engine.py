"""
site_engine.py - deterministic site scorecard, competition set and product-mix
benchmark for pin-based feasibility reports.

WHY THIS EXISTS
---------------
The 18-Nov Hinjewadi report scored the site 49/80 from web searches. Three of
eight parameters were scored 4/10 for "not verified" - a penalty for the
model's own failure to look something up, not a property of the site - and the
competitive score used the whole micromarket, not the 3 km the user asked for.
The product mix was a restatement of the market flat-type table with "exact
unit-allocation percentages have not been computed".

Everything here is computed from data already in our own database:
  - LF project rows within the radius (pin_projects)          -> competition
  - LF project flat typology (pin_typology)                    -> product mix
  - LF market flat-type and ticket-band absorption             -> demand
  - OpenStreetMap POIs loaded into Neo4j (pin_poi), optional   -> amenities
Every score has a printed rule. A parameter without evidence is NOT SCORED and
is left out of the total; it is never given a placeholder score.
"""
import math
import re
import statistics

# ── Query parsing ─────────────────────────────────────────────────────────────
_M_UNIT = r"(?:m|mt|mts|mtr|mtrs|meter|metre|meters|metres)"
_ROAD_CTX = re.compile(r"road|width|wide|frontage|facing|abutting|\bdp\b|access", re.I)
_NOT_ROAD = re.compile(r"height|tall|setback|margin|deep|depth|level", re.I)


def parse_road_widths(text):
    """Every road width stated in the query, in order, de-duplicated (metres).

    The 18-Nov query said "a road with width of 18M and Scenario B is 24M".
    The old regex wanted "road width 18" or "18 m road" and read nothing, so
    the appraisal ran at a silent FSI 2.0 while the regulatory section quoted
    2.10 and 2.30. Any metre figure with road context within 60 characters is
    a width; one with height/setback context is not.
    """
    t = text or ""
    out = []
    for m in re.finditer(r"(?<![\d.])(\d{1,2}(?:\.\d+)?)\s*" + _M_UNIT + r"(?![a-z])", t, re.I):
        lo, hi = max(0, m.start() - 60), min(len(t), m.end() + 40)
        win_before = t[lo:m.start()]
        win = t[lo:hi]
        if not _ROAD_CTX.search(win):
            continue
        # height / setback words exclude a figure only within its own clause,
        # so "height 70 m, 24 m wide road" keeps the 24
        before = re.split(r"[,;]", t[max(0, m.start() - 25):m.start()])[-1]
        after = re.split(r"[,;]", t[m.end():m.end() + 15])[0]
        if _NOT_ROAD.search(before) or _NOT_ROAD.search(after):
            continue
        v = float(m.group(1))
        if 3 <= v <= 90 and v not in out:
            out.append(v)
    for m in re.finditer(r"(\d{2,3})\s*(?:ft|feet|foot)\s*(?:wide\s+)?(?:dp\s+)?road", t, re.I):
        v = round(float(m.group(1)) * 0.3048, 1)
        if 3 <= v <= 90 and v not in out:
            out.append(v)
    return out


def scenario_labels(text, widths):
    """'Scenario A' / 'Scenario B' as the user named them, else A, B, C..."""
    labels = re.findall(r"scenario\s*([a-z0-9])\b", text or "", re.I)
    labels = [l.upper() for l in labels]
    seen = []
    for l in labels:
        if l not in seen:
            seen.append(l)
    return [seen[i] if i < len(seen) else chr(65 + i) for i in range(len(widths))]


def parse_radius_km(text, default=3.0):
    t = text or ""
    m = (re.search(r"within\s*(?:a\s*)?(\d{1,2}(?:\.\d+)?)\s*(?:km|kms|kilomet)", t, re.I)
         or re.search(r"(\d{1,2}(?:\.\d+)?)\s*(?:km|kms)\s*(?:radius|dist|distance|catchment)", t, re.I)
         or re.search(r"radius\s*(?:of)?\s*(\d{1,2}(?:\.\d+)?)\s*(?:km|kms)", t, re.I))
    if m:
        v = float(m.group(1))
        if 0.5 <= v <= 10:
            return v
    return default


def parse_target_margin(text):
    """'35% net profit', 'net 35% profit', 'margin of 25%', 'hurdle 30%'."""
    t = (text or "").lower()
    pats = [
        r"(\d{1,2}(?:\.\d+)?)\s*%\s*(?:net\s+)?(?:profit|margin|pat\b|return on revenue)",
        r"net\s+(\d{1,2}(?:\.\d+)?)\s*%\s*(?:profit|margin)",
        r"(?:profit|margin|hurdle)\s*(?:target|hurdle|requirement|expectation)?\s*(?:of|at|is|=|:|-)?\s*(\d{1,2}(?:\.\d+)?)\s*%",
    ]
    for p in pats:
        m = re.search(p, t)
        if m and not re.search(r"irr", t[max(0, m.start() - 12):m.end() + 6]):
            v = float(m.group(1))
            if 5 <= v <= 60:
                return v
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def km_between(lat1, lng1, lat2, lng2):
    r, p = 6371.0, math.radians
    a = (math.sin(p(lat2 - lat1) / 2) ** 2
         + math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lng2 - lng1) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def _num(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _sizes(s):
    """'889, 911' -> [889, 911]."""
    if s is None:
        return []
    if isinstance(s, (int, float)):
        return [float(s)] if s else []
    return [float(x) for x in re.findall(r"\d+(?:\.\d+)?", str(s).replace(",", " ")) if float(x) > 100]


def _band(total, out_of=80):
    """ONE band table for the whole product (the prompt had two that disagreed:
    70/55/40 in one place, 65/45 in another). Scaled when parameters are not
    scored, so 42/60 bands like 56/80."""
    x = total * 80.0 / out_of if out_of else 0
    return "STRONG" if x >= 70 else "MODERATE" if x >= 55 else "WEAK" if x >= 40 else "POOR"


band80 = _band


# ── Configuration vocabulary ──────────────────────────────────────────────────
CONFIG_ORDER = ["Studio/1RK", "1BHK", "1.5BHK", "2BHK", "2.5BHK", "3BHK",
                "3.5BHK", "4BHK", "4.5BHK+", "Penthouse/Duplex"]
MARKET_GROUP = {"Studio/1RK": "Studio/1RK", "1BHK": "1BHK", "1.5BHK": "1.5/2BHK",
                "2BHK": "1.5/2BHK", "2.5BHK": "2.5/3BHK", "3BHK": "2.5/3BHK",
                "3.5BHK": "3.5/4BHK", "4BHK": "3.5/4BHK", "4.5BHK+": "5BHK+",
                "Penthouse/Duplex": "Penthouse/Duplex"}


def norm_config(label):
    """Project typology label -> configuration ('2 1/2 BHK' -> '2.5BHK')."""
    s = str(label or "").strip().lower()
    if not s:
        return None
    if s.startswith(("penthouse", "duplex", "villa", "row")):
        return "Penthouse/Duplex"
    if "studio" in s or re.search(r"\b1\s*/?\s*2?\s*rk\b|^1rk|^rk", s):
        return "Studio/1RK"
    m = re.match(r"(\d)(?:\s*(?:1/2|\.5|½))?", s)
    if not m:
        return None
    n = int(m.group(1))
    half = bool(re.match(r"\d\s*(?:1/2|\.5|½)", s))
    if n >= 5 or (n == 4 and half):
        return "4.5BHK+"
    return f"{n}{'.5' if half else ''}BHK"


def norm_market_group(label):
    """LF market flat-type label -> group ('2.5/3 BHK' -> '2.5/3BHK')."""
    s = str(label or "").lower().replace(" ", "")
    if "studio" in s or "rk" in s:
        return "Studio/1RK"
    if "penthouse" in s or "duplex" in s:
        return "Penthouse/Duplex"
    if s.startswith("5") or "above" in s:
        return "5BHK+"
    for g in ("1.5/2BHK", "2.5/3BHK", "3.5/4BHK"):
        if s.startswith(g.lower()):
            return g
    if s.startswith("1bhk"):
        return "1BHK"
    if s.startswith("2bhk"):
        return "1.5/2BHK"
    if s.startswith("3bhk"):
        return "2.5/3BHK"
    if s.startswith("4bhk"):
        return "3.5/4BHK"
    return None


# ── Competition set ───────────────────────────────────────────────────────────
def competition_set(pin_rows, radius_km):
    """LF projects within the radius, ranked by annual sales, with catchment totals."""
    rows = []
    for r in pin_rows or []:
        d = _num(r.get("distance_km"))
        if d is None or d > radius_km + 1e-9:
            continue
        supply = _num(r.get("total_supply"), 0) or 0
        sold_pct = _num(r.get("sold_pct"), 0) or 0
        unsold = _num(r.get("unsold_units"))
        if unsold is None:
            unsold = max(0.0, supply * (1 - sold_pct / 100.0))
        rows.append(dict(
            project=r.get("project"), builder=r.get("builder"), distance_km=d,
            annual_sales=_num(r.get("annual_sales"), 0) or 0, total_supply=supply,
            sold_pct=sold_pct, unsold=round(unsold),
            saleable_psf=_num(r.get("saleable_psf")), velocity=_num(r.get("velocity")),
            months_inventory=_num(r.get("months_inventory")),
            launch=r.get("launch"), possession=r.get("possession"),
            cagr_3yr=_num(r.get("cagr_3yr")), configs=r.get("configs")))
    rows.sort(key=lambda x: (-x["annual_sales"], x["distance_km"]))
    ann = sum(x["annual_sales"] for x in rows)
    uns = sum(x["unsold"] for x in rows)
    sup = sum(x["total_supply"] for x in rows if x["velocity"] is not None)
    vel = (sum((x["velocity"] or 0) * x["total_supply"] for x in rows if x["velocity"] is not None) / sup
           if sup else None)
    pw = [(x["saleable_psf"], x["annual_sales"]) for x in rows if x["saleable_psf"] and x["annual_sales"]]
    psf = sum(p * w for p, w in pw) / sum(w for _, w in pw) if pw else None
    return dict(radius_km=radius_km, projects=rows, n=len(rows),
                n_selling=sum(1 for x in rows if x["annual_sales"] > 0),
                annual_sales=round(ann), unsold=round(uns),
                months_inventory=round(uns / (ann / 12.0), 1) if ann else None,
                velocity_pct=round(vel, 2) if vel is not None else None,
                absorption_psf=round(psf) if psf else None)


# ── Site scorecard ────────────────────────────────────────────────────────────
ROAD_RULE = "road width: <9 m 2; 9-12 m 4; 12-15 m 5; 15-18 m 6; 18-24 m 7; 24-30 m 8; 30-45 m 9; 45 m+ 10"


def _road_score(w):
    for lim, s in ((9, 2), (12, 4), (15, 5), (18, 6), (24, 7), (30, 8), (45, 9)):
        if w < lim:
            return s
    return 10


def _dist_score(km, bands):
    for lim, s in bands:
        if km <= lim:
            return s
    return bands[-1][1] if bands else 1


def score_site(*, road_widths, comp, market, poi=None, poi_loaded=False):
    """Eight parameters, each with evidence, rule, score (or None = not scored).

    comp     competition_set(...) for the user's radius
    market   {'velocity_pct','months_inv','price_psf','qtr','price_yoy_pct'}
    poi      rows {'category','name','distance_km','status'} within 8 km
    """
    poi = poi or []
    rows = []

    def add(n, name, evidence, rule, score, source):
        rows.append(dict(n=n, parameter=name, evidence=evidence, rule=rule,
                         score=score, source=source))

    def nearest(cats):
        c = [p for p in poi if p.get("category") in cats and p.get("distance_km") is not None]
        return min(c, key=lambda p: p["distance_km"]) if c else None

    def count(cats, within):
        return sum(1 for p in poi if p.get("category") in cats and (p.get("distance_km") or 99) <= within)

    # 1 Connectivity - the frontage the user states; one score per scenario
    if road_widths:
        sc = {w: _road_score(w) for w in road_widths}
        ev = "; ".join(f"{w:g} m frontage -> {s}/10" for w, s in sc.items())
        add(1, "Connectivity (road frontage)", ev, ROAD_RULE, sc, "user-stated road width")
    else:
        add(1, "Connectivity (road frontage)", "road width not stated", ROAD_RULE, None, "-")

    # 2 Public transit
    bands_t = [(0.5, 10), (1.0, 9), (1.5, 8), (2.0, 7), (3.0, 6), (5.0, 4), (8.0, 2), (999, 1)]
    st = nearest({"metro_station", "rail_station"})
    if st:
        s = _dist_score(st["distance_km"], bands_t)
        uc = str(st.get("status") or "").lower() in ("construction", "proposed", "planned")
        if uc:
            s = max(1, s - 1)
        add(2, "Public transit", f"nearest station {st['name'] or 'unnamed'} at {st['distance_km']:.1f} km"
            + (" (under construction: -1)" if uc else ""),
            "station <=0.5 km 10; <=1 9; <=1.5 8; <=2 7; <=3 6; <=5 4; <=8 2; else 1; -1 if not yet operating",
            s, "OpenStreetMap layer")
    else:
        add(2, "Public transit", "no station layer loaded for this city" if not poi_loaded
            else "no station within 8 km in the POI layer",
            "distance to nearest metro/rail station", None if not poi_loaded else 1, "OpenStreetMap layer")

    # 3 Corporate demand drivers
    emp = nearest({"it_park", "office_cluster"})
    if emp:
        n3 = count({"it_park", "office_cluster"}, 5.0)
        s = _dist_score(emp["distance_km"], [(1, 10), (2, 9), (3, 8), (5, 6), (8, 4), (12, 2), (999, 1)])
        add(3, "Corporate demand drivers", f"nearest employment cluster {emp['name'] or 'unnamed'} "
            f"at {emp['distance_km']:.1f} km; {n3} within 5 km",
            "nearest IT park / office cluster <=1 km 10; <=2 9; <=3 8; <=5 6; <=8 4; <=12 2; else 1",
            s, "OpenStreetMap layer")
    else:
        add(3, "Corporate demand drivers", "no employment layer loaded for this city" if not poi_loaded
            else "no IT park / office cluster within 8 km in the POI layer",
            "distance to nearest IT park / office cluster", None if not poi_loaded else 1, "OpenStreetMap layer")

    # 4 Social infrastructure
    def _cnt_pts(n, steps):
        pts = 0
        for lim, p in steps:
            if n >= lim:
                pts = p
        return pts
    if poi_loaded:
        h, rt = count({"hospital"}, 3.0), count({"mall", "supermarket"}, 3.0)
        nh = nearest({"hospital"})
        s = min(10, _cnt_pts(h, [(1, 2), (2, 3), (4, 4), (7, 5)]) + _cnt_pts(rt, [(1, 2), (2, 3), (4, 4), (7, 5)]))
        add(4, "Social infrastructure", f"{h} hospitals and {rt} malls/supermarkets within 3 km"
            + (f"; nearest hospital {nh['distance_km']:.1f} km" if nh else ""),
            "hospitals within 3 km: 1->2, 2-3->3, 4-6->4, 7+->5  plus  malls/supermarkets same scale",
            max(1, s), "OpenStreetMap layer")
    else:
        add(4, "Social infrastructure", "no amenity layer loaded for this city",
            "hospitals + retail within 3 km", None, "OpenStreetMap layer")

    # 5 Education
    if poi_loaded:
        sc_, co = count({"school"}, 3.0), count({"college", "university"}, 5.0)
        s = min(10, _cnt_pts(sc_, [(1, 2), (3, 4), (6, 6), (11, 7)]) + _cnt_pts(co, [(1, 2), (2, 3)]))
        add(5, "Educational institutions", f"{sc_} schools within 3 km; {co} colleges/universities within 5 km",
            "schools within 3 km: 1-2->2, 3-5->4, 6-10->6, 11+->7  plus  colleges within 5 km: 1->2, 2+->3",
            max(1, s), "OpenStreetMap layer")
    else:
        add(5, "Educational institutions", "no amenity layer loaded for this city",
            "schools within 3 km + colleges within 5 km", None, "OpenStreetMap layer")

    # 6 Future growth catalysts - price momentum the catchment has ACHIEVED,
    #   plus infrastructure under construction nearby
    cagrs = [p["cagr_3yr"] for p in comp["projects"] if p.get("cagr_3yr") is not None]
    yoy = market.get("price_yoy_pct")
    if cagrs or yoy is not None:
        if cagrs:
            base_v, base_lbl = statistics.median(cagrs), f"median 3-yr price CAGR of {len(cagrs)} catchment projects"
        else:
            base_v, base_lbl = yoy, "market absorption-price change, latest quarter vs a year earlier"
        s = _dist_score(-base_v, [(-8, 7), (-6, 6), (-4, 5), (-2, 4), (0, 3), (999, 2)])
        ev = f"{base_lbl}: {base_v:.1f}%"
        uc = [p for p in poi if p.get("category") in ("metro_station", "rail_station")
              and str(p.get("status") or "").lower() in ("construction", "proposed", "planned")
              and (p.get("distance_km") or 99) <= 2.0]
        if uc:
            s += 2
            ev += f"; station under construction within 2 km ({uc[0]['name'] or 'unnamed'}): +2"
        launches = sum(1 for p in comp["projects"] if p.get("launch") and _recent(p["launch"], market.get("qtr")))
        if launches >= 2:
            s += 1
            ev += f"; {launches} launches in the catchment in the last 12 months: +1"
        add(6, "Future growth catalysts", ev,
            "price CAGR >=8% 7; 6-8 6; 4-6 5; 2-4 4; 0-2 3; <0 2; +2 station under construction <=2 km; +1 for 2+ launches in 12 months",
            min(10, s), "LF project data" + (" + OpenStreetMap" if uc else ""))
    else:
        add(6, "Future growth catalysts", "no price-growth series retrieved",
            "price momentum + infrastructure", None, "LF")

    # 7 Competitive landscape - the catchment the user asked about, not the city
    if comp["n"] and comp["months_inventory"] is not None:
        mi = comp["months_inventory"]
        s = _dist_score(mi, [(12, 9), (18, 8), (24, 6), (30, 5), (42, 3), (999, 2)])
        ev = (f"{comp['n']} projects within {comp['radius_km']:g} km: {comp['unsold']:,} unsold vs "
              f"{comp['annual_sales']:,} sold a year -> {mi:g} months of inventory")
        mv, cv = market.get("velocity_pct"), comp.get("velocity_pct")
        if mv and cv is not None:
            if cv >= mv:
                s += 1
                ev += f"; catchment velocity {cv}% >= market {mv}%: +1"
            elif cv < 0.75 * mv:
                s -= 1
                ev += f"; catchment velocity {cv}% < 75% of market {mv}%: -1"
            else:
                ev += f"; catchment velocity {cv}% vs market {mv}%"
        add(7, "Competitive landscape", ev,
            "catchment months of inventory <12 9; 12-18 8; 18-24 6; 24-30 5; 30-42 3; >42 2; +/-1 for velocity vs market",
            max(1, min(10, s)), "LF project data")
    else:
        add(7, "Competitive landscape", "no LF projects with coordinates within the radius",
            "catchment months of inventory", None, "LF")

    # 8 Catchment quality - depth of proven demand and the price it pays
    if comp["n"]:
        a = comp["annual_sales"]
        s = _dist_score(-a, [(-1500, 9), (-1000, 8), (-600, 7), (-300, 5), (-100, 3), (999, 2)])
        ev = f"{a:,} units a year absorbed by {comp['n_selling']} selling projects within {comp['radius_km']:g} km"
        mp, cp = market.get("price_psf"), comp.get("absorption_psf")
        if mp and cp:
            d = (cp / mp - 1) * 100
            if d >= 5:
                s += 1
            elif d <= -10:
                s -= 1
            ev += f"; catchment absorption price Rs.{cp:,} vs market Rs.{int(mp):,} ({d:+.1f}%{', +1' if d >= 5 else ', -1' if d <= -10 else ''})"
        add(8, "Catchment quality (demand depth)", ev,
            "annual absorption within radius >=1,500 9; 1,000-1,499 8; 600-999 7; 300-599 5; 100-299 3; <100 2; +1 if catchment price >=5% above market, -1 if >=10% below",
            max(1, min(10, s)), "LF project data")
    else:
        add(8, "Catchment quality (demand depth)", "no LF projects within the radius",
            "annual absorption within radius", None, "LF")

    # totals - one per scenario when connectivity differs by road width
    fixed = [r["score"] for r in rows if r["n"] != 1 and isinstance(r["score"], (int, float))]
    scored_fixed = len(fixed)
    totals = {}
    conn = rows[0]["score"]
    if isinstance(conn, dict):
        for w, s in conn.items():
            out_of = (scored_fixed + 1) * 10
            totals[w] = dict(total=sum(fixed) + s, out_of=out_of, band=_band(sum(fixed) + s, out_of))
    else:
        out_of = scored_fixed * 10
        totals[None] = dict(total=sum(fixed), out_of=out_of, band=_band(sum(fixed), out_of))
    unscored = [r["parameter"] for r in rows if r["score"] is None]
    return dict(rows=rows, totals=totals, unscored=unscored)


def _recent(month_year, qtr_label):
    """True if 'Mon YYYY' falls within 12 months before the data quarter's end."""
    s = str(month_year or "")
    q = re.match(r"Q([1-4])\s*(\d{2})-(\d{2})", str(qtr_label or ""))
    m = re.match(r"([A-Za-z]{3})\w*\s+(\d{4})", s)
    iso = re.match(r"(\d{4})-(\d{2})", s)
    if not q or not (m or iso):
        return False
    if m:
        mons = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
        if m.group(1).lower() not in mons:
            return False
        mon, y = mons.index(m.group(1).lower()) + 1, int(m.group(2))
    else:
        y, mon = int(iso.group(1)), int(iso.group(2))
    qn, fy0 = int(q.group(1)), 2000 + int(q.group(2))
    end_m = {1: 6, 2: 9, 3: 12, 4: 3}[qn]
    end_y = fy0 if qn < 4 else fy0 + 1
    months_before = (end_y - y) * 12 + (end_m - mon)
    return 0 <= months_before < 12


def render_scorecard(sc, radius_km, labels=None):
    L = ["=== COMPUTED SITE SCORECARD (AUTHORITATIVE - REPRODUCE VERBATIM, DO NOT RESCORE) ===",
         "Each score is computed from the evidence shown by the printed rule. Copy the table "
         "and the total exactly. Do not add web findings to a score, do not change a score, and "
         "do not give a score to a row marked NOT SCORED - say what data would score it.",
         "", "| Parameter | Evidence | Rule | Score |", "|---|---|---|---|"]
    for r in sc["rows"]:
        s = r["score"]
        if isinstance(s, dict):
            shown = " / ".join(f"{v}/10 ({w:g} m)" for w, v in s.items())
        elif s is None:
            shown = "NOT SCORED"
        else:
            shown = f"{s}/10"
        L.append(f"| {r['n']}. {r['parameter']} | {r['evidence']} | {r['rule']} | {shown} |")
    L.append("")
    lab = dict(zip(sorted(k for k in sc["totals"] if k is not None), labels or [])) if labels else {}
    for w, t in sc["totals"].items():
        name = (f"Scenario {lab.get(w, '')} ({w:g} m road)" if w is not None else "Site")
        scaled = f" (= {round(t['total'] * 80 / t['out_of'])}/80 equivalent)" if t["out_of"] != 80 and t["out_of"] else ""
        L.append(f"- **{name}: {t['total']}/{t['out_of']}{scaled} - {t['band']}**")
    if sc["unscored"]:
        L.append(f"- Not scored (no evidence in the database, excluded from the total, not penalised): "
                 + ", ".join(sc["unscored"]) + ". Load the OpenStreetMap layer for this city "
                 "(load_poi_city.py) to score them.")
    L.append("- Band table: >=70/80 STRONG | 55-69 MODERATE | 40-54 WEAK | <40 POOR "
             "(pro-rata when a parameter is not scored).")
    L.append("=== END COMPUTED SITE SCORECARD ===")
    return "\n".join(L)


def render_competition(comp, top=15):
    L = [f"=== COMPUTED COMPETITION SET - ALL LF PROJECTS WITHIN {comp['radius_km']:g} KM OF THE PIN ===",
         f"{comp['n']} projects lie within {comp['radius_km']:g} km ({comp['n_selling']} recorded sales in the last year). "
         "Ranked by annual sales. Use THIS list for the benchmark; do not substitute the city top-10 "
         "and do not shorten the radius.", "",
         "| Rank | Project | Builder | Distance | Annual sales | Supply | Sold | Unsold | Velocity | MI | Saleable PSF |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
    for i, p in enumerate(comp["projects"][:top], 1):
        vel = "-" if p["velocity"] is None else f"{p['velocity']:.2f}%"
        mi = "-" if p["months_inventory"] is None else str(int(p["months_inventory"]))
        psf = "-" if not p["saleable_psf"] else f"Rs.{int(p['saleable_psf']):,}"
        L.append(f"| {i} | {p['project']} | {p['builder'] or '-'} | {p['distance_km']:.1f} km | "
                 f"{int(p['annual_sales']):,} | {int(p['total_supply']):,} | {p['sold_pct']:.0f}% | {p['unsold']:,} | "
                 f"{vel} | {mi} | {psf} |")
    if comp["n"] > top:
        L.append(f"| | ... {comp['n'] - top} more within the radius with lower sales | | | | | | | | | |")
    L.append("")
    L.append(f"Catchment totals ({comp['radius_km']:g} km): annual sales {comp['annual_sales']:,} units; "
             f"unsold {comp['unsold']:,} units; months of inventory {comp['months_inventory']}; "
             f"supply-weighted velocity {comp['velocity_pct']}%/month; absorption-weighted saleable price "
             f"Rs.{comp['absorption_psf']:,}." if comp["absorption_psf"] else "")
    L.append("=== END COMPETITION SET ===")
    return "\n".join(L)


# ── Product mix ───────────────────────────────────────────────────────────────
def _wmean(pairs):
    pairs = [(v, w) for v, w in pairs if v and w]
    return sum(v * w for v, w in pairs) / sum(w for _, w in pairs) if pairs else None


def benchmark_configs(typology_rows):
    """Per configuration across the competition set: supply, sold, sizes, rates, tickets."""
    by = {}
    for r in typology_rows or []:
        c = norm_config(r.get("flat"))
        if not c:
            continue
        sup = _num(r.get("supply_units"), 0) or 0
        if sup <= 0:
            continue
        sold = _num(r.get("sold_units"), 0) or 0
        sz = _sizes(r.get("saleable_sizes")) or [x for x in (_num(r.get("min_saleable")), _num(r.get("max_saleable"))) if x]
        cz = _sizes(r.get("carpet_sizes")) or [x for x in (_num(r.get("min_carpet")), _num(r.get("max_carpet"))) if x]
        rate = _num(r.get("absorption_rate")) or _num(r.get("wtavg_rate")) or _num(r.get("avg_rate"))
        if rate is not None and rate <= 0:
            rate = _num(r.get("wtavg_rate")) or _num(r.get("avg_rate"))
        by.setdefault(c, []).append(dict(
            project=r.get("project"), distance_km=_num(r.get("distance_km")), supply=sup, sold=sold,
            sold_pct=sold / sup * 100.0, size=statistics.mean(sz) if sz else None,
            size_min=min(sz) if sz else None, size_max=max(sz) if sz else None,
            carpet=statistics.mean(cz) if cz else None, rate=rate if rate and rate > 0 else None,
            cost_min=_num(r.get("min_cost_lacs")), cost_max=_num(r.get("max_cost_lacs")),
            project_velocity=_num(r.get("project_velocity")), project_annual=_num(r.get("project_annual_sales"))))
    out = {}
    for c, lst in by.items():
        sup = sum(x["supply"] for x in lst)
        sold = sum(x["sold"] for x in lst)
        sold_pct = sold / sup * 100.0 if sup else 0
        # "What sells NOW": projects still selling this configuration (stock
        # left, sales in the last year), at or above the median CURRENT
        # velocity. Life-to-date sold-% was the first criterion tried and it
        # picked sold-out projects launched years ago at lower prices - their
        # rates are history, not the market a launch competes in.
        live = [x for x in lst if x["supply"] - x["sold"] > 0 and (x["project_annual"] or 0) > 0
                and x["project_velocity"] is not None and x["supply"] >= 20]
        if len(live) >= 2:
            med = statistics.median([x["project_velocity"] for x in live])
            best = sorted([x for x in live if x["project_velocity"] >= med],
                          key=lambda x: (-x["project_velocity"], -x["supply"]))
            basis = "current velocity"
        else:
            eligible = [x for x in lst if x["supply"] >= 20] or lst
            med = statistics.median([x["sold_pct"] for x in eligible])
            best = sorted([x for x in eligible if x["sold_pct"] >= med],
                          key=lambda x: (-x["sold_pct"], -x["supply"]))
            basis = "life-to-date sold % (fewer than 2 projects still selling)"
        def med_of(key, src):
            v = [x[key] for x in src if x.get(key)]
            return statistics.median(v) if v else None
        out[c] = dict(
            config=c, projects=len(lst), supply=round(sup), sold=round(sold), unsold=round(sup - sold),
            sold_pct=round(sold_pct, 1),
            size_avg=_wmean([(x["size"], x["supply"]) for x in lst]),
            size_min=min((x["size_min"] for x in lst if x["size_min"]), default=None),
            size_max=max((x["size_max"] for x in lst if x["size_max"]), default=None),
            carpet_avg=_wmean([(x["carpet"], x["supply"]) for x in lst]),
            rate_avg=_wmean([(x["rate"], x["supply"]) for x in lst]),
            cost_min=min((x["cost_min"] for x in lst if x["cost_min"]), default=None),
            cost_max=max((x["cost_max"] for x in lst if x["cost_max"]), default=None),
            best=[x["project"] for x in best[:3]],
            best_sold_pct=[round(x["sold_pct"]) for x in best[:3]],
            best_velocity=[x["project_velocity"] for x in best[:3]], best_basis=basis, best_n=len(best),
            live_projects=len(live),
            best_size=med_of("size", best), best_carpet=med_of("carpet", best),
            best_rate=med_of("rate", best), detail=sorted(lst, key=lambda x: -x["sold_pct"]))
    return out


def _parse_band_lakhs(label):
    """'80 Lac - 1 Crore' -> (80, 100); '<50L' -> (0, 50)."""
    s = str(label or "").lower().replace("lacs", "lac").replace("lakhs", "lac").replace("crores", "crore")
    parts = re.findall(r"(\d+(?:\.\d+)?)\s*(lac|l\b|crore|cr\b)?", s)
    vals = []
    for num, unit in parts:
        v = float(num)
        if unit.startswith("c"):
            v *= 100
        vals.append(v)
    # a unit written only once ("1-1.25 Crore") applies to both ends
    if len(vals) == 2 and "crore" in s and "lac" not in s and vals[0] < vals[1] / 10:
        vals[0] *= 100
    if len(vals) == 1:
        return (0.0, vals[0]) if ("<" in s or "below" in s or "under" in s) else (vals[0], 1e9)
    if len(vals) >= 2:
        return (vals[0], vals[1])
    return None


def recommend_mix(bench, market_flats, ticket_rows=None, *, comp=None, carpet_factor=None,
                  sellout_months=36):
    """The recommended unit mix, sizes and price points, each tied to its benchmark.

    Weights start from MARKET ABSORPTION (LF annual sales by flat type, the
    authoritative demand series), split within a market group by what the
    catchment has actually sold, then tilted by the catchment's competing
    inventory: a configuration whose share of demand exceeds its share of
    unsold stock nearby is under-supplied and gains weight; one carrying more
    than its share of the overhang loses it. Configurations under 3% of market
    demand are not allocated.
    """
    groups = {}
    for r in market_flats or []:
        g = norm_market_group(r.get("flat_type") or r.get("Flat"))
        if not g:
            continue
        a = _num(r.get("annual_sales") if "annual_sales" in r else r.get("Annual Sales (Units)"), 0) or 0
        mi = _num(r.get("months_inv") if "months_inv" in r else r.get("Months Inventory"))
        unsold = _num(r.get("unsold") if "unsold" in r else r.get("Unsold (Units)"), 0)
        vel = _num(r.get("velocity") if "velocity" in r else r.get("Monthly Sales Velocity (%)"))
        psf = _num(r.get("saleable_psf") if "saleable_psf" in r else r.get("Wt Avg Saleable Price"))
        g0 = groups.setdefault(g, dict(annual=0, unsold=0, mi=None, velocity=None, psf=None))
        g0["annual"] += a
        g0["unsold"] += unsold or 0
        g0["mi"], g0["velocity"], g0["psf"] = mi, vel, psf
    tot_dem = sum(g["annual"] for g in groups.values())
    if not tot_dem:
        return None

    configs = [c for c in CONFIG_ORDER if c in bench] or []
    # demand share per configuration
    D = {}
    for c in CONFIG_ORDER:
        g = MARKET_GROUP[c]
        if g not in groups:
            continue
        siblings = [x for x in CONFIG_ORDER if MARKET_GROUP[x] == g and x in bench]
        gshare = groups[g]["annual"] / tot_dem
        if not siblings:
            continue
        sold_tot = sum(bench[x]["sold"] for x in siblings)
        if c in siblings:
            D[c] = gshare * (bench[c]["sold"] / sold_tot if sold_tot else 1.0 / len(siblings))
    uns_tot = sum(bench[c]["unsold"] for c in D) or 1
    rows, weights = {}, {}
    for c, d in D.items():
        g = groups[MARKET_GROUP[c]]
        u = bench[c]["unsold"] / uns_tot
        gap = d / u if u > 0 else 1.5
        tilt = min(1.4, max(0.7, math.sqrt(gap)))
        mi = g["mi"]
        mif = 1.1 if mi is not None and mi <= 18 else 1.0 if mi is None or mi <= 30 else 0.85 if mi <= 48 else 0.7
        include = d >= 0.03
        weights[c] = d * tilt * mif if include else 0.0
        rows[c] = dict(demand_share=d, inventory_share=u, gap=gap, tilt=tilt, mi_factor=mif,
                       market_group=MARKET_GROUP[c], market_annual=g["annual"], market_mi=mi,
                       market_velocity=g["velocity"], include=include)
    wt = sum(weights.values())
    if not wt:
        return None
    # round to 5% steps (largest remainder), minimum 5% for an included config
    raw = {c: w / wt * 100 for c, w in weights.items() if w > 0}
    steps = {c: max(1, int(v // 5)) for c, v in raw.items()}
    while sum(steps.values()) < 20:
        c = max(raw, key=lambda k: raw[k] - steps[k] * 5)
        steps[c] += 1
    while sum(steps.values()) > 20:
        c = max((k for k in steps if steps[k] > 1), key=lambda k: steps[k] * 5 - raw[k])
        steps[c] -= 1
    mix = {c: s * 5 for c, s in steps.items()}

    # sizes and prices from the projects that sell
    out = []
    for c in CONFIG_ORDER:
        if c not in rows:
            continue
        b, r = bench[c], rows[c]
        size = b["best_size"] or b["size_avg"]
        carpet = b["best_carpet"] or b["carpet_avg"] or (size * carpet_factor if size and carpet_factor else None)
        rate = b["best_rate"] or b["rate_avg"]
        size = round(size / 5) * 5 if size else None
        carpet = round(carpet / 5) * 5 if carpet else None
        rate = round(rate / 10) * 10 if rate else None
        ticket = size * rate / 1e5 if size and rate else None
        band = None
        if ticket and ticket_rows:
            for t in ticket_rows:
                rng = _parse_band_lakhs(t.get("band") or t.get("Costrange"))
                if rng and rng[0] <= ticket < rng[1]:
                    band = dict(label=t.get("band") or t.get("Costrange"),
                                annual=_num(t.get("annual_sales") if "annual_sales" in t else t.get("Annual Sales (Units)")),
                                mi=_num(t.get("months_inv") if "months_inv" in t else t.get("Months Inventory")),
                                velocity=_num(t.get("velocity") if "velocity" in t else t.get("Monthly Sales Velocity (%)")))
                    break
        out.append(dict(config=c, mix_pct=mix.get(c, 0), size=size, carpet=carpet, rate=rate,
                        ticket_lakhs=round(ticket, 1) if ticket else None, ticket_band=band,
                        bench=b, **r))
    alloc = [o for o in out if o["mix_pct"] > 0]
    avg_size = sum(o["mix_pct"] * o["size"] for o in alloc if o["size"]) / sum(o["mix_pct"] for o in alloc if o["size"])
    area_w = [(o["rate"], o["mix_pct"] * o["size"]) for o in alloc if o["rate"] and o["size"]]
    mix_psf = sum(r_ * w for r_, w in area_w) / sum(w for _, w in area_w) if area_w else None
    cshare = (comp["annual_sales"] / sum(g["annual"] for g in groups.values())) if comp and comp.get("annual_sales") else None
    return dict(rows=out, avg_size=round(avg_size), mix_psf=round(mix_psf) if mix_psf else None,
                n_projects=len({d["project"] for b in bench.values() for d in b["detail"]}),
                catchment_share=cshare, market_groups=groups, sellout_months=sellout_months)


def allocate_units(mix, saleable_sqft):
    """Units per configuration for a given saleable area (one scenario)."""
    alloc = [o for o in mix["rows"] if o["mix_pct"] > 0 and o["size"]]
    total_units = saleable_sqft / mix["avg_size"]
    res, used = [], 0
    for o in alloc:
        u = int(round(total_units * o["mix_pct"] / 100.0))
        res.append(dict(config=o["config"], units=u, area=u * o["size"],
                        revenue_cr=u * o["size"] * (o["rate"] or 0) / 1e7))
        used += u * o["size"]
    return dict(total_units=sum(r["units"] for r in res), rows=res,
                area_allocated=used, revenue_cr=sum(r["revenue_cr"] for r in res))


def capture_check(mix, alloc):
    """Share of the catchment's monthly demand for each configuration the
    project must win to sell out on schedule."""
    if not mix.get("catchment_share"):
        return []
    out = []
    by = {o["config"]: o for o in mix["rows"]}
    for a in alloc["rows"]:
        o = by[a["config"]]
        g = mix["market_groups"][o["market_group"]]
        sib = [x for x in mix["rows"] if x["market_group"] == o["market_group"]]
        sold_tot = sum(x["bench"]["sold"] for x in sib) or 1
        demand_m = g["annual"] * (o["bench"]["sold"] / sold_tot) * mix["catchment_share"] / 12.0
        need_m = a["units"] / mix["sellout_months"]
        out.append(dict(config=a["config"], need_per_month=round(need_m, 1),
                        catchment_demand_per_month=round(demand_m, 1),
                        capture_pct=round(need_m / demand_m * 100, 0) if demand_m else None))
    return out


def render_mix(mix, bench, comp, scenarios=None):
    """scenarios: list of (label, width_m, saleable_sqft) for unit allocation."""
    L = [f"=== COMPUTED PRODUCT MIX - BENCHMARKED ON {mix['n_projects']} LF PROJECTS WITHIN "
         f"{comp['radius_km']:g} KM (AUTHORITATIVE - REPRODUCE, DO NOT RE-DERIVE) ===",
         "Step 5 of the report IS this block. Present the benchmark table, the recommended mix "
         "with its units, sizes and prices, and the capture check. Name the benchmark projects. "
         "Do not replace these figures with market-level flat-type averages.", "",
         "**A. What the competition offers and how it has sold (LF project typology, life-to-date)**", "",
         "| Configuration | Projects (still selling) | Supply | Sold | Unsold | Saleable size avg (range) | Carpet avg | Rate PSF (supply-wtd) | Ticket range | Fastest current sellers (velocity) |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for c in CONFIG_ORDER:
        if c not in bench:
            continue
        b = bench[c]
        rng = f"{b['size_avg']:,.0f} ({b['size_min']:,.0f}-{b['size_max']:,.0f})" if b["size_avg"] else "-"
        tk = (f"Rs.{b['cost_min']:,.0f}-{b['cost_max']:,.0f} L" if b["cost_min"] and b["cost_max"] else "-")
        if b["best_basis"] == "current velocity":
            best = ", ".join(f"{p} ({v:.1f}%/mo)" for p, v in zip(b["best"], b["best_velocity"]))
        else:
            best = ", ".join(f"{p} ({s}% sold)" for p, s in zip(b["best"], b["best_sold_pct"])) + " [no live sellers]"
        L.append(f"| {c} | {b['projects']} ({b['live_projects']}) | {b['supply']:,} | {b['sold_pct']:.0f}% | {b['unsold']:,} | {rng} | "
                 f"{'-' if not b['carpet_avg'] else format(round(b['carpet_avg']), ',')} | "
                 f"{'-' if not b['rate_avg'] else 'Rs.' + format(round(b['rate_avg']), ',')} | {tk} | {best} |")
    L += ["", "**B. Demand vs competing inventory (market absorption from LF flat-type series; inventory within the radius)**", "",
          "| Configuration | Market group annual sales | Market MI | Demand share | Share of catchment unsold | Gap | Allocated |",
          "|---|---|---|---|---|---|---|"]
    for o in mix["rows"]:
        L.append(f"| {o['config']} | {o['market_group']}: {int(o['market_annual']):,} | "
                 f"{'-' if o['market_mi'] is None else int(o['market_mi'])} | {o['demand_share']*100:.1f}% | "
                 f"{o['inventory_share']*100:.1f}% | {o['gap']:.2f} ({'under-supplied' if o['gap'] > 1.1 else 'over-supplied' if o['gap'] < 0.9 else 'balanced'}) | "
                 f"{o['mix_pct']}%{'' if o['include'] else ' (below 3% of market demand)'} |")
    L += ["", "**C. Recommended mix - sizes and prices set at what the best-selling competitors achieve**", "",
          "| Configuration | Mix (units) | Saleable size | Carpet | Rate PSF | Ticket | Ticket band: market annual sales / MI | Benchmark |",
          "|---|---|---|---|---|---|---|---|"]
    for o in mix["rows"]:
        if not o["mix_pct"]:
            continue
        tb = o["ticket_band"]
        tbs = (f"{tb['label']}: {int(tb['annual'] or 0):,} / {'-' if tb['mi'] is None else int(tb['mi'])}"
               if tb else "-")
        L.append(f"| {o['config']} | {o['mix_pct']}% | {o['size']:,} sq.ft | "
                 f"{'-' if not o['carpet'] else format(o['carpet'], ',') + ' sq.ft'} | "
                 f"{'-' if not o['rate'] else 'Rs.' + format(o['rate'], ',')} | "
                 f"{'-' if not o['ticket_lakhs'] else 'Rs.' + format(o['ticket_lakhs'], ',.1f') + ' L'} | {tbs} | "
                 f"median of {o['bench']['best_n']} fastest sellers"
                 f"{' incl. ' if o['bench']['best_n'] > 3 else ': '}{', '.join(o['bench']['best'])} |")
    L.append("")
    L.append(f"Mix-weighted average unit {mix['avg_size']:,} sq.ft saleable; mix-weighted rate "
             f"Rs.{mix['mix_psf']:,} PSF. These feed the economics as DERIVED inputs.")
    if scenarios:
        for lab, w, sale in scenarios:
            al = allocate_units(mix, sale)
            cap = capture_check(mix, al)
            L += ["", f"**D{'' if lab is None else ' - Scenario ' + lab}{'' if w is None else f' ({w:g} m road)'}: "
                  f"{al['total_units']:,} units on {int(sale):,} sq.ft saleable**", "",
                  "| Configuration | Units | Saleable area | Revenue (Rs.Cr) | Must sell / month | Catchment demand / month | Capture needed |",
                  "|---|---|---|---|---|---|---|"]
            cmap = {c["config"]: c for c in cap}
            for r in al["rows"]:
                c = cmap.get(r["config"], {})
                L.append(f"| {r['config']} | {r['units']:,} | {r['area']:,.0f} | {r['revenue_cr']:,.2f} | "
                         f"{c.get('need_per_month', '-')} | {c.get('catchment_demand_per_month', '-')} | "
                         f"{'-' if c.get('capture_pct') is None else str(int(c['capture_pct'])) + '%'} |")
            L.append(f"| Total | {al['total_units']:,} | {al['area_allocated']:,.0f} | {al['revenue_cr']:,.2f} | | | |")
    L += ["", "Method: demand share = LF market annual sales by flat type (split within a group by what "
          "the catchment has sold); gap = demand share / share of unsold stock within the radius; weight = "
          "demand x sqrt(gap) bounded 0.7-1.4, x0.85 where market MI is 30-48 months and x0.7 above 48; "
          "configurations under 3% of demand are not allocated; rounded to 5%. Sizes and rates are the "
          "median of the competitors still selling that configuration at or above the median current velocity (projects with stock left and sales in the last year). Capture "
          f"needed = units / {mix['sellout_months']} months against the catchment's share of market demand "
          "for that configuration; above 35% is aggressive.",
          "=== END COMPUTED PRODUCT MIX ==="]
    return "\n".join(L)
