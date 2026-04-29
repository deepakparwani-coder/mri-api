"""
build_graph_v3_gurugram.py  (v2-schema-native, multi-city)
──────────────────────────────────────────────────────────────────────────────
Loader for the LF Market data → Neo4j Aura.

Despite the filename, this loader emits the v2 schema convention DIRECTLY
(YEARLY_SNAPSHOT, DEVELOPED_BY, SALEABLE_PRICE_AT, CONSTRUCTION_STAGE_SALES,
DistanceRange, UnitSizeBand, TicketSizeBand, etc.) so the post-load
20-step migration cypher (gurugram_schema_migration.cypher) is no longer
needed for fresh loads. That file is preserved as a historical artifact.

Multi-city: city is configurable via --city. IMMUTABLE_CITIES is auto-derived
at runtime from existing City nodes (every City except the one being loaded),
so adding a new city requires no edits to this file beyond adding an entry
to CITY_EXPECTATIONS and SUB_REGION_RULES if you want region classification.

Performance: all rows for a file are parsed in Python first, then written
via a single UNWIND-batched Cypher call. ~2 min for 108 Gurugram files.

Usage:
  # Load Gurugram (defaults)
  python build_graph_v3_gurugram.py --dir "./NCR NEW BUILD"

  # Load a different city
  python build_graph_v3_gurugram.py --dir "./Mumbai" --city Mumbai \\
        --quarter "Q3 25-26" --fy "FY-2025-2026"

  # Re-load (upsert) an existing city
  python build_graph_v3_gurugram.py --dir "./NCR NEW BUILD" --allow-upsert
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import ServiceUnavailable

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# Defaults — all overridable via CLI / env. Run with --city Mumbai etc.
# IMMUTABLE_CITIES is auto-derived (every City node EXCEPT the one being loaded)
# at runtime in main(), so adding a new city doesn't require editing this file.
# ─────────────────────────────────────────────────────────────────────────────

CITY_NAME = "Gurugram"          # default; CLI --city overrides
SOURCE_LABEL = "NCR"            # default; CLI --source overrides
LATEST_QUARTER = "Q3 25-26"     # default; CLI --quarter overrides
LATEST_FY = "FY-2025-2026"      # default; CLI --fy overrides

# Per-city expected counts. Used only by post-load verification, never by writes.
# Populate when you add a city; missing entries skip the count check (warns instead of fails).
CITY_EXPECTATIONS = {
    "Gurugram": {
        "projects": 71,
        "micromarkets": 37,
        "projects_by_region": {"Gurugram": 62, "Sohna": 8, "Dwarka": 1},
        "mm_by_region": {"Gurugram": 30, "Sohna": 6, "Dwarka": 1},
    },
    # Add Mumbai/Bangalore/etc. here as you onboard them.
}

# Populated at runtime from CITY_EXPECTATIONS[CITY_NAME] for backwards compat
EXPECTED_PROJECTS = None
EXPECTED_MICROMARKETS = None
EXPECTED_BY_REGION = {}
EXPECTED_MM_BY_REGION = {}

# Auto-derived in main() — every City node EXCEPT the one being loaded
IMMUTABLE_CITIES: list[str] = []

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("build_graph_v3_gurugram.log", mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger("gurugram_loader")


# ════════════════════════════════════════════════════════════════════════════
# PARSERS (unchanged from v1)
# ════════════════════════════════════════════════════════════════════════════

def parse_date(raw: Any) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, (int, float)) and 25000 < raw < 80000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=int(raw))).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return None
    if hasattr(raw, "strftime"):
        try:
            return raw.strftime("%Y-%m-%d")
        except ValueError:
            return None
    try:
        parsed = pd.to_datetime(str(raw), errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_range(raw: Any) -> tuple[Optional[float], Optional[float]]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None, None
    s = str(raw).replace(",", "").replace("Rs ", "").replace(" Lacs", "").replace(" ", "").strip()
    if not s or s.lower() in ("nan", "na", "n/a", "none"):
        return None, None
    m = re.match(r"^(-?\d+\.?\d*)-(-?\d+\.?\d*)$", s)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    try:
        v = float(s)
        return v, v
    except ValueError:
        return None, None


def parse_int(raw: Any) -> Optional[int]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        return int(float(str(raw).replace(",", "")))
    except (ValueError, TypeError):
        return None


def parse_float(raw: Any) -> Optional[float]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        return float(str(raw).replace(",", "").replace("%", ""))
    except (ValueError, TypeError):
        return None


def parse_flat_types(raw: Any) -> list[str]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    return [t.strip() for t in str(raw).split(",") if t.strip()]


def classify_sub_region(location: Optional[str]) -> str:
    """
    Classify a MicroMarket name into a coarser sub-region label.
    Rules are city-specific. To add a city, extend SUB_REGION_RULES below.
    Falls back to CITY_NAME if no rule matches (i.e., one big region).
    """
    low = (location or "").lower()
    rules = SUB_REGION_RULES.get(CITY_NAME, [])
    for keyword, label in rules:
        if keyword in low:
            return label
    return CITY_NAME


# Per-city sub-region keyword → label rules. List order = priority.
# To add a city, add an entry. Empty list = whole city is one sub-region.
SUB_REGION_RULES: dict[str, list[tuple[str, str]]] = {
    "Gurugram": [
        ("sohna",    "Sohna"),
        ("dwarka",   "Dwarka"),
        ("gurgaon",  "Gurugram"),
        ("gurugram", "Gurugram"),
    ],
    # Mumbai/Bangalore/etc. — extend here when onboarding.
}


def normalize_rera(raw: Any) -> tuple[Optional[str], str]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None, "Not Available"
    s = str(raw).strip()
    if not s or s.lower() == "not available":
        return None if not s else s, "Not Available"
    if s.upper().startswith("GGM/"):
        return s, "Registered"
    return s, "Not Available"


def read_lf_excel(path: Path) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_excel(path, header=6)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all").reset_index(drop=True)
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        return df
    except Exception as e:
        log.warning(f"Failed to read {path.name}: {e}")
        return None


def clean_props(d: dict) -> dict:
    """Remove keys with None values — keeps MERGE+SET concise."""
    return {k: v for k, v in d.items() if v is not None}


# ════════════════════════════════════════════════════════════════════════════
# CONNECTION & PRE-FLIGHT
# ════════════════════════════════════════════════════════════════════════════

def get_driver() -> Driver:
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not all([uri, user, pwd]):
        log.error("Missing env vars: NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD")
        sys.exit(1)
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        driver.verify_connectivity()
        log.info(f"Neo4j connection verified: {uri}")
        return driver
    except ServiceUnavailable as e:
        log.error(f"Cannot reach Neo4j: {e}")
        sys.exit(1)


def preflight_checks(session: Session, allow_upsert: bool) -> dict:
    # If we're loading "Gurugram", check the legacy mis-named "Gurgaon" is absent
    # (Gurgaon is the old British spelling; we standardized on Gurugram).
    if CITY_NAME == "Gurugram":
        res = session.run("MATCH (c:City {name:'Gurgaon'}) RETURN count(c) AS n").single()
        if res["n"] > 0:
            log.warning(
                "Pre-flight WARNING: 8 legacy 'Gurgaon'-named projects exist (separate from Gurugram). "
                "Loader will proceed; clean those up separately if desired."
            )

    res = session.run("MATCH (c:City {name: $city}) RETURN count(c) AS n",
                      city=CITY_NAME).single()
    if res["n"] > 0:
        if not allow_upsert:
            log.error(f"Pre-flight FAILED: {CITY_NAME} already exists. Pass --allow-upsert to re-load.")
            sys.exit(1)
        log.warning(f"{CITY_NAME} exists — proceeding in upsert mode")
    else:
        log.info(f"✓ {CITY_NAME} city is absent — clean load")

    baseline = {}
    for city in IMMUTABLE_CITIES:
        r = session.run(
            """MATCH (c:City {name:$city})
               OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm:MicroMarket)
               OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p:Project)
               RETURN count(DISTINCT mm) AS mm, count(DISTINCT p) AS p""",
            city=city
        ).single()
        baseline[city] = {"mm": r["mm"], "projects": r["p"]}
        log.info(f"✓ Baseline {city}: {r['mm']} micromarkets, {r['p']} projects")
    return baseline


def create_constraints(session: Session) -> None:
    constraints = [
        # Core entity constraints — city-scoped uniqueness for project + micromarket
        "CREATE CONSTRAINT project_name_city IF NOT EXISTS FOR (p:Project) REQUIRE (p.name, p.city_name) IS UNIQUE",
        "CREATE CONSTRAINT micromarket_name_city IF NOT EXISTS FOR (m:MicroMarket) REQUIRE (m.name, m.city_name) IS UNIQUE",
        "CREATE CONSTRAINT builder_name IF NOT EXISTS FOR (b:Builder) REQUIRE b.name IS UNIQUE",
        "CREATE CONSTRAINT quarter_name IF NOT EXISTS FOR (q:Quarter) REQUIRE q.name IS UNIQUE",
        "CREATE CONSTRAINT fiscal_year_name IF NOT EXISTS FOR (fy:FiscalYear) REQUIRE fy.name IS UNIQUE",
        "CREATE CONSTRAINT flattype_name IF NOT EXISTS FOR (ft:FlatType) REQUIRE ft.name IS UNIQUE",
        "CREATE CONSTRAINT stage_name IF NOT EXISTS FOR (s:ConstructionStage) REQUIRE s.name IS UNIQUE",
        # v2 dimension labels (replaced PriceBand/SizeBand/DistanceBand from v3)
        "CREATE CONSTRAINT priceband_name IF NOT EXISTS FOR (pb:PriceBand) REQUIRE pb.name IS UNIQUE",
        "CREATE CONSTRAINT ticketband_name IF NOT EXISTS FOR (tb:TicketSizeBand) REQUIRE tb.name IS UNIQUE",
        "CREATE CONSTRAINT unitsizeband_name_area IF NOT EXISTS FOR (sb:UnitSizeBand) REQUIRE (sb.name, sb.area_type) IS UNIQUE",
        "CREATE CONSTRAINT distancerange_name IF NOT EXISTS FOR (dr:DistanceRange) REQUIRE dr.name IS UNIQUE",
        "CREATE CONSTRAINT possession_year IF NOT EXISTS FOR (py:PossessionYear) REQUIRE py.year IS UNIQUE",
        "CREATE CONSTRAINT city_name IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE",
    ]
    for c in constraints:
        session.run(c)
    log.info(f"✓ Created/verified {len(constraints)} constraints")


# ════════════════════════════════════════════════════════════════════════════
# EXTRACTORS — parse a file into a list of row dicts. No Neo4j calls here.
# ════════════════════════════════════════════════════════════════════════════

def extract_project_rows(path: Path, extra_keys: list[str] = None) -> list[dict]:
    """Extract project-level rows with all core Project properties + any extras."""
    df = read_lf_excel(path)
    if df is None or "Project Name" not in df.columns:
        return []
    rows = []
    for _, r in df.iterrows():
        name = (r.get("Project Name") or "")
        if pd.isna(name) or not str(name).strip():
            continue
        name = str(name).strip()

        pid = r.get("Project Id")
        pid = str(pid).strip() if pid is not None and not pd.isna(pid) else None
        loc = r.get("Location")
        loc = str(loc).strip() if loc is not None and not pd.isna(loc) else None
        dev = r.get("Developer Name") or r.get("Builder")
        dev = str(dev).strip() if dev is not None and not pd.isna(dev) else None

        sale_rate_min, sale_rate_max = parse_range(r.get("Saleable Rate (Rs/PSF)"))
        carp_rate_min, carp_rate_max = parse_range(r.get("Carpet Rate (Rs/PSF)") or r.get("Carpet Price (Rs/Psf)"))
        sale_size_min, sale_size_max = parse_range(r.get("Saleable Size (Sq.Ft.)"))
        carp_size_min, carp_size_max = parse_range(r.get("Carpet Size (Sq.Ft.)"))
        cost_min, cost_max = parse_range(r.get("Total Cost (Rs.Lacs)"))
        rera_verb, rera_status = normalize_rera(r.get("RERA Registered"))

        row = {
            "name": name,
            "project_id": pid,
            "location": loc,
            "sub_region": classify_sub_region(loc),
            "developer": dev,
            "launch_date": parse_date(r.get("Launch Date") or r.get("Project Startdate")),
            "possession_date": parse_date(r.get("Possession Date") or r.get("Project Enddate")),
            "total_supply_units": parse_int(r.get("Total Supply (Units)")),
            "total_supply_sqft": parse_int(r.get("Total Supply (Sq.Ft.)")),
            "sold_pct": parse_float(r.get("Sold as on Date (%)")),
            "unsold_pct": parse_float(r.get("Unsold as on Date (%)")),
            "project_size": parse_int(r.get("Project Size")),
            "flat_types": parse_flat_types(r.get("Flat Type")),
            "saleable_rate_min_psf": sale_rate_min,
            "saleable_rate_max_psf": sale_rate_max,
            "carpet_rate_min_psf": carp_rate_min,
            "carpet_rate_max_psf": carp_rate_max,
            "saleable_size_min_sqft": sale_size_min,
            "saleable_size_max_sqft": sale_size_max,
            "carpet_size_min_sqft": carp_size_min,
            "carpet_size_max_sqft": carp_size_max,
            "total_cost_min_lacs": cost_min,
            "total_cost_max_lacs": cost_max,
            "monthly_velocity": parse_float(r.get("Monthly Sales Velocity")),
            "annual_months_inv": parse_int(r.get("Annual Months Inventory")),
            "quarterly_months_inv": parse_int(r.get("Quarterly Months Inventory")),
            "rera_registered": rera_verb,
            "rera_status": rera_status,
        }
        # Extras (for Top-10 files' snapshot-specific data)
        for k in (extra_keys or []):
            row[k] = r.get(k)
        rows.append(row)
    return rows


def extract_pivoted_summary(path: Path, time_cols_regex: str, metric_map: dict, float_metrics: set) -> list[dict]:
    """
    Yearly_Marker / Quarterly_Marker are pivoted: rows=metrics, cols=time periods.
    Returns list of {"period": "Q3 25-26", **metric_props}
    """
    try:
        df = pd.read_excel(path, header=6)
        df.columns = [str(c).strip() for c in df.columns]
    except Exception as e:
        log.warning(f"Failed to read {path.name}: {e}")
        return []
    metric_col = df.columns[0]
    time_cols = [c for c in df.columns[1:] if re.match(time_cols_regex, c)]
    rows = []
    for tcol in time_cols:
        props = {"period": tcol}
        for _, r in df.iterrows():
            mname = str(r.get(metric_col) or "").strip()
            prop = metric_map.get(mname)
            if prop:
                val = parse_float(r.get(tcol)) if mname in float_metrics else parse_int(r.get(tcol))
                if val is not None:
                    props[prop] = val
        rows.append(props)
    return rows


# ════════════════════════════════════════════════════════════════════════════
# BATCHED WRITERS — one Cypher call per batch via UNWIND
# ════════════════════════════════════════════════════════════════════════════

class FastLoader:
    def __init__(self, session: Session):
        self.session = session
        # name -> id, to hard-fail on (same name, different id)
        self.project_id_map: dict[str, str] = {}

    # ── Project + MicroMarket + Builder batch writer ────────────────────────

    def write_projects(self, rows: list[dict], file_name: str) -> None:
        if not rows:
            return

        # §7.3 duplicate-name check
        for r in rows:
            pid = r.get("project_id")
            if pid:
                existing = self.project_id_map.get(r["name"])
                if existing and existing != pid:
                    log.error(
                        f"DUPLICATE PROJECT NAME with different IDs: "
                        f"'{r['name']}' has id={existing} and id={pid} (file={file_name}). Halting."
                    )
                    sys.exit(2)
                self.project_id_map[r["name"]] = pid

        # Single Cypher call for ALL rows in this file
        # NOTE: this writes v2-convention property names directly
        # (monthly_velocity not monthly_sales_velocity, sold_pct not sold_percent, etc.)
        # and uses DEVELOPED_BY instead of BUILT_BY. The v3→v2 migration cypher
        # is no longer needed for fresh loads.
        self.session.run(
            """
            UNWIND $rows AS row
            MERGE (c:City {name: $city})
            MERGE (p:Project {name: row.name, city_name: $city})
            SET p.project_id              = coalesce(row.project_id, p.project_id),
                p.launch_date             = coalesce(row.launch_date, p.launch_date),
                p.possession_date         = coalesce(row.possession_date, p.possession_date),
                p.total_supply_units      = coalesce(row.total_supply_units, p.total_supply_units),
                p.total_supply_sqft       = coalesce(row.total_supply_sqft, p.total_supply_sqft),
                p.sold_pct                = coalesce(row.sold_pct, p.sold_pct),
                p.unsold_pct              = coalesce(row.unsold_pct, p.unsold_pct),
                p.project_size            = coalesce(row.project_size, p.project_size),
                p.flat_types              = CASE WHEN size(row.flat_types) > 0 THEN row.flat_types ELSE p.flat_types END,
                p.saleable_rate_min_psf   = coalesce(row.saleable_rate_min_psf, p.saleable_rate_min_psf),
                p.saleable_rate_max_psf   = coalesce(row.saleable_rate_max_psf, p.saleable_rate_max_psf),
                p.carpet_rate_min_psf     = coalesce(row.carpet_rate_min_psf, p.carpet_rate_min_psf),
                p.carpet_rate_max_psf     = coalesce(row.carpet_rate_max_psf, p.carpet_rate_max_psf),
                p.saleable_size_min_sqft  = coalesce(row.saleable_size_min_sqft, p.saleable_size_min_sqft),
                p.saleable_size_max_sqft  = coalesce(row.saleable_size_max_sqft, p.saleable_size_max_sqft),
                p.carpet_size_min_sqft    = coalesce(row.carpet_size_min_sqft, p.carpet_size_min_sqft),
                p.carpet_size_max_sqft    = coalesce(row.carpet_size_max_sqft, p.carpet_size_max_sqft),
                p.total_cost_min_lacs     = coalesce(row.total_cost_min_lacs, p.total_cost_min_lacs),
                p.total_cost_max_lacs     = coalesce(row.total_cost_max_lacs, p.total_cost_max_lacs),
                p.monthly_velocity        = coalesce(row.monthly_velocity, p.monthly_velocity),
                p.annual_months_inv       = coalesce(row.annual_months_inv, p.annual_months_inv),
                p.quarterly_months_inv    = coalesce(row.quarterly_months_inv, p.quarterly_months_inv),
                p.rera_registered         = coalesce(row.rera_registered, p.rera_registered),
                p.rera_status             = coalesce(row.rera_status, p.rera_status)

            // Micromarket
            FOREACH (_ IN CASE WHEN row.location IS NULL THEN [] ELSE [1] END |
              MERGE (m:MicroMarket {name: row.location, city_name: $city})
                ON CREATE SET m.sub_region = row.sub_region
                ON MATCH  SET m.sub_region = row.sub_region
              MERGE (c)-[:HAS_MICROMARKET]->(m)
              MERGE (p)-[:IN_MICROMARKET]->(m)
              MERGE (m)-[:HAS_PROJECT]->(p)
            )

            // Builder — v2 uses DEVELOPED_BY (was BUILT_BY in v3)
            FOREACH (_ IN CASE WHEN row.developer IS NULL THEN [] ELSE [1] END |
              MERGE (b:Builder {name: row.developer})
              MERGE (p)-[:DEVELOPED_BY]->(b)
            )
            """,
            rows=rows, city=CITY_NAME
        )

    # ── Top-10 project snapshot relationship ────────────────────────────────

    def write_top10_rel(self, rows: list[dict]) -> None:
        if not rows:
            return
        # Ensure latest quarter exists
        self.write_quarter_batch([LATEST_QUARTER])
        payload = []
        for r in rows:
            payload.append({
                "name": r["name"],
                "annual_sales_units": r.get("annual_sales_units"),
                "annual_sales_sqft": r.get("annual_sales_sqft"),
                "annual_value_cr": r.get("annual_value_cr"),
                "sales_units": r.get("sales_units"),
                "monthly_velocity": r.get("monthly_velocity"),
                "annual_months_inv": r.get("annual_months_inv"),
                "quarterly_months_inv": r.get("quarterly_months_inv"),
                "sold_pct": r.get("sold_pct"),
                "unsold_pct": r.get("unsold_pct"),
            })
        self.session.run(
            """
            UNWIND $rows AS row
            MATCH (p:Project {name: row.name, city_name: $city})
            MATCH (q:Quarter {name: $qn})
            MERGE (p)-[r:PROJECT_TOP_SALES]->(q)
            SET r.annual_sales_units    = coalesce(row.annual_sales_units, r.annual_sales_units),
                r.annual_sales_sqft     = coalesce(row.annual_sales_sqft, r.annual_sales_sqft),
                r.annual_value_cr       = coalesce(row.annual_value_cr, r.annual_value_cr),
                r.sales_units           = coalesce(row.sales_units, r.sales_units),
                r.monthly_velocity      = coalesce(row.monthly_velocity, r.monthly_velocity),
                r.annual_months_inv     = coalesce(row.annual_months_inv, r.annual_months_inv),
                r.quarterly_months_inv  = coalesce(row.quarterly_months_inv, r.quarterly_months_inv),
                r.sold_pct              = coalesce(row.sold_pct, r.sold_pct),
                r.unsold_pct            = coalesce(row.unsold_pct, r.unsold_pct)
            """,
            rows=payload, city=CITY_NAME, qn=LATEST_QUARTER
        )

    # ── New launch relationship ─────────────────────────────────────────────

    def write_new_launch_rel(self, rows: list[dict]) -> None:
        if not rows:
            return
        self.session.run(
            """
            UNWIND $rows AS row
            MATCH (p:Project {name: row.name, city_name: $city})
            MATCH (c:City {name: $city})
            MERGE (p)-[r:NEW_LAUNCH]->(c)
            SET r.launch_date = coalesce(row.launch_date, r.launch_date),
                r.end_date    = coalesce(row.possession_date, r.end_date),
                p.status      = 'NEW_LAUNCH'
            """,
            rows=rows, city=CITY_NAME
        )

    # ── Time nodes (Quarter / FiscalYear) ───────────────────────────────────

    def write_quarter_batch(self, names: list[str]) -> None:
        payload = []
        for n in names:
            m = re.match(r"Q(\d)\s+(\d{2})-(\d{2})", n.strip())
            if m:
                q = int(m.group(1)); y1 = int(m.group(2)); y2 = int(m.group(3))
                payload.append({"name": n, "fy_short": f"FY{y1}-{y2}", "quarter_num": q,
                                "sort_order": (2000 + y1) * 10 + q})
            else:
                payload.append({"name": n, "fy_short": None, "quarter_num": None, "sort_order": None})
        if not payload:
            return
        self.session.run(
            """
            UNWIND $rows AS row
            MERGE (q:Quarter {name: row.name})
              ON CREATE SET q.fy_short = row.fy_short, q.quarter_num = row.quarter_num, q.sort_order = row.sort_order
              ON MATCH  SET q.fy_short = coalesce(row.fy_short, q.fy_short),
                            q.quarter_num = coalesce(row.quarter_num, q.quarter_num),
                            q.sort_order = coalesce(row.sort_order, q.sort_order)
            """,
            rows=payload
        )

    def write_fy_batch(self, names: list[str]) -> None:
        payload = []
        for n in names:
            m = re.match(r"FY-?(\d{4})-(\d{4})", n)
            if m:
                end_year = int(m.group(2))
                payload.append({
                    "name": n,
                    "start_year": int(m.group(1)),
                    "end_year": end_year,
                    "sort_order": end_year,  # v2 queries sort by this
                })
            else:
                payload.append({"name": n, "start_year": None, "end_year": None, "sort_order": None})
        if not payload:
            return
        self.session.run(
            """
            UNWIND $rows AS row
            MERGE (fy:FiscalYear {name: row.name})
              ON CREATE SET fy.start_year = row.start_year,
                            fy.end_year   = row.end_year,
                            fy.sort_order = row.sort_order
              ON MATCH  SET fy.sort_order = coalesce(fy.sort_order, row.sort_order)
            """,
            rows=payload
        )

    # ── City-level time-series (ANNUAL_SNAPSHOT, MARKET_SNAPSHOT) ───────────

    def write_city_to_time_rel(self, rows: list[dict], rel_type: str, time_label: str) -> None:
        """rows: each has 'period' (name of Quarter/FY) + metric props."""
        if not rows:
            return
        # Ensure time nodes exist
        periods = [r["period"] for r in rows]
        if time_label == "Quarter":
            self.write_quarter_batch(periods)
        else:
            self.write_fy_batch(periods)

        # Build dynamic SET clause based on all observed prop keys across rows
        all_keys = set()
        for r in rows:
            all_keys.update(k for k in r.keys() if k != "period")
        set_clause = ",\n".join(f"r.{k} = coalesce(row.{k}, r.{k})" for k in sorted(all_keys))
        if not set_clause:
            return

        cypher = f"""
            UNWIND $rows AS row
            MATCH (c:City {{name: $city}})
            MATCH (t:{time_label} {{name: row.period}})
            MERGE (c)-[r:{rel_type}]->(t)
            SET {set_clause}
        """
        self.session.run(cypher, rows=rows, city=CITY_NAME)

    # ── Simple dim-node writers (FlatType, Stage, PriceBand, SizeBand, ...) ─

    def write_dim_rel(self, rows: list[dict], dim_label: str, dim_match_props: list[str],
                      rel_type: str, extra_merge_props: dict = None) -> None:
        """
        rows: each has the dim-match props + metric props.
        dim_match_props: e.g. ['name'] for FlatType, ['name','basis'] for PriceBand.
        """
        if not rows:
            return

        # First MERGE the dim nodes
        dim_match_cypher = ", ".join(f"{k}: row.{k}" for k in dim_match_props)
        self.session.run(
            f"UNWIND $rows AS row MERGE (d:{dim_label} {{{dim_match_cypher}}})",
            rows=rows
        )

        # Collect metric property keys
        all_keys = set()
        for r in rows:
            all_keys.update(k for k in r.keys() if k not in dim_match_props)
        set_clause = ",\n".join(f"r.{k} = coalesce(row.{k}, r.{k})" for k in sorted(all_keys))
        if not set_clause:
            return

        cypher = f"""
            UNWIND $rows AS row
            MATCH (c:City {{name: $city}})
            MATCH (d:{dim_label} {{{dim_match_cypher}}})
            MERGE (c)-[r:{rel_type}]->(d)
            SET {set_clause}
        """
        self.session.run(cypher, rows=rows, city=CITY_NAME)

    def merge_city(self) -> None:
        self.session.run(
            """
            MERGE (c:City {name: $name})
            SET c.source_label = $src,
                c.latest_quarter = $lq,
                c.latest_fiscal_year = $lfy,
                c.last_refreshed = date()
            """,
            name=CITY_NAME, src=SOURCE_LABEL, lq=LATEST_QUARTER, lfy=LATEST_FY
        )

    def finalize_city_counts(self) -> dict:
        counts = self.session.run(
            """MATCH (c:City {name: $city})
               OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm)
               OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p)
               RETURN count(DISTINCT mm) AS mmc, count(DISTINCT p) AS pc""",
            city=CITY_NAME
        ).single()
        self.session.run(
            """MATCH (c:City {name: $city})
               SET c.project_count=$pc, c.micromarket_count=$mmc, c.last_refreshed=date()""",
            city=CITY_NAME, pc=counts["pc"], mmc=counts["mmc"]
        )
        return {"projects": counts["pc"], "micromarkets": counts["mmc"]}

    # ── Post-load denormalization (migration steps 11, 13, 14) ──────────────
    # Must run AFTER all files are loaded. Denormalizes data from rels onto
    # Project nodes so downstream queries can read p.location, p.saleable_rate_psf,
    # p.annual_sales_units etc. directly without traversing.

    def post_load_denormalize(self) -> dict:
        log.info("─" * 70)
        log.info("POST-LOAD DENORMALIZATION")
        log.info("─" * 70)
        stats = {}

        # Step 11 equivalent: copy PROJECT_TOP_SALES{latest_quarter} props onto Project
        r = self.session.run(
            """
            MATCH (p:Project {city_name: $city})-[r:PROJECT_TOP_SALES]->(q:Quarter {name: $qn})
            SET p.annual_sales_units    = coalesce(r.annual_sales_units, p.annual_sales_units),
                p.annual_sales_sqft     = coalesce(r.annual_sales_sqft, p.annual_sales_sqft),
                p.annual_value_cr       = coalesce(r.annual_value_cr, p.annual_value_cr),
                p.monthly_velocity      = coalesce(r.monthly_velocity, p.monthly_velocity),
                p.annual_months_inv     = coalesce(r.annual_months_inv, p.annual_months_inv),
                p.quarterly_months_inv  = coalesce(r.quarterly_months_inv, p.quarterly_months_inv),
                p.sold_pct              = coalesce(r.sold_pct, p.sold_pct),
                p.unsold_pct            = coalesce(r.unsold_pct, p.unsold_pct)
            RETURN count(p) AS n
            """,
            city=CITY_NAME, qn=LATEST_QUARTER
        ).single()
        stats["top_sales_denormalized"] = r["n"]
        log.info(f"  Top-sales denormalized onto Project: {r['n']}")

        # Step 13: build *_range strings + *_psf single values from min/max pairs
        r = self.session.run(
            """
            MATCH (p:Project {city_name: $city})
            SET p.saleable_rate_range = CASE
                  WHEN p.saleable_rate_min_psf IS NULL THEN NULL
                  WHEN p.saleable_rate_min_psf = p.saleable_rate_max_psf THEN toString(toInteger(p.saleable_rate_min_psf))
                  ELSE toString(toInteger(p.saleable_rate_min_psf)) + '-' + toString(toInteger(p.saleable_rate_max_psf))
                END,
                p.saleable_rate_psf = CASE
                  WHEN p.saleable_rate_min_psf IS NULL THEN NULL
                  ELSE toInteger((p.saleable_rate_min_psf + p.saleable_rate_max_psf) / 2)
                END,
                p.carpet_rate_range = CASE
                  WHEN p.carpet_rate_min_psf IS NULL THEN NULL
                  WHEN p.carpet_rate_min_psf = p.carpet_rate_max_psf THEN toString(toInteger(p.carpet_rate_min_psf))
                  ELSE toString(toInteger(p.carpet_rate_min_psf)) + '-' + toString(toInteger(p.carpet_rate_max_psf))
                END,
                p.carpet_rate_psf = CASE
                  WHEN p.carpet_rate_min_psf IS NULL THEN NULL
                  ELSE toInteger((p.carpet_rate_min_psf + p.carpet_rate_max_psf) / 2)
                END,
                p.saleable_size_range = CASE
                  WHEN p.saleable_size_min_sqft IS NULL THEN NULL
                  WHEN p.saleable_size_min_sqft = p.saleable_size_max_sqft THEN toString(toInteger(p.saleable_size_min_sqft))
                  ELSE toString(toInteger(p.saleable_size_min_sqft)) + '-' + toString(toInteger(p.saleable_size_max_sqft))
                END,
                p.carpet_size_range = CASE
                  WHEN p.carpet_size_min_sqft IS NULL THEN NULL
                  WHEN p.carpet_size_min_sqft = p.carpet_size_max_sqft THEN toString(toInteger(p.carpet_size_min_sqft))
                  ELSE toString(toInteger(p.carpet_size_min_sqft)) + '-' + toString(toInteger(p.carpet_size_max_sqft))
                END,
                p.total_cost_range = CASE
                  WHEN p.total_cost_min_lacs IS NULL THEN NULL
                  WHEN p.total_cost_min_lacs = p.total_cost_max_lacs THEN toString(p.total_cost_min_lacs)
                  ELSE toString(p.total_cost_min_lacs) + '-' + toString(p.total_cost_max_lacs)
                END
            RETURN count(p) AS n
            """,
            city=CITY_NAME
        ).single()
        stats["range_strings_built"] = r["n"]
        log.info(f"  Range strings + PSF medians built: {r['n']}")

        # Step 14: copy MicroMarket name onto p.location (for old queries that read p.location)
        r = self.session.run(
            """
            MATCH (p:Project {city_name: $city})-[:IN_MICROMARKET]->(m:MicroMarket)
            SET p.location = m.name
            RETURN count(p) AS n
            """,
            city=CITY_NAME
        ).single()
        stats["locations_denormalized"] = r["n"]
        log.info(f"  Locations denormalized onto Project: {r['n']}")

        return stats


# ════════════════════════════════════════════════════════════════════════════
# FILE HANDLERS — parse a file, call one write method
# ════════════════════════════════════════════════════════════════════════════

# Metric maps for pivoted marker files
MARKER_METRIC_MAP = {
    # v2 convention property names emitted directly (was renamed in migration step 1+2)
    "Marketable Supply (Unit)": "supply_units",
    "Marketable Supply (mn Sq.Ft)": "supply_sqft",
    "Sales (Units)": "sales_units",
    "Sales (mn Sq.Ft)": "sales_sqft",
    "Value of Stock Sold (Rs.Cr)": "value_sold_cr",
    "Unsold Stock (Units)": "unsold_units",
    "Unsold Stock (mn Sq.Ft)": "unsold_sqft",
    "Unsold Stock Value (Rs.Cr)": "unsold_value_cr",
    "Cost of Flat (Rs.lac)": "cost_of_flat_lacs",
    "Sales Velocity (%)": "velocity_pct",
    "Months Inventory": "months_inv",
    "Newsupply (Units)": "new_supply_units",
    "Newsupply (mn Sq.Ft)": "new_supply_sqft",
}
MARKER_FLOAT_METRICS = {
    "Marketable Supply (mn Sq.Ft)", "Sales (mn Sq.Ft)", "Value of Stock Sold (Rs.Cr)",
    "Unsold Stock (mn Sq.Ft)", "Unsold Stock Value (Rs.Cr)", "Cost of Flat (Rs.lac)",
    "Sales Velocity (%)", "Newsupply (mn Sq.Ft)",
}

# Column → property maps for band files
BAND_COL_MAP = {
    "Annual Sales (Units)": "annual_sales_units",
    "Annual Sales (Sq.Ft.)": "annual_sales_sqft",
    "Sales (Units)": "sales_units",
    "Sales (Sq.Ft.)": "sales_sqft",
    "Unsold (Units)": "unsold_units",
    "Unsold (Sq.Ft.)": "unsold_sqft",
    "Annual Marketable Supply (Units)": "annual_marketable_supply_units",
    "Annual Marketable Supply (Sq.Ft.)": "annual_marketable_supply_sqft",
    "Quarterly Marketable Supply (Units)": "quarterly_marketable_supply_units",
    "Quarterly Marketable Supply (Sq.Ft.)": "quarterly_marketable_supply_sqft",
    "Total Supply (Units)": "total_supply_units",
    "Total Supply (Sq.Ft.)": "total_supply_sqft",
    "Wt Avg Saleable Area Price (Rs/PSF)": "wt_avg_saleable_price_psf",
    "Wt Avg Carpet Area Price (Rs/PSF)": "wt_avg_carpet_price_psf",
    "Annual Months Inventory": "annual_months_inventory",
    "Quarterly Months Inventory": "quarterly_months_inventory",
    "Monthly Months Inventory": "monthly_months_inventory",
    "Monthly Sales Velocity (%)": "monthly_sales_velocity_pct",
    "Product Efficiency (%)": "product_efficiency_pct",
    "Min Size": "min_size",
    "Max Size": "max_size",
    "Min Cost (Rs.Lacs)": "min_cost_lacs",
    "Max Cost (Rs.Lacs)": "max_cost_lacs",
}

FLAT_PERF_COL_MAP = {
    "Saleable Min Size": "saleable_min_size",
    "Saleable Max Size": "saleable_max_size",
    "Carpet Min Size": "carpet_min_size",
    "Carpet Max Size": "carpet_max_size",
    "Min Cost(Rs.Lacs)": "min_cost_lacs",
    "Max Cost(Rs.Lacs)": "max_cost_lacs",
    "Annual Sales (Units)": "annual_sales_units",
    "Annual Sales (Sq.Ft.)": "annual_sales_sqft",
    "Unsold (Units)": "unsold_units",
    "Unsold (Sq.Ft.)": "unsold_sqft",
    "Sales (Units)": "sales_units",
    "Sales (Sq.Ft.)": "sales_sqft",
    "Annual Marketable Supply (Units)": "annual_marketable_supply_units",
    "Annual Marketable Supply (Sq.Ft.)": "annual_marketable_supply_sqft",
    "Quarterly Marketable Supply (Units)": "quarterly_marketable_supply_units",
    "Quarterly Marketable Supply (Sq.Ft.)": "quarterly_marketable_supply_sqft",
    "Total Supply (Units)": "total_supply_units",
    "Total Supply (Sq.Ft.)": "total_supply_sqft",
    "Wt Avg Saleable Area Price (Rs/PSF)": "wt_avg_saleable_price_psf",
    "Wt Avg Carpet Area Price (Rs/PSF)": "wt_avg_carpet_price_psf",
    "Annual Months Inventory": "annual_months_inventory",
    "Quarterly Months Inventory": "quarterly_months_inventory",
    "Monthly Sales Velocity (%)": "monthly_sales_velocity_pct",
    "Product Efficiency (%)": "product_efficiency_pct",
}


def handle_comparables(path: Path, fl: FastLoader) -> None:
    rows = extract_project_rows(path)
    fl.write_projects(rows, path.name)


def handle_top10(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Project Name" not in df.columns:
        return
    project_rows = extract_project_rows(path)
    fl.write_projects(project_rows, path.name)

    # Collect snapshot-specific data for PROJECT_TOP_SALES
    snapshot_rows = []
    for _, r in df.iterrows():
        name = r.get("Project Name")
        if pd.isna(name) or not str(name).strip():
            continue
        snapshot_rows.append({
            "name": str(name).strip(),
            "annual_sales_units": parse_int(r.get("Annual Sales (Units)")),
            "annual_sales_sqft": parse_int(r.get("Annual Sales (Sq.Ft.)")),
            "annual_value_cr": parse_float(r.get("Annual Value of Sales (Rs.Cr.)")),
            "sales_units": parse_int(r.get("Sales (Units)")),
            "monthly_velocity": parse_float(r.get("Monthly Sales Velocity")),
            "annual_months_inv": parse_int(r.get("Annual Months Inventory")),
            "quarterly_months_inv": parse_int(r.get("Quarterly Months Inventory")),
            "sold_pct": parse_float(r.get("Sold as on Date (%)")),
            "unsold_pct": parse_float(r.get("Unsold as on Date (%)")),
        })
    fl.write_top10_rel(snapshot_rows)


def handle_new_launch(path: Path, fl: FastLoader) -> None:
    project_rows = extract_project_rows(path)
    fl.write_projects(project_rows, path.name)
    # Emit NEW_LAUNCH for each
    launch_rows = [{"name": r["name"], "launch_date": r["launch_date"],
                    "possession_date": r["possession_date"]} for r in project_rows]
    fl.write_new_launch_rel(launch_rows)


def handle_yearly_summary(path: Path, fl: FastLoader) -> None:
    rows = extract_pivoted_summary(path, r"FY-\d{4}-\d{4}", MARKER_METRIC_MAP, MARKER_FLOAT_METRICS)
    fl.write_city_to_time_rel(rows, "YEARLY_SNAPSHOT", "FiscalYear")


def handle_quarterly_summary(path: Path, fl: FastLoader) -> None:
    rows = extract_pivoted_summary(path, r"Q\d\s+\d{2}-\d{2}", MARKER_METRIC_MAP, MARKER_FLOAT_METRICS)
    fl.write_city_to_time_rel(rows, "MARKET_SNAPSHOT", "Quarter")


def handle_price_timeseries(path: Path, fl: FastLoader, rel_type: str, prefix: str) -> None:
    df = read_lf_excel(path)
    if df is None or "Financial Quarter" not in df.columns:
        return
    # v2-convention property names: minimum/maximum/new_supply_price
    stat_map = {
        f"Wt Avg {prefix} Price": "wt_avg",
        f"Absorption {prefix} Price": "absorption",
        f"Average {prefix} Price": "average",
        f"Median {prefix} Price": "median",
        f"Minimum {prefix} Price": "minimum",
        f"Maximum {prefix} Price": "maximum",
        "Wt Avg New Supply Price": "new_supply_price",
    }
    rows = []
    for _, r in df.iterrows():
        q = str(r.get("Financial Quarter") or "").strip()
        if not q:
            continue
        row = {"period": q}
        for xl, prop in stat_map.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_city_to_time_rel(rows, rel_type, "Quarter")


def handle_months_inventory(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Financial Quarter" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        q = str(r.get("Financial Quarter") or "").strip()
        if not q:
            continue
        rows.append({"period": q, "months_inv": parse_int(r.get("Months Inventory"))})
    fl.write_city_to_time_rel(rows, "MONTHLY_INVENTORY", "Quarter")


def handle_sales_velocity(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Financial Quarter" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        q = str(r.get("Financial Quarter") or "").strip()
        if not q:
            continue
        rows.append({"period": q, "velocity_pct": parse_float(r.get("Sales Velocity (Monthly)"))})
    fl.write_city_to_time_rel(rows, "SALES_VELOCITY", "Quarter")


def handle_quarterly_sales_supply(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Financial Quarter" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        q = str(r.get("Financial Quarter") or "").strip()
        if not q:
            continue
        rows.append({
            "period": q,
            "sales_units": parse_int(r.get("Quarterly Sales (Units)")),
            "sales_sqft": parse_float(r.get("Quarterly Sales (mn Sq.Ft)")),
            "supply_units": parse_int(r.get("Marketable Supply (Units)")),
            "supply_sqft": parse_float(r.get("Marketable Supply (mn Sq.Ft)")),
        })
    fl.write_city_to_time_rel(rows, "QUARTERLY_TREND", "Quarter")


def handle_flat_simple(path: Path, fl: FastLoader, rel_type: str,
                       col_units: str, col_sqft: str, prop_units: str, prop_sqft: str) -> None:
    df = read_lf_excel(path)
    if df is None or "Flat" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        ft = str(r.get("Flat") or "").strip()
        if not ft:
            continue
        row = {"name": ft}
        u = parse_int(r.get(col_units))
        s = parse_int(r.get(col_sqft))
        if u is not None: row[prop_units] = u
        if s is not None: row[prop_sqft] = s
        rows.append(row)
    fl.write_dim_rel(rows, "FlatType", ["name"], rel_type)


def handle_flat_performance(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Flat" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        ft = str(r.get("Flat") or "").strip()
        if not ft:
            continue
        row = {"name": ft}
        for xl, prop in FLAT_PERF_COL_MAP.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_dim_rel(rows, "FlatType", ["name"], "FLAT_PERFORMANCE")


def handle_stage_simple(path: Path, fl: FastLoader, rel_type: str,
                        col_units: str, col_sqft: str, prop_units: str, prop_sqft: str) -> None:
    df = read_lf_excel(path)
    if df is None or "Progress" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        st = str(r.get("Progress") or "").strip()
        if not st:
            continue
        row = {"name": st}
        u = parse_int(r.get(col_units))
        s = parse_int(r.get(col_sqft))
        if u is not None: row[prop_units] = u
        if s is not None: row[prop_sqft] = s
        rows.append(row)
    fl.write_dim_rel(rows, "ConstructionStage", ["name"], rel_type)


def handle_price_band(path: Path, fl: FastLoader, basis: str) -> None:
    df = read_lf_excel(path)
    if df is None:
        return
    range_col = "carpet Price Range" if basis == "carpet" else "saleable Price Range"
    if range_col not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        band = str(r.get(range_col) or "").strip()
        if not band:
            continue
        row = {"name": band, "basis": basis}
        for xl, prop in BAND_COL_MAP.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_dim_rel(rows, "PriceBand", ["name", "basis"], "PRICE_BAND_PERFORMANCE")


def handle_size_band(path: Path, fl: FastLoader, basis: str) -> None:
    df = read_lf_excel(path)
    if df is None:
        return
    range_col = "carpet Size Range" if basis == "carpet" else "saleable Size Range"
    if range_col not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        band = str(r.get(range_col) or "").strip()
        if not band:
            continue
        # v2 schema: UnitSizeBand has 'name', 'range', 'area_type'
        # (replaces SizeBand with 'name'+'basis')
        row = {"name": band, "range": band, "area_type": basis}
        for xl, prop in BAND_COL_MAP.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_dim_rel(rows, "UnitSizeBand", ["name", "area_type"], "UNIT_SIZE_PERFORMANCE")


def handle_ticket_band(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Costrange" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        band = str(r.get("Costrange") or "").strip()
        if not band:
            continue
        # v2 schema: TicketSizeBand has 'name'+'range' (replaces PriceBand{basis:'ticket'})
        row = {"name": band, "range": band}
        for xl, prop in BAND_COL_MAP.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_dim_rel(rows, "TicketSizeBand", ["name"], "TICKET_SIZE_PERFORMANCE")


def handle_distance_band(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Distance Range" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        band = str(r.get("Distance Range") or "").strip()
        if not band:
            continue
        # v2 schema: DistanceRange has 'name'+'range' (replaces DistanceBand)
        row = {"name": band, "range": band}
        for xl, prop in BAND_COL_MAP.items():
            if xl in df.columns:
                v = parse_float(r.get(xl))
                if v is not None:
                    row[prop] = v
        rows.append(row)
    fl.write_dim_rel(rows, "DistanceRange", ["name"], "DISTANCE_PERFORMANCE")


def handle_possession(path: Path, fl: FastLoader) -> None:
    df = read_lf_excel(path)
    if df is None or "Possession Year" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        y = parse_int(r.get("Possession Year"))
        if y is None:
            continue
        # v2 prop names that match cypher_queries.py expectations
        row = {"year": y,
               "marketable_supply_units": parse_int(r.get("Marketable Supply (Units)")),
               "marketable_supply_sqft": parse_float(r.get("Marketable Supply (mn Sq.Ft)")),
               "sales_units": parse_int(r.get("Sales (Units)")),
               "sales_sqft": parse_float(r.get("Sales (mn Sq.Ft)"))}
        rows.append(row)
    fl.write_dim_rel(rows, "PossessionYear", ["year"], "POSSESSION_DISTRIBUTION")


# ════════════════════════════════════════════════════════════════════════════
# ROUTING
# ════════════════════════════════════════════════════════════════════════════

FILE_ORDER_PRIORITY = [
    "List_of_Comparables_Projects",
    "Top_10_Project_Data_(ANNUALSALES)",
    "New_Launch_Project_Details",
    "Yearly_Marker_Summary",
    "Quarterly_Marker_Summary",
    "Carpet_Area_Price_(Rs_PSF)_Data",
    "Saleable_Area_Price_(Rs_PSF)_Data",
    "Months_Inventory_(Months)_Data",
    "Sales_Velocity_(%_Monthly_Sales)_Data",
    "Quarterly_Sales_&_Marketable_Supply_Data",
    "Quarterly_Sales_Data_as_per_Construction_Stage",
    "Annual_Sales_Data_as_per_Construction_Stage",
    "Unsold_Stock_Data_as_per_Construction_Stage",
    "Quarterly_Sales_Data",
    "Annual_Sales_Data",
    "Unsold_Stock_Data",
    "Flat_Type_Analysis_Data",
    "Price_Range_Analysis_(carpet_area_price)_Data",
    "Price_Range_Analysis_(saleable_area_price)_Data",
    "Unit_Size_Range_Analysis_(as_per_carpet_area)_Data",
    "Unit_Size_Range_Analysis_(as_per_saleable_area)_Data",
    "Unit_Ticket_Size_Analysis_Data",
    "Distance_Range_Analysis_Data",
    "Possession_Wise_Marketable_Supply",
]


def sort_key(path: Path) -> tuple[int, str]:
    name = path.name
    for idx, prefix in enumerate(FILE_ORDER_PRIORITY):
        if name.startswith(prefix):
            return (idx, name)
    return (999, name)


def route_file(path: Path, fl: FastLoader) -> None:
    name = path.name
    if name.startswith("List_of_Comparables_Projects"):
        handle_comparables(path, fl)
    elif name.startswith("Top_10_Project_Data_(ANNUALSALES)"):
        handle_top10(path, fl)
    elif name.startswith("New_Launch_Project_Details"):
        handle_new_launch(path, fl)
    elif name == "Yearly_Marker_Summary.xlsx":
        handle_yearly_summary(path, fl)
    elif name.startswith("Quarterly_Marker_Summary"):
        handle_quarterly_summary(path, fl)
    elif name.startswith("Carpet_Area_Price_(Rs_PSF)_Data"):
        handle_price_timeseries(path, fl, "CARPET_PRICE_AT", "Carpet")
    elif name.startswith("Saleable_Area_Price_(Rs_PSF)_Data"):
        handle_price_timeseries(path, fl, "SALEABLE_PRICE_AT", "Saleable")
    elif name.startswith("Months_Inventory_(Months)_Data"):
        handle_months_inventory(path, fl)
    elif name.startswith("Sales_Velocity_(%_Monthly_Sales)_Data"):
        handle_sales_velocity(path, fl)
    elif name.startswith("Quarterly_Sales_&_Marketable_Supply_Data"):
        handle_quarterly_sales_supply(path, fl)
    elif name.startswith("Quarterly_Sales_Data_as_per_Construction_Stage"):
        # v2: all three stage files write to ONE rel CONSTRUCTION_STAGE_SALES
        # with distinct prop sets that MERGE/coalesce together
        handle_stage_simple(path, fl, "CONSTRUCTION_STAGE_SALES",
                            "Sales (Units)", "Sales (Sq.Ft)", "qtr_sales_units", "qtr_sales_sqft")
    elif name.startswith("Annual_Sales_Data_as_per_Construction_Stage"):
        handle_stage_simple(path, fl, "CONSTRUCTION_STAGE_SALES",
                            "Annual Sales (Units)", "Annual Sales (Sq.Ft)",
                            "annual_sales_units", "annual_sales_sqft")
    elif name.startswith("Unsold_Stock_Data_as_per_Construction_Stage"):
        handle_stage_simple(path, fl, "CONSTRUCTION_STAGE_SALES",
                            "Unsold (Units)", "Unsold (Sq.Ft)", "unsold_units", "unsold_sqft")
    elif name.startswith("Quarterly_Sales_Data"):
        handle_flat_simple(path, fl, "FLAT_QUARTERLY_SALES",
                           "Sales (Units)", "Sales (Sqft)", "sales_units", "sales_sqft")
    elif name.startswith("Annual_Sales_Data"):
        handle_flat_simple(path, fl, "FLAT_ANNUAL_SALES",
                           "Annual Sales (Units)", "Annual Sales (mn Sqft)",
                           "annual_sales_units", "annual_sales_sqft")
    elif name.startswith("Unsold_Stock_Data"):
        handle_flat_simple(path, fl, "FLAT_UNSOLD",
                           "Unsold (Units)", "Unsold (Sqft)", "unsold_units", "unsold_sqft")
    elif name.startswith("Flat_Type_Analysis_Data"):
        handle_flat_performance(path, fl)
    elif name.startswith("Price_Range_Analysis_(carpet_area_price)_Data"):
        handle_price_band(path, fl, "carpet")
    elif name.startswith("Price_Range_Analysis_(saleable_area_price)_Data"):
        handle_price_band(path, fl, "saleable")
    elif name.startswith("Unit_Size_Range_Analysis_(as_per_carpet_area)_Data"):
        handle_size_band(path, fl, "carpet")
    elif name.startswith("Unit_Size_Range_Analysis_(as_per_saleable_area)_Data"):
        handle_size_band(path, fl, "saleable")
    elif name.startswith("Unit_Ticket_Size_Analysis_Data"):
        handle_ticket_band(path, fl)
    elif name.startswith("Distance_Range_Analysis_Data"):
        handle_distance_band(path, fl)
    elif name.startswith("Possession_Wise_Marketable_Supply"):
        handle_possession(path, fl)
    else:
        log.warning(f"UNROUTED file: {name}")


# ════════════════════════════════════════════════════════════════════════════
# VERIFICATION (§9)
# ════════════════════════════════════════════════════════════════════════════

def run_verification(session: Session, baseline: dict) -> bool:
    all_ok = True
    log.info("═" * 70)
    log.info(f"POST-LOAD VERIFICATION ({CITY_NAME})")
    log.info("═" * 70)

    r = session.run(
        "MATCH (c:City {name: $city}) RETURN c.latest_quarter AS lq, "
        "c.project_count AS pc, c.micromarket_count AS mmc",
        city=CITY_NAME
    ).single()
    if r is None:
        log.error(f"❌ 9.1: {CITY_NAME} City node NOT found")
        return False
    log.info(f"9.1 City: latest_quarter={r['lq']}, projects={r['pc']}, micromarkets={r['mmc']}")
    if r["lq"] != LATEST_QUARTER:
        log.error(f"❌ Expected latest_quarter={LATEST_QUARTER}, got {r['lq']}")
        all_ok = False

    rows = list(session.run(
        """MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(mm:MicroMarket)
           RETURN mm.sub_region AS region, count(mm) AS n ORDER BY region""",
        city=CITY_NAME
    ))
    actual_mm = {r["region"]: r["n"] for r in rows}
    log.info(f"9.2 MicroMarket by sub-region: {actual_mm}")

    rows = list(session.run(
        """MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(mm:MicroMarket)
              -[:HAS_PROJECT]->(p:Project)
           RETURN mm.sub_region AS region, count(DISTINCT p) AS n ORDER BY region""",
        city=CITY_NAME
    ))
    actual_p = {r["region"]: r["n"] for r in rows}
    total_projects = sum(actual_p.values())
    log.info(f"9.3 Projects by sub-region: {actual_p} (total={total_projects})")

    # 9.4 — DEVELOPED_BY (was BUILT_BY in v3 schema)
    r = session.run(
        """MATCH (p:Project {city_name: $city})-[:DEVELOPED_BY]->(b:Builder)
           RETURN count(DISTINCT b) AS n""",
        city=CITY_NAME
    ).single()
    log.info(f"9.4 Builders touching {CITY_NAME}: {r['n']}")

    rows = list(session.run(
        """MATCH (c:City {name: $city})-[r]->(q:Quarter {name: $q})
           RETURN type(r) AS rt, count(*) AS n ORDER BY rt""",
        city=CITY_NAME, q=LATEST_QUARTER
    ))
    log.info(f"9.5 Relationships to {LATEST_QUARTER}: " + ", ".join(f"{r['rt']}={r['n']}" for r in rows))

    for city in IMMUTABLE_CITIES:
        r = session.run(
            """MATCH (c:City {name:$city})
               OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm:MicroMarket)
               OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p:Project)
               RETURN count(DISTINCT mm) AS mm, count(DISTINCT p) AS p""",
            city=city
        ).single()
        base = baseline.get(city, {})
        if r["mm"] != base.get("mm") or r["p"] != base.get("projects"):
            log.error(f"❌ 9.6 CRITICAL: {city} changed! baseline={base}, now mm={r['mm']} p={r['p']}")
            all_ok = False
        else:
            log.info(f"9.6 ✓ {city} unchanged: {r['mm']} MM, {r['p']} projects")

    r1 = session.run(
        "MATCH (mm:MicroMarket {city_name: $city}) "
        "WHERE NOT (mm)<-[:HAS_MICROMARKET]-(:City) RETURN count(mm) AS n",
        city=CITY_NAME
    ).single()
    r2 = session.run(
        "MATCH (p:Project {city_name: $city}) "
        "WHERE NOT (p)-[:IN_MICROMARKET]->(:MicroMarket) RETURN count(p) AS n",
        city=CITY_NAME
    ).single()
    if r1["n"] > 0 or r2["n"] > 0:
        log.warning(f"⚠ 9.7 Orphans: {r1['n']} MicroMarkets, {r2['n']} Projects")
    else:
        log.info("9.7 ✓ No orphans")

    rows = list(session.run(
        """MATCH (p:Project {city_name: $city})
           WITH p.name AS n, count(*) AS c WHERE c > 1
           RETURN n, c""",
        city=CITY_NAME
    ))
    if rows:
        log.error(f"❌ 9.8 Duplicate project names: {[(r['n'], r['c']) for r in rows]}")
        all_ok = False
    else:
        log.info("9.8 ✓ No duplicate project names")

    log.info("═" * 70)
    return all_ok


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> int:
    global CITY_NAME, SOURCE_LABEL, LATEST_QUARTER, LATEST_FY
    global IMMUTABLE_CITIES, EXPECTED_PROJECTS, EXPECTED_MICROMARKETS
    global EXPECTED_BY_REGION, EXPECTED_MM_BY_REGION

    parser = argparse.ArgumentParser(description="Load LF market data into Neo4j (v2-schema-native)")
    parser.add_argument("--dir", required=True, help="Directory of .xlsx files")
    parser.add_argument("--city", default=CITY_NAME,
                        help=f"City to load (default: {CITY_NAME})")
    parser.add_argument("--source", default=SOURCE_LABEL,
                        help=f"Source label (default: {SOURCE_LABEL})")
    parser.add_argument("--quarter", default=LATEST_QUARTER,
                        help=f"Latest quarter (default: {LATEST_QUARTER})")
    parser.add_argument("--fy", default=LATEST_FY,
                        help=f"Latest fiscal year (default: {LATEST_FY})")
    parser.add_argument("--allow-upsert", action="store_true",
                        help="Allow re-loading an existing city (upsert mode)")
    args = parser.parse_args()

    # Apply CLI overrides to the module-level globals every helper reads
    CITY_NAME = args.city
    SOURCE_LABEL = args.source
    LATEST_QUARTER = args.quarter
    LATEST_FY = args.fy

    # Pull expectations for THIS city; warn (don't fail) if not registered
    exp = CITY_EXPECTATIONS.get(CITY_NAME, {})
    if exp:
        EXPECTED_PROJECTS = exp.get("projects")
        EXPECTED_MICROMARKETS = exp.get("micromarkets")
        EXPECTED_BY_REGION = exp.get("projects_by_region", {})
        EXPECTED_MM_BY_REGION = exp.get("mm_by_region", {})
    else:
        log.warning(f"No CITY_EXPECTATIONS entry for {CITY_NAME!r} — skipping count expectations")

    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        log.error(f"Not a directory: {data_dir}")
        return 1

    files = sorted(data_dir.glob("*.xlsx"), key=sort_key)
    log.info(f"Found {len(files)} Excel files in {data_dir}")
    if len(files) == 0:
        log.error("No .xlsx files found")
        return 1

    driver = get_driver()
    start = time.time()
    try:
        with driver.session() as session:
            # Auto-derive IMMUTABLE_CITIES = every existing City EXCEPT the one being loaded.
            # This way, adding a new city doesn't require editing this file.
            other_cities = list(session.run(
                "MATCH (c:City) WHERE c.name <> $city RETURN c.name AS name ORDER BY c.name",
                city=CITY_NAME
            ))
            IMMUTABLE_CITIES = [r["name"] for r in other_cities]
            log.info(f"Loading: {CITY_NAME!r}   "
                     f"Immutable (must not change): {IMMUTABLE_CITIES or '(none)'}")

            baseline = preflight_checks(session, args.allow_upsert)
            create_constraints(session)

            fl = FastLoader(session)
            fl.merge_city()

            for i, f in enumerate(files, 1):
                t0 = time.time()
                try:
                    route_file(f, fl)
                    log.info(f"[{i}/{len(files)}] {f.name}  ({time.time()-t0:.1f}s)")
                except Exception as e:
                    log.exception(f"Failed: {f.name}: {e}")
                    continue

            counts = fl.finalize_city_counts()
            log.info(f"✓ City node updated: {counts['projects']} projects, "
                     f"{counts['micromarkets']} micromarkets")

            # Post-load denormalization (replaces migration steps 11/13/14)
            denorm = fl.post_load_denormalize()
            log.info(f"✓ Denormalization complete: {denorm}")

            log.info(f"✓ Total load time: {time.time()-start:.1f}s")

            ok = run_verification(session, baseline)
            return 0 if ok else 3

    except Exception as e:
        log.exception(f"Fatal error: {e}")
        return 3
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
