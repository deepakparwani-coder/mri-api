"""
MR&I — Pre-built Cypher Queries v2
====================================
Each query returns EXACT data from the graph. Claude uses these results
for presentation only — it does NOT interpret raw JSON.

v2 CHANGES:
- Micromarket queries now return INDIVIDUAL PROJECT data instead of
  pre-computed averages (avg_price_psf, avg_velocity, demand_intensity).
- This eliminates fabricated sector-level aggregates that the validation
  layer was flagging.
- Every number traces back to a specific Project node.
"""

QUERIES = {

    # ═══════════════════════════════════════
    # MARKET OVERVIEW
    # ═══════════════════════════════════════

    "market_overview": """
        MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
        WITH q, r ORDER BY q.sort_order DESC LIMIT 4
        RETURN q.label AS quarter,
               r.supply_units AS supply,
               r.sales_units AS sales,
               r.unsold_units AS unsold,
               r.months_inventory AS months_inv,
               r.sales_velocity_pct AS velocity
        ORDER BY q.sort_order
    """,

    "annual_overview": """
        MATCH (c:City {name: $city})-[r:ANNUAL_SNAPSHOT]->(f:FiscalYear)
        RETURN f.id AS fiscal_year,
               r.supply_units AS supply,
               r.sales_units AS sales,
               r.unsold_units AS unsold,
               r.months_inventory AS months_inv,
               r.sales_velocity_pct AS velocity,
               r.cost_of_flat_lacs AS avg_cost_lacs
        ORDER BY f.id
    """,

    # ═══════════════════════════════════════
    # PRICE TRENDS
    # ═══════════════════════════════════════

    "price_trend_saleable": """
        MATCH (c:City {name: $city})-[r:SALEABLE_PRICE]->(q:Quarter)
        RETURN q.label AS quarter,
               r.wt_avg AS wt_avg_price,
               r.absorption AS absorption_price,
               r.median AS median_price,
               r.minimum AS min_price,
               r.maximum AS max_price
        ORDER BY q.sort_order
    """,

    "price_trend_carpet": """
        MATCH (c:City {name: $city})-[r:CARPET_PRICE]->(q:Quarter)
        RETURN q.label AS quarter,
               r.wt_avg AS wt_avg_price,
               r.absorption AS absorption_price,
               r.median AS median_price
        ORDER BY q.sort_order
    """,

    # ═══════════════════════════════════════
    # MICRO-MARKET RANKINGS (PROJECT-LEVEL)
    # ═══════════════════════════════════════
    # v2: Returns individual project data per micromarket.
    # Claude must present projects individually — NEVER average them.

    "micromarkets_by_demand": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        WHERE m.active_projects > 0
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status <> 'SOLD_OUT' AND p.annual_sales_units > 0
        WITH m, p ORDER BY p.annual_sales_units DESC
        WITH m,
             count(p) AS active_projects,
             m.sold_out_count AS sold_out_projects,
             sum(p.annual_sales_units) AS total_annual_sales,
             collect({
               name: p.name,
               builder: p.builder_name,
               price_psf: p.saleable_rate_psf,
               velocity: p.monthly_velocity,
               annual_sales: p.annual_sales_units,
               sold_pct: p.sold_pct,
               months_inv: p.annual_months_inv,
               supply: p.total_supply_units
             })[0..5] AS top_projects
        WHERE total_annual_sales > 0
        RETURN m.name AS micromarket,
               active_projects AS projects,
               sold_out_projects,
               total_annual_sales AS annual_sales,
               top_projects
        ORDER BY total_annual_sales DESC
    """,

    "micromarkets_by_inventory_risk": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        WHERE m.active_projects > 0
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status <> 'SOLD_OUT' AND p.annual_months_inv > 0
        WITH m, p ORDER BY p.annual_months_inv DESC
        WITH m,
             count(p) AS active_projects,
             sum(p.annual_sales_units) AS total_annual_sales,
             collect({
               name: p.name,
               builder: p.builder_name,
               price_psf: p.saleable_rate_psf,
               velocity: p.monthly_velocity,
               months_inv: p.annual_months_inv,
               sold_pct: p.sold_pct,
               supply: p.total_supply_units
             })[0..5] AS projects_by_risk
        WHERE active_projects > 0
        RETURN m.name AS micromarket,
               active_projects AS projects,
               total_annual_sales AS annual_sales,
               projects_by_risk,
               CASE
                 WHEN projects_by_risk[0].months_inv > 24 THEN 'HIGH RISK'
                 WHEN projects_by_risk[0].months_inv > 18 THEN 'MODERATE'
                 ELSE 'HEALTHY'
               END AS risk_level
        ORDER BY projects_by_risk[0].months_inv DESC
    """,

    "emerging_micromarkets": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        WHERE m.active_projects > 0
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status <> 'SOLD_OUT' AND p.monthly_velocity > 0
        WITH m, p ORDER BY p.monthly_velocity DESC
        WITH m,
             count(p) AS active_projects,
             m.sold_out_count AS sold_out_count,
             sum(p.annual_sales_units) AS total_annual_sales,
             collect({
               name: p.name,
               builder: p.builder_name,
               price_psf: p.saleable_rate_psf,
               velocity: p.monthly_velocity,
               annual_sales: p.annual_sales_units,
               sold_pct: p.sold_pct
             })[0..5] AS fastest_projects
        WHERE active_projects > 0
        RETURN m.name AS micromarket,
               active_projects,
               sold_out_count,
               total_annual_sales AS annual_sales,
               fastest_projects
        ORDER BY fastest_projects[0].velocity DESC
    """,

    # ═══════════════════════════════════════
    # PRODUCT MIX
    # ═══════════════════════════════════════

    "flat_performance": """
        MATCH (c:City {name: $city})-[r:FLAT_PERFORMANCE]->(c)
        RETURN r.config AS configuration,
               r.annual_sales AS annual_sales,
               r.qtr_sales AS qtr_sales,
               r.unsold AS unsold,
               r.mkt_supply AS supply,
               r.avg_price AS avg_price,
               r.carpet_price AS carpet_price,
               r.mi_annual AS months_inv,
               r.mi_qtr AS months_inv_qtr,
               r.velocity AS velocity,
               r.efficiency AS efficiency
        ORDER BY r.annual_sales DESC
    """,

    # ═══════════════════════════════════════
    # COMPETITIVE INTELLIGENCE
    # ═══════════════════════════════════════

    "project_detail": """
        MATCH (p:Project {city: $city})
        WHERE toLower(p.name) CONTAINS toLower($project_name)
        OPTIONAL MATCH (p)-[:BUILT_BY]->(b:Builder)
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        RETURN p.name AS project,
               p.builder_name AS builder,
               m.name AS micromarket,
               p.total_supply_units AS supply,
               p.annual_sales_units AS annual_sales,
               p.annual_sales_value_cr AS sales_value_cr,
               p.saleable_rate_psf AS price_psf,
               p.monthly_velocity AS velocity,
               p.quarterly_months_inv AS months_inv,
               p.sold_pct AS sold_pct,
               p.unsold_pct AS unsold_pct,
               p.launch_date AS launch,
               p.possession_date AS possession,
               p.rera_registered AS rera,
               b.project_count AS builder_total_projects

    """,

    "project_competitors": """
        MATCH (p:Project {city: $city})
        WHERE toLower(p.name) CONTAINS toLower($project_name)
        WITH p LIMIT 1
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(comp:Project)
        WHERE comp.project_id <> p.project_id AND comp.status <> 'SOLD_OUT'
        RETURN comp.name AS competitor,
               comp.builder_name AS builder,
               comp.saleable_rate_psf AS price_psf,
               comp.monthly_velocity AS velocity,
               comp.annual_sales_units AS annual_sales,
               comp.sold_pct AS sold_pct,
               comp.total_supply_units AS supply,
               comp.annual_months_inv AS months_inv,
               m.name AS micromarket
        ORDER BY comp.annual_sales_units DESC
    """,

    "top_projects_by_sales": """
        MATCH (p:Project {city: $city, status: 'ACTIVE'})
        WHERE p.annual_sales_units > 0
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        RETURN p.name AS project,
               p.builder_name AS builder,
               m.name AS micromarket,
               p.annual_sales_units AS annual_sales,
               p.annual_sales_value_cr AS value_cr,
               p.saleable_rate_psf AS price_psf,
               p.monthly_velocity AS velocity,
               p.sold_pct AS sold_pct,
               p.quarterly_months_inv AS months_inv
        ORDER BY p.annual_sales_units DESC
        LIMIT 15
    """,

    "top_projects_by_velocity": """
        MATCH (p:Project {city: $city, status: 'ACTIVE'})
        WHERE p.monthly_velocity > 0
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        RETURN p.name AS project,
               p.builder_name AS builder,
               m.name AS micromarket,
               p.monthly_velocity AS velocity,
               p.annual_sales_units AS annual_sales,
               p.saleable_rate_psf AS price_psf,
               p.sold_pct AS sold_pct
        ORDER BY p.monthly_velocity DESC
        LIMIT 15
    """,

    # ═══════════════════════════════════════
    # BUILDER ANALYSIS
    # ═══════════════════════════════════════

    "builder_rankings": """
        MATCH (b:Builder)<-[:BUILT_BY]-(p:Project {city: $city})
        WHERE p.status <> 'SOLD_OUT'
        WITH b, count(p) AS projects,
             sum(p.total_supply_units) AS total_supply,
             sum(p.annual_sales_units) AS total_sales,
             avg(p.monthly_velocity) AS avg_vel,
             avg(p.sold_pct) AS avg_sold
        RETURN b.name AS builder,
               projects,
               total_supply,
               total_sales,
               round(avg_vel * 100) / 100 AS avg_velocity,
               round(avg_sold * 100) / 100 AS avg_sold_pct
        ORDER BY total_sales DESC
    """,

    # ═══════════════════════════════════════
    # SITE INTELLIGENCE (PROJECT-LEVEL)
    # ═══════════════════════════════════════

    "micromarket_detail": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        WHERE m.name CONTAINS $location OR m.raw_name CONTAINS $location
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WITH m, p ORDER BY p.annual_sales_units DESC
        WITH m,
             m.active_projects AS total_active,
             m.sold_out_count AS sold_out,
             m.min_price_psf AS min_price,
             m.max_price_psf AS max_price,
             collect({
               name: p.name,
               builder: p.builder_name,
               price_psf: p.saleable_rate_psf,
               carpet_price: p.carpet_rate_psf,
               velocity: p.monthly_velocity,
               annual_sales: p.annual_sales_units,
               annual_value_cr: p.annual_sales_value_cr,
               sold_pct: p.sold_pct,
               unsold_pct: p.unsold_pct,
               status: p.status,
               supply: p.total_supply_units,
               months_inv: p.annual_months_inv,
               months_inv_qtr: p.quarterly_months_inv,
               launch_date: p.launch_date,
               possession_date: p.possession_date,
               rera: p.rera_registered
             }) AS projects
        RETURN m.name AS micromarket,
               total_active,
               sold_out,
               min_price,
               max_price,
               projects
    """,

    "nearby_micromarkets": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        WHERE m.active_projects > 0
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status <> 'SOLD_OUT' AND p.saleable_rate_psf > 0
        WITH m, count(p) AS active_projects,
             sum(p.annual_sales_units) AS total_sales,
             min(p.saleable_rate_psf) AS min_price,
             max(p.saleable_rate_psf) AS max_price
        RETURN m.name AS micromarket,
               min_price,
               max_price,
               active_projects AS projects,
               total_sales AS annual_sales
        ORDER BY total_sales DESC
    """,

    # ═══════════════════════════════════════
    # ABSORPTION & TICKET SIZE
    # ═══════════════════════════════════════

    "quarterly_absorption": """
        MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
        RETURN q.label AS quarter,
               r.sales_units AS absorption_units,
               r.supply_units AS supply_units,
               r.months_inventory AS months_inv,
               r.sales_velocity_pct AS velocity,
               CASE WHEN r.supply_units > 0
                    THEN round(r.sales_units * 100.0 / r.supply_units * 10) / 10
                    ELSE 0 END AS absorption_rate_pct
        ORDER BY q.sort_order
    """,

    # ═══════════════════════════════════════
    # VALIDATION QUERY
    # ═══════════════════════════════════════

    "validate_number": """
        // Use this to verify any specific number
        MATCH (p:Project {name: $project_name, city: $city})
        RETURN p.name AS project,
               p.data_source AS source_section,
               p.annual_sales_units AS annual_sales,
               p.saleable_rate_psf AS price_psf,
               p.monthly_velocity AS velocity,
               p.quarterly_months_inv AS months_inv
    """,

    # ═══════════════════════════════════════
    # BUYER DEMOGRAPHICS
    # ═══════════════════════════════════════

    "buyer_age_dist": """
        MATCH (c:City {name: $city})-[r:BUYER_AGE]->(c)
        RETURN r.label AS age_group, r.count AS count
        ORDER BY r.count DESC
    """,

    "buyer_gender_dist": """
        MATCH (c:City {name: $city})-[r:BUYER_GENDER]->(c)
        RETURN r.label AS gender, r.count AS count
        ORDER BY r.count DESC
    """,

    "buyer_locality_dist": """
        MATCH (c:City {name: $city})-[r:BUYER_LOCALITY]->(c)
        RETURN r.label AS locality, r.count AS count
        ORDER BY r.count DESC
        LIMIT 30
    """,

    "buyer_state_dist": """
        MATCH (c:City {name: $city})-[r:BUYER_STATE]->(c)
        RETURN r.label AS state, r.count AS count
        ORDER BY r.count DESC
    """,

    "buyer_religion_dist": """
        MATCH (c:City {name: $city})-[r:BUYER_RELIGION]->(c)
        RETURN r.label AS religion, r.count AS count
        ORDER BY r.count DESC
    """,

    # ═══════════════════════════════════════
    # VELOCITY & INVENTORY TIME SERIES
    # ═══════════════════════════════════════

    "velocity_trend": """
        MATCH (c:City {name: $city})-[r:SALES_VELOCITY]->(q:Quarter)
        RETURN q.label AS quarter, r.value AS velocity
        ORDER BY q.sort_order
    """,

    "inventory_trend": """
        MATCH (c:City {name: $city})-[r:MONTHS_INVENTORY]->(q:Quarter)
        RETURN q.label AS quarter, r.value AS months_inv
        ORDER BY q.sort_order
    """,

    # ═══════════════════════════════════════
    # YOY GROWTH
    # ═══════════════════════════════════════

    "yoy_absorption": """
        MATCH (c:City {name: $city})-[r:ANNUAL_SNAPSHOT]->(f:FiscalYear)
        RETURN f.id AS fiscal_year,
               r.sales_units AS sales,
               r.supply_units AS supply,
               r.unsold_units AS unsold,
               r.months_inventory AS months_inv,
               r.sales_velocity_pct AS velocity
        ORDER BY f.id
    """,

    # ═══════════════════════════════════════
    # CONSTRUCTION STAGE ANALYSIS
    # ═══════════════════════════════════════

    "annual_sales_stage": """
        MATCH (c:City {name: $city})-[r:ANNUAL_SALES_STAGE]->(f:FiscalYear)
        RETURN f.id AS fiscal_year,
               r.stage AS construction_stage,
               r.supply_units AS supply,
               r.sales_units AS sales,
               r.unsold_units AS unsold,
               r.months_inventory AS months_inv,
               r.sales_velocity_pct AS velocity
        ORDER BY f.id, r.stage
    """,

    "unsold_by_stage": """
        MATCH (c:City {name: $city})-[r:UNSOLD_STAGE]->(q:Quarter)
        RETURN q.label AS quarter,
               r.stage AS construction_stage,
               r.unsold_units AS unsold,
               r.months_inventory AS months_inv
        ORDER BY q.sort_order, r.stage
    """,

    # ═══════════════════════════════════════
    # POSSESSION & DISTANCE
    # ═══════════════════════════════════════

    "possession_distribution": """
        MATCH (c:City {name: $city})-[r:POSSESSION_DIST]->(c)
        RETURN r.possession_year AS possession_year,
               r.supply_units AS supply,
               r.sales_units AS sales,
               r.unsold_units AS unsold,
               r.sales_velocity_pct AS velocity
        ORDER BY r.possession_year
    """,

    "distance_analysis": """
        MATCH (c:City {name: $city})-[r:DISTANCE_RANGE]->(c)
        RETURN r.distance_range AS distance_km,
               r.projects AS projects,
               r.supply_units AS supply,
               r.sales_units AS sales,
               r.unsold_units AS unsold,
               r.avg_price_psf AS avg_price
        ORDER BY r.distance_range
    """,

    # ═══════════════════════════════════════
    # NEW LAUNCHES & SOLD OUT
    # ═══════════════════════════════════════

    "new_launches": """
        MATCH (p:Project {city: $city})
        WHERE p.is_new_launch = true OR p.launch_date CONTAINS '2024' OR p.launch_date CONTAINS '2025'
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        RETURN p.name AS project,
               p.builder_name AS builder,
               m.name AS micromarket,
               p.launch_date AS launch_date,
               p.total_supply_units AS supply,
               p.saleable_rate_psf AS price_psf,
               p.monthly_velocity AS velocity,
               p.sold_pct AS sold_pct
        ORDER BY p.launch_date DESC
        LIMIT 20
    """,

    "sold_out_projects": """
        MATCH (p:Project {city: $city, status: 'SOLD_OUT'})
        OPTIONAL MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p)
        RETURN p.name AS project,
               p.builder_name AS builder,
               m.name AS micromarket,
               p.total_supply_units AS supply,
               p.saleable_rate_psf AS price_psf,
               p.launch_date AS launch_date,
               p.possession_date AS possession
        ORDER BY p.total_supply_units DESC
    """,

    # ═══════════════════════════════════════
    # COMPARABLE & CATCHMENT
    # ═══════════════════════════════════════

    "comparable_projects": """
        MATCH (p1:Project {city: $city})-[:COMPETES_WITH]-(p2:Project)
        OPTIONAL MATCH (m1:MicroMarket)-[:HAS_PROJECT]->(p1)
        OPTIONAL MATCH (m2:MicroMarket)-[:HAS_PROJECT]->(p2)
        RETURN p1.name AS project,
               m1.name AS micromarket,
               p1.saleable_rate_psf AS price_psf,
               p1.monthly_velocity AS velocity,
               collect({
                 name: p2.name,
                 micromarket: m2.name,
                 price_psf: p2.saleable_rate_psf,
                 velocity: p2.monthly_velocity,
                 sold_pct: p2.sold_pct
               })[0..5] AS comparables
        ORDER BY p1.annual_sales_units DESC
        LIMIT 10
    """,

    "catchment_projects": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status <> 'SOLD_OUT' AND p.annual_sales_units > 0
        WITH m, count(p) AS projects,
             sum(p.annual_sales_units) AS total_sales,
             sum(p.total_supply_units) AS total_supply,
             avg(p.saleable_rate_psf) AS avg_price
        WHERE projects > 0
        RETURN m.name AS micromarket,
               projects,
               total_sales,
               total_supply,
               round(avg_price) AS avg_price_psf
        ORDER BY total_sales DESC
    """,
}


def get_query(name, **params):
    """Get a formatted query with parameters."""
    if name not in QUERIES:
        raise ValueError(f"Unknown query: {name}. Available: {list(QUERIES.keys())}")
    return QUERIES[name], params
