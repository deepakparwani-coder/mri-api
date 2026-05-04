"""
MR&I — Pre-built Cypher Queries v4
====================================
Rewritten for Graph Schema v2 (15 node types, 25 relationship types).
Every query maps to proper typed relationships AND carries a human-readable
description used to ground Claude's interpretation of the rows.

v4 CHANGES (this revision):
- Each query is now a {"description", "cypher"} dict (was a flat string).
- QueryRegistry wrapper preserves backward-compatible QUERIES[name] -> cypher
  string access — existing call sites keep working.
- Descriptions enumerate columns, units, and the user-question phrasings
  each query is meant to answer. Used by app.py to inject semantic context
  into the prompt before Claude sees the rows.

v3 CHANGES (carried forward):
- All queries use v2 relationship names (MARKET_SNAPSHOT, YEARLY_SNAPSHOT,
  SALEABLE_PRICE_AT, CARPET_PRICE_AT, QUARTERLY_TREND, FLAT_PERFORMANCE,
  CONSTRUCTION_STAGE_SALES, DISTANCE_PERFORMANCE, TICKET_SIZE_PERFORMANCE,
  UNIT_SIZE_PERFORMANCE, POSSESSION_DISTRIBUTION, BUYER_PROFILE, COMPETES_WITH).
"""


QUERY_DEFINITIONS = {

    # ═══════════════════════════════════════
    # MARKET OVERVIEW (L0)
    # ═══════════════════════════════════════

    "market_overview": {
        "description": (
            "City-level snapshot for the latest 4 quarters. Returns per quarter: "
            "supply (marketable units), sales (units), unsold (units), months_inv "
            "(months of inventory), velocity (% monthly, median across projects), "
            "price_psf (₹ per saleable sqft, weighted average on sold units), "
            "cost_lacs (avg base cost of flat in ₹ lakhs), new_supply (newly "
            "launched units that quarter), and value_sold_cr (business turnover, "
            "₹ crores). Answers: 'how is the market', 'market summary', "
            "'market health check', 'latest market position'."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
            WITH q, r ORDER BY q.sort_order DESC LIMIT 4
            RETURN q.name AS quarter,
                   r.supply_units AS supply,
                   r.sales_units AS sales,
                   r.unsold_units AS unsold,
                   r.months_inv AS months_inv,
                   r.velocity_pct AS velocity,
                   r.wt_avg_price_psf AS price_psf,
                   r.cost_of_flat_lacs AS cost_lacs,
                   r.new_supply_units AS new_supply,
                   r.value_sold_cr AS value_sold_cr
            ORDER BY q.sort_order
        """,
    },

    "annual_overview": {
        "description": (
            "Fiscal-year totals for all available FYs (typically 5). Per FY: "
            "supply (units), sales (units), unsold (units), months_inv, "
            "velocity (%), avg_cost_lacs (avg base cost of flat in ₹ lakhs), "
            "value_sold_cr (annual business turnover in ₹ crores), and "
            "unsold_value_cr (₹ crores tied up in unsold stock). Use for YoY "
            "comparisons and 'how has the market grown' questions. NOTE: months_inv "
            "and velocity are LF medians, not means — see Glossary."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:YEARLY_SNAPSHOT]->(fy:FiscalYear)
            RETURN fy.name AS fiscal_year,
                   r.supply_units AS supply,
                   r.sales_units AS sales,
                   r.unsold_units AS unsold,
                   r.months_inv AS months_inv,
                   r.velocity_pct AS velocity,
                   r.cost_of_flat_lacs AS avg_cost_lacs,
                   r.value_sold_cr AS value_sold_cr,
                   r.unsold_value_cr AS unsold_value_cr
            ORDER BY fy.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # PRICE TRENDS (L0)
    # ═══════════════════════════════════════

    "price_trend_saleable": {
        "description": (
            "Saleable-area PSF time series across all available quarters. Per "
            "quarter: wt_avg_price (weighted average ₹/saleable sqft on full "
            "marketable supply), absorption_price (Wt. Avg. Price on Sold — "
            "what units actually transacted at), median_price, min_price, "
            "max_price, and new_supply_price (Wt. Avg. on units launched that "
            "quarter). All values in ₹ per saleable sqft. Use for price-trend, "
            "rate-trend, asking-vs-absorption questions. Note: 'absorption_price' "
            "is the LF-canonical 'Wt. Avg. Price on Sold'."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:SALEABLE_PRICE_AT]->(q:Quarter)
            RETURN q.name AS quarter,
                   r.wt_avg AS wt_avg_price,
                   r.absorption AS absorption_price,
                   r.median AS median_price,
                   r.minimum AS min_price,
                   r.maximum AS max_price,
                   r.new_supply_price AS new_supply_price
            ORDER BY q.sort_order
        """,
    },

    "price_trend_carpet": {
        "description": (
            "Carpet-area PSF time series — same shape as price_trend_saleable "
            "but on RERA-basis carpet area. Carpet PSF is typically 30–40% "
            "higher than saleable PSF for the same flat because carpet area "
            "excludes balconies, walls, and common-area loading. Use only when "
            "the user explicitly asks for carpet pricing or RERA-basis pricing. "
            "All values in ₹ per carpet sqft."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:CARPET_PRICE_AT]->(q:Quarter)
            RETURN q.name AS quarter,
                   r.wt_avg AS wt_avg_price,
                   r.absorption AS absorption_price,
                   r.median AS median_price,
                   r.minimum AS min_price,
                   r.maximum AS max_price,
                   r.new_supply_price AS new_supply_price
            ORDER BY q.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # QUARTERLY TRENDS (L0)
    # ═══════════════════════════════════════

    "quarterly_absorption": {
        "description": (
            "Quarterly sales and supply, both in units AND in million sqft, "
            "across all available quarters. Per quarter: sales_units, "
            "sales_sqft (million sqft), supply_units, supply_sqft (million "
            "sqft). Use for QoQ comparison, absorption trend, 'how is "
            "absorption tracking', or whenever the user wants area-basis "
            "rather than unit-basis numbers."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:QUARTERLY_TREND]->(q:Quarter)
            RETURN q.name AS quarter,
                   r.sales_units AS sales_units,
                   r.sales_sqft AS sales_sqft,
                   r.supply_units AS supply_units,
                   r.supply_sqft AS supply_sqft
            ORDER BY q.sort_order
        """,
    },

    "velocity_trend": {
        "description": (
            "Monthly Sales Velocity time series across all quarters where data "
            "exists. Returns quarter and velocity (% per month, LF median across "
            "projects). Use when the user asks about velocity over time, "
            "speed-of-sales trend, or whether absorption pace is accelerating "
            "or slowing."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
            WHERE r.velocity_pct IS NOT NULL
            RETURN q.name AS quarter, r.velocity_pct AS velocity
            ORDER BY q.sort_order
        """,
    },

    "inventory_trend": {
        "description": (
            "Months-of-Inventory time series across all quarters. Returns "
            "quarter and months_inventory (months to clear unsold stock at "
            "current velocity). Use when the user asks about inventory build-up "
            "over time, stock liquidation pace, or 'is inventory rising'."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
            WHERE r.months_inv IS NOT NULL
            RETURN q.name AS quarter, r.months_inv AS months_inventory
            ORDER BY q.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # PRODUCT INTELLIGENCE (L0+L1)
    # ═══════════════════════════════════════

    "flat_performance": {
        "description": (
            "Per-flat-type city-level performance, ordered by annual sales (highest "
            "first). Per flat type (1BHK / 2BHK / 3BHK / 4BHK / Studio / etc.): "
            "category (mass/mid/premium/luxury), annual_sales (units), unsold "
            "(units), velocity (% monthly), months_inv (annual basis), "
            "efficiency (Product Efficiency = sales/total supply ratio, %), "
            "saleable_psf and carpet_psf (₹), min_size and max_size (saleable "
            "sqft), min_cost and max_cost (₹ lakhs), total_supply (units). Use "
            "for product-mix, BHK-demand, configuration-performance questions."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:FLAT_PERFORMANCE]->(ft:FlatType)
            RETURN ft.name AS flat_type,
                   ft.category AS category,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.velocity_pct AS velocity,
                   r.months_inv_annual AS months_inv,
                   r.efficiency_pct AS efficiency,
                   r.wt_avg_saleable_psf AS saleable_psf,
                   r.wt_avg_carpet_psf AS carpet_psf,
                   r.saleable_min_size AS min_size,
                   r.saleable_max_size AS max_size,
                   r.min_cost_lacs AS min_cost,
                   r.max_cost_lacs AS max_cost,
                   r.total_supply_units AS total_supply
            ORDER BY r.annual_sales_units DESC
        """,
    },

    "ticket_size": {
        "description": (
            "Performance by ticket-size band (e.g. '<50L', '50L–1Cr', '1–2Cr', "
            "'2–5Cr', '>5Cr'). Per band: annual_sales, qtr_sales, unsold, supply "
            "(all in units), saleable_psf (₹), velocity (%), efficiency (Product "
            "Efficiency %), months_inv. Bands are pre-defined by LF and ordered "
            "low-to-high. Use for affordability-segment, budget-band, "
            "price-segment questions."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:TICKET_SIZE_PERFORMANCE]->(ts:TicketSizeBand)
            RETURN ts.range AS ticket_range,
                   r.annual_sales_units AS annual_sales,
                   r.qtr_sales_units AS qtr_sales,
                   r.unsold_units AS unsold,
                   r.supply_units AS supply,
                   r.wt_avg_saleable_psf AS saleable_psf,
                   r.velocity_pct AS velocity,
                   r.efficiency_pct AS efficiency,
                   r.months_inv AS months_inv
            ORDER BY ts.sort_order
        """,
    },

    "unit_size_saleable": {
        "description": (
            "Performance by unit-size band on SALEABLE area basis (e.g. "
            "'500–750', '750–1000', '1000–1500' sqft). Per band: flat_types "
            "(comma-separated list of BHK configs that fall in this band), "
            "annual_sales (units), unsold (units), saleable_psf (₹), velocity "
            "(%), efficiency (%). Use for size-preference, unit-size-mix, "
            "compact-vs-large questions on saleable-area basis."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:UNIT_SIZE_PERFORMANCE]->(us:UnitSizeBand)
            WHERE us.area_type = 'saleable'
            RETURN us.range AS size_range,
                   r.flat_types AS flat_types,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.wt_avg_saleable_psf AS saleable_psf,
                   r.velocity_pct AS velocity,
                   r.efficiency_pct AS efficiency
            ORDER BY us.sort_order
        """,
    },

    "unit_size_carpet": {
        "description": (
            "Performance by unit-size band on CARPET area basis (RERA basis). "
            "Same shape as unit_size_saleable but with carpet_psf instead of "
            "saleable_psf, and the size bands are smaller (carpet is 70–75% of "
            "saleable). Use only when the user explicitly asks for carpet-basis "
            "size analysis."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:UNIT_SIZE_PERFORMANCE]->(us:UnitSizeBand)
            WHERE us.area_type = 'carpet'
            RETURN us.range AS size_range,
                   r.flat_types AS flat_types,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.wt_avg_carpet_psf AS carpet_psf,
                   r.velocity_pct AS velocity,
                   r.efficiency_pct AS efficiency
            ORDER BY us.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # CONSTRUCTION STAGE (L1)
    # ═══════════════════════════════════════

    "construction_stage": {
        "description": (
            "Sales and unsold stock distribution across construction stages "
            "(typically: New Launch, Excavation, Plinth, Substructure, "
            "Superstructure, Finishing, Ready). Per stage: annual_sales (units), "
            "annual_sales_sqft (million sqft), qtr_sales (units), unsold "
            "(units), unsold_sqft (million sqft). Use for under-construction-vs-"
            "ready questions, 'where is supply stuck', construction-progress-"
            "vs-demand analysis."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:CONSTRUCTION_STAGE_SALES]->(cs:ConstructionStage)
            RETURN cs.name AS stage,
                   r.annual_sales_units AS annual_sales,
                   r.annual_sales_sqft AS annual_sales_sqft,
                   r.qtr_sales_units AS qtr_sales,
                   r.unsold_units AS unsold,
                   r.unsold_sqft AS unsold_sqft
            ORDER BY cs.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # DISTANCE ANALYSIS (L0)
    # ═══════════════════════════════════════

    "distance_analysis": {
        "description": (
            "Performance by distance-from-city-centre band (e.g. '0–5 km', "
            "'5–10 km', '10–15 km', etc.). Per band: annual_sales (units), "
            "unsold (units), supply (units), total_supply (units), saleable_psf "
            "(₹), velocity (%), months_inv (annual basis), efficiency (Product "
            "Efficiency %). Use for proximity-pricing, distance-from-CBD, "
            "core-vs-periphery questions."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:DISTANCE_PERFORMANCE]->(dr:DistanceRange)
            RETURN dr.range AS distance_range,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.supply_units AS supply,
                   r.total_supply_units AS total_supply,
                   r.wt_avg_saleable_psf AS saleable_psf,
                   r.velocity_pct AS velocity,
                   r.months_inv_annual AS months_inv,
                   r.efficiency_pct AS efficiency
            ORDER BY dr.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # POSSESSION DISTRIBUTION (L1)
    # ═══════════════════════════════════════

    "possession_distribution": {
        "description": (
            "Marketable supply and sales distribution by year of expected "
            "possession (e.g. 2025, 2026, 2027, 2028, 'Ready', 'Beyond 2030'). "
            "Per year: supply_units, supply_sqft (million sqft), sales_units, "
            "sales_sqft (million sqft). Use for handover-timeline, "
            "possession-year, ready-vs-future-supply questions."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:POSSESSION_DISTRIBUTION]->(py:PossessionYear)
            RETURN py.year AS year,
                   r.marketable_supply_units AS supply_units,
                   r.marketable_supply_sqft AS supply_sqft,
                   r.sales_units AS sales_units,
                   r.sales_sqft AS sales_sqft
            ORDER BY py.year
        """,
    },

    # ═══════════════════════════════════════
    # PROJECT INTELLIGENCE (L1)
    # ═══════════════════════════════════════

    "top_projects_by_sales": {
        "description": (
            "Top 10 projects in the city ranked by annual sales (highest first). "
            "Per project: project name, builder, location (micromarket), "
            "annual_sales (units), total_supply (units), sold_pct (%), velocity "
            "(% monthly), price_range (saleable PSF range string e.g. "
            "'12000–15000'), price_psf (median saleable PSF, ₹), rera (RERA "
            "registration ID), months_inv. Answers: 'top projects', "
            "'best-selling projects', 'leading developers' projects'."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE p.annual_sales_units IS NOT NULL AND p.annual_sales_units > 0
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder, p.location AS location,
                   p.annual_sales_units AS annual_sales,
                   p.total_supply_units AS total_supply,
                   p.sold_pct AS sold_pct,
                   p.monthly_velocity AS velocity,
                   p.saleable_rate_range AS price_range,
                   p.saleable_rate_psf AS price_psf,
                   p.rera_registered AS rera,
                   p.annual_months_inv AS months_inv
            ORDER BY p.annual_sales_units DESC
            LIMIT 10
        """,
    },

    "top_projects_by_velocity": {
        "description": (
            "Top 10 projects ranked by monthly sales velocity (% per month, "
            "fastest first). Per project: name, builder, location, velocity, "
            "annual_sales, sold_pct, price_range, price_psf. Answers: "
            "'fastest-selling projects', 'highest velocity', 'projects "
            "absorbing fastest'. Note: high velocity AND low total supply often "
            "co-occur — small-inventory projects can show high % velocity but "
            "low absolute sales."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE p.monthly_velocity IS NOT NULL AND p.monthly_velocity > 0
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder, p.location AS location,
                   p.monthly_velocity AS velocity,
                   p.annual_sales_units AS annual_sales,
                   p.sold_pct AS sold_pct,
                   p.saleable_rate_range AS price_range,
                   p.saleable_rate_psf AS price_psf
            ORDER BY p.monthly_velocity DESC
            LIMIT 10
        """,
    },

    "project_detail": {
        "description": (
            "Single-project deep-dive matched by partial name (case-insensitive "
            "CONTAINS). Returns ALL data for the project: name, builder, "
            "location (micromarket), project_id, total_supply (units), "
            "total_supply_sqft, annual_sales, sold_pct, unsold_pct, velocity, "
            "months_inv (annual + quarterly), saleable price range and PSF, "
            "carpet price range and PSF, flat_types, saleable and carpet size "
            "ranges, total cost range (₹ lakhs), RERA ID, launch_date, "
            "possession_date, annual_value_cr (annual revenue in ₹ cr). Use "
            "when the user names a specific project. Multiple matches may "
            "return — show all so user can disambiguate."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE toLower(p.name) CONTAINS toLower($project_name)
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder, p.location AS location,
                   p.project_id AS project_id,
                   p.total_supply_units AS total_supply,
                   p.total_supply_sqft AS total_supply_sqft,
                   p.annual_sales_units AS annual_sales,
                   p.sold_pct AS sold_pct,
                   p.unsold_pct AS unsold_pct,
                   p.monthly_velocity AS velocity,
                   p.annual_months_inv AS months_inv,
                   p.quarterly_months_inv AS qtr_months_inv,
                   p.saleable_rate_range AS saleable_price_range,
                   p.saleable_rate_psf AS saleable_psf,
                   p.carpet_rate_range AS carpet_price_range,
                   p.carpet_rate_psf AS carpet_psf,
                   p.flat_types AS flat_types,
                   p.saleable_size_range AS saleable_sizes,
                   p.carpet_size_range AS carpet_sizes,
                   p.total_cost_range AS cost_range,
                   p.rera_registered AS rera,
                   p.launch_date AS launch_date,
                   p.possession_date AS possession_date,
                   p.annual_value_cr AS annual_value_cr
        """,
    },

    "project_competitors": {
        "description": (
            "Top 10 direct competitors of the named project, found via "
            "COMPETES_WITH graph relationship (typically same micromarket + "
            "comparable price band). Per competitor: name, builder, "
            "annual_sales, sold_pct, velocity, price_psf, price_range. Use "
            "after project_detail to show competitive set. Order: highest "
            "annual sales first."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE toLower(p.name) CONTAINS toLower($project_name)
            MATCH (p)-[:COMPETES_WITH]->(comp:Project)
            WHERE comp.annual_sales_units IS NOT NULL
            OPTIONAL MATCH (comp)-[:DEVELOPED_BY]->(b:Builder)
            RETURN comp.name AS project, b.name AS builder,
                   comp.annual_sales_units AS annual_sales,
                   comp.sold_pct AS sold_pct,
                   comp.monthly_velocity AS velocity,
                   comp.saleable_rate_psf AS price_psf,
                   comp.saleable_rate_range AS price_range
            ORDER BY comp.annual_sales_units DESC
            LIMIT 10
        """,
    },

    "comparable_projects": {
        "description": (
            "All projects in the city, ordered by annual sales (descending) — "
            "without the LIMIT 10 cap that top_projects_by_sales applies. Per "
            "project: name, builder, location, total_supply, annual_sales, "
            "sold_pct, price_psf, price_range, velocity, flat_types. Use for "
            "'show me all projects', 'comparable set across the city', or when "
            "user wants more than 10 entries."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder, p.location AS location,
                   p.total_supply_units AS total_supply,
                   p.annual_sales_units AS annual_sales,
                   p.sold_pct AS sold_pct,
                   p.saleable_rate_psf AS price_psf,
                   p.saleable_rate_range AS price_range,
                   p.monthly_velocity AS velocity,
                   p.flat_types AS flat_types
            ORDER BY p.annual_sales_units DESC
        """,
    },

    "new_launches": {
        "description": (
            "Projects flagged status='NEW_LAUNCH' (launched in the most recent "
            "quarter), ordered by total_supply (largest first). Per project: "
            "name, builder, location, total_supply, saleable_psf, carpet_psf, "
            "flat_types, launch_date, possession_date. Answers: 'new launches', "
            "'recently launched projects', 'what's coming up'. NOTE: NEW_LAUNCH "
            "is a one-quarter-only flag — last quarter's new launches are no "
            "longer flagged in the current quarter."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE p.status = 'NEW_LAUNCH'
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder, p.location AS location,
                   p.total_supply_units AS total_supply,
                   p.saleable_rate_psf AS price_psf,
                   p.carpet_rate_psf AS carpet_psf,
                   p.flat_types AS flat_types,
                   p.launch_date AS launch_date,
                   p.possession_date AS possession_date
            ORDER BY p.total_supply_units DESC
        """,
    },

    "project_count": {
        "description": (
            "Total count of projects in the city. Returns single row with "
            "total_projects (integer). Use for 'how many projects', "
            "'project universe size', or as a sanity check before deeper "
            "queries."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            RETURN count(p) AS total_projects
        """,
    },

    # ═══════════════════════════════════════
    # MICROMARKET INTELLIGENCE (L0+L1)
    # ═══════════════════════════════════════

    "micromarket_list": {
        "description": (
            "All micromarkets (sectors / locations) in the city, ordered by "
            "total annual sales (highest first). Per micromarket: name, "
            "project_count, total_sales (units), avg_price_psf (₹, simple "
            "average across projects in that micromarket). Use for 'list "
            "sectors', 'which areas', 'micromarket overview', or as a "
            "navigation step before drilling into one specific area."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
            OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
            WITH m, count(p) AS project_count,
                 sum(p.annual_sales_units) AS total_sales,
                 avg(p.saleable_rate_psf) AS avg_price
            RETURN m.name AS micromarket,
                   project_count,
                   total_sales,
                   round(avg_price) AS avg_price_psf
            ORDER BY total_sales DESC
        """,
    },

    "micromarkets_by_demand": {
        "description": (
            "Micromarkets ranked by total demand (sum of annual sales across "
            "active projects, highest first). Per micromarket: name, "
            "active_projects (projects with sales > 0), total_demand (sum of "
            "annual sales units), and top_projects (list of up to 5 projects "
            "with their sales, velocity, price). Use for 'hottest "
            "micromarkets', 'where is demand highest', 'acquisition targets'. "
            "IMPORTANT: This is LF Sales Velocity / sales aggregation, NOT an "
            "invented 'demand intensity' score — refuse to use the term "
            "'demand intensity' as if it were an LF metric."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
            OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
            WHERE p.annual_sales_units > 0
            WITH m, count(p) AS active_projects,
                 sum(p.annual_sales_units) AS total_demand,
                 collect({name: p.name, sales: p.annual_sales_units,
                          velocity: p.monthly_velocity, price: p.saleable_rate_psf}) AS projects
            RETURN m.name AS micromarket,
                   active_projects,
                   total_demand,
                   projects[0..5] AS top_projects
            ORDER BY total_demand DESC
        """,
    },

    "micromarkets_by_inventory_risk": {
        "description": (
            "Micromarkets ranked by average months-of-inventory across their "
            "projects (highest first = highest risk of unsold-stock pile-up). "
            "Per micromarket: name, avg_months_inventory (rounded to 1 "
            "decimal), projects_with_data (count), risky_projects (list of up "
            "to 5 projects with their MI and unsold %). Industry rule of "
            "thumb: MI > 24 months = elevated risk; MI > 36 months = high "
            "risk. NOTE: thresholds are rules of thumb, NOT from the LF "
            "Glossary."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
            OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
            WHERE p.annual_months_inv IS NOT NULL AND p.annual_months_inv > 0
            WITH m, avg(p.annual_months_inv) AS avg_mi,
                 count(p) AS projects_with_data,
                 collect({name: p.name, mi: p.annual_months_inv, unsold_pct: p.unsold_pct}) AS projects
            WHERE avg_mi > 0
            RETURN m.name AS micromarket,
                   round(avg_mi, 1) AS avg_months_inventory,
                   projects_with_data,
                   projects[0..5] AS risky_projects
            ORDER BY avg_mi DESC
        """,
    },

    "micromarket_detail": {
        "description": (
            "All projects within a single micromarket, matched by partial "
            "name on m.name (case-insensitive CONTAINS). Use this for sector-"
            "specific queries like 'projects in Sector 71'. Per project: "
            "name, builder, total_supply, annual_sales, sold_pct, velocity, "
            "price_psf, price_range, months_inv, rera. Order: highest annual "
            "sales first. WARNING: substring matching can false-match — "
            "'Sector 7' may also match 'Sector 70'/'Sector 71'. Pendency "
            "P3-#6 fixes this; until then, prefer exact-token user input."
        ),
        "cypher": """
            MATCH (m:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE m.city_name = $city AND toLower(m.name) CONTAINS toLower($location)
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder,
                   p.total_supply_units AS total_supply,
                   p.annual_sales_units AS annual_sales,
                   p.sold_pct AS sold_pct,
                   p.monthly_velocity AS velocity,
                   p.saleable_rate_psf AS price_psf,
                   p.saleable_rate_range AS price_range,
                   p.annual_months_inv AS months_inv,
                   p.rera_registered AS rera
            ORDER BY p.annual_sales_units DESC
        """,
    },

    "emerging_micromarkets": {
        "description": (
            "Micromarkets that contain at least one NEW_LAUNCH project, ranked "
            "by count of new launches (highest first). Returns micromarket "
            "name and new_launches count. Use for 'emerging areas', "
            "'upcoming sectors', 'where are launches happening'. NOTE: this "
            "captures THIS QUARTER'S new launches only — historical launches "
            "no longer carry the NEW_LAUNCH status."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
            OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
            WHERE p.status = 'NEW_LAUNCH'
            WITH m, count(p) AS new_launches
            WHERE new_launches > 0
            RETURN m.name AS micromarket, new_launches
            ORDER BY new_launches DESC
        """,
    },

    "nearby_micromarkets": {
        "description": (
            "Same as micromarket_list but a smaller, faster variant — returns "
            "micromarket, projects (count), sales (sum of annual sales). Use "
            "as a lightweight context-padding query when the main intent is "
            "elsewhere but a quick city-wide micromarket index is useful for "
            "the answer."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
            OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
            WITH m, count(p) AS projects, sum(p.annual_sales_units) AS sales
            RETURN m.name AS micromarket, projects, sales
            ORDER BY sales DESC
        """,
    },

    # ═══════════════════════════════════════
    # BUILDER INTELLIGENCE (L1)
    # ═══════════════════════════════════════

    "builder_rankings": {
        "description": (
            "Top 15 builders/developers in the city, ranked by total annual "
            "sales across all their projects. Per builder: name, projects "
            "(count of active projects in the city), total_sales (units), "
            "total_supply (units), project_names (sample of up to 3). "
            "Answers: 'top builders', 'leading developers', 'who is biggest "
            "in <city>', 'developer market share by units'. Group-level "
            "consolidation (DLF Group vs DLF Phase 5) is NOT done here — "
            "each registered builder entity is a separate row."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)-[:DEVELOPED_BY]->(b:Builder)
            WHERE p.annual_sales_units > 0
            WITH b, count(p) AS projects,
                 sum(p.annual_sales_units) AS total_sales,
                 sum(p.total_supply_units) AS total_supply,
                 collect(p.name)[0..3] AS project_names
            RETURN b.name AS builder,
                   projects,
                   total_sales,
                   total_supply,
                   project_names
            ORDER BY total_sales DESC
            LIMIT 15
        """,
    },

    # ═══════════════════════════════════════
    # BUYER DEMOGRAPHICS (IGR — Hinjewadi only as of v2)
    # ═══════════════════════════════════════

    "buyer_age_dist": {
        "description": (
            "Buyer age-group distribution from IGR (sub-registrar) data. Per "
            "row: age_group (e.g. '25-34', '35-44'), buyers (count). Ordered "
            "by count desc. Available only for cities where BUYER_PROFILE data "
            "has been loaded — currently HINJEWADI ONLY. If row_count is 0, "
            "tell the user this segment isn't available for the selected city."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'age'
            RETURN bs.value AS age_group, bs.count AS buyers
            ORDER BY bs.count DESC
        """,
    },

    "buyer_gender_dist": {
        "description": (
            "Buyer gender distribution from IGR registration data. Per row: "
            "gender (Male/Female/Joint), buyers (count). Hinjewadi only as "
            "of v2. Joint registrations typically dominate."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'gender'
            RETURN bs.value AS gender, bs.count AS buyers
            ORDER BY bs.count DESC
        """,
    },

    "buyer_locality_dist": {
        "description": (
            "Top 30 buyer source-localities (where the buyers came FROM, by "
            "registration address). Per row: locality, buyers (count). Use "
            "for catchment analysis, 'where are buyers coming from'. "
            "Hinjewadi only."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'locality'
            RETURN bs.value AS locality, bs.count AS buyers
            ORDER BY bs.count DESC
            LIMIT 30
        """,
    },

    "buyer_state_dist": {
        "description": (
            "Buyer source-state distribution (out-of-state vs local demand). "
            "Per row: state, buyers (count). For Hinjewadi this typically "
            "shows Maharashtra dominance with notable Karnataka/AP minority. "
            "Hinjewadi only."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'state'
            RETURN bs.value AS state, bs.count AS buyers
            ORDER BY bs.count DESC
        """,
    },

    "buyer_religion_dist": {
        "description": (
            "Buyer religion distribution (inferred from registration name "
            "patterns). Per row: religion, buyers. Hinjewadi only. Treat as "
            "directional, not exact — the inference is name-pattern based, "
            "not self-reported."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'religion'
            RETURN bs.value AS religion, bs.count AS buyers
            ORDER BY bs.count DESC
        """,
    },

    "buyer_language_dist": {
        "description": (
            "Buyer language/community distribution (e.g. Marathi, Hindi, "
            "Telugu, Tamil — inferred from name patterns). Per row: language, "
            "buyers. Hinjewadi only. Same caveat as religion: inferred, not "
            "self-reported."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
            WHERE bs.dimension = 'language'
            RETURN bs.value AS language, bs.count AS buyers
            ORDER BY bs.count DESC
        """,
    },

    # ═══════════════════════════════════════
    # YoY ANALYSIS
    # ═══════════════════════════════════════

    "yoy_absorption": {
        "description": (
            "Slim-column variant of annual_overview optimised for YoY "
            "comparison charts. Per FY: sales (units), supply (units), "
            "velocity (%), months_inv. Use specifically for year-over-year "
            "growth questions where the user wants a clean trend table "
            "rather than the wider annual_overview output."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:YEARLY_SNAPSHOT]->(fy:FiscalYear)
            RETURN fy.name AS fiscal_year,
                   r.sales_units AS sales,
                   r.supply_units AS supply,
                   r.velocity_pct AS velocity,
                   r.months_inv AS months_inv
            ORDER BY fy.sort_order
        """,
    },

    # ═══════════════════════════════════════
    # VALIDATION
    # ═══════════════════════════════════════

    "validate_number": {
        "description": (
            "INTERNAL VALIDATION QUERY — used by the bot to cross-check a "
            "specific number it's about to state. Matches a project by partial "
            "name and returns the canonical values: project, builder, "
            "annual_sales, total_supply, sold_pct, price_psf, velocity. NOT "
            "for direct user-facing answers — use project_detail instead, "
            "which returns the full superset of fields."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            WHERE toLower(p.name) CONTAINS toLower($project_name)
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project, b.name AS builder,
                   p.annual_sales_units AS annual_sales,
                   p.total_supply_units AS total_supply,
                   p.sold_pct AS sold_pct,
                   p.saleable_rate_psf AS price_psf,
                   p.monthly_velocity AS velocity
        """,
    },

    # ═══════════════════════════════════════
    # PRICE RANGE ANALYSIS (Gurugram-specific extension)
    # ═══════════════════════════════════════

    "price_range_carpet": {
        "description": (
            "Performance by carpet-PSF price band (e.g. '<8000', '8000-12000', "
            "'12000-18000', '>18000'). Per band: price_range, annual_sales, "
            "unsold, total_supply, carpet_psf, velocity, efficiency, "
            "months_inv. Available only where PRICE_BAND_PERFORMANCE rels "
            "have been loaded — currently GURUGRAM ONLY. If row_count is 0, "
            "say this analysis isn't available for the selected city."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:PRICE_BAND_PERFORMANCE]->(pb:PriceBand {basis: 'carpet'})
            RETURN pb.name AS price_range,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.total_supply_units AS total_supply,
                   r.wt_avg_carpet_price_psf AS carpet_psf,
                   r.monthly_sales_velocity_pct AS velocity,
                   r.product_efficiency_pct AS efficiency,
                   r.annual_months_inventory AS months_inv
            ORDER BY pb.name
        """,
    },

    "price_range_saleable": {
        "description": (
            "Saleable-PSF version of price_range_carpet. Same shape, same "
            "Gurugram-only availability. Use for price-segment performance "
            "questions on saleable-area basis."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[r:PRICE_BAND_PERFORMANCE]->(pb:PriceBand {basis: 'saleable'})
            RETURN pb.name AS price_range,
                   r.annual_sales_units AS annual_sales,
                   r.unsold_units AS unsold,
                   r.total_supply_units AS total_supply,
                   r.wt_avg_saleable_price_psf AS saleable_psf,
                   r.monthly_sales_velocity_pct AS velocity,
                   r.product_efficiency_pct AS efficiency,
                   r.annual_months_inventory AS months_inv
            ORDER BY pb.name
        """,
    },

    # ═══════════════════════════════════════
    # RERA STATUS (Gurugram-specific)
    # ═══════════════════════════════════════

    "projects_by_rera_status": {
        "description": (
            "All projects with their RERA registration status, ordered first "
            "by rera_status then by annual sales. Per project: name, builder, "
            "location, rera_number (RERA registration ID, e.g. "
            "'GGM/123/2024'), rera_status (Active / Expired / Pending / Not "
            "Registered), total_supply, sold_pct. Use for compliance, "
            "RERA-vetting, regulatory-check questions. Availability: Gurugram "
            "(complete), partial elsewhere."
        ),
        "cypher": """
            MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
            OPTIONAL MATCH (p)-[:DEVELOPED_BY]->(b:Builder)
            RETURN p.name AS project,
                   b.name AS builder,
                   p.location AS location,
                   p.rera_registered AS rera_number,
                   p.rera_status AS rera_status,
                   p.total_supply_units AS total_supply,
                   p.sold_pct AS sold_pct
            ORDER BY p.rera_status, p.annual_sales_units DESC
        """,
    },

}


# =============================================================================
# QueryRegistry — backward-compatible accessor
# =============================================================================
#
# Existing code does:  cypher = QUERIES[name]              (returns string)
# After this patch:    cypher = QUERIES[name]              (still returns string)
#                      desc = QUERIES.description(name)    (NEW)
#                      entry = QUERIES.entry(name)         (NEW — full dict)
#                      "name" in QUERIES                   (still works)
#
# This means run_query() can be upgraded incrementally without breaking any
# existing call site. The string-access path is the legacy contract; the new
# methods are additive.

class QueryRegistry:
    """Dict-like wrapper that exposes both the legacy cypher-string contract
    and the new description accessor."""

    def __init__(self, definitions: dict):
        self._defs = definitions

    def __contains__(self, name):
        return name in self._defs

    def __getitem__(self, name):
        # Legacy: return the cypher string
        return self._defs[name]["cypher"]

    def __iter__(self):
        return iter(self._defs)

    def __len__(self):
        return len(self._defs)

    def description(self, name: str) -> str:
        return self._defs.get(name, {}).get("description", "")

    def entry(self, name: str) -> dict:
        return dict(self._defs.get(name, {}))

    def keys(self):
        return self._defs.keys()

    def items(self):
        # Yield (name, cypher) pairs to match legacy dict semantics
        for name, entry in self._defs.items():
            yield name, entry["cypher"]


QUERIES = QueryRegistry(QUERY_DEFINITIONS)
