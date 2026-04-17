"""
MR&I — Pre-built Cypher Queries v3
====================================
Rewritten for Graph Schema v2 (15 node types, 25 relationship types).
Every query maps to proper typed relationships.

v3 CHANGES:
- All queries use v2 relationship names (MARKET_SNAPSHOT, YEARLY_SNAPSHOT,
  SALEABLE_PRICE_AT, CARPET_PRICE_AT, QUARTERLY_TREND, FLAT_PERFORMANCE,
  CONSTRUCTION_STAGE_SALES, DISTANCE_PERFORMANCE, TICKET_SIZE_PERFORMANCE,
  UNIT_SIZE_PERFORMANCE, POSSESSION_DISTRIBUTION, BUYER_PROFILE, COMPETES_WITH)
- New queries: construction_stage, distance_analysis, ticket_size, unit_size,
  possession_distribution, new_launches, comparable_projects, price_range,
  project_count, micromarket_list
"""

QUERIES = {

    # ═══════════════════════════════════════
    # MARKET OVERVIEW (L0)
    # ═══════════════════════════════════════

    "market_overview": """
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

    "annual_overview": """
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

    # ═══════════════════════════════════════
    # PRICE TRENDS (L0)
    # ═══════════════════════════════════════

    "price_trend_saleable": """
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

    "price_trend_carpet": """
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

    # ═══════════════════════════════════════
    # QUARTERLY TRENDS (L0)
    # ═══════════════════════════════════════

    "quarterly_absorption": """
        MATCH (c:City {name: $city})-[r:QUARTERLY_TREND]->(q:Quarter)
        RETURN q.name AS quarter,
               r.sales_units AS sales_units,
               r.sales_sqft AS sales_sqft,
               r.supply_units AS supply_units,
               r.supply_sqft AS supply_sqft
        ORDER BY q.sort_order
    """,

    "velocity_trend": """
        MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
        WHERE r.velocity_pct IS NOT NULL
        RETURN q.name AS quarter, r.velocity_pct AS velocity
        ORDER BY q.sort_order
    """,

    "inventory_trend": """
        MATCH (c:City {name: $city})-[r:MARKET_SNAPSHOT]->(q:Quarter)
        WHERE r.months_inv IS NOT NULL
        RETURN q.name AS quarter, r.months_inv AS months_inventory
        ORDER BY q.sort_order
    """,

    # ═══════════════════════════════════════
    # PRODUCT INTELLIGENCE (L0+L1)
    # ═══════════════════════════════════════

    "flat_performance": """
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

    "ticket_size": """
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

    "unit_size_saleable": """
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

    "unit_size_carpet": """
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

    # ═══════════════════════════════════════
    # CONSTRUCTION STAGE (L1)
    # ═══════════════════════════════════════

    "construction_stage": """
        MATCH (c:City {name: $city})-[r:CONSTRUCTION_STAGE_SALES]->(cs:ConstructionStage)
        RETURN cs.name AS stage,
               r.annual_sales_units AS annual_sales,
               r.annual_sales_sqft AS annual_sales_sqft,
               r.qtr_sales_units AS qtr_sales,
               r.unsold_units AS unsold,
               r.unsold_sqft AS unsold_sqft
        ORDER BY cs.sort_order
    """,

    # ═══════════════════════════════════════
    # DISTANCE ANALYSIS (L0)
    # ═══════════════════════════════════════

    "distance_analysis": """
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

    # ═══════════════════════════════════════
    # POSSESSION DISTRIBUTION (L1)
    # ═══════════════════════════════════════

    "possession_distribution": """
        MATCH (c:City {name: $city})-[r:POSSESSION_DISTRIBUTION]->(py:PossessionYear)
        RETURN py.year AS year,
               r.marketable_supply_units AS supply_units,
               r.marketable_supply_sqft AS supply_sqft,
               r.sales_units AS sales_units,
               r.sales_sqft AS sales_sqft
        ORDER BY py.year
    """,

    # ═══════════════════════════════════════
    # PROJECT INTELLIGENCE (L1)
    # ═══════════════════════════════════════

    "top_projects_by_sales": """
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

    "top_projects_by_velocity": """
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

    "project_detail": """
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

    "project_competitors": """
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

    "comparable_projects": """
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

    "new_launches": """
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

    "project_count": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(:MicroMarket)-[:HAS_PROJECT]->(p:Project)
        RETURN count(p) AS total_projects
    """,

    # ═══════════════════════════════════════
    # MICROMARKET INTELLIGENCE (L0+L1)
    # ═══════════════════════════════════════

    "micromarket_list": """
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

    "micromarkets_by_demand": """
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

    "micromarkets_by_inventory_risk": """
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

    "micromarket_detail": """
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

    "emerging_micromarkets": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WHERE p.status = 'NEW_LAUNCH'
        WITH m, count(p) AS new_launches
        WHERE new_launches > 0
        RETURN m.name AS micromarket, new_launches
        ORDER BY new_launches DESC
    """,

    # ═══════════════════════════════════════
    # BUILDER INTELLIGENCE (L1)
    # ═══════════════════════════════════════

    "builder_rankings": """
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

    # ═══════════════════════════════════════
    # BUYER DEMOGRAPHICS (IGR)
    # ═══════════════════════════════════════

    "buyer_age_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'age'
        RETURN bs.value AS age_group, bs.count AS buyers
        ORDER BY bs.count DESC
    """,

    "buyer_gender_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'gender'
        RETURN bs.value AS gender, bs.count AS buyers
        ORDER BY bs.count DESC
    """,

    "buyer_locality_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'locality'
        RETURN bs.value AS locality, bs.count AS buyers
        ORDER BY bs.count DESC
        LIMIT 30
    """,

    "buyer_state_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'state'
        RETURN bs.value AS state, bs.count AS buyers
        ORDER BY bs.count DESC
    """,

    "buyer_religion_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'religion'
        RETURN bs.value AS religion, bs.count AS buyers
        ORDER BY bs.count DESC
    """,

    "buyer_language_dist": """
        MATCH (c:City {name: $city})-[:BUYER_PROFILE]->(bs:BuyerSegment)
        WHERE bs.dimension = 'language'
        RETURN bs.value AS language, bs.count AS buyers
        ORDER BY bs.count DESC
    """,

    # ═══════════════════════════════════════
    # YoY ANALYSIS
    # ═══════════════════════════════════════

    "yoy_absorption": """
        MATCH (c:City {name: $city})-[r:YEARLY_SNAPSHOT]->(fy:FiscalYear)
        RETURN fy.name AS fiscal_year,
               r.sales_units AS sales,
               r.supply_units AS supply,
               r.velocity_pct AS velocity,
               r.months_inv AS months_inv
        ORDER BY fy.sort_order
    """,

    # ═══════════════════════════════════════
    # VALIDATION
    # ═══════════════════════════════════════

    "validate_number": """
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

    # ═══════════════════════════════════════
    # NEARBY / ALL MICROMARKETS
    # ═══════════════════════════════════════

    "nearby_micromarkets": """
        MATCH (c:City {name: $city})-[:HAS_MICROMARKET]->(m:MicroMarket)
        OPTIONAL MATCH (m)-[:HAS_PROJECT]->(p:Project)
        WITH m, count(p) AS projects, sum(p.annual_sales_units) AS sales
        RETURN m.name AS micromarket, projects, sales
        ORDER BY sales DESC
    """,

    # ═══════════════════════════════════════
    # PRICE RANGE ANALYSIS (new for Gurugram)
    # ═══════════════════════════════════════

    "price_range_carpet": """
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

    "price_range_saleable": """
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

    # ═══════════════════════════════════════
    # RERA STATUS (new for Gurugram — data didn't exist in old Gurgaon)
    # ═══════════════════════════════════════

    "projects_by_rera_status": """
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
}
