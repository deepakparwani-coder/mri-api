// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║  PATCHED LOADER VERIFICATION                                              ║
// ║  Run these queries TWICE:                                                 ║
// ║    (1) On your CURRENT production Aura → save outputs as "BASELINE"       ║
// ║    (2) On the SANDBOX where you ran the patched loader → save as "NEW"    ║
// ║  Then compare. Numbers should match within ±1 (or +/- a known delta).    ║
// ║                                                                           ║
// ║  If a number differs → the patched loader has a regression vs migration   ║
// ║  cypher.  DO NOT promote the patch to production until rows match.        ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// V.1 — TOP-LEVEL CITY COUNTS
// Should match exactly between baseline and sandbox.
// ─────────────────────────────────────────────────────────────────────────────
MATCH (c:City {name:'Gurugram'})
OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm:MicroMarket)
OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p:Project)
RETURN c.name AS city,
       c.project_count AS city_project_count_prop,
       c.micromarket_count AS city_mm_count_prop,
       count(DISTINCT mm) AS actual_mm,
       count(DISTINCT p) AS actual_projects;
// EXPECT: city='Gurugram', mm=37, projects=71


// ─────────────────────────────────────────────────────────────────────────────
// V.2 — RELATIONSHIP TYPE INVENTORY (the heart of the v2 schema)
// Every rel listed here MUST exist with the correct count.
// If old-name rels (BUILT_BY, ANNUAL_SNAPSHOT, etc.) appear → REGRESSION.
// ─────────────────────────────────────────────────────────────────────────────
MATCH (c:City {name:'Gurugram'})-[r]->(x)
RETURN type(r) AS rel_type, labels(x) AS target, count(*) AS cnt
ORDER BY rel_type, target;
// EXPECT (paste current production output here as your baseline):
//   YEARLY_SNAPSHOT       → FiscalYear  : 5
//   MARKET_SNAPSHOT       → Quarter     : 12
//   SALEABLE_PRICE_AT     → Quarter     : 46
//   CARPET_PRICE_AT       → Quarter     : 46
//   QUARTERLY_TREND       → Quarter     : 46
//   MONTHLY_INVENTORY     → Quarter     : 46
//   SALES_VELOCITY        → Quarter     : 46
//   FLAT_PERFORMANCE      → FlatType    : 26
//   FLAT_QUARTERLY_SALES  → FlatType    : 32
//   FLAT_ANNUAL_SALES     → FlatType    : 32
//   FLAT_UNSOLD           → FlatType    : 32
//   UNIT_SIZE_PERFORMANCE → UnitSizeBand: 57
//   TICKET_SIZE_PERFORMANCE→ TicketSizeBand: 41
//   PRICE_BAND_PERFORMANCE→ PriceBand   : 98   (if loader handles this elsewhere)
//   DISTANCE_PERFORMANCE  → DistanceRange: 19
//   POSSESSION_DISTRIBUTION→ PossessionYear: 10
//   CONSTRUCTION_STAGE_SALES→ ConstructionStage: 9
//   HAS_MICROMARKET       → MicroMarket : 37
//   NEW_LAUNCH            → Project     : 10


// ─────────────────────────────────────────────────────────────────────────────
// V.3 — REGRESSION GUARD: NO old-name rels should remain
// Any non-zero count = regression. Patched loader must NOT emit these.
// ─────────────────────────────────────────────────────────────────────────────
MATCH ()-[r]->()
WHERE type(r) IN [
  'ANNUAL_SNAPSHOT', 'BUILT_BY', 'SALEABLE_PRICE', 'CARPET_PRICE',
  'QUARTERLY_SALES', 'STAGE_ANNUAL_SALES', 'STAGE_QUARTERLY_SALES',
  'STAGE_UNSOLD', 'SIZE_BAND_PERFORMANCE', 'TICKET_BAND_PERFORMANCE',
  'DISTANCE_BAND_PERFORMANCE'
]
RETURN type(r) AS old_name, count(*) AS cnt
ORDER BY old_name;
// EXPECT: 0 rows. ANY row here = regression.


// ─────────────────────────────────────────────────────────────────────────────
// V.4 — REGRESSION GUARD: NO old-name node labels should remain
// SizeBand, DistanceBand, PriceBand-with-basis-property all gone.
// ─────────────────────────────────────────────────────────────────────────────
CALL db.labels() YIELD label
WHERE label IN ['SizeBand', 'DistanceBand']
RETURN label;
// EXPECT: 0 rows.


// ─────────────────────────────────────────────────────────────────────────────
// V.5 — PROJECT NODE PROPERTY KEYS
// Verify the v2 Project property names exist; old names are gone.
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram'})
WITH p LIMIT 5
WITH keys(p) AS props
UNWIND props AS prop
RETURN DISTINCT prop ORDER BY prop;
// EXPECT to SEE: location, sold_pct, unsold_pct, monthly_velocity,
//                annual_months_inv, quarterly_months_inv,
//                saleable_rate_psf, saleable_rate_range, status (NEW_LAUNCH ones)
// EXPECT to NOT see: sold_percent, unsold_percent, monthly_sales_velocity,
//                    annual_months_inventory, quarterly_months_inventory


// ─────────────────────────────────────────────────────────────────────────────
// V.6 — PROJECT.location DENORMALIZATION (post_load_denormalize step)
// Every Project should have p.location matching its MicroMarket name.
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram'})-[:IN_MICROMARKET]->(m:MicroMarket)
WITH p, m, p.location = m.name AS matches
RETURN matches, count(*) AS n
ORDER BY matches;
// EXPECT: matches=true, n=71. matches=false should be 0.


// ─────────────────────────────────────────────────────────────────────────────
// V.7 — RANGE STRINGS BUILT (post_load_denormalize step)
// Sample 5 projects: confirm saleable_rate_range, total_cost_range etc. populated.
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram'})
WHERE p.saleable_rate_min_psf IS NOT NULL
RETURN p.name,
       p.saleable_rate_min_psf, p.saleable_rate_max_psf,
       p.saleable_rate_range, p.saleable_rate_psf,
       p.total_cost_min_lacs, p.total_cost_max_lacs, p.total_cost_range
LIMIT 5;
// EXPECT: range and psf median both populated. range looks like "12000-15000".


// ─────────────────────────────────────────────────────────────────────────────
// V.8 — DEVELOPED_BY (was BUILT_BY)
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram'})-[:DEVELOPED_BY]->(b:Builder)
RETURN count(*) AS developed_by_rels, count(DISTINCT b) AS distinct_builders;
// EXPECT current production: developed_by_rels=72, distinct_builders=~30


// ─────────────────────────────────────────────────────────────────────────────
// V.9 — PROJECT_TOP_SALES rel exists with v2 prop names
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram'})-[r:PROJECT_TOP_SALES]->(q:Quarter {name:'Q3 25-26'})
WHERE r.annual_value_cr IS NOT NULL
RETURN p.name, r.annual_sales_units, r.annual_sales_sqft, r.annual_value_cr,
       r.monthly_velocity, r.annual_months_inv, r.sold_pct, r.unsold_pct
LIMIT 5;
// EXPECT: 5 rows with all props populated; old names like
//   r.annual_value_sales_cr / r.annual_months_inventory should NOT exist.


// ─────────────────────────────────────────────────────────────────────────────
// V.10 — Quarter & FY sort_order populated
// ─────────────────────────────────────────────────────────────────────────────
MATCH (q:Quarter) WHERE q.sort_order IS NULL RETURN count(q) AS quarters_missing_sort_order;
// EXPECT: 0

MATCH (fy:FiscalYear) WHERE fy.sort_order IS NULL RETURN count(fy) AS fy_missing_sort_order;
// EXPECT: 0


// ─────────────────────────────────────────────────────────────────────────────
// V.11 — NEW_LAUNCH projects tagged with status
// ─────────────────────────────────────────────────────────────────────────────
MATCH (p:Project {city_name:'Gurugram', status:'NEW_LAUNCH'})
RETURN count(p) AS tagged_new_launch_projects;
// EXPECT: ~10 (matches NEW_LAUNCH rel count from V.2)


// ─────────────────────────────────────────────────────────────────────────────
// V.12 — IMMUTABLE CITIES UNCHANGED
// Hinjewadi must be exactly as before. Kolkata wiped (we did this manually).
// ─────────────────────────────────────────────────────────────────────────────
MATCH (c:City)
OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm:MicroMarket)
OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p:Project)
RETURN c.name AS city,
       count(DISTINCT mm) AS micromarkets,
       count(DISTINCT p) AS projects
ORDER BY c.name;
// EXPECT: Gurugram=37/71, Hinjewadi=11/93. (Kolkata gone.)
//         Hinjewadi numbers must be IDENTICAL between baseline and sandbox runs.
