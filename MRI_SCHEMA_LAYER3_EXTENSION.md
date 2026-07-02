# MRI Schema Extension — Layer 3 (Subject-Project Benchmarking)

**Status:** Draft for review · **Naming convention:** v2 (consistent with production Gurugram/Hinjewadi)
**Source files:** 15 Kolkata Layer 3 files (13× `Project_and_Benchmark_Location_Price_Trend_*`, `ProjectDetailsFlatwise`, `Project_Marketable_Wings`)
**Subject project in this dataset:** Hive Urban Utopia (Block-1, 16 units, Plinth, launched 02-Aug-2025)
**Data recency:** benchmark series runs Q1 11-12 → Q2 25-26 (58 quarters)

---

## 1. Design principles

1. **Reuse existing rel-type names at a new grain, never invent parallel names.** The city grain already has `SALEABLE_PRICE_AT`, `FLAT_PERFORMANCE`, `TICKET_SIZE_PERFORMANCE`. Layer 3 writes the *same relationship types* from `Project` and `MicroMarket` nodes. The query layer distinguishes grain by the start-node label, not the rel name.
2. **The "gap" is computed at query time, never stored.** Project-vs-benchmark deltas are derived in Cypher from the two series; storing them would create staleness bugs.
3. **One relationship per (entity, dimension-value), many properties.** The 13 files collapse into 4 relationship writes, not 13.
4. **Subject projects are flagged, not special-cased.** `p.is_subject_project = true` + `(p)-[:BENCHMARKED_AGAINST]->(mm)` lets queries discover which projects carry deep-dive data.

---

## 2. File → graph mapping

### Group A — Quarterly price series (files _0 through _6 → ONE rel type)

Seven files, each one metric, same 58-quarter axis, each with a Project column and a Location column.

| File suffix | Metric column pair | Property name |
|---|---|---|
| (none) | Wt Avg Saleable Price | `wt_avg_unsold` |
| _1 | Absorption Saleable Price | `absorption_price` |
| _2 | Median Saleable Price | `median_price` |
| _3 | Minimum Saleable Price | `minimum` |
| _4 | Maximum Saleable Price | `maximum` |
| _5 | Average Saleable Price | `average_price` |
| _6 | Wt Avg New Supply Price | `new_supply_price` |

**Writes:**
```
(Project {name})-[:SALEABLE_PRICE_AT {wt_avg_unsold, absorption_price,
    median_price, minimum, maximum, average_price, new_supply_price}]->(Quarter)

(MicroMarket {name})-[:SALEABLE_PRICE_AT {same 7 props}]->(Quarter)
```
- Project side: only quarters where the project column is non-null (post-launch, ~Q2 25-26 onward).
- MicroMarket side: full 58-quarter benchmark history. **This is new capability** — production currently has price trends only at City grain; this gives every benchmarked micromarket its own series.
- Property names `minimum` / `maximum` / `new_supply_price` deliberately match the loader-patch renames at city grain.
- Loader merges the 7 files on `Financial Quarter` before writing (one MERGE per quarter, not seven).

### Group B — Distribution comparisons (files _7/_8, _9/_10, _11/_12 → three rel types)

Each pair = one dimension × (supply file + sales file) × (unit % + sqft %). Merge each pair into one rel with 4 properties: `supply_unit_pct, supply_sqft_pct, sales_unit_pct, sales_sqft_pct`.

| File pair | Dimension axis | Target node (existing label) | Rel type (existing name) |
|---|---|---|---|
| _7 + _8 | Flat (1BHK…OpenPlot bands) | `FlatType {name}` | `FLAT_PERFORMANCE` |
| _9 + _10 | Cost Range (₹ bands to 8–9 Cr) | `TicketSizeBand {name}` | `TICKET_SIZE_PERFORMANCE` |
| _11 + _12 | Price Range (Rs PSF bands) | `PriceBand {name}` | `PRICE_RANGE_PERFORMANCE` |

Written twice per row: once from `Project`, once from `MicroMarket` (the benchmark).
Note: the Flat axis here includes OpenPlot size bands (e.g. "OpenPlot 2501-3000") — these MERGE as `FlatType` nodes like any other; do not route them to `UnitSizeBand`.

### ProjectDetailsFlatwise → flat-level configuration

```
(Project)-[:FLAT_CONFIGURATION {total_supply_units, sold_units, sold_pct,
    min_saleable_size, max_saleable_size, min_carpet_size, max_carpet_size,
    saleable_sizes, carpet_sizes, min_cost_lacs, max_cost_lacs}]->(FlatType)
```
Distinct from `FLAT_PERFORMANCE` (distribution %) — this is absolute unit config + ticket range per typology. `sold_pct` matches the Project-node property rename from the loader patch.

### Project_Marketable_Wings → new node label `Wing`

The only genuinely new label in this extension.

```
(Project)-[:HAS_WING]->(Wing {name, status, supply_units, supply_sqft,
    sold_units, sold_sqft, unsold_units, unsold_sqft,
    saleable_price, carpet_price, launch_date, possession_date,
    construction_status, construction_pct, flat_type,
    sold_pct, saleable_size, carpet_size, total_cost})
```
Constraint: `(Wing.name, project)` uniqueness via composite key `wing_id = project_name + '|' + wing_name` (wing names like "Block - 1" repeat across projects).

### Subject-project markers (loader writes once per subject project)

```
p.is_subject_project = true
p.benchmark_data_through = 'Q2 25-26'
(p)-[:BENCHMARKED_AGAINST]->(mm:MicroMarket)
```

---

## 3. Summary of graph deltas

| Item | Kind | New? |
|---|---|---|
| `Wing` | node label | ✅ new (+ uniqueness constraint) |
| `HAS_WING`, `FLAT_CONFIGURATION`, `BENCHMARKED_AGAINST`, `PRICE_RANGE_PERFORMANCE` | rel types | ✅ new |
| `SALEABLE_PRICE_AT`, `FLAT_PERFORMANCE`, `TICKET_SIZE_PERFORMANCE` | rel types | ♻️ reused at Project + MicroMarket grain |
| `is_subject_project`, `benchmark_data_through` | Project props | ✅ new |

No existing relationship or property is renamed. Zero impact on current Gurugram/Hinjewadi queries.

---

## 4. The queries this unlocks (for `cypher_queries.py`)

**`project_benchmark_price_gap`** — the core developer question:
```cypher
MATCH (p:Project {name:$project_name})-[pr:SALEABLE_PRICE_AT]->(q:Quarter)
MATCH (p)-[:BENCHMARKED_AGAINST]->(mm:MicroMarket)-[lr:SALEABLE_PRICE_AT]->(q)
RETURN q.name AS quarter,
       pr.wt_avg_unsold  AS project_asking,  lr.wt_avg_unsold  AS location_asking,
       pr.absorption_price AS project_transacted, lr.absorption_price AS location_transacted,
       round(100.0*(pr.wt_avg_unsold - lr.wt_avg_unsold)/lr.wt_avg_unsold,1) AS asking_gap_pct
ORDER BY q.sort_order
```

**`project_benchmark_flat_mix`** — "is my product mix aligned with what the catchment absorbs?":
```cypher
MATCH (p:Project {name:$project_name})-[pf:FLAT_PERFORMANCE]->(ft:FlatType)
MATCH (p)-[:BENCHMARKED_AGAINST]->(mm)-[lf:FLAT_PERFORMANCE]->(ft)
RETURN ft.name AS flat_type,
       pf.supply_unit_pct AS my_supply_share, lf.supply_unit_pct AS market_supply_share,
       lf.sales_unit_pct  AS market_sales_share,
       lf.sales_unit_pct - lf.supply_unit_pct AS market_demand_supply_gap
ORDER BY market_sales_share DESC
```
(`market_demand_supply_gap > 0` ⇒ typology sells faster than it's supplied — build more of it.)

**`micromarket_price_trend`** — free by-product; also answers non-subject queries:
```cypher
MATCH (mm:MicroMarket {name:$location})-[r:SALEABLE_PRICE_AT]->(q:Quarter)
RETURN q.name, r.wt_avg_unsold, r.absorption_price, r.new_supply_price
ORDER BY q.sort_order
```

**`project_wing_status`** — wing-level sales/construction detail:
```cypher
MATCH (p:Project {name:$project_name})-[:HAS_WING]->(w:Wing)
RETURN w.name, w.status, w.construction_status, w.construction_pct,
       w.supply_units, w.sold_units, w.sold_pct, w.saleable_price
```

---

## 5. Loader implementation notes (`build_graph_v3_gurugram.py`)

1. New route entries: `Project_and_Benchmark_Location_Price_Trend*` → `handle_benchmark_trend` (Group A) or `handle_benchmark_distribution` (Group B) — disambiguate by header row 7 (`Financial Quarter` vs `Flat`/`Cost Range`/`Price Range`). Do NOT key on the file-number suffix; portal download order isn't guaranteed.
2. All files share the layout: 6 blank rows, header at row 7 (index 6), data from row 8. `pd.read_excel(f, header=6)`.
3. Group A: outer-join the 7 frames on quarter; skip project-side MERGE where project value is null.
4. The subject project's micromarket comes from `--subject-mm` CLI arg (or resolved from `Catchment_Projects`); the loader needs it to write the MicroMarket-side series and `BENCHMARKED_AGAINST`.
5. Add `--subject-project` CLI arg alongside `--city` so the same loader handles future subject projects in any city.
6. `CITY_EXPECTATIONS` for Kolkata gains: `wings ≥ 1`, `benchmark_quarters = 58`.

---

## 6. What this makes possible (product view)

With this loaded, the bot answers, from `[DB]`-badged data alone:
- "How is Hive Urban Utopia priced vs its location?" → 58-quarter benchmark + post-launch gap.
- "Is its 3BHK-only mix right for this catchment?" → flat-mix demand-supply gap table.
- "Which ticket bands absorb here?" → Group B ticket distribution.
- "Wing-level sales status?" → Wing nodes.

And the template scales: any developer's project = one LF deep-dive download + `--subject-project` load.
