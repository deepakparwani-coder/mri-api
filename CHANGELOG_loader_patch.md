# build_graph_v3_gurugram.py — PATCHED (v2-schema-native)

**Patch date:** 29 April 2026
**Replaces:** v3-emitting loader + 20-step `gurugram_schema_migration.cypher` chaser
**Why:** loader now writes v2 schema names directly. Migration cypher is no longer needed for fresh loads. Future reloads of Gurugram, or first loads of any new city, just work.

---

## How to use

```bash
# Default: load Gurugram from a directory of 108 .xlsx files
python build_graph_v3_gurugram.py --dir "./NCR NEW BUILD"

# Load a different city (just point at its xlsx folder)
python build_graph_v3_gurugram.py --dir "./Mumbai" --city Mumbai \
       --quarter "Q3 25-26" --fy "FY-2025-2026"

# Re-load (upsert) over existing data
python build_graph_v3_gurugram.py --dir "./NCR NEW BUILD" --allow-upsert
```

Connection comes from env vars (unchanged): `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`.

---

## What changed vs. the previous file

### A. Relationship type renames (loader now emits v2 names directly)

| Old (v3, what loader used to emit) | New (v2, what queries read) |
|---|---|
| `ANNUAL_SNAPSHOT` | `YEARLY_SNAPSHOT` |
| `SALEABLE_PRICE` | `SALEABLE_PRICE_AT` |
| `CARPET_PRICE` | `CARPET_PRICE_AT` |
| `QUARTERLY_SALES` | `QUARTERLY_TREND` |
| `BUILT_BY` | `DEVELOPED_BY` |
| `STAGE_QUARTERLY_SALES` + `STAGE_ANNUAL_SALES` + `STAGE_UNSOLD` (3 rels) | merged into `CONSTRUCTION_STAGE_SALES` (1 rel, props overlaid) |
| `SIZE_BAND_PERFORMANCE` → `SizeBand` | `UNIT_SIZE_PERFORMANCE` → `UnitSizeBand` |
| `TICKET_BAND_PERFORMANCE` → `PriceBand{basis:'ticket'}` | `TICKET_SIZE_PERFORMANCE` → `TicketSizeBand` |
| `DISTANCE_BAND_PERFORMANCE` → `DistanceBand` | `DISTANCE_PERFORMANCE` → `DistanceRange` |

### B. Property renames (also v2-native now)

**On `MARKET_SNAPSHOT` and `YEARLY_SNAPSHOT` rels** (via `MARKER_METRIC_MAP`):
`marketable_supply_units` → `supply_units`, `sales_sqft_mn` → `sales_sqft`, `value_of_stock_sold_cr` → `value_sold_cr`, `unsold_sqft_mn` → `unsold_sqft`, `sales_velocity_pct` → `velocity_pct`, `months_inventory` → `months_inv`, `newsupply_units` → `new_supply_units`, etc.

**On `SALEABLE_PRICE_AT` / `CARPET_PRICE_AT` rels** (via price-timeseries handler):
`min` → `minimum`, `max` → `maximum`, `wt_avg_new_supply` → `new_supply_price`.

**On `Project` nodes** (via `extract_project_rows` + `write_projects`):
`sold_percent` → `sold_pct`, `unsold_percent` → `unsold_pct`, `monthly_sales_velocity` → `monthly_velocity`, `annual_months_inventory` → `annual_months_inv`, `quarterly_months_inventory` → `quarterly_months_inv`.

**On `PROJECT_TOP_SALES` rel** (via `write_top10_rel` + `handle_top10`):
`annual_value_sales_cr` → `annual_value_cr`, `annual_months_inventory` → `annual_months_inv`, `quarterly_months_inventory` → `quarterly_months_inv`.

**On `POSSESSION_DISTRIBUTION` rel** (bug fix — was NEVER aligned with queries):
`marketable_supply_sqft_mn` → `marketable_supply_sqft`, `sales_sqft_mn` → `sales_sqft`. Existing migration cypher missed this; query was broken silently.

**On `FLAT_ANNUAL_SALES` rel** (bug fix — same root cause):
`annual_sales_sqft_mn` → `annual_sales_sqft`.

### C. Post-load denormalization (replaces migration steps 11/13/14)

New `FastLoader.post_load_denormalize()` runs after all files are loaded:

1. Copies metrics from `PROJECT_TOP_SALES{Q3 25-26}` rel onto Project node (so `top_projects_by_sales` query can read `p.annual_sales_units` directly without traversing).
2. Builds range strings (`p.saleable_rate_range = "12000-15000"`) and PSF medians (`p.saleable_rate_psf = 13500`) from min/max pairs.
3. Copies `MicroMarket.name` onto `Project.location` so legacy queries reading `p.location` still work.

### D. NEW_LAUNCH tagging
`write_new_launch_rel` now also sets `p.status = 'NEW_LAUNCH'` (replaces migration step 15).

### E. Quarter and FiscalYear `sort_order`
- `Quarter.sort_order` was already correct.
- `FiscalYear.sort_order` now set on creation (was migration step 17).

### F. City-agnostic
- `--city`, `--source`, `--quarter`, `--fy` are CLI args.
- `IMMUTABLE_CITIES` is auto-derived at runtime as "every existing City except the one being loaded" — adding a new city no longer requires editing this file.
- `classify_sub_region` is data-driven via `SUB_REGION_RULES` dict; add an entry for new cities.
- `CITY_EXPECTATIONS` dict centralizes per-city expected counts; missing entries warn rather than fail.
- All Cypher in `finalize_city_counts`, `preflight_checks`, `run_verification` parameterized on `$city`.

### G. Constraints updated
v3 label constraints (`PriceBand{name,basis}`, `SizeBand{name,basis}`, `DistanceBand`) replaced with v2 constraints (`PriceBand{name}`, `TicketSizeBand{name}`, `UnitSizeBand{name,area_type}`, `DistanceRange`). Added missing `City{name}` constraint.

---

## What did NOT change

- `BAND_COL_MAP` and `FLAT_PERF_COL_MAP` — these prop names (`monthly_sales_velocity_pct`, `annual_marketable_supply_units`, etc.) are correct as-is and are read by `cypher_queries.py` under those names. Migration cypher never touched them.
- File parsing / Excel-reading logic — unchanged.
- Performance characteristics — unchanged (still ~2 minutes for 108 files).

---

## Migration path for the production graph

**Do not reload production yet.** Production is currently in a working v2 state (verified: `YEARLY_SNAPSHOT=5`, `SALEABLE_PRICE_AT=46`, `DEVELOPED_BY=72`). The patched loader is intended for:

1. **Verification first** — run on a sandbox Aura instance, then run `sandbox_verification.cypher` to confirm output matches production state. Only after rows match, consider promoting.
2. **Future reloads** — next time Gurugram needs a refresh (data update, bug fix), run the patched loader instead of "load v3 + apply 20-step migration."
3. **New cities** — when onboarding Mumbai/Bangalore/etc., use the patched loader from day one. No migration step needed.

---

## Files

- `build_graph_v3_gurugram.py` — patched loader (drop-in replacement)
- `sandbox_verification.cypher` — 12 verification queries to run on sandbox vs production for diff
- `gurugram_schema_migration.cypher` — **archived as historical artifact**. Do not run after the patched loader. Keep in repo for reference only.

---

## Pendency status after this patch

| Pendency | Status |
|---|---|
| P1-#1 Run migration cypher | **Obsolete** — patched loader makes this unnecessary |
| P3-#5 Patch loader to v2 names | **Done** (this changelog) |
| P1-#2 Push to GitHub / Railway | Still pending — you push when ready |
| P2-#3 Table rendering bug | Still pending |
| P2-#4 PDF page-break bug | Still pending |
| P3-#6 MicroMarket false-match | Still pending |
| P3-#7 Embedded KB JSON decision | Still pending |
| P4-#9 Glossary enforcement | Still pending |
| P4-#10 Google Maps integration | Still pending |
