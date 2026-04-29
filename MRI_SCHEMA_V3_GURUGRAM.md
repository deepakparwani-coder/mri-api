# MRI Schema v3 — Gurugram Load Design

**Status:** Draft for sign-off
**Author:** Claude (design), Deepak (owner)
**Date:** 17 April 2026
**Supersedes:** Previous Gurgaon subgraph (deleted 17 April 2026)

---

## 1. Scope and intent

This document specifies the complete schema, data lineage, and loader behavior for the new Gurugram subgraph in the MRI Neo4j Aura instance (`c26f3089.databases.neo4j.io`).

**Input:** 108 Excel files in `Gurugram_NEW_BUILD/NCR NEW BUILD/`, covering 24 unique report types.
**Output:** One new `City: Gurugram` subgraph in the existing v2 graph, sitting alongside Hinjewadi and Kolkata without modifying either.
**Out of scope:** Any changes to Hinjewadi or Kolkata nodes, relationships, or Cypher queries. Multi-city comparison logic. UI changes.

**Frozen decisions (from chat with Deepak):**

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Micromarket names kept verbatim | Loader idempotence; no re-normalization risk |
| 2 | Three sub-region values: Gurugram, Sohna, Dwarka | Future-proof even if Dwarka stays small |
| 3 | `Project Name` as primary key; `project_id` as property | Cross-city query consistency + migration path |
| 4 | Read all 108 files, MERGE-idempotent, log conflicts | "Used complete dataset" defensibility |
| 5 | Separate `build_graph_v3_gurugram.py` loader | Preserves v2 for rollback |
| 6 | Store RERA registration as Project property | Enables regulatory-filter queries |

---

## 2. Scope summary of the new data

Derived from exhaustive inspection of all 108 files:

| Dimension | Count |
|-----------|-------|
| Unique projects (by Project Id) | 71 |
| Unique micromarkets | 37 |
| Unique developers | 44 |
| Quarters covered | Q2 14-15 through Q3 25-26 (46 quarters) |
| Fiscal years covered | FY-2021-2022 through FY-2025-2026 (5 fiscal years of summary data) |
| Latest quarter | **Q3 25-26** (Oct–Dec 2025) |

**Sub-region distribution of 71 projects:**
- Gurugram proper: 62 projects (87%)
- Sohna: 8 projects (11%)
- Dwarka (single micromarket "Sector - 36A, Dwarka"): 1 project (1%)

---

## 3. Schema

### 3.1 New / reused node labels

| Label | Reused from v2? | Purpose |
|-------|-----------------|---------|
| `City` | Reused | New instance `{name: "Gurugram"}` |
| `MicroMarket` | Reused | 37 new instances |
| `Project` | Reused | 71 new instances |
| `Builder` | Reused | 44 new instances (MERGE on name; any that already exist from Hinjewadi/Kolkata are deduplicated) |
| `Quarter` | Reused | MERGE — most likely already exist from Hinjewadi/Kolkata loads |
| `FiscalYear` | Reused | MERGE — likely already exist |
| `FlatType` | Reused | e.g., `{name: "3BHK"}`, `{name: "Duplex/Penthouse"}` |
| `ConstructionStage` | Reused | e.g., `{name: "Plinth"}`, `{name: "Finishing"}` |
| `PriceBand` | **New for Gurugram** | For price-range analysis aggregates; one per range like `"Rs 50-75 Lacs"` |
| `SizeBand` | **New for Gurugram** | For unit-size-range aggregates; one per range like `"500-750 sqft"` |
| `DistanceBand` | **New for Gurugram** | For distance-range analysis; e.g. `"0-2 KM"`, `"8-10 KM"` |
| `PossessionYear` | **New for Gurugram** | For possession-wise distribution; simple `{year: 2027}` |

**Note on the four "new" labels:** These categorical aggregates (price bands, size bands, distance bands, possession years) were present as raw columns in the old v2 schema but not modeled as nodes. Modeling them as nodes now lets queries like *"what's the absorption in the Rs 75L-1Cr band"* resolve in one hop, rather than string-parsing a property. Retrofit to Hinjewadi/Kolkata is explicitly out of scope for this doc.

### 3.2 Node property specifications

#### `City: Gurugram`

| Property | Type | Value | Source |
|----------|------|-------|--------|
| `name` | string | `"Gurugram"` | Hardcoded (overrides LF's "NCR" label) |
| `source_label` | string | `"NCR"` | From `New_Launch_Project_Details.City` column — audit trail for future confusion |
| `latest_quarter` | string | `"Q3 25-26"` | Derived from max quarter found across all files |
| `latest_fiscal_year` | string | `"FY-2025-2026"` | Derived from Yearly_Marker_Summary |
| `last_refreshed` | date | Load-run timestamp | `datetime().date()` at load time |
| `project_count` | int | 71 | Computed at end of load |
| `micromarket_count` | int | 37 | Computed at end of load |

#### `MicroMarket`

| Property | Type | Value | Source |
|----------|------|-------|--------|
| `name` | string | Verbatim location string | `Location` column from any project-level file |
| `sub_region` | string | `"Gurugram"` / `"Sohna"` / `"Dwarka"` | See §5 classification rule |
| `project_count` | int | Computed at end of load | — |

**Uniqueness:** `(name, city_name)` — a MicroMarket node belongs to exactly one City.

#### `Project`

| Property | Type | Source |
|----------|------|--------|
| `name` | string **[PK within city]** | `Project Name` column |
| `project_id` | string | `Project Id` column (e.g., "137314") |
| `launch_date` | string | `Launch Date` column (see §6.2 for format handling) |
| `possession_date` | string | `Possession Date` column (see §6.2) |
| `total_supply_units` | int | `Total Supply (Units)` |
| `total_supply_sqft` | int | `Total Supply (Sq.Ft.)` |
| `sold_percent` | float | `Sold as on Date (%)` |
| `unsold_percent` | float | `Unsold as on Date (%)` if present |
| `project_size` | int | `Project Size` if present (from Top 10 files) |
| `flat_types` | list[string] | `Flat Type` parsed as comma-separated (e.g., `["2BHK", "3BHK", "4BHK"]`) |
| `saleable_rate_min_psf` | int | Parsed min of `Saleable Rate (Rs/PSF)` when it's a range like `"5700"` or `"5700-8028"` |
| `saleable_rate_max_psf` | int | Parsed max of same |
| `carpet_rate_min_psf` | int | Parsed min of `Carpet Rate (Rs/PSF)` |
| `carpet_rate_max_psf` | int | Parsed max |
| `saleable_size_min_sqft` | int | Parsed min of `Saleable Size (Sq.Ft.)` |
| `saleable_size_max_sqft` | int | Parsed max |
| `carpet_size_min_sqft` | int | Parsed min of `Carpet Size (Sq.Ft.)` |
| `carpet_size_max_sqft` | int | Parsed max |
| `total_cost_min_lacs` | float | Parsed min of `Total Cost (Rs.Lacs)` |
| `total_cost_max_lacs` | float | Parsed max |
| `monthly_sales_velocity` | float | `Monthly Sales Velocity` when present |
| `annual_months_inventory` | int | `Annual Months Inventory` |
| `quarterly_months_inventory` | int | `Quarterly Months Inventory` |
| `rera_registered` | string | `RERA Registered` column; values like `"GGM/954/686/2025/57"` or `"Not Available"` |
| `rera_status` | string | Derived: `"Registered"` if `rera_registered` starts with `"GGM/"`; else `"Not Available"` |

**Uniqueness constraint:** `(name, city_name)` within the Gurugram subgraph. Duplicate detection rule — if during load the same (name, city) is seen with different `project_id`, **loader halts with error** (see §7 Safety).

#### `Builder`

| Property | Type | Source |
|----------|------|--------|
| `name` | string **[PK]** | `Builder` / `Developer Name` column |

Same `Builder` node may already exist from Hinjewadi/Kolkata loads — MERGE prevents duplication.

#### `Quarter`

| Property | Type | Value |
|----------|------|-------|
| `name` | string **[PK]** | e.g., `"Q3 25-26"` |
| `fy_short` | string | Derived `"FY25-26"` |
| `quarter_num` | int | 1–4 derived from name |
| `sort_order` | int | Computed as `year*10 + quarter` for easy `ORDER BY` |

#### `FiscalYear`

| Property | Type | Value |
|----------|------|-------|
| `name` | string **[PK]** | e.g., `"FY-2025-2026"` |
| `start_year` | int | 2025 |
| `end_year` | int | 2026 |

#### `FlatType`

| Property | Type | Value |
|----------|------|-------|
| `name` | string **[PK]** | Verbatim from `Flat` column: `"1BHK"`, `"2.5/3 BHK"`, `"Duplex/Penthouse"`, `"Floors"`, etc. |

#### `ConstructionStage`

| Property | Type | Value |
|----------|------|-------|
| `name` | string **[PK]** | Verbatim from `Progress` column |

#### `PriceBand`, `SizeBand`, `DistanceBand`, `PossessionYear` — new aggregate-dimension nodes

| Label | PK | Example |
|-------|----|---------| 
| `PriceBand` | `(name, basis)` where `basis ∈ {"carpet", "saleable"}` | `{name: "Rs 50-75 Lacs", basis: "carpet"}` |
| `SizeBand` | `(name, basis)` where `basis ∈ {"carpet", "saleable"}` | `{name: "500-750 sqft", basis: "saleable"}` |
| `DistanceBand` | `name` | `{name: "8-10 KM"}` |
| `PossessionYear` | `year` | `{year: 2027}` |

### 3.3 Relationship schema

All relationships exist within the Gurugram subgraph (sourced from/terminating at Gurugram-scoped nodes) unless otherwise noted.

| Relationship | From → To | Properties | Source file(s) |
|--------------|-----------|------------|----------------|
| `HAS_MICROMARKET` | `City` → `MicroMarket` | — | Implicit from any project-level file |
| `IN_MICROMARKET` | `Project` → `MicroMarket` | — | Project-level files (`Location` column) |
| `BUILT_BY` | `Project` → `Builder` | — | Project-level files |
| `HAS_PROJECT` | `MicroMarket` → `Project` | — | Reverse of `IN_MICROMARKET` (inserted for symmetry with v2) |
| `CARPET_PRICE` | `City` → `Quarter` | `wt_avg`, `absorption`, `average`, `median`, `min`, `max`, `wt_avg_new_supply` | `Carpet_Area_Price_(Rs_PSF)_Data*.xlsx` |
| `SALEABLE_PRICE` | `City` → `Quarter` | `wt_avg`, `absorption`, `average`, `median`, `min`, `max`, `wt_avg_new_supply` | `Saleable_Area_Price_(Rs_PSF)_Data*.xlsx` |
| `MARKET_SNAPSHOT` | `City` → `Quarter` | `marketable_supply_units`, `marketable_supply_sqft_mn`, `sales_units`, `sales_sqft_mn`, `value_of_stock_sold_cr`, `unsold_units`, `unsold_sqft_mn`, `unsold_value_cr`, `cost_of_flat_lacs`, `sales_velocity_pct`, `months_inventory`, `newsupply_units`, `newsupply_sqft_mn` | `Quarterly_Marker_Summary*.xlsx` |
| `ANNUAL_SNAPSHOT` | `City` → `FiscalYear` | Same metrics as MARKET_SNAPSHOT | `Yearly_Marker_Summary.xlsx` |
| `MONTHLY_INVENTORY` | `City` → `Quarter` | `months_inventory` | `Months_Inventory_(Months)_Data.xlsx` |
| `SALES_VELOCITY` | `City` → `Quarter` | `velocity_pct` | `Sales_Velocity_(%_Monthly_Sales)_Data.xlsx` |
| `QUARTERLY_SALES` | `City` → `Quarter` | `sales_units`, `marketable_supply_units`, `sales_sqft_mn`, `marketable_supply_sqft_mn` | `Quarterly_Sales_&_Marketable_Supply_Data*.xlsx` |
| `FLAT_PERFORMANCE` | `City` → `FlatType` | All columns from `Flat_Type_Analysis_Data` (24 metrics; see §3.4) | `Flat_Type_Analysis_Data*.xlsx` |
| `FLAT_ANNUAL_SALES` | `City` → `FlatType` | `annual_sales_units`, `annual_sales_sqft_mn` | `Annual_Sales_Data.xlsx` |
| `FLAT_QUARTERLY_SALES` | `City` → `FlatType` | `sales_units`, `sales_sqft` | `Quarterly_Sales_Data*.xlsx` |
| `FLAT_UNSOLD` | `City` → `FlatType` | `unsold_units`, `unsold_sqft` | `Unsold_Stock_Data*.xlsx` |
| `STAGE_ANNUAL_SALES` | `City` → `ConstructionStage` | `annual_sales_units`, `annual_sales_sqft` | `Annual_Sales_Data_as_per_Construction_Stage*.xlsx` |
| `STAGE_QUARTERLY_SALES` | `City` → `ConstructionStage` | `sales_units`, `sales_sqft` | `Quarterly_Sales_Data_as_per_Construction_Stage*.xlsx` |
| `STAGE_UNSOLD` | `City` → `ConstructionStage` | `unsold_units`, `unsold_sqft` | `Unsold_Stock_Data_as_per_Construction_Stage*.xlsx` |
| `PRICE_BAND_PERFORMANCE` | `City` → `PriceBand` | 22 columns from Price_Range_Analysis (see §3.4) | `Price_Range_Analysis_*.xlsx` |
| `SIZE_BAND_PERFORMANCE` | `City` → `SizeBand` | 22 columns from Unit_Size_Range_Analysis | `Unit_Size_Range_Analysis_*.xlsx` |
| `DISTANCE_BAND_PERFORMANCE` | `City` → `DistanceBand` | 19 columns from Distance_Range_Analysis | `Distance_Range_Analysis_*.xlsx` |
| `TICKET_BAND_PERFORMANCE` | `City` → `PriceBand` with `basis: "ticket"` | 18 columns from Unit_Ticket_Size_Analysis | `Unit_Ticket_Size_Analysis_Data*.xlsx` |
| `POSSESSION_DISTRIBUTION` | `City` → `PossessionYear` | `marketable_supply_units`, `marketable_supply_sqft_mn`, `sales_units`, `sales_sqft_mn` | `Possession_Wise_Marketable_Supply_&_Sales_Distribution_Data*.xlsx` |
| `NEW_LAUNCH` | `Project` → `City` | `launch_date`, `end_date` | `New_Launch_Project_Details*.xlsx` (indicates project is a *recent* launch) |
| `PROJECT_TOP_SALES` | `Project` → `Quarter` | Full Top-10 row: `annual_sales_units`, `annual_sales_sqft`, `annual_value_sales_cr`, `monthly_velocity`, etc. | `Top_10_Project_Data_(ANNUALSALES)*.xlsx` — represents ranking as of the Q3 25-26 snapshot |

### 3.4 Property packing

For relationships carrying 20+ metrics (e.g. `FLAT_PERFORMANCE`, `PRICE_BAND_PERFORMANCE`), properties are stored on the relationship itself rather than on a separate "snapshot" node. Rationale: these aren't time-series (they're one-snapshot-per-dimension values), so a relationship with properties is the cleanest fit. Cypher queries remain one-hop: `MATCH (c:City {name:'Gurugram'})-[r:FLAT_PERFORMANCE]->(ft:FlatType {name:'3BHK'}) RETURN r.annual_sales_units`.

Property naming convention: `snake_case`, lowercase, no special chars. Spaces, `(`, `)`, `%`, `.` stripped from Excel headers. Example mapping:

| Excel column | Cypher property |
|--------------|-----------------|
| `Annual Sales (Units)` | `annual_sales_units` |
| `Wt Avg Saleable Area Price (Rs/PSF)` | `wt_avg_saleable_price_psf` |
| `Product Efficiency (%)` | `product_efficiency_pct` |
| `Annual Marketable Supply (Sq.Ft.)` | `annual_marketable_supply_sqft` |

---

## 4. File-to-schema mapping (authoritative)

For each of the 24 report types, this specifies exactly which nodes and relationships to emit. The loader processes files in the order listed below — critical because project-level files create `Project` nodes that later aggregate files depend on for sanity-check reconciliation (not for MERGE — MERGE works regardless of order).

| # | File base name | Count | Grain | Node emits | Relationship emits |
|---|----------------|-------|-------|------------|---------------------|
| 1 | `List_of_Comparables_Projects*.xlsx` | 10 | Project | `Project`, `MicroMarket`, `Builder` | `IN_MICROMARKET`, `HAS_PROJECT`, `BUILT_BY`, `HAS_MICROMARKET` |
| 2 | `Top_10_Project_Data_(ANNUALSALES)*.xlsx` | 9 | Project | `Project` (upsert), `MicroMarket`, `Builder` | Same as #1 + `PROJECT_TOP_SALES` |
| 3 | `New_Launch_Project_Details*.xlsx` | 4 | Project | `Project` (upsert), `MicroMarket`, `Builder` | Same as #1 + `NEW_LAUNCH` |
| 4 | `Yearly_Marker_Summary.xlsx` | 1 | City × FY | `FiscalYear` | `ANNUAL_SNAPSHOT` |
| 5 | `Quarterly_Marker_Summary*.xlsx` | 3 | City × Quarter | `Quarter` | `MARKET_SNAPSHOT` |
| 6 | `Carpet_Area_Price_(Rs_PSF)_Data*.xlsx` | 7 | City × Quarter | `Quarter` | `CARPET_PRICE` |
| 7 | `Saleable_Area_Price_(Rs_PSF)_Data*.xlsx` | 7 | City × Quarter | `Quarter` | `SALEABLE_PRICE` |
| 8 | `Months_Inventory_(Months)_Data.xlsx` | 1 | City × Quarter | `Quarter` | `MONTHLY_INVENTORY` |
| 9 | `Sales_Velocity_(%_Monthly_Sales)_Data.xlsx` | 1 | City × Quarter | `Quarter` | `SALES_VELOCITY` |
| 10 | `Quarterly_Sales_&_Marketable_Supply_Data*.xlsx` | 2 | City × Quarter | `Quarter` | `QUARTERLY_SALES` |
| 11 | `Quarterly_Sales_Data*.xlsx` | 4 | City × FlatType | `FlatType` | `FLAT_QUARTERLY_SALES` |
| 12 | `Annual_Sales_Data*.xlsx` | 4 | City × FlatType | `FlatType` | `FLAT_ANNUAL_SALES` |
| 13 | `Unsold_Stock_Data*.xlsx` | 4 | City × FlatType | `FlatType` | `FLAT_UNSOLD` |
| 14 | `Flat_Type_Analysis_Data*.xlsx` | 13 | City × FlatType | `FlatType` | `FLAT_PERFORMANCE` |
| 15 | `Quarterly_Sales_Data_as_per_Construction_Stage*.xlsx` | 2 | City × Stage | `ConstructionStage` | `STAGE_QUARTERLY_SALES` |
| 16 | `Annual_Sales_Data_as_per_Construction_Stage*.xlsx` | 2 | City × Stage | `ConstructionStage` | `STAGE_ANNUAL_SALES` |
| 17 | `Unsold_Stock_Data_as_per_Construction_Stage*.xlsx` | 2 | City × Stage | `ConstructionStage` | `STAGE_UNSOLD` |
| 18 | `Price_Range_Analysis_(carpet_area_price)_Data*.xlsx` | 4 | City × PriceBand(carpet) | `PriceBand` | `PRICE_BAND_PERFORMANCE` |
| 19 | `Price_Range_Analysis_(saleable_area_price)_Data*.xlsx` | 4 | City × PriceBand(saleable) | `PriceBand` | `PRICE_BAND_PERFORMANCE` |
| 20 | `Unit_Size_Range_Analysis_(as_per_carpet_area)_Data*.xlsx` | 5 | City × SizeBand(carpet) | `SizeBand` | `SIZE_BAND_PERFORMANCE` |
| 21 | `Unit_Size_Range_Analysis_(as_per_saleable_area)_Data*.xlsx` | 5 | City × SizeBand(saleable) | `SizeBand` | `SIZE_BAND_PERFORMANCE` |
| 22 | `Unit_Ticket_Size_Analysis_Data*.xlsx` | 4 | City × PriceBand(ticket) | `PriceBand` | `TICKET_BAND_PERFORMANCE` |
| 23 | `Distance_Range_Analysis_Data*.xlsx` | 8 | City × DistanceBand | `DistanceBand` | `DISTANCE_BAND_PERFORMANCE` |
| 24 | `Possession_Wise_Marketable_Supply_&_Sales_Distribution_Data*.xlsx` | 2 | City × PossessionYear | `PossessionYear` | `POSSESSION_DISTRIBUTION` |

**Total: 108 files across 24 types, mapping to ~11 core node labels and ~22 relationship types.**

---

## 5. Sub-region classification rule

Applied to every `Location` string read from project-level files. First match wins.

```python
def classify_sub_region(location: str) -> str:
    low = (location or "").lower()
    if "sohna" in low:
        return "Sohna"
    if "dwarka" in low:
        return "Dwarka"
    if "gurgaon" in low or "gurugram" in low:
        return "Gurugram"
    # Fallback: log warning, mark as Gurugram (parent city catch-all)
    logger.warning(f"Location '{location}' matched no sub-region rule — defaulting to Gurugram")
    return "Gurugram"
```

**Manual overrides:** None at this time. If LF adds Pataudi/Jhajjar/Manesar data in future refreshes, this function needs one more line per sub-region.

**Expected distribution after load:**
- Gurugram sub-region: 30 micromarkets, 62 projects
- Sohna sub-region: 6 micromarkets, 8 projects
- Dwarka sub-region: 1 micromarket, 1 project

---

## 6. Data-cleaning and edge-case handling

### 6.1 Header row detection

All LF Excel files consistently have **data starting at row 7** (0-indexed row 6 as header). Loader uses `pd.read_excel(file, header=6)`. If a file violates this (e.g., headers at row 5), loader logs a warning and attempts auto-detection by scanning first 10 rows for non-NaN density > 80%.

### 6.2 Date handling

Two formats observed in the same dataset:

| File | Format observed |
|------|-----------------|
| `List_of_Comparables_Projects` | `Launch Date` as string `"Oct 2011"`, `Possession Date` as ISO `"2027-02-27 00:00:00"` |
| `Top_10_Project_Data` | Both dates as Excel serial integers (e.g., `45281` = 28 Nov 2023) |
| `New_Launch_Project_Details` | Both as ISO strings |

**Loader approach — convert everything to ISO `YYYY-MM-DD` string:**

```python
def parse_date(raw) -> str | None:
    if raw is None or pd.isna(raw):
        return None
    # Excel serial
    if isinstance(raw, (int, float)) and raw > 30000:
        return (datetime(1899, 12, 30) + timedelta(days=int(raw))).strftime("%Y-%m-%d")
    # Already a pandas Timestamp
    if hasattr(raw, "strftime"):
        return raw.strftime("%Y-%m-%d")
    # String like "Oct 2011" — parse to first of month
    try:
        return pd.to_datetime(str(raw), errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        logger.warning(f"Unparseable date: {raw!r}")
        return None
```

### 6.3 Range-string parsing

Columns like `"5700-8028"`, `"Rs 50-75 Lacs"`, `"1300-2225"` need min/max split.

```python
def parse_range(raw) -> tuple[float | None, float | None]:
    if raw is None or pd.isna(raw):
        return None, None
    s = str(raw).replace(",", "").replace("Rs ", "").replace(" Lacs", "").strip()
    if "-" in s:
        parts = [p.strip() for p in s.split("-", 1)]
        try: return float(parts[0]), float(parts[1])
        except ValueError: return None, None
    try:
        v = float(s)
        return v, v  # single value = min and max are equal
    except ValueError:
        return None, None
```

### 6.4 Flat Type parsing

Flat Type values include `"2BHK, 3BHK, 4BHK"`, `"IndependentFloors2BHK, 3BHK"`, `"Villas"`.

Loader splits on comma, strips whitespace, stores as list. Each individual flat type also gets a `FlatType` node MERGEd — but the loader does NOT explode the project into one-project-per-flat-type relationships (that would cause double-counting).

### 6.5 RERA field normalization

Values observed: `"GGM/954/686/2025/57,GGM/814/..."` (multi-registration comma-separated), `"Not Available"`, empty strings, `NaN`.

- `rera_registered` property: verbatim string from column (or `null` if NaN/empty)
- `rera_status` derived property: `"Registered"` if non-empty and not `"Not Available"`, else `"Not Available"`

### 6.6 Numeric vs string for sector names

Observation: some micromarkets have hyphens inconsistently — `"Sector - 4, Sohna"` vs `"Sector 35, Sohna"`. **Loader keeps both variants as separate MicroMarket nodes** (per Decision 1 — verbatim). Possible future cleanup task for a canonicalization pass, but NOT part of v3 load.

### 6.7 Empty / NaN values

- NaN → `null` in Cypher
- Empty string → `null`
- Zero → `0` (preserved, meaningful data)
- Negative numbers (e.g., QoQ Change % can be negative) → preserved

---

## 7. Idempotency and safety

### 7.1 MERGE-everything

Every node and relationship use Cypher `MERGE`, not `CREATE`. Rerunning the loader on the same files is safe — no duplicates. Properties use `SET` (overwrite), so re-run applies the latest values.

### 7.2 Conflict logging

When two files produce different values for the same relationship property (e.g., carpet price for Q3 25-26 differs between `Carpet_Area_Price_(Rs_PSF)_Data.xlsx` and `Carpet_Area_Price_(Rs_PSF)_Data (3).xlsx`), the loader:

1. Applies last-write-wins (file alphabetical order processed)
2. Logs the conflict to `conflicts.log`: `[{timestamp, file, property, old_value, new_value, cypher_pattern}]`
3. Continues load

At end of load, if conflict count > 50, loader prints warning: `"Unusually high conflict count — review conflicts.log before trusting this load"`.

### 7.3 Duplicate project-name detection (hard failure)

If the loader encounters two *different* `project_id` values for the same `(project_name, city='Gurugram')` pair — it **halts**. No partial write. Reason: this ambiguity needs human decision before corrupting the graph.

Example of what triggers: Excel file A has `{name: "Birla Pravaah", project_id: "149581"}`, file B has `{name: "Birla Pravaah", project_id: "150100"}`. Halt.

### 7.4 Transaction boundaries

Each file is loaded in its own Neo4j transaction. If file N fails mid-way, files 1..N-1 remain committed; file N is rolled back. Loader prints a resume-from-file-N hint on failure.

### 7.5 Pre-load safety checks

Before any writes, loader verifies:
1. Neo4j connection reachable
2. Existing `City: Gurgaon` does NOT exist (previous deletion was complete). If it exists, halt with error.
3. Existing `City: Gurugram` does NOT exist (not already loaded). If it exists, prompt for confirmation to proceed with upsert.
4. Hinjewadi and Kolkata project counts (baseline). Rechecked post-load; mismatch = warning.

---

## 8. Cypher constraints to create (one-time)

Run before first load:

```cypher
CREATE CONSTRAINT gurugram_project_name_city IF NOT EXISTS 
  FOR (p:Project) REQUIRE (p.name, p.city_name) IS UNIQUE;

CREATE CONSTRAINT micromarket_name_city IF NOT EXISTS
  FOR (m:MicroMarket) REQUIRE (m.name, m.city_name) IS UNIQUE;

CREATE CONSTRAINT builder_name IF NOT EXISTS
  FOR (b:Builder) REQUIRE b.name IS UNIQUE;

CREATE CONSTRAINT quarter_name IF NOT EXISTS
  FOR (q:Quarter) REQUIRE q.name IS UNIQUE;

CREATE CONSTRAINT fiscal_year_name IF NOT EXISTS
  FOR (fy:FiscalYear) REQUIRE fy.name IS UNIQUE;

CREATE CONSTRAINT flattype_name IF NOT EXISTS
  FOR (ft:FlatType) REQUIRE ft.name IS UNIQUE;

CREATE CONSTRAINT stage_name IF NOT EXISTS
  FOR (s:ConstructionStage) REQUIRE s.name IS UNIQUE;

CREATE CONSTRAINT priceband_name_basis IF NOT EXISTS
  FOR (pb:PriceBand) REQUIRE (pb.name, pb.basis) IS UNIQUE;

CREATE CONSTRAINT sizeband_name_basis IF NOT EXISTS
  FOR (sb:SizeBand) REQUIRE (sb.name, sb.basis) IS UNIQUE;

CREATE CONSTRAINT distanceband_name IF NOT EXISTS
  FOR (db:DistanceBand) REQUIRE db.name IS UNIQUE;

CREATE CONSTRAINT possession_year IF NOT EXISTS
  FOR (py:PossessionYear) REQUIRE py.year IS UNIQUE;
```

**Note on `city_name` property:** MicroMarket and Project nodes will store a denormalized `city_name` property (duplicating the parent City's name) so the above constraints work. This trades a tiny amount of duplication for dramatically simpler query/constraint logic. Already the pattern used in v2 for Hinjewadi/Kolkata, per the existing codebase.

---

## 9. Post-load verification query pack

Run these queries after loading completes. Each has an expected result range — mismatches indicate a load problem.

```cypher
// 9.1 — Gurugram City node exists with expected metadata
MATCH (c:City {name:'Gurugram'})
RETURN c.name, c.latest_quarter, c.latest_fiscal_year, 
       c.project_count, c.micromarket_count;
// Expected: name='Gurugram', latest_quarter='Q3 25-26', project_count=71, micromarket_count=37
```

```cypher
// 9.2 — MicroMarket count by sub-region
MATCH (c:City {name:'Gurugram'})-[:HAS_MICROMARKET]->(mm:MicroMarket)
RETURN mm.sub_region AS region, count(mm) AS micromarket_count
ORDER BY region;
// Expected: Dwarka=1, Gurugram=30, Sohna=6
```

```cypher
// 9.3 — Project count by sub-region
MATCH (c:City {name:'Gurugram'})-[:HAS_MICROMARKET]->(mm:MicroMarket)-[:HAS_PROJECT]->(p:Project)
RETURN mm.sub_region AS region, count(DISTINCT p) AS project_count
ORDER BY region;
// Expected: Dwarka=1, Gurugram=62, Sohna=8 (total=71)
```

```cypher
// 9.4 — Builders touching Gurugram
MATCH (p:Project)-[:BUILT_BY]->(b:Builder)
WHERE (p)-[:IN_MICROMARKET]->(:MicroMarket)<-[:HAS_MICROMARKET]-(:City {name:'Gurugram'})
RETURN count(DISTINCT b) AS builder_count;
// Expected: 44
```

```cypher
// 9.5 — Latest quarter present on all time-series relationships
MATCH (c:City {name:'Gurugram'})-[r]->(q:Quarter {name:'Q3 25-26'})
RETURN type(r) AS rel_type, count(*) AS cnt
ORDER BY rel_type;
// Expected: CARPET_PRICE=1, SALEABLE_PRICE=1, MARKET_SNAPSHOT=1, MONTHLY_INVENTORY=1, SALES_VELOCITY=1, QUARTERLY_SALES=1
```

```cypher
// 9.6 — Sanity check: Hinjewadi and Kolkata untouched
MATCH (c:City)
OPTIONAL MATCH (c)-[:HAS_MICROMARKET]->(mm:MicroMarket)
OPTIONAL MATCH (mm)-[:HAS_PROJECT]->(p:Project)
RETURN c.name AS city, count(DISTINCT mm) AS mm, count(DISTINCT p) AS projects
ORDER BY city;
// Expected: Gurugram 37/71, Hinjewadi 11/93, Kolkata 21/41
```

```cypher
// 9.7 — No orphan MicroMarkets or Projects
MATCH (mm:MicroMarket) WHERE NOT (mm)<-[:HAS_MICROMARKET]-(:City) RETURN count(mm);
MATCH (p:Project) WHERE NOT (p)-[:IN_MICROMARKET]->(:MicroMarket) RETURN count(p);
// Expected: both return 0
```

```cypher
// 9.8 — No duplicate (name, city) projects  
MATCH (p:Project {city_name:'Gurugram'})
WITH p.name AS name, count(*) AS cnt
WHERE cnt > 1
RETURN name, cnt;
// Expected: empty result
```

---

## 10. Open questions (resolve before loader is written)

None at this time. All prior open items resolved via Decisions 1–6.

If the loader encounters a data shape not anticipated here, it logs and halts rather than guessing. The design doc is then updated with the new rule before re-running.

---

## 11. What this doc does NOT commit to

Explicit out-of-scope items for this schema revision:

- Retrofit of `PriceBand`/`SizeBand`/`DistanceBand`/`PossessionYear` nodes to Hinjewadi and Kolkata subgraphs
- Migration from name-based Project PK to ID-based PK for Hinjewadi and Kolkata
- Update of `cypher_queries.py` (done in a separate step after load completes)
- Intent classifier regex updates for Gurugram-specific queries (separate task)
- UI city-selector update to show "Gurugram" instead of "Gurgaon" (frontend change, separate task)

---

## 12. Sign-off

- [ ] Deepak reviewed §3 Schema
- [ ] Deepak reviewed §4 File-to-schema mapping
- [ ] Deepak reviewed §5 Sub-region classification rule
- [ ] Deepak reviewed §6 Edge-case handling
- [ ] Deepak reviewed §7 Safety rules
- [ ] Approved to proceed with loader implementation

Once all boxes checked, Claude writes `build_graph_v3_gurugram.py` strictly against this spec.
