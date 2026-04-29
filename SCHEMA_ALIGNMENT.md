# Schema Alignment — Gurugram to v2 Conventions

## Relationship renames
| My v3 (wrong) | v2 convention (correct) |
|---|---|
| ANNUAL_SNAPSHOT | YEARLY_SNAPSHOT |
| SALEABLE_PRICE | SALEABLE_PRICE_AT |
| CARPET_PRICE | CARPET_PRICE_AT |
| QUARTERLY_SALES | QUARTERLY_TREND |
| BUILT_BY | DEVELOPED_BY |
| STAGE_ANNUAL_SALES, STAGE_QUARTERLY_SALES, STAGE_UNSOLD | all merged into CONSTRUCTION_STAGE_SALES |
| SIZE_BAND_PERFORMANCE | UNIT_SIZE_PERFORMANCE |
| TICKET_BAND_PERFORMANCE | TICKET_SIZE_PERFORMANCE |
| DISTANCE_BAND_PERFORMANCE | DISTANCE_PERFORMANCE |

## Node label renames
| My v3 | v2 |
|---|---|
| SizeBand with basis="carpet"/"saleable" | UnitSizeBand with area_type="carpet"/"saleable" |
| PriceBand with basis="ticket" | TicketSizeBand |
| PriceBand with basis="carpet"/"saleable" | (keep — new category) |
| DistanceBand | DistanceRange |
| PossessionYear | (keep — exists in v2) |

## Property renames on MARKET_SNAPSHOT
| My v3 | v2 |
|---|---|
| marketable_supply_units | supply_units |
| marketable_supply_sqft_mn | supply_sqft |
| sales_sqft_mn | sales_sqft |
| unsold_sqft_mn | unsold_sqft |
| months_inventory | months_inv |
| value_of_stock_sold_cr | value_sold_cr |
| unsold_value_cr | unsold_value_cr (unchanged) |
| newsupply_units | new_supply_units |
| newsupply_sqft_mn | new_supply_sqft |
| sales_velocity_pct | velocity_pct |

## Property renames on YEARLY_SNAPSHOT
same as MARKET_SNAPSHOT

## Property renames on SALEABLE_PRICE_AT / CARPET_PRICE_AT
| My v3 | v2 |
|---|---|
| min | minimum |
| max | maximum |
| wt_avg_new_supply | new_supply_price |

## Project property denormalizations (from Top-10 files)
Add to Project node:
- p.annual_sales_units, p.annual_sales_sqft, p.annual_value_cr
- p.monthly_velocity (rename from p.monthly_sales_velocity)
- p.sold_pct (rename from p.sold_percent)
- p.unsold_pct (rename from p.unsold_percent)
- p.annual_months_inv (rename from p.annual_months_inventory)
- p.quarterly_months_inv (rename from p.quarterly_months_inventory)
- p.saleable_rate_psf (weighted avg if range, else value)
- p.carpet_rate_psf (same)
- p.saleable_rate_range (string "min-max")
- p.carpet_rate_range (same)
- p.saleable_size_range, p.carpet_size_range, p.total_cost_range
- p.location (denorm from linked MicroMarket)
- p.status = 'NEW_LAUNCH' for projects emitted by new launch files

## New relationships/nodes kept as-is (no conflict with v2)
- PriceBand {name, basis: "carpet"|"saleable"} — new concept
- PRICE_BAND_PERFORMANCE
- NEW_LAUNCH (Project→City) — my new tag
- PROJECT_TOP_SALES (Project→Quarter) — my new relationship
