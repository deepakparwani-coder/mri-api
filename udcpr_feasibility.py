"""
udcpr_feasibility.py — Trusted regulatory knowledge layer for land feasibility.

WHY THIS EXISTS
---------------
The feasibility feature must give developers UDCPR-grounded go/no-go analysis
WITHOUT the model inventing FSI/setback numbers and WITHOUT depending on the
open web. This module is the single trusted source for the quantitative
regulations. Values are extracted VERBATIM from the Updated UDCPR-2022
(Maharashtra). Each value carries its regulation number so the bot can cite it.

SCOPE GUARD
-----------
UDCPR applies to MAHARASHTRA ONLY (Pune/PCMC/PMRDA, Nagpur, Nashik, Thane, etc.,
excluding Greater Mumbai/MIDC/NAINA per the front matter). It does NOT apply to
Kolkata / West Bengal — there the framework is WBHIRA + the local Building Rules
(KMC / KMDA / HIRA-WB). is_udcpr_applicable() enforces this so the bot never
applies Maharashtra FSI numbers to a Kolkata plot.
"""

# ──────────────────────────────────────────────────────────────────────────
# TABLE 6-G — PERMISSIBLE FSI BY ROAD WIDTH  (UDCPR Reg. 6.3, non-congested)
# Columns are verbatim. "muncorp" = all Municipal Corporations + CIDCO-as-NTDA.
# "other" = remaining authorities/areas. Basic FSI is uniform at 1.10.
# Each row: (max_potential includes basic + premium + TDR, NOT ancillary).
# ──────────────────────────────────────────────────────────────────────────
TABLE_6G = [
    # road_min, road_max(excl), basic, muncorp_premium, muncorp_tdr, muncorp_max,
    #                            other_premium,  other_tdr,  other_max
    (0.0,  9.0, 1.10, 0.00, 0.00, 1.10, 0.00, 0.00, 1.10),
    (9.0, 12.0, 1.10, 0.50, 0.40, 2.00, 0.30, 0.30, 1.70),
    (12.0,15.0, 1.10, 0.50, 0.65, 2.25, 0.30, 0.60, 2.00),
    (15.0,24.0, 1.10, 0.50, 0.90, 2.50, 0.30, 0.70, 2.10),
    (24.0,30.0, 1.10, 0.50, 1.15, 2.75, 0.30, 0.90, 2.30),
    (30.0,1e9, 1.10, 0.50, 1.40, 3.00, 0.30, 1.10, 2.50),
]

# Reg. 6.3 Note (i): ancillary-area FSI on premium, OVER AND ABOVE the above.
ANCILLARY_FSI_RESIDENTIAL = 0.60   # 60% of proposed FSI
ANCILLARY_FSI_NONRESID    = 0.80   # 80% of proposed FSI
# Applies to all zones and to TOD/PMAY/ITP/IT/MHADA etc. (except SRA rehab).

# Reg. 6.2.3(b): for heights above Table 6-D, side/rear marginal distance = H/5,
# capped at 12 m. Front margin per Table 6-D regardless of height.
SIDE_REAR_MARGIN_FACTOR = 0.20     # H/5
SIDE_REAR_MARGIN_CAP_M  = 12.0
GROUP_HOUSING_MIN_SETBACK_M = 3.0  # Reg. 6.2.5 (internal road)

MAHARASHTRA_AUTHORITIES = {
    "pune", "pcmc", "pimpri", "chinchwad", "pmrda", "nagpur", "nashik", "nasik",
    "thane", "kalyan", "dombivli", "navi mumbai", "vasai", "virar", "kolhapur",
    "aurangabad", "sambhaji nagar", "solapur", "amravati", "nanded", "cidco",
    "hinjewadi", "wakad", "baner", "ravet", "talegaon", "chakan",
}


def is_udcpr_applicable(location_text: str) -> bool:
    """True only for Maharashtra planning areas covered by UDCPR-2022."""
    if not location_text:
        return False
    t = location_text.lower()
    if any(x in t for x in ("kolkata", "west bengal", "howrah", "rajarhat",
                            "new town", "salt lake", "behala")):
        return False
    return any(a in t for a in MAHARASHTRA_AUTHORITIES) or "maharashtra" in t


def fsi_potential(road_width_m: float, authority: str = "muncorp") -> dict:
    """Return the FSI breakdown for a given abutting road width (UDCPR 6.3).
    authority: 'muncorp' (Municipal Corp/CIDCO) or 'other'."""
    for lo, hi, basic, mc_p, mc_t, mc_max, ot_p, ot_t, ot_max in TABLE_6G:
        if lo <= road_width_m < hi:
            if authority == "muncorp":
                premium, tdr, max_pot = mc_p, mc_t, mc_max
            else:
                premium, tdr, max_pot = ot_p, ot_t, ot_max
            return {
                "road_width_m": road_width_m,
                "authority": authority,
                "basic_fsi": basic,
                "premium_fsi": premium,
                "tdr_loading": tdr,
                "max_potential_fsi": max_pot,        # excl. ancillary
                "reg": "UDCPR 6.3 / Table 6-G",
            }
    return {}


def effective_fsi(road_width_m: float, authority: str = "muncorp",
                  use: str = "residential") -> dict:
    """Max potential incl. ancillary-area FSI (Reg. 6.3 Note i)."""
    base = fsi_potential(road_width_m, authority)
    if not base:
        return {}
    anc = ANCILLARY_FSI_RESIDENTIAL if use == "residential" else ANCILLARY_FSI_NONRESID
    base["ancillary_fsi"] = round(base["max_potential_fsi"] * anc, 3)
    base["effective_max_fsi"] = round(base["max_potential_fsi"] * (1 + anc), 3)
    base["ancillary_reg"] = "UDCPR 6.3 Note (i)"
    return base


def side_rear_margin(height_m: float) -> dict:
    """Side/rear marginal distance for tall buildings (Reg. 6.2.3)."""
    m = min(height_m * SIDE_REAR_MARGIN_FACTOR, SIDE_REAR_MARGIN_CAP_M)
    return {"height_m": height_m, "side_rear_margin_m": round(m, 2),
            "rule": "H/5, max 12 m", "reg": "UDCPR 6.2.3(b)"}


# ──────────────────────────────────────────────────────────────────────────
# LEVER REGISTRY — every feasibility insight the bot should consider, mapped to
# its UDCPR chapter. Items marked NUMBERS_PENDING need their tables extracted
# next (Ch 8 parking, Ch 4 zone-use matrix, 3.4/3.5/3.7 open-space & min-plot).
# The bot may DISCUSS these levers and cite the regulation, but must NOT state a
# specific numeric value that is not present in this module.
# ──────────────────────────────────────────────────────────────────────────
FEASIBILITY_LEVERS = {
    "zone_permissibility": {"reg": "UDCPR Ch 4 (4.2–4.27)",
        "q": "Is the intended use permitted in the plot's zone?", "numbers": "PENDING"},
    "permissible_fsi":     {"reg": "UDCPR 6.3 / Table 6-G", "numbers": "LOADED"},
    "ancillary_fsi":       {"reg": "UDCPR 6.3 Note (i)",    "numbers": "LOADED"},
    "setbacks_height":     {"reg": "UDCPR 6.2, 6.10",       "numbers": "PARTIAL (H/5 loaded)"},
    "parking":             {"reg": "UDCPR Ch 8 (8.1–8.2)",  "numbers": "PENDING"},
    "recreational_open_space": {"reg": "UDCPR 3.4",         "numbers": "PENDING"},
    "amenity_space":       {"reg": "UDCPR 3.5",             "numbers": "PENDING"},
    "min_plot_area":       {"reg": "UDCPR 3.7",             "numbers": "PENDING"},
    "inclusive_housing":   {"reg": "UDCPR 3.8",             "numbers": "PENDING"},
    "tdr":                 {"reg": "UDCPR Ch 11 (11.2)",    "numbers": "PARTIAL (loading in 6-G)"},
    "higher_fsi_schemes":  {"reg": "UDCPR Ch 7 (7.1, 7.10)", "numbers": "PENDING"},
    "green_building":      {"reg": "UDCPR 7.10, 13.2–13.5", "numbers": "PENDING"},
    "integrated_township": {"reg": "UDCPR 14.1 (ITP)",      "numbers": "PENDING"},
    "tod":                 {"reg": "UDCPR 14.2 (TOD)",      "numbers": "PENDING"},
    "affordable_pmay":     {"reg": "UDCPR 14.3 / 14.4",     "numbers": "PENDING"},
    "it_township":         {"reg": "UDCPR 14.10 (IITP)",    "numbers": "PENDING"},
    "site_restrictions":   {"reg": "UDCPR 3.1 (flood/road/rail/airport/monument/defense)",
        "numbers": "rules, not numeric"},
    "access_road_width":   {"reg": "UDCPR 3.2, 3.3.8",      "numbers": "PENDING"},
    "city_specific":       {"reg": "UDCPR Ch 10 (Pune 10.1, PCMC, Nagpur, Nashik…)",
        "numbers": "PENDING"},
}
