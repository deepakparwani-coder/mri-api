#!/usr/bin/env python3
"""
test_probe.py — the checks are tested against the reports that actually shipped.

A scoring script you have not tested is worse than no scoring script: it gives a
number, and a number gets believed. So every check here is fed the real failing
text from the August/September reports and must catch it, and fed the corrected
text and must pass it.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from quality_probe import CHECKS, PROBES  # noqa: E402

FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


def c(name, text):
    return CHECKS[name](text)[0]


# ── the real text, from the reports on file ────────────────────────────────
BAD_ABSTENTION = """
=== COMPUTED FEASIBILITY: NOT RUN ===
The deterministic calculator did NOT run, because these inputs are missing:
  - construction cost per sq.ft

Nevertheless, assuming a typical Rs.2,400 PSF build cost, the project shows a
Net Margin of 23.4% and an IRR of 48.4%.
"""
GOOD_ABSTENTION = """
=== COMPUTED FEASIBILITY: NOT RUN ===
The deterministic calculator did NOT run, because these inputs are missing:
  - construction cost per sq.ft
Supply it and re-run. Do not assume FSI/FAR, efficiency, construction cost,
collection profile, IRR or margin.
"""
NEG_LAND = "Maximum viable land cost at the current base price: Rs.(-27.48 Cr)."
POS_LAND = "Maximum viable land cost at the current base price: Rs.41.20 Cr."
TWO_VERDICTS = ("Executive verdict: GO, margin ~23.4%.\n"
                "... Scorecard total 58/80. Final recommendation: NO-GO.")
ONE_VERDICT = "Executive verdict: GO, margin ~23.4%. Final recommendation: GO."
RATIO_FAKE = ("Absorption price on carpet: Rs.11,412 PSF "
              "(derived at 0.74 x the saleable price).")
REAL_CARPET = ("Carpet-basis absorption price: Rs.11,516 PSF "
               "(LF carpet series, Q1 26-27).")
PROSE_MATHS = "Total revenue is 4,35,600 x 12,029 = Rs.524 Cr across the scheme."
NO_PROSE_MATHS = ("Total revenue is Rs.524 Cr (see the computed block; all "
                  "arithmetic is performed there).")
STALE_PRICE = "Absorption price: Rs.4,100 PSF, saleable basis."
CURRENT_PRICE = "Absorption price: Rs.8,455 PSF, saleable basis (Q1 26-27)."
NO_DISTANCE = ("| Project | Developer | Sales |\n"
               "| Godrej Woodscape | Godrej | 412 |")
WITH_DISTANCE = ("Projects within a radius of 5 km of the pin:\n"
                 "| Project | Distance (km) | Developer |\n"
                 "| Godrej Woodscape | 1.4 | Godrej |")
TRUNCATED = "…the scorecard shows\n\n[Response truncated - token ceiling reached]"


def main() -> int:
    print("\n1. THE FABRICATION AFTER AN ABSTENTION IS CAUGHT")
    check("a P&L after NOT RUN fails", not c("no_pnl_after_abstention", BAD_ABSTENTION))
    check("the evidence names the offending figure",
          "IRR" in CHECKS["no_pnl_after_abstention"](BAD_ABSTENTION)[1])
    check("a clean abstention passes", c("no_pnl_after_abstention", GOOD_ABSTENTION))
    check("abstention is detected at all", c("abstained", GOOD_ABSTENTION))
    check("a report with no abstention block cannot pass the P&L check",
          not c("no_pnl_after_abstention", "Net Margin 23.4%, IRR 48.4%."))

    print("\n2. THE -27.48 Cr LAND COST IS CAUGHT")
    check("negative land cost fails", not c("no_negative_land_cost", NEG_LAND))
    check("a positive one passes", c("no_negative_land_cost", POS_LAND))

    print("\n3. THE 65/60/60 SELF-CONTRADICTION IS CAUGHT")
    check("GO and NO-GO together fails", not c("one_verdict", TWO_VERDICTS))
    check("a single verdict passes", c("one_verdict", ONE_VERDICT))
    check("'do not proceed' counts as NO-GO",
          not c("one_verdict", "Verdict: GO. We do not proceed at this price."))

    print("\n4. THE FABRICATED CARPET PSF IS CAUGHT")
    check("a 0.74 conversion fails", not c("no_ratio_conversion", RATIO_FAKE))
    check("the real LF carpet series passes", c("no_ratio_conversion", REAL_CARPET))
    check("a carpet answer is recognised", c("carpet_answered", REAL_CARPET))
    check("an absent carpet answer fails",
          not c("carpet_answered", "Carpet data is not available in the LF dataset."))

    print("\n5. PROSE ARITHMETIC IS CAUGHT")
    check("an inline calculation fails", not c("no_prose_arithmetic", PROSE_MATHS))
    check("a reference to the computed block passes",
          c("no_prose_arithmetic", NO_PROSE_MATHS))
    check("the multiplication sign variants are all caught",
          not c("no_prose_arithmetic", "12,029 * 4,356 = 52,41,63,324")
          and not c("no_prose_arithmetic", "524 Cr / 5 = 104.8 Cr"))

    print("\n6. THE STALE PRICE SERIES IS CAUGHT")
    check("Rs.4,100 PSF is flagged", not c("price_is_current", STALE_PRICE))
    check("Rs.8,455 PSF passes", c("price_is_current", CURRENT_PRICE))
    check("a report with no PSF figure is not failed",
          c("price_is_current", "Sales velocity is 3.15% monthly."))

    print("\n7. THE PIN OBJECTION IS CAUGHT")
    check("a table with no distance column fails", not c("has_distance_column", NO_DISTANCE))
    check("a table with one passes", c("has_distance_column", WITH_DISTANCE))
    check("an unstated radius fails", not c("radius_stated", NO_DISTANCE))
    check("a stated radius passes", c("radius_stated", WITH_DISTANCE))

    print("\n8. AN INCOMPLETE REPORT IS NEVER SCORED AS GOOD")
    check("a truncation marker fails", not c("complete", TRUNCATED))
    check("a finished report passes", c("complete", CURRENT_PRICE))

    print("\n9. THE PROBE SET IS COHERENT")
    ids = [p["id"] for p in PROBES]
    check("probe ids are unique", len(ids) == len(set(ids)))
    unknown = [w for p in PROBES for w in p["want"] if w not in CHECKS]
    check("every requested check exists", not unknown, ", ".join(unknown))
    check("the missing-input probe is the strictest",
          {"abstained", "no_pnl_after_abstention"} <=
          set(next(p for p in PROBES if p["id"] == "feas_missing_input")["want"]))
    check("every probe demands completeness",
          all("complete" in p["want"] for p in PROBES))

    print("\n10. A GOOD REPORT IS NOT FAILED BY ANY CHECK")
    # The opposite failure mode, and the more dangerous one: a scorer that cries
    # wolf gets ignored, and then it is worth nothing when it is finally right.
    CLEAN = """
## Executive verdict: GO

Projects within a radius of 5 km of the pin:

| Project | Distance (km) | Wt Avg Saleable Price |
| Godrej Woodscape | 1.4 | Rs.8,455 PSF |
| Prestige Raintree Park | 2.1 | Rs.9,120 PSF |

Carpet-basis absorption price: Rs.11,516 PSF (LF carpet series, Q1 26-27).

=== COMPUTED FEASIBILITY ===
Revenue Rs.524 Cr, net margin 23.4%, IRR 48.4%.
All arithmetic is performed in the computed block above.

The developer can go ahead on this basis. Final recommendation: GO.
"""
    for name in sorted(CHECKS):
        if name in ("abstained", "no_pnl_after_abstention"):
            continue          # these are for the abstention probe only
        ok, ev = CHECKS[name](CLEAN)
        check(f"{name} does not false-positive", ok, ev)

    print("\n" + "=" * 66)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
