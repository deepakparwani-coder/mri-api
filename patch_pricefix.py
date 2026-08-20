#!/usr/bin/env python3
"""
patch_pricefix.py — the feasibility engine was priced off the OLDEST quarter.

THE BUG
-------
`_lf_price_and_velocity()` scanned every row of every query result and kept the
FIRST value whose column name contained "absorption":

    for row in rows:
        for k, v in row.items():
            if absorption is None and "absorption" in k.lower():
                absorption = v            # <- first row wins

`price_trend_saleable` returns `ORDER BY q.sort_order`, i.e. oldest quarter
first. So "first row" is the OLDEST absorption price in the series. For
Hinjewadi that is Rs.5,239 PSF. The current quarter, Q1 26-27, is Rs.8,455 PSF -
the LAST row.

The engine then priced a 2026 project at a several-years-stale rate, produced a
-23.7% margin and a NO-GO, and computed a maximum viable land cost of
Rs.-27.48 Cr, which is the arithmetically correct answer to a question nobody
asked: "what land price makes this work if flats sell for Rs.5,239?"

The same bug hit velocity - `velocity_trend` is also ordered oldest-first, so
the absorption pace came from the oldest quarter too.

Worse, the label was already claiming otherwise:

    inp.price_psf_source = f"LF ..., {lf_basis} price, latest quarter"

It said "latest quarter". It was never the latest quarter. The model was handed
a stale number wearing a fresh label, noticed the contradiction against the LF
rows it could see, and improvised - which is how one report ended up carrying
NO-GO in Step 0 and GO in Step 3B.

THE FIX
-------
1. Resolve price and velocity from NAMED queries, not by scanning every column
   in every result. Blind scanning is what let an unrelated series supply the
   number in the first place.
2. Sort by the quarter label and take the LATEST row, not the first. Where a
   quarter label is absent, positional order is the fallback and is recorded.
3. Carry the quarter through, print it in the DIAG log, and state it inside the
   computed block. A stale price can no longer be invisible.
4. ABSTAIN rather than guess: if the chosen price is far from the other
   latest-quarter candidates, emit no block at all. A missing block makes the
   model do prose maths, which is worse but visibly worse. A wrong block is
   confidently wrong.
5. Never print a negative "maximum viable land cost" as if it were a price. If
   the economics do not support any land cost, say that in words.
6. Forbid the two-verdict report in the prompt: if the model believes the
   computed price is wrong, it must stop and say so, not publish both answers
   and let the reader choose.

    python patch_pricefix.py /path/to/mri-api/app.py [--feasibility feasibility.py]
"""
import argparse
import re
import shutil
import sys
from pathlib import Path

# ── the replacement resolver ───────────────────────────────────────────────
NEW_RESOLVER = '''def _quarter_sort_key(label):
    """('Q1 26-27') -> (2026, 1). None when the label is not an LF FY quarter."""
    m = re.match(r"\\s*Q([1-4])\\s*(\\d{2})\\s*-\\s*(\\d{2})\\s*$", str(label or ""))
    if not m:
        return None
    return (2000 + int(m.group(2)), int(m.group(1)))


_QUARTER_COLS = ("quarter", "fy_qtr", "fy_quarter", "period")


def _latest_row(result):
    """Return (row, quarter_label, ordering) for the most RECENT row.

    LF time series come back `ORDER BY q.sort_order`, so the newest quarter is
    the LAST row. Taking the first row - which is what this code used to do -
    prices a 2026 project off the oldest quarter on file.
    """
    rows = result.get("data") or result.get("rows") or []
    if not isinstance(rows, list):
        return None, None, None
    dated = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        label = next((row[c] for c in _QUARTER_COLS if row.get(c)), None)
        dated.append((_quarter_sort_key(label), idx, row, label))
    if not dated:
        return None, None, None
    if any(d[0] is not None for d in dated):
        dated.sort(key=lambda d: (d[0] is None, d[0] or (0, 0), d[1]))
        ordering = "quarter label"
    else:
        ordering = "row order (no quarter column)"
    _, _, row, label = dated[-1]
    return row, label, ordering


# Only these queries may supply a price. Scanning every column of every result
# is how an unrelated series got to decide what a project sells for.
_PRICE_QUERIES = ("price_trend_saleable", "micromarket_price_trend", "price_trend")
_CARPET_PRICE_QUERIES = ("price_trend_carpet",)
_VELOCITY_QUERIES = ("velocity_trend",)


def _lf_price_and_velocity(data_results, want_carpet=False):
    """Latest-quarter absorption price and velocity, with provenance.

    Returns (price, velocity, basis, quarter, detail). Absorption price - what
    units actually transacted at - is preferred over the weighted average on
    marketable supply, which is the asking price and runs higher.
    """
    names = _CARPET_PRICE_QUERIES if want_carpet else _PRICE_QUERIES
    candidates = []                      # (basis, value, quarter, query, ordering)
    for r in data_results or []:
        qn = str(r.get("query") or "")
        if qn not in names:
            continue
        row, quarter, ordering = _latest_row(r)
        if not row:
            continue
        for field, basis in (("absorption_price", "absorption"),
                             ("wt_avg_price", "weighted average (asking)")):
            try:
                fv = float(row.get(field))
            except (TypeError, ValueError):
                continue
            if 1000 < fv < 100000:
                candidates.append((basis, fv, quarter, qn, ordering))

    price = basis = quarter = None
    detail = "no price series in the retrieved data"
    if candidates:
        absorb = [c for c in candidates if c[0] == "absorption"]
        chosen = (absorb or candidates)[0]
        basis, price, quarter, qname, ordering = chosen
        detail = f"{basis}, {quarter or 'undated'}, via {qname}, ordered by {ordering}"

        # Sanity guard. Compare like with like: if two sources both claim to be
        # the latest absorption price and disagree wildly, we do not know which
        # series is right, so we publish nothing. Comparing the pick against the
        # median of ALL candidates would not catch this - the pick often IS the
        # median.
        same_basis = sorted(c[1] for c in candidates if c[0] == basis)
        if len(same_basis) > 1 and (
                (same_basis[-1] - same_basis[0]) / same_basis[0] > 0.25):
            return None, None, None, None, (
                f"ABSTAINED: sources disagree on the latest {basis} price "
                f"({[round(v) for v in same_basis]}) - refusing to guess")

    velocity = None
    for r in data_results or []:
        if str(r.get("query") or "") in _VELOCITY_QUERIES:
            row, _, _ = _latest_row(r)
            if row:
                try:
                    fv = float(row.get("velocity"))
                except (TypeError, ValueError):
                    fv = None
                if fv is not None and 0 < fv < 50:
                    velocity = fv
                    break

    return price, velocity, basis, quarter, detail


def build_feasibility_block(user_query, data_results):
    """Return (markdown_block, diagnostic) - block is None when not applicable."""
    if not _FEAS_OK:
        return None, "calculator unavailable"
    try:
        inp = parse_feasibility_inputs(user_query)
    except Exception as e:
        return None, f"parse error: {e}"
    if inp is None:
        return None, "not a feasibility query"

    want_carpet = bool(re.search(r"carpet|rera.basis", user_query or "", re.I))
    lf_price, lf_vel, lf_basis, lf_qtr, lf_detail = _lf_price_and_velocity(
        data_results, want_carpet=want_carpet)
    if str(lf_detail).startswith("ABSTAINED"):
        return None, lf_detail

    if inp.price_psf is None and lf_price:
        inp.price_psf = lf_price
        inp.price_psf_source = (
            f"LF knowledge base, {lf_basis} price, {lf_qtr or 'latest available quarter'}")
    if inp.monthly_velocity_pct is None and lf_vel:
        inp.monthly_velocity_pct = lf_vel

    if not inp.is_sufficient() or inp.price_psf is None:
        return None, "insufficient inputs: " + ", ".join(inp.missing())
    try:
        from feasibility import compute_with_launch_plan as _feas_full
        block = _feas_render(_feas_full(inp))
    except Exception as e:
        return None, f"compute error: {e}"

    # State the price and its vintage inside the block. A stale input caused a
    # report to carry NO-GO in its verdict and GO in its economics; making the
    # basis visible means any recurrence is caught by the reader, not tolerated
    # by the model.
    header = (
        f"> **Price basis for every figure below: Rs.{int(inp.price_psf):,} PSF"
        f"{' (carpet)' if want_carpet else ' (saleable)'}"
        f" - LF {lf_basis or 'price'}, {lf_qtr or 'latest available quarter'}.**\\n"
        "> This is the latest quarter in the LF series. If any LF figure quoted "
        "elsewhere in this report contradicts it, STOP: report the discrepancy "
        "as the finding and do not issue a verdict.\\n"
    )
    return header + "\\n" + block, f"computed [{lf_detail}]"
'''

OLD_DIAG = '''    print(f"[DIAG-8] FEASIBILITY_CALC: {_feas_why}")'''
NEW_DIAG = '''    print(f"[DIAG-8] FEASIBILITY_CALC: {_feas_why}")
    # The price and its quarter are the single most consequential inputs in the
    # whole report. Log them explicitly so a stale one is greppable, not
    # inferred three reports later from a margin that looked odd.
    print(f"[DIAG-8b] FEASIBILITY_PRICE_PROVENANCE: {_feas_why}")'''

PROMPT_ANCHOR = "Follow this EXACT framework."
PROMPT_ADD = """Follow this EXACT framework.

**ONE VERDICT ONLY.**
Where a COMPUTED FEASIBILITY block is supplied it is the sole source of every
number in the report. If you believe its selling price contradicts the LF data
you can see, that is a DATA FAULT, not something to work around: say so at the
top, state both figures, and stop. Do not publish a P&L under two prices, do not
put NO-GO in the verdict and GO in the economics, and do not tell the reader to
"extend the table mentally". A report that says "the price input is wrong, here
is the evidence" is useful. A report carrying two opposite verdicts is not.
"""

# ── feasibility.py: stop printing a negative land cost as if it were a price ──
OLD_LAND = ('''    L.append(f"- Maximum viable land cost at {i['target_margin_pct']}% margin: '''
            '''**Rs.{r['max_viable_land_cr']} Cr**")''')
NEW_LAND = '''    if r["max_viable_land_cr"] > 0:
        L.append(f"- Maximum viable land cost at {i['target_margin_pct']}% margin: "
                 f"**Rs.{r['max_viable_land_cr']} Cr**")
    else:
        # A negative figure here is arithmetically real but meaningless as a
        # price - it says the build does not cover itself at this selling price,
        # so no land cost works. Printing "Rs.-27.48 Cr" invites the reader to
        # think the model is broken. Say what it means instead.
        L.append(f"- Maximum viable land cost at {i['target_margin_pct']}% margin: "
                 f"**none** - at Rs.{r['price_psf']:,} PSF the project does not "
                 f"cover its construction and financing, so no land price makes "
                 f"it viable. Check the selling price before reading further.")'''


def patch_app(path: Path) -> bool:
    src = path.read_text(encoding="utf-8")
    orig = src

    start = src.find("def _lf_price_and_velocity")
    end = src.find("# ── Generation deadline", start)
    if start == -1 or end == -1:
        print("  ! could not locate the resolver / build_feasibility_block span")
        return False
    if "_quarter_sort_key" in src:
        print("  ! price fix already present")
        return False

    src = src[:start] + NEW_RESOLVER + "\n\n" + src[end:]
    print("  replaced: price/velocity resolver - latest quarter, named queries, abstain guard")

    if OLD_DIAG in src:
        src = src.replace(OLD_DIAG, NEW_DIAG, 1)
        print("  added: price provenance to the diagnostic log")

    if PROMPT_ANCHOR in src and "ONE VERDICT ONLY" not in src:
        src = src.replace(PROMPT_ANCHOR, PROMPT_ADD, 1)
        print("  added: one-verdict rule to the system prompt")

    if src == orig:
        return False
    shutil.copy2(path, path.with_suffix(path.suffix + ".pre_pricefix"))
    path.write_text(src, encoding="utf-8")
    print(f"  written {path}")
    return True


def patch_feasibility(path: Path) -> bool:
    src = path.read_text(encoding="utf-8")
    if "no land price makes" in src:
        print("  ! land-cost rendering already fixed")
        return False
    target = None
    for line in src.splitlines():
        if "Maximum viable land cost at" in line:
            target = line
            break
    if target is None:
        print("  ! land-cost line not found")
        return False
    src = src.replace(target, NEW_LAND, 1)
    shutil.copy2(path, path.with_suffix(path.suffix + ".pre_pricefix"))
    path.write_text(src, encoding="utf-8")
    print("  fixed: negative maximum-viable-land now reads as words, not a price")
    print(f"  written {path}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("app")
    ap.add_argument("--feasibility")
    a = ap.parse_args()

    ok = patch_app(Path(a.app))
    if a.feasibility:
        ok = patch_feasibility(Path(a.feasibility)) or ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
