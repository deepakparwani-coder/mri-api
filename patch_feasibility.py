#!/usr/bin/env python3
"""
patch_feasibility.py — route feasibility arithmetic through Python, not prose.

Inserts a computed-feasibility block into the model's context whenever a
feasibility query carries enough parameters. Selling price and velocity are
pulled from the LF data already fetched for the query, so the calculation stays
anchored to the knowledge base rather than to anything the model recalls.

If parameters cannot be read confidently, nothing is injected and behaviour is
exactly as before - no regression, no guessing.

    python patch_feasibility.py /path/to/mri-api/app.py
"""
import shutil
import sys
from pathlib import Path

HELPER = '''

# ── Deterministic feasibility ───────────────────────────────────────────────
try:
    from feasibility import parse_feasibility_inputs, \\
        compute_with_launch_plan as _feas_compute, render_markdown as _feas_render
    _FEAS_OK = True
except Exception as _e:            # module missing -> behave exactly as before
    print(f"  [FEAS] calculator unavailable ({_e}); falling back to prose maths")
    _FEAS_OK = False


def _lf_price_and_velocity(data_results):
    """Pull price and monthly velocity out of the LF rows already fetched.

    Preference order matters. ABSORPTION price is what units actually transact
    at; weighted-average-on-marketable-supply is the asking price and runs
    higher (Hinjewadi Q1 26-27: absorption 8,455 vs asking 8,571). Revenue
    projection uses the transacted figure - the more conservative and the more
    defensible of the two.
    """
    absorption = wt_avg = velocity = None
    for r in data_results or []:
        rows = r.get("data") or r.get("rows") or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                if v in (None, ""):
                    continue
                lk = str(k).lower()
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if 1000 < fv < 100000:
                    if absorption is None and "absorption" in lk:
                        absorption = fv
                    elif wt_avg is None and ("wt_avg" in lk or "price_psf" in lk
                                             or lk in ("price", "wt_avg_price", "saleable_price")):
                        wt_avg = fv
                if velocity is None and "velocit" in lk and 0 < fv < 50:
                    velocity = fv
    return (absorption or wt_avg), velocity, ("absorption" if absorption else
                                              "weighted average (asking)" if wt_avg else None)


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

    lf_price, lf_vel, lf_basis = _lf_price_and_velocity(data_results)
    if inp.price_psf is None and lf_price:
        inp.price_psf = lf_price
        inp.price_psf_source = f"LF knowledge base, {lf_basis} price, latest quarter"
    if inp.monthly_velocity_pct is None and lf_vel:
        inp.monthly_velocity_pct = lf_vel

    if not inp.is_sufficient() or inp.price_psf is None:
        return None, "insufficient inputs: " + ", ".join(inp.missing())
    try:
        from feasibility import compute_with_launch_plan as _feas_full
        return _feas_render(_feas_full(inp)), "computed"
    except Exception as e:
        return None, f"compute error: {e}"

'''

INJECT = '''    # Feasibility arithmetic is computed in Python and handed to the model as
    # fixed data. Prose maths produced a 7% area error, a sensitivity matrix
    # where 12 of 20 cells did not reconcile, and an IRR off by 18 points -
    # none of which raised an error.
    _feas_block, _feas_why = build_feasibility_block(user_query, data_results)
    print(f"[DIAG-8] FEASIBILITY_CALC: {_feas_why}")
    if _feas_block:
        data_text = data_text + "\\n\\n" + _feas_block

    # Step 4: Build messages
'''


def main(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    orig = src

    anchor = "@app.route('/api/raw', methods=['POST'])"
    if "build_feasibility_block" in src:
        print("  ! already patched")
    elif anchor not in src:
        print("  ! anchor for helper not found")
        return 1
    else:
        src = src.replace(anchor, HELPER.lstrip("\n") + "\n" + anchor, 1)
        print("  added: feasibility helper + LF price/velocity extraction")

    marker = "    # Step 4: Build messages\n"
    if "_feas_block" in src and "FEASIBILITY_CALC" in src:
        pass
    if marker not in src:
        print("  ! could not find the message-build step")
        return 1
    src = src.replace(marker, INJECT, 1)
    print("  added: computed block injected into the model context")

    if src == orig:
        print("nothing changed")
        return 1
    backup = path.with_suffix(path.suffix + ".pre_feasibility_fix")
    shutil.copy2(path, backup)
    path.write_text(src, encoding="utf-8")
    print(f"\nwritten {path}   (backup: {backup.name})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(Path(sys.argv[1])))
