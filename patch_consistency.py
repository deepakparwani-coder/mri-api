#!/usr/bin/env python3
"""
patch_consistency.py — extend the cross-section check from the site score to the
figures a developer actually decides on.

THE FAULT (Whitefield report, 26 Aug 2026 — not caught in the room)
-------------------------------------------------------------------
                        STEP 0 (read first)      STEP 3D (the detail)
  Breakeven             Rs.10,200 PSF            Rs.13,200 PSF
  vs market Rs.12,172   "19% headroom"           "Rs.1,028 BELOW breakeven"
  Max viable land       Rs.480 Cr at Rs.12,172   Rs.330 Cr at Rs.12,172
  Margin at market      "22-25% STRONG"          negative

At the Rs.400 Cr ask, Step 3 puts the deal Rs.70 Cr OVER the maximum viable land
cost. Step 0 called it CONDITIONAL GO. Both halves cannot be true, and Step 0 —
the half a developer reads first and quotes back to his board — is the invented
one.

Note that each section is internally consistent: with breakeven 10,200 against a
market of 12,172, "19% headroom" is arithmetically right. The contradiction only
appears when you compare sections. So the check has to be cross-sectional; a
sentence-level check sees nothing wrong.

WHAT THIS ADDS
--------------
Three checks, chosen to be precise rather than broad — a validator that cries
wolf gets ignored:

1. BREAKEVEN IS ONE NUMBER. Every "breakeven ... Rs.N PSF" in the report must be
   the same N. This is what fires on the case above.
2. MAX VIABLE LAND IS A FUNCTION OF PRICE. Collect (price, land) pairs. One
   price mapping to two different land costs is an error; so is one land cost
   claimed at two different prices.
3. A GO VERDICT MUST CLEAR ITS OWN BAR. If any stated maximum viable land cost
   is below the stated land cost and the verdict is GO or CONDITIONAL GO, say so.

Plus a prompt rule: headline figures are computed once and quoted, never
restated from memory in the executive verdict.

    python patch_consistency.py mri_v3.html app.py
"""
import shutil
import sys
from pathlib import Path

CALL_OLD = """  // 3. A scorecard is a number the reader is asked to act on, so it has to be
  // auditable from the rows on the page.
  validateScorecard(fullText).forEach(function(w) { allWarnings.push(w); });"""

CALL_NEW = """  // 3. A scorecard is a number the reader is asked to act on, so it has to be
  // auditable from the rows on the page.
  validateScorecard(fullText).forEach(function(w) { allWarnings.push(w); });

  // 4. The headline financials must agree across sections. Each section can be
  // internally consistent and still contradict the next one - that is exactly
  // how a report shipped "19% headroom" in Step 0 and "below breakeven" in
  // Step 3, off two different breakeven figures.
  validateFigures(fullText).forEach(function(w) { allWarnings.push(w); });"""

FN_ANCHOR = "function validateScorecard(text) {"

FIGURES_FN = '''// Cross-section consistency for the numbers a developer decides on.
//
// From the 26-Aug Whitefield report:
//   Step 0  breakeven Rs.10,200 | max viable land Rs.480 Cr at Rs.12,172 | GO
//   Step 3D breakeven Rs.13,200 | max viable land Rs.330 Cr at Rs.12,172
// The land ask was Rs.400 Cr. Step 3 puts it Rs.70 Cr over the bar; Step 0 said
// CONDITIONAL GO. Each section reads fine on its own, so only a cross-section
// check finds it.
//
// Deliberately narrow. Margin and IRR legitimately differ by scenario, so they
// are not compared - a validator that fires on correct reports gets ignored.
function _cnum(s) {
  var v = parseFloat(String(s).replace(/[, ]/g, ""));
  return isFinite(v) ? v : null;
}

function _uniq(a) {
  return a.filter(function (v, i, arr) { return arr.indexOf(v) === i; });
}

function validateFigures(text) {
  var w = [], m;
  if (!text) return w;

  // ── 1. breakeven must be a single number ────────────────────────────────
  // "PSF" can sit on either side of the number - "Breakeven Price PSF: Rs.10,200"
  // as well as "Breakeven ... Rs.13,200 PSF" - so match the number and then
  // require PSF somewhere in the whole match. The gap must allow "Rs." itself,
  // so it cannot exclude "." (that bug made this find nothing at all).
  var be = [], reBe = /break\\s*-?\\s*even[^\\n|]{0,70}?Rs\\.?\\s*([\\d,]+)(?!\\s*(?:Cr|Crore))/gi;
  while ((m = reBe.exec(text)) !== null) {
    if (!/PSF|per\\s*sq/i.test(m[0])) continue;   // a rupee figure, but not a rate
    var v = _cnum(m[1]);
    if (v !== null && v > 500) be.push(v);
  }
  var beU = _uniq(be);
  if (beU.length > 1) {
    w.push("Breakeven price is stated as Rs." + beU.join(" and Rs.") +
           " PSF in different sections - these cannot all be right.");
  }

  // ── 2. maximum viable land cost is a function of price ──────────────────
  // Capture the land figure and, where the same clause names one, the price it
  // was computed at.
  var pairs = [], reLand =
    /max(?:imum)?\\s+viable\\s+land[^\\n|]{0,80}?Rs\\.?\\s*\\(?\\s*(-?[\\d,]+(?:\\.\\d+)?)\\s*\\)?\\s*(?:Cr|Crore)/gi;
  while ((m = reLand.exec(text)) !== null) {
    var land = _cnum(m[1]);
    if (land === null) continue;
    // The price must come from THIS clause. Looking backwards picked up the
    // previous line's breakeven and paired it with the wrong land figure.
    // Two real shapes:
    //   "Max Viable Land (at Rs.12,172 PSF, 15% margin) ~Rs.330 Cr"  -> inside
    //   "Max Viable Land: Rs.480 Crores (at Rs.12,172 PSF pricing)"  -> just after
    var tail = text.slice(m.index + m[0].length,
                          m.index + m[0].length + 80).split("\\n")[0];
    var rePsf = /Rs\\.?\\s*([\\d,]+)\\s*PSF/i;
    var pm = rePsf.exec(m[0]) || rePsf.exec(tail);
    pairs.push({ land: land, price: pm ? _cnum(pm[1]) : null });
  }
  var byPrice = {};
  pairs.forEach(function (p) {
    if (p.price === null) return;
    (byPrice[p.price] = byPrice[p.price] || []).push(p.land);
  });
  Object.keys(byPrice).forEach(function (price) {
    var lands = _uniq(byPrice[price]);
    if (lands.length > 1) {
      w.push("Maximum viable land cost at Rs." + Number(price).toLocaleString() +
             " PSF is given as Rs." + lands.join(" Cr and Rs.") +
             " Cr in different sections.");
    }
  });
  var byLand = {};
  pairs.forEach(function (p) {
    if (p.price === null) return;
    (byLand[p.land] = byLand[p.land] || []).push(p.price);
  });
  Object.keys(byLand).forEach(function (land) {
    var prices = _uniq(byLand[land]);
    if (prices.length > 1) {
      w.push("A maximum viable land cost of Rs." + land +
             " Cr is attributed to two different prices (Rs." +
             prices.join(" and Rs.") + " PSF).");
    }
  });

  // ── 3. a GO verdict has to clear its own bar ────────────────────────────
  // "Land Cost" also occurs inside "Maximum Viable Land Cost", which would make
  // the bar look like the ask. Skip any match preceded by "viable".
  var ask = null, reAsk =
    /land\\s*(?:cost|price|ask|acquisition)[^\\n|]{0,40}?Rs\\.?\\s*([\\d,]+(?:\\.\\d+)?)\\s*(?:Cr|Crore)/gi;
  while ((m = reAsk.exec(text)) !== null) {
    var lead = text.slice(Math.max(0, m.index - 30), m.index + 12);
    if (/viable/i.test(lead)) continue;
    ask = _cnum(m[1]);
    break;
  }
  if (ask === null) {
    var am2 = /Rs\\.?\\s*([\\d,]+(?:\\.\\d+)?)\\s*(?:Cr|Crore)[^\\n|]{0,30}?land\\s*(?:cost|ask)/i.exec(text);
    if (am2) ask = _cnum(am2[1]);
  }
  var isGo = /VERDICT\\s*:?\\s*(CONDITIONAL\\s+)?GO\\b/i.test(text) &&
             !/VERDICT\\s*:?\\s*NO[\\s-]*GO/i.test(text);
  if (ask !== null && isGo && pairs.length) {
    var below = pairs.filter(function (p) { return p.land < ask; });
    if (below.length && below.length === pairs.length) {
      w.push("Verdict is GO, but every maximum viable land cost stated (Rs." +
             _uniq(below.map(function (p) { return p.land; })).join(" Cr, Rs.") +
             " Cr) is below the Rs." + ask + " Cr land cost.");
    } else if (below.length) {
      w.push("Verdict is GO, but at least one maximum viable land cost (Rs." +
             _uniq(below.map(function (p) { return p.land; })).join(" Cr, Rs.") +
             " Cr) is below the Rs." + ask + " Cr land cost - state which price " +
             "the verdict assumes.");
    }
  }

  return w;
}

'''

PROMPT_ANCHOR = "Follow this EXACT framework."
PROMPT_BLOCK = """Follow this EXACT framework.

**THE EXECUTIVE VERDICT QUOTES; IT DOES NOT RESTATE.**
Write STEP 0 last, after the economics exist, and copy its figures from the body
verbatim. Never write a headline number from memory or from a rough sense of the
answer - that is how one report carried breakeven Rs.10,200 in Step 0 and
Rs.13,200 in Step 3, and told a developer he had 19% headroom when he was
Rs.1,028 PSF short.
- Breakeven, margin, IRR, equity multiple, maximum viable land and the site
  score appear ONCE as computed values. Every later mention repeats those exact
  digits.
- A maximum viable land cost is meaningless without the price it assumes. Always
  write it as "Rs.X Cr at Rs.Y PSF".
- Before issuing GO or CONDITIONAL GO, check the land cost against the maximum
  viable land cost at the price you are recommending. If the ask exceeds it, the
  verdict is NO-GO or CONDITIONAL on a higher price - say which.

Follow this EXACT framework:"""


def patch_html(path: Path) -> bool:
    src = path.read_text(encoding="utf-8")
    if "validateFigures" in src:
        print("  ! figure checks already present")
        return False
    if src.count(CALL_OLD) != 1 or src.count(FN_ANCHOR) != 1:
        print(f"  ! anchors: call={src.count(CALL_OLD)} fn={src.count(FN_ANCHOR)} (expected 1 and 1)")
        return False
    src = src.replace(CALL_OLD, CALL_NEW, 1)
    src = src.replace(FN_ANCHOR, FIGURES_FN + FN_ANCHOR, 1)
    shutil.copy2(path, path.with_suffix(path.suffix + ".pre_consistency"))
    path.write_text(src, encoding="utf-8")
    print("  added: breakeven / max-viable-land / GO-vs-ask cross-section checks")
    return True


def patch_app(path: Path) -> bool:
    src = path.read_text(encoding="utf-8")
    if "THE EXECUTIVE VERDICT QUOTES" in src:
        print("  ! verdict rule already present")
        return False
    if src.count(PROMPT_ANCHOR) != 1:
        print(f"  ! framework anchor found {src.count(PROMPT_ANCHOR)} times")
        return False
    src = src.replace(PROMPT_ANCHOR, PROMPT_BLOCK, 1)
    shutil.copy2(path, path.with_suffix(path.suffix + ".pre_consistency"))
    path.write_text(src, encoding="utf-8")
    print("  added: Step 0 quotes the body; land cost checked against the bar")
    return True


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    a = patch_html(Path(sys.argv[1]))
    b = patch_app(Path(sys.argv[2]))
    return 0 if (a or b) else 1


if __name__ == "__main__":
    sys.exit(main())
