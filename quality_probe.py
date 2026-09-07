#!/usr/bin/env python3
"""
quality_probe.py — turn "the quality feels worse" into a number.

You changed provider, model and speed at the same time and are now judging the
result by eye, on different reports each time. That cannot converge. This runs
the SAME fixed queries against your deployed API, applies deterministic checks
drawn from the rules we actually built, and writes a scored run to disk. Then it
diffs two runs so a configuration change has an answer instead of an impression.

    # baseline
    MRI_URL=https://mri-api.onrender.com python3 quality_probe.py run --label claude

    # change Render env vars, redeploy, then
    MRI_URL=https://mri-api.onrender.com python3 quality_probe.py run --label terra-med

    python3 quality_probe.py compare claude terra-med

WHAT IS AND IS NOT MEASURED
    Measured: rule obedience, and latency. Every check below corresponds to a
    specific failure that actually shipped and cost you something - the -27.48
    Cr land cost, the fabricated carpet PSF, the benchmark projects 14 km from
    the pin, the report that stopped mid-table, the 65/60/60 scorecard.

    Not measured: whether the prose is good. No script can score that, and
    pretending otherwise would be the same mistake as scoring a report by
    whether it looks confident. Read the two reports side by side for that; the
    script tells you which one to trust while you do.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RUNS = Path(__file__).parent / "runs"
POLL_SECS = 1.5
MAX_WAIT_SECS = 900


# ── the fixed query set ────────────────────────────────────────────────────
# Fixed on purpose. A moving query set measures the queries, not the model.
PROBES = [
    {
        "id": "feas_complete",
        "city": "Hinjewadi",
        "query": ("Feasibility for a 5 acre plot at 18.5913, 73.7389 in Hinjewadi. "
                  "Land cost Rs.9 Cr per acre. Construction cost Rs.2,600 per sq.ft "
                  "saleable. FSI 2.0. Give me the full appraisal."),
        "want": ["computed_block_ran", "no_prose_arithmetic", "one_verdict",
                 "complete", "no_negative_land_cost", "price_is_current"],
    },
    {
        "id": "feas_missing_input",
        "city": "Hinjewadi",
        "query": ("Feasibility for a 5 acre plot at 18.5913, 73.7389 in Hinjewadi. "
                  "Land cost Rs.9 Cr per acre. Give me the full appraisal."),
        # THE ONE THAT MATTERS MOST. Construction cost is absent. The calculator
        # must abstain visibly and the model must not fill the hole with a
        # plausible number. Everything else on this list is a detail beside it.
        "want": ["abstained", "no_pnl_after_abstention", "no_prose_arithmetic",
                 "complete"],
    },
    {
        "id": "proximity",
        "city": "Whitefield",
        "query": ("Top 10 projects in the closest vicinity of 12.9698, 77.7500. "
                  "Show me the competition benchmark set."),
        "want": ["has_distance_column", "radius_stated", "complete"],
    },
    {
        "id": "carpet_basis",
        "city": "Hinjewadi",
        "query": "What is the carpet area price PSF in Hinjewadi, latest quarter?",
        "want": ["carpet_answered", "no_ratio_conversion", "derived_labelled",
                 "complete"],
    },
    {
        "id": "market_overview",
        "city": "Whitefield",
        "query": "Give me a market overview for Whitefield.",
        "want": ["price_is_current", "complete", "no_prose_arithmetic"],
    },
    {
        "id": "micromarket_rank",
        "city": "Kolkata",
        "query": "Rank the micromarkets in Kolkata by demand.",
        "want": ["complete", "no_prose_arithmetic"],
    },
]


# ── the checks ─────────────────────────────────────────────────────────────
# Each returns (passed: bool, evidence: str). Evidence is the point: a failing
# check must hand you the exact text so you can confirm it yourself rather than
# trusting this script the way we were trusting the reports.

TRUNCATION = ("[Response truncated", "[Generation stopped", "⚠ INCOMPLETE")

# GO / NO-GO are an uppercase convention in these reports, so the token match
# stays case-SENSITIVE. Matching them case-insensitively would read the ordinary
# English "go" in "the developer will go ahead" as a verdict and invent a
# contradiction. The one lowercase phrase worth catching is spelled out below.
VERDICT_RE = re.compile(r"\bNO[\s-]?GO\b|\bGO\b")
NO_GO_PHRASE_RE = re.compile(r"\bdo(?:es)?\s+not\s+proceed\b|\bnot\s+recommended\b",
                             re.IGNORECASE)

# "4,35,600 x 12,029 = Rs.524 Cr" or "Rs.524 Cr / 5 = 104.8 Cr".
# Both operands and the result may carry a unit or a currency prefix; the first
# version of this regex allowed neither and therefore caught neither of the two
# forms that actually appear in the reports.
_UNIT = (r"(?:\s*(?:Cr|Crore|Crores|Lakh|Lakhs|PSF|sq\.?\s*ft|sq\.?\s*m|"
         r"units?|acres?|%))?")
_NUM = r"(?:Rs\.?|₹)?\s*[\d,]+(?:\.\d+)?" + _UNIT
PROSE_MATHS_RE = re.compile(
    _NUM + r"\s*(?:[x×*/÷]|multiplied by|divided by)\s*" + _NUM +
    r"\s*=\s*(?:Rs\.?|₹)?\s*[\d,]",
    re.IGNORECASE)
# a carpet/saleable ratio being applied rather than a carpet series being read
RATIO_RE = re.compile(r"0\.7[0-9]\s*(?:[x×*]|ratio|factor|of\s+saleable)", re.IGNORECASE)
MONEY_NEG_RE = re.compile(r"\(\s*-\s*[\d,]+(?:\.\d+)?\s*(?:Cr|Crore)", re.IGNORECASE)
DIST_HDR_RE = re.compile(r"\|\s*(?:distance|dist\.?|km|distance_km|distance \(km\))\s*\|",
                         re.IGNORECASE)
RADIUS_RE = re.compile(r"\bradius\b[^.\n]{0,40}?\b\d+(?:\.\d+)?\s*km", re.IGNORECASE)
# any four-digit PSF figure, used to spot a stale-series answer
PSF_RE = re.compile(r"(?:Rs\.?|₹)\s*([\d,]{3,9})\s*(?:per\s*sq\.?\s*ft|PSF|/sq\.?ft)",
                    re.IGNORECASE)


def _evidence(text: str, m, span: int = 90) -> str:
    if not m:
        return ""
    a = max(0, m.start() - span // 2)
    return "…" + text[a:m.end() + span // 2].replace("\n", " ") + "…"


def c_complete(t):
    hit = next((k for k in TRUNCATION if k in t), None)
    return (hit is None, f"found {hit!r}" if hit else "")


def c_computed_block_ran(t):
    ok = "COMPUTED FEASIBILITY" in t and "NOT RUN" not in t
    return (ok, "" if ok else "no computed block, or it abstained")


def c_abstained(t):
    ok = "COMPUTED FEASIBILITY: NOT RUN" in t
    return (ok, "" if ok else "the calculator did not visibly abstain")


def c_no_pnl_after_abstention(t):
    """After NOT RUN, no P&L may appear. This is the fabrication check."""
    if "COMPUTED FEASIBILITY: NOT RUN" not in t:
        return (False, "abstention block absent, so this cannot pass")
    tail = t.split("COMPUTED FEASIBILITY: NOT RUN", 1)[1]
    banned = []
    for word in ("IRR", "Maximum Viable Land", "Net Margin", "Gross Margin",
                 "Breakeven", "Profit"):
        m = re.search(rf"{re.escape(word)}[^\n]{{0,40}}?[\d]", tail, re.IGNORECASE)
        if m:
            banned.append(f"{word}: {_evidence(tail, m)}")
    return (not banned, " | ".join(banned))


def c_no_prose_arithmetic(t):
    m = PROSE_MATHS_RE.search(t)
    return (m is None, _evidence(t, m))


def c_one_verdict(t):
    """A GO in the summary and a NO-GO in the table is the 26-Aug failure."""
    verdicts = {("NO-GO" if v.replace(" ", "").replace("-", "") == "NOGO" else "GO")
                for v in VERDICT_RE.findall(t)}
    if NO_GO_PHRASE_RE.search(t):
        verdicts.add("NO-GO")
    return (len(verdicts) <= 1, f"conflicting verdicts: {sorted(verdicts)}"
            if len(verdicts) > 1 else "")


def c_no_negative_land_cost(t):
    m = MONEY_NEG_RE.search(t)
    return (m is None, _evidence(t, m))


def c_has_distance_column(t):
    m = DIST_HDR_RE.search(t)
    return (m is not None, "" if m else "no table carries a distance column")


def c_radius_stated(t):
    m = RADIUS_RE.search(t)
    return (m is not None, "" if m else "the search radius is never stated")


def c_carpet_answered(t):
    ok = re.search(r"\bcarpet\b[^.\n]{0,60}?(?:Rs\.?|₹)\s*[\d,]{3,}", t, re.IGNORECASE)
    return (ok is not None, "" if ok else "no carpet price returned")


def c_no_ratio_conversion(t):
    m = RATIO_RE.search(t)
    return (m is None, _evidence(t, m))


def c_derived_labelled(t):
    """If the word 'derived' appears it must be attached to a basis statement."""
    if "erived" not in t:
        return (True, "")
    ok = re.search(r"[Dd]erived[^.\n]{0,80}?(?:not in|LF|source|basis)", t)
    return (ok is not None, "a figure is called derived without naming its basis")


def c_price_is_current(t):
    """The stale-series bug printed a 2014 price. Anything under Rs.5,000 PSF
    in these micromarkets is almost certainly the oldest row, not the newest."""
    vals = [int(v.replace(",", "")) for v in PSF_RE.findall(t)]
    vals = [v for v in vals if 1000 <= v <= 100000]
    if not vals:
        return (True, "no PSF figure to check")
    low = [v for v in vals if v < 5000]
    return (not low, f"suspiciously old-looking PSF values: {low}" if low else "")


CHECKS = {name[2:]: fn for name, fn in list(globals().items()) if name.startswith("c_")}


# ── driving the API ────────────────────────────────────────────────────────
def _post(url, payload, timeout=60):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def _get(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def run_probe(base, probe):
    t0 = time.time()
    started = _post(f"{base}/api/query/async",
                    {"query": probe["query"], "city": probe["city"], "stream": True})
    job = started.get("job_id")
    if not job:
        return {"error": f"no job id: {started}"}
    text, cursor, first_byte = "", 0, None
    while True:
        time.sleep(POLL_SECS)
        r = _get(f"{base}/api/query/result/{job}?cursor={cursor}")
        if r.get("delta"):
            if first_byte is None:
                first_byte = time.time() - t0
            text += r["delta"]
            cursor = r["cursor"]
        if r.get("status") in ("done", "error"):
            return {"text": text, "status": r["status"], "error": r.get("error"),
                    "partial": r.get("partial"), "meta": r.get("done_meta"),
                    "elapsed": round(time.time() - t0, 1),
                    "first_byte": round(first_byte, 1) if first_byte else None,
                    "chars": len(text)}
        if time.time() - t0 > MAX_WAIT_SECS:
            return {"text": text, "status": "timeout", "elapsed": MAX_WAIT_SECS,
                    "chars": len(text)}


def cmd_run(args):
    base = (os.environ.get("MRI_URL") or args.url or "").rstrip("/")
    if not base:
        sys.exit("set MRI_URL or pass --url")
    try:
        health = _get(f"{base}/api/health")
    except Exception as e:
        sys.exit(f"cannot reach {base}/api/health: {e}")
    print(f"target   {base}")
    print(f"provider {health.get('llm_provider')}  model {health.get('llm_model')}  "
          f"reasoning {health.get('llm_reasoning', 'n/a')}  build {health.get('build')}\n")

    only = set(args.only.split(",")) if args.only else None
    results, scored = [], 0
    for probe in PROBES:
        if only and probe["id"] not in only:
            continue
        print(f"── {probe['id']} ({probe['city']})")
        try:
            out = run_probe(base, probe)
        except Exception as e:
            out = {"error": repr(e), "text": "", "chars": 0, "elapsed": None}
        text = out.get("text", "")
        checks = {}
        for name in probe["want"]:
            fn = CHECKS.get(name)
            if fn is None:
                checks[name] = {"pass": None, "evidence": "unknown check"}
                continue
            ok, ev = fn(text)
            checks[name] = {"pass": bool(ok), "evidence": ev}
            print(f"   {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {ev}" if ev else ""))
        passed = sum(1 for c in checks.values() if c["pass"])
        scored += passed
        print(f"   {passed}/{len(checks)}   {out.get('elapsed')}s"
              f"  first byte {out.get('first_byte')}s  {out.get('chars')} chars\n")
        results.append({**{k: v for k, v in out.items() if k != "text"},
                        "id": probe["id"], "checks": checks, "text": text})

    total = sum(len(r["checks"]) for r in results)
    RUNS.mkdir(exist_ok=True)
    path = RUNS / f"{args.label}.json"
    path.write_text(json.dumps({
        "label": args.label,
        "when": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "health": health, "results": results,
        "score": {"passed": scored, "total": total},
    }, indent=2), encoding="utf-8")
    print("=" * 70)
    print(f"{args.label}: {scored}/{total} checks passed  →  {path}")
    return 0 if scored == total else 1


def cmd_compare(args):
    runs = []
    for label in (args.a, args.b):
        p = RUNS / f"{label}.json"
        if not p.exists():
            sys.exit(f"no run named {label!r} (looked in {p})")
        runs.append(json.loads(p.read_text(encoding="utf-8")))
    a, b = runs

    def cfg(r):
        h = r["health"]
        return (f"{h.get('llm_provider')}/{h.get('llm_model')}"
                f" @ {h.get('llm_reasoning', 'n/a')}")

    print(f"A  {a['label']:<16} {cfg(a)}   {a['score']['passed']}/{a['score']['total']}")
    print(f"B  {b['label']:<16} {cfg(b)}   {b['score']['passed']}/{b['score']['total']}\n")

    ai = {r["id"]: r for r in a["results"]}
    bi = {r["id"]: r for r in b["results"]}
    print(f"{'probe':<22}{'A':>10}{'B':>10}   {'A secs':>8}{'B secs':>8}")
    print("-" * 62)
    for pid in [p["id"] for p in PROBES]:
        ra, rb = ai.get(pid), bi.get(pid)
        if not ra or not rb:
            continue
        pa = sum(1 for c in ra["checks"].values() if c["pass"])
        pb = sum(1 for c in rb["checks"].values() if c["pass"])
        n = len(ra["checks"])
        flag = "  <-- worse" if pb < pa else ("  <-- better" if pb > pa else "")
        print(f"{pid:<22}{pa}/{n:<8}{pb}/{n:<8}"
              f"{str(ra.get('elapsed')):>8}{str(rb.get('elapsed')):>8}{flag}")

    print("\nREGRESSIONS  (passed in A, failed in B)")
    found = False
    for pid in [p["id"] for p in PROBES]:
        ra, rb = ai.get(pid), bi.get(pid)
        if not ra or not rb:
            continue
        for name, ca in ra["checks"].items():
            cb = rb["checks"].get(name, {})
            if ca.get("pass") and cb.get("pass") is False:
                found = True
                print(f"  {pid} / {name}")
                if cb.get("evidence"):
                    print(f"      {cb['evidence']}")
    if not found:
        print("  none")

    ta = sum(r.get("elapsed") or 0 for r in a["results"])
    tb = sum(r.get("elapsed") or 0 for r in b["results"])
    print(f"\ntotal wall clock   A {ta:.0f}s    B {tb:.0f}s")
    if tb < ta and b["score"]["passed"] < a["score"]["passed"]:
        print("B is faster and worse - that is a trade, not an improvement. "
              "Decide it explicitly.")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the probe set and save a scored run")
    r.add_argument("--label", required=True, help="name for this configuration")
    r.add_argument("--url", default="", help="API base (or set MRI_URL)")
    r.add_argument("--only", default="", help="comma-separated probe ids")
    r.set_defaults(fn=cmd_run)
    c = sub.add_parser("compare", help="diff two saved runs")
    c.add_argument("a")
    c.add_argument("b")
    c.set_defaults(fn=cmd_compare)
    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
