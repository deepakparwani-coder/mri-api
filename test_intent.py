#!/usr/bin/env python3
"""
test_intent.py — the progress line must describe what the server is doing.

"Reading the graph and computing feasibility..." was hardcoded in the frontend
and shown over every query, including price lookups that run no calculator at
all. The fix is not a better string: it is that the label and the token ceiling
now come from ONE function, so they cannot disagree.
"""
import re
import sys
from pathlib import Path

APP = (Path("/root/llm/app.py")).read_text(encoding="utf-8")
HTML = (Path("/root/llm/mri_v3.html")).read_text(encoding="utf-8")

FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


# lift describe_intent out of app.py - importing app.py needs Neo4j and an SDK
ns = {"re": re}
fre = APP.index("_FEASIBILITY_RE = re.compile(")
exec(APP[fre:APP.index("def needs_web(query)")], ns)
lab = APP.index("_INTENT_LABELS = (")
exec(APP[lab:APP.index("def build_feasibility_block")], ns)
describe_intent = ns["describe_intent"]


CASES = [
    # query, expect_feasibility, expected label fragment
    ("Feasibility for a 5 acre plot at Hinjewadi, land cost Rs.9 Cr per acre",
     True, "feasibility"),
    ("What FSI applies to this plot?", True, "feasibility"),
    ("Site intelligence for this google maps pin", True, "feasibility"),
    ("What is the carpet area price PSF in Hinjewadi?", False, "carpet"),
    ("Show me RERA carpet basis rates", False, "carpet"),
    ("Top 10 projects near 12.9698, 77.7500", False, "pin"),
    ("Projects in the closest vicinity of Whitefield", False, "distance"),
    ("What is the sales velocity in Whitefield?", False, "absorption"),
    ("Months inventory for Kolkata", False, "absorption"),
    ("Price trend for Whitefield over the last 8 quarters", False, "price series"),
    ("Compare Godrej Woodscape versus Prestige Raintree Park", False, "Comparing"),
    ("Rank the micromarkets in Kolkata by demand", False, "Ranking the market"),
    ("Tell me about Kolkata", False, "Reading the graph"),
]


def main() -> int:
    print("\n1. THE LABEL MATCHES THE QUERY")
    for q, want_feas, frag in CASES:
        got = describe_intent(q)
        ok = got["is_feasibility"] == want_feas and frag.lower() in got["label"].lower()
        check(f"{q[:46]!r}", ok, f"{got['label']!r} feas={got['is_feasibility']}")

    print("\n2. THE ORIGINAL BUG IS GONE")
    non_feas = [q for q, f, _ in CASES if not f]
    check("no non-feasibility query claims feasibility",
          all(not describe_intent(q)["is_feasibility"] for q in non_feas))
    check("no non-feasibility query shows the word 'feasibility'",
          all("feasib" not in describe_intent(q)["label"].lower() for q in non_feas))

    print("\n3. ONE SOURCE OF TRUTH")
    check("the token ceiling is derived from describe_intent",
          'is_feasibility = describe_intent(user_query)["is_feasibility"]' in APP)
    check("the old inline regex is gone",
          "is_feasibility = bool(re.search(" not in APP)
    check("the feasibility pattern is defined once",
          APP.count("_FEASIBILITY_RE = re.compile(") == 1)
    check("the web router uses the same pattern",
          "if _FEASIBILITY_RE.search(q):" in APP)
    check("no second inline feasibility regex survives",
          "r'feasib|plot" not in APP.replace("_FEASIBILITY_RE = re.compile(", "", 1)
          or APP.count("feasib|plot") == 1)
    check("the async submit returns the label", '"status_label": intent["label"]' in APP)

    print("\n4. THE FRONTEND NO LONGER GUESSES")
    check("the hardcoded string is gone",
          "Reading the graph and computing feasibility" not in HTML)
    check("it reads the server's label", "if (j.status_label) statusLabel = j.status_label;" in HTML)
    check("it is used in the ticking line", "hint(firstToken ? (statusLabel" in HTML)
    check("an old backend degrades to a neutral line",
          'var statusLabel = "Reading the graph";' in HTML)
    check("the declaration precedes poll()",
          HTML.index('var statusLabel = "Reading the graph";') < HTML.index("function poll()"))

    print("\n5. ORDERING IS DELIBERATE, NOT ACCIDENTAL")
    # "feasibility for a plot near 12.97, 77.75" is a feasibility query that also
    # mentions a pin. Feasibility must win, or the token ceiling drops to 8000.
    mixed = "Feasibility for a 5 acre plot near 12.9698, 77.7500 with carpet pricing"
    check("feasibility outranks pin and carpet", describe_intent(mixed)["is_feasibility"],
          describe_intent(mixed)["label"])
    # a carpet PRICE question must not be swallowed by the generic price rule
    check("carpet outranks the generic price rule",
          "carpet" in describe_intent("carpet price psf trend").get("label", "").lower())
    # the two detectors used to disagree on exactly these
    for q in ("due diligence on this land parcel", "land acquisition appraisal"):
        check(f"{q!r} is feasibility for BOTH consumers",
              describe_intent(q)["is_feasibility"] and ns["_FEASIBILITY_RE"].search(q) is not None)

    print("\n" + "=" * 66)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
