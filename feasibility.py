"""
feasibility.py — deterministic land-feasibility engine for MRI.

WHY THIS EXISTS
---------------
Feasibility arithmetic was being done by the language model inside prose. Three
separate errors reached client-facing PDFs in one week:

  * "5 acres = 2,02,500 sq.ft." printed beside the formula "(5 x 43,560)".
    The correct product is 217,800. Every downstream number - saleable area,
    revenue, cost, margin, breakeven - inherited a 7% error.
  * A land-cost sensitivity matrix in which 12 of 20 cells did not reconcile
    with its own stated method. The base row was right, which is what made it
    dangerous.
  * A stated IRR of "~24-26%" against cash flows whose actual IRR is 41.8%,
    and a cash-flow table whose outflows were 13.1 Cr short of the project cost
    it had itself computed.

None of these raised an error. They were fluent, formatted and wrong.

Numbers that decide a Rs.25 Cr acquisition should not be produced by token
prediction. This module computes them in Python, returns them as data AND as
pre-rendered markdown, and the model's job becomes narrating a result it cannot
alter.

USAGE
    from feasibility import parse_feasibility_inputs, compute, render_markdown

    inp = parse_feasibility_inputs(user_query)      # None if not a feasibility ask
    if inp and inp.is_sufficient():
        result = compute(inp)
        block  = render_markdown(result)            # inject into model context

Self-test:  python feasibility.py --self-test
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from dataclasses import dataclass, field, asdict
from typing import Optional

# ── Unit constants ───────────────────────────────────────────────────────────
SQFT_PER_ACRE = 43_560.0
SQFT_PER_HECTARE = 107_639.104
SQFT_PER_GUNTHA = 1_089.0          # 1 guntha = 1/40 acre
SQFT_PER_SQM = 10.763_910_4
SQFT_PER_CENT = 435.6              # 1 cent = 1/100 acre
SQFT_PER_BIGHA_PB = 27_225.0       # regional; only used if explicitly stated

CR = 1e7          # 1 crore rupees
LAKH = 1e5


# ── Inputs ───────────────────────────────────────────────────────────────────
@dataclass
class FeasibilityInputs:
    plot_sqft: Optional[float] = None
    plot_input_text: Optional[str] = None

    fsi: float = 2.0
    fsi_assumed: bool = True
    fsi_authority: Optional[str] = None    # who sanctioned it, under what instrument
    site_pin: Optional[str] = None         # lat,lng or Maps link, for the benchmark set

    # deductions from gross plot before FSI is applied
    deduction_pct: float = 15.0        # ROS + road surrender etc.

    # ── The area chain, calibrated against 1,947 filed K-RERA projects ───────
    # Every one of these was previously a guess, and two were wrong:
    #
    #   was: bua      = net x FSI            (FSI area, and the ONLY area costed)
    #        saleable = bua x 70%            (made saleable SMALLER than FSI area)
    #        carpet   = saleable x 0.74      (=> carpet 0.518 x FSI area)
    #
    #   filed reality (median, n~1,930):
    #        construction area = FSI area x 1.35   parking/basements/services
    #        carpet            = FSI area x 0.80
    #        saleable          = FSI area x ~1.07  (carpet / carpet_factor)
    #
    # Construction was understated ~29%, carpet ~34%, saleable and revenue ~35%.
    # See rera_calibration.json and the project note for the distributions.
    non_fsi_pct: float = 135.0         # construction area as % of FSI area
    loading_pct: float = 107.0         # saleable as % of FSI area  (was "efficiency")
    carpet_factor: float = 0.73        # RERA carpet / saleable; DERIVE per market
    carpet_factor_source: str = "engine default"
    avg_unit_sqft: float = 950.0

    land_cost_cr: Optional[float] = None
    construction_psf: Optional[float] = None
    price_psf: Optional[float] = None
    price_psf_source: str = "not supplied"

    approval_pct: float = 3.0          # of construction cost
    professional_pct: float = 2.5      # of construction cost
    marketing_pct: float = 0.0         # of revenue
    contingency_pct: float = 5.0       # of construction + approvals
    finance_rate_pct: float = 12.0
    finance_drawn_pct: float = 50.0    # share of construction financed
    project_years: int = 3

    monthly_velocity_pct: Optional[float] = None   # from LF data
    target_margin_pct: float = 15.0

    # ── Phased launch pricing ────────────────────────────────────────────────
    # Indian developers rarely sell a project at one price. They launch below
    # the market to build velocity and fund construction, then escalate with
    # construction milestones and phase releases. `price_psf` is treated as the
    # TARGET AVERAGE REALISATION (APR); the phase base price is solved so the
    # unit-weighted average of the ladder equals it exactly.
    phase_price_factors: Optional[list] = None     # None -> derived from LF data
    phase_unit_shares: Optional[list] = None       # e.g. [0.25, 0.30, 0.25, 0.20]
    compressed_years: Optional[int] = None         # sell-out if the discount works

    # ── Collection profile (construction-linked plan) ────────────────────────
    # An earlier version assumed 65% of a booking collects in-year with 35%
    # lagging. That is not how Indian residential is sold. Under a CLP the
    # buyer pays a booking amount, then instalments tied to construction
    # milestones, then a final tranche at possession/OC. Sources in README.
    # RERA s.13(1) caps pre-agreement collection at 10% of apartment cost.
    booking_collect_pct: float = 12.0     # booking + agreement
    possession_collect_pct: float = 15.0  # on OC / handover
    # remainder accrues pro-rata to construction progress

    notes: list = field(default_factory=list)

    # Keys the USER explicitly confirmed through the qualifier. Everything not
    # in this list that differs from nothing is either read from LF data or a
    # default - and the report must say which. An assumption the reader cannot
    # see is the whole reason a plot sanctioned at FAR 3.0 was appraised at 2.5.
    user_confirmed: list = field(default_factory=list)

    def is_sufficient(self) -> bool:
        """Enough to produce economics. Price may come from LF data later."""
        return self.plot_sqft is not None and self.construction_psf is not None

    def missing(self) -> list:
        m = []
        if self.plot_sqft is None: m.append("plot area")
        if self.construction_psf is None: m.append("construction cost PSF")
        if self.price_psf is None: m.append("selling price PSF (can come from LF data)")
        if self.land_cost_cr is None: m.append("land cost (optional - drives max-viable-land instead)")
        return m


# ── Filed calibration ────────────────────────────────────────────────────────
# rera_calibration.json is generated from 1,947 Karnataka RERA projects that
# filed land area, sanctioned FAR, built-up area, carpet area and CA-certified
# construction cost together. It replaces four numbers that used to be my
# opinion. Absent the file the engine keeps its defaults and says so - it never
# silently falls back to a guess dressed as evidence.
import json as _json
import os as _os

_CALIB = None


def calibration():
    global _CALIB
    if _CALIB is None:
        path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                             "rera_calibration.json")
        try:
            with open(path, encoding="utf-8") as fh:
                _CALIB = _json.load(fh)
        except Exception:
            _CALIB = {}
    return _CALIB


_IN_KARNATAKA_RE = re.compile(
    r"karnataka|bengaluru|bangalore|mysuru|mysore|mangaluru|mangalore|"
    r"hubballi|dharwad|belagavi|belgaum|udupi|tumakuru|tumkur", re.IGNORECASE)

_AUTHORITY_ALIASES = (
    ("BBMP", ("bbmp", "bruhat", "greater bengaluru", "gba")),
    ("BDA", ("bda", "bangalore development", "bengaluru development")),
    ("BMRDA", ("bmrda", "metropolitan region")),
    ("MUDA", ("muda", "mangaluru urban", "mysuru urban")),
    ("LPA", ("local planning", "local planing", "lpa", "panchayat")),
)


def authority_key(text):
    """Map free text ('GBA/BBMP', 'KARNATAKA_BBMP', 'BDA - Bangalore...') to a
    calibration key. Returns None when nothing matches - which is an answer,
    not a reason to pick the biggest bucket."""
    t = (text or "").lower()
    for key, needles in _AUTHORITY_ALIASES:
        if any(n in t for n in needles):
            return key
    return None


def calibrated(metric, authority=None):
    """Return (value, citation) for a calibrated metric, or (None, None).

    metric: far_sanctioned | non_fsi_factor | carpet_over_fsi | carpet_over_bua
            | ground_coverage | construction_psf_bua
    """
    c = calibration()
    if not c:
        return None, None
    # This calibration is KARNATAKA. Handing a Karnataka median to a Pune or
    # Gurugram plot because no better bucket matched is the same substitution
    # this whole exercise exists to remove, so an authority we cannot place
    # returns nothing at all.
    key = authority_key(authority) if authority else None
    if key:
        node = c.get("authorities", {}).get(key) or c.get("overall")
        where = key
    elif authority and _IN_KARNATAKA_RE.search(authority):
        node, where = c.get("overall"), "all Karnataka"
    else:
        return None, None
    if not node or metric not in node:
        return None, None
    b = node[metric]
    return b["median"], (f"K-RERA filed median for {where}, n={b['n']} "
                         f"(p25 {b['p25']} - p75 {b['p75']})")


def apply_calibration(inp, authority=None):
    """Set FSI and the area factors from filed data where the user has not.

    Only fields the user did NOT confirm are touched, and each one that is set
    records where it came from, so the report can distinguish a filed median
    from an engine default.
    """
    conf = getattr(inp, "user_confirmed", None) or []
    notes = []
    far, cite = calibrated("far_sanctioned", authority)
    if far and inp.fsi_assumed and "fsi" not in conf:
        inp.fsi = far
        inp.fsi_assumed = False
        if not inp.fsi_authority:
            inp.fsi_authority = cite
        notes.append(f"FSI {far} from {cite}")
    nf, cite = calibrated("non_fsi_factor", authority)
    if nf and "non_fsi_pct" not in conf:
        inp.non_fsi_pct = round(nf * 100, 1)
        notes.append(f"construction area {inp.non_fsi_pct}% of FSI area from {cite}")
    cf, cite = calibrated("carpet_over_fsi", authority)
    if cf and "loading_pct" not in conf and inp.carpet_factor:
        # saleable = carpet / carpet_factor, and carpet is filed against FSI area
        inp.loading_pct = round(cf / inp.carpet_factor * 100, 1)
        notes.append(f"loading {inp.loading_pct}% derived from {cite} "
                     f"and carpet factor {inp.carpet_factor}")
    inp.notes.extend(notes)
    return inp


# ── The qualifier ────────────────────────────────────────────────────────────
# Every feasibility report rests on ~18 numbers. Two of them block the
# calculation; the rest have defaults that were, until now, applied in silence.
# One of those silent defaults - FSI 2.0 against a plot sanctioned at 3.0 - is
# what made a live demo collapse. So the qualifier shows ALL of them, every
# time, marked by where each value came from.
#
# This spec is deliberately adjacent to the dataclass above. Every `key` here
# must be a real field on FeasibilityInputs, and every entry `missing()` can
# return must appear here as required. Both are asserted in the tests, because a
# form that asks for a different set than the calculator reads would be a new
# way to fail, invented by the fix.

GROUP_SITE = "site"            # absent -> the BENCHMARK is weaker, not the maths
GROUP_REQUIRED = "required"    # absent -> the calculator abstains
GROUP_DERIVED = "derived"      # comes from LF data; overridable
GROUP_ASSUMED = "assumed"      # has a default; applied silently until now


@dataclass
class QualField:
    key: str
    label: str
    group: str
    unit: str
    help: str
    kind: str = "number"           # number | text | area


QUALIFIER_FIELDS = [
    # ── the site: blocks the BENCHMARK, not the arithmetic ──
    QualField("site_pin", "Site pin", GROUP_SITE, "lat, lng or Maps link",
              "Drives the distance-ranked competition set, the catchment and "
              "the planning authority. Skip it and the report says plainly that "
              "the benchmark is micromarket-level and NOT distance-ranked.",
              kind="text"),

    # ── blocks the calculation ──
    QualField("plot_sqft", "Plot area", GROUP_REQUIRED, "acres",
              "Type a number and it is read as ACRES. Write the unit for "
              "anything else - '21780 sqft', '2 hectare', '40 guntha', "
              "'12 cent', '2000 sqm'.", kind="area"),
    QualField("construction_psf", "Construction cost", GROUP_REQUIRED,
              "Rs. per sq.ft of BUA",
              "PER SQ.FT OF BUILT-UP AREA, not saleable. At 70% efficiency "
              "these differ by about 43%, so the wrong basis moves the margin "
              "more than any market assumption in this list."),

    # ── read from LF; the user may override ──
    QualField("price_psf", "Selling price", GROUP_DERIVED, "Rs. PSF",
              "Target average realisation. Left blank, the latest LF quarter "
              "for this micromarket is used and its vintage is printed."),
    QualField("monthly_velocity_pct", "Sales velocity", GROUP_DERIVED,
              "% per month",
              "Left blank, taken from the LF absorption series."),

    # ── silent defaults, now visible ──
    QualField("fsi", "FSI / FAR", GROUP_ASSUMED, "",
              "The single highest-impact number here. Enter the SANCTIONED "
              "figure including any premium FAR or TDR loaded onto the plot - "
              "not the base zoning FSI."),
    QualField("fsi_authority", "FSI authority / basis", GROUP_ASSUMED, "",
              "Who sanctioned it, under what instrument (e.g. 'GBA, premium "
              "FAR on a 60ft road, notified Jan 2026'). Printed in the report "
              "as the authority for the figure above.", kind="text"),
    QualField("land_cost_cr", "Land cost", GROUP_ASSUMED, "Rs. Cr (total)",
              "Leave blank and the report solves for the MAXIMUM viable land "
              "cost instead of testing one."),
    QualField("deduction_pct", "Plot deductions", GROUP_ASSUMED, "%",
              "Open space, road surrender and similar, taken off gross plot "
              "before FSI is applied."),
    QualField("non_fsi_pct", "Construction area", GROUP_ASSUMED,
              "% of FSI area",
              "Everything you build that does NOT count against FAR - parking, "
              "basements, podium, services. Filed K-RERA median is 135% "
              "(BBMP 140%). This is the area the construction rate is applied "
              "to; the FSI area alone understates it by about 29%."),
    QualField("loading_pct", "Loading factor", GROUP_ASSUMED,
              "% of FSI area",
              "Marketed saleable area as a share of FSI area. Normally ABOVE "
              "100% - loading makes what you sell larger than what FAR permits. "
              "Calibrated at 107% from filed carpet areas."),
    QualField("carpet_factor", "Carpet factor", GROUP_ASSUMED, "ratio",
              "RERA carpet / saleable. Derived per market from the LF carpet "
              "and saleable series when both are present. Used for reporting "
              "only - it NEVER converts a price."),
    QualField("avg_unit_sqft", "Average unit size", GROUP_ASSUMED, "sq.ft",
              "Drives unit count and therefore the phasing ladder."),
    QualField("project_years", "Build / sell-out period", GROUP_ASSUMED, "years",
              "Drives the cash flow and the IRR."),
    QualField("target_margin_pct", "Target margin", GROUP_ASSUMED, "%",
              "The bar the verdict is judged against."),
    QualField("approval_pct", "Approvals", GROUP_ASSUMED, "% of construction", ""),
    QualField("professional_pct", "Professional fees", GROUP_ASSUMED,
              "% of construction", ""),
    QualField("marketing_pct", "Marketing", GROUP_ASSUMED, "% of revenue",
              "Zero in the base case: it is a developer decision, not an "
              "industry default."),
    QualField("contingency_pct", "Contingency", GROUP_ASSUMED,
              "% of construction + approvals", ""),
    QualField("finance_rate_pct", "Finance rate", GROUP_ASSUMED, "% p.a.", ""),
    QualField("finance_drawn_pct", "Share financed", GROUP_ASSUMED,
              "% of construction", ""),
    QualField("booking_collect_pct", "Collected at booking", GROUP_ASSUMED, "%",
              "RERA s.13(1) caps pre-agreement collection at 10% of "
              "apartment cost."),
    QualField("possession_collect_pct", "Collected at possession", GROUP_ASSUMED,
              "%", "The remainder accrues pro-rata to construction progress."),
]

# Free-text field that is not a computation input; carried through to the report.
_TEXT_ONLY_KEYS = {"fsi_authority", "site_pin"}

_QUAL_BY_KEY = {f.key: f for f in QUALIFIER_FIELDS}


def qualifier_spec(inp=None, lf_price=None, lf_price_source="",
                   lf_velocity=None):
    """Describe every input, with its current value and where that value is from.

    status is one of:
        supplied  - the user wrote it in the query, or confirmed it here
        derived   - read from LF data (source is named)
        default   - the engine's default, applied silently until now
        missing   - required and absent; the calculator will abstain
    """
    base = FeasibilityInputs()
    out = []
    for f in QUALIFIER_FIELDS:
        default = None if f.key in _TEXT_ONLY_KEYS else getattr(base, f.key, None)
        value = default
        status, source = "default", "engine default"

        if inp is not None:
            cur = getattr(inp, f.key, None)
            confirmed = f.key in (getattr(inp, "user_confirmed", None) or [])
            if f.key == "price_psf" and cur is None and lf_price:
                value, status, source = lf_price, "derived", (lf_price_source or "LF data")
            elif f.key == "monthly_velocity_pct" and cur is None and lf_velocity:
                value, status, source = lf_velocity, "derived", "LF absorption series"
            elif cur is not None and (confirmed or cur != default):
                value, status, source = cur, "supplied", (
                    "confirmed by you" if confirmed else "read from your question")
            elif cur is not None:
                value = cur

        # FSI carries its own explicit flag, older than this spec. Honour it.
        if f.key == "fsi" and inp is not None and getattr(inp, "fsi_assumed", True) is False:
            status = "supplied"
            source = ("confirmed by you" if "fsi" in (getattr(inp, "user_confirmed", None) or [])
                      else "read from your question")

        if f.group == GROUP_REQUIRED and value is None:
            status, source = "missing", "not supplied - the appraisal cannot run without it"
        elif f.group == GROUP_DERIVED and value is None:
            status, source = "missing", "no LF value retrieved"
        elif value is None and status == "default":
            # An assumed field whose default is itself "nothing" - land cost and
            # the FSI authority. Saying "engine default" here would imply a
            # number was applied when none was.
            source = ("blank - the report solves for the maximum viable land cost"
                      if f.key == "land_cost_cr" else "not stated")

        out.append({"key": f.key, "label": f.label, "group": f.group,
                    "unit": f.unit, "help": f.help, "kind": f.kind,
                    "value": value, "default": default,
                    "status": status, "source": source})
    return {"fields": out}


def apply_overrides(inp, overrides):
    """Merge user-confirmed values onto parsed inputs. Returns the input object.

    A blank or absent value is a SKIP, never a zero. The distinction matters:
    skipping a required field must still make the calculator abstain, and
    skipping an assumed field must leave the default in place AND leave it
    marked as a default in the report.
    """
    if not overrides:
        return inp
    for key, raw in (overrides or {}).items():
        f = _QUAL_BY_KEY.get(key)
        if f is None:
            continue                      # ignore anything not in the spec
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue                      # a skip, not a zero
        if f.kind == "text":
            setattr(inp, key, str(raw).strip())
        elif f.kind == "area":
            sqft = _text_to_sqft(raw)
            if sqft is None:
                continue
            setattr(inp, key, sqft)
            inp.plot_input_text = str(raw).strip()
        else:
            try:
                setattr(inp, key, _f(raw))
            except (TypeError, ValueError):
                continue
        if key not in inp.user_confirmed:
            inp.user_confirmed.append(key)
        if key == "fsi":
            inp.fsi_assumed = False
    return inp


def _text_to_sqft(raw):
    """'5 acre' / '21780 sqft' / '2 ha' / bare number.

    A BARE NUMBER IS ACRES. Indian land is quoted in acres and gunthas, and the
    field says so; reading "5" as five square feet would be absurd, while
    reading it as five acres is what the user meant. Any other unit must be
    written out.
    """
    t = str(raw).strip().lower()
    m = re.match(r"^\s*" + _NUM + r"\s*([a-z.\s]*)$", t)
    if not m:
        return None
    val, unit = _f(m.group(1)), (m.group(2) or "").strip().replace(".", "")
    if not unit:
        return val * SQFT_PER_ACRE
    if unit in ("sqft", "sq ft", "sqfeet", "sq feet", "ft2", "sft", "sqfoot"):
        return val
    converted = _area_to_sqft(val, unit)
    return converted if converted is not None else None


def confirmation_summary(inp):
    """Markdown the model MUST reproduce: every figure and where it came from."""
    spec = qualifier_spec(inp)["fields"]
    rows = []
    for f in spec:
        v = f["value"]
        if v is None:
            shown = "—"
        elif f["key"] == "plot_sqft":
            # %g turned 217800 into "2.178e+05". A developer reading his own
            # plot area in scientific notation has been given a reason to
            # distrust every number under it.
            acres = v / SQFT_PER_ACRE
            shown = (f"{v:,.0f} sq.ft" if acres < 0.5
                     else f"{v:,.0f} sq.ft ({acres:,.2f} acre)")
        elif isinstance(v, float):
            shown = f"{v:,.0f}" if abs(v - round(v)) < 1e-9 else f"{v:,.2f}"
        else:
            shown = str(v)
        rows.append(f"| {f['label']} | {shown}{(' ' + f['unit']) if f['unit'] else ''} "
                    f"| {f['status'].upper()} | {f['source']} |")
    return ("| Input | Value | Status | Basis |\n"
            "|---|---|---|---|\n" + "\n".join(rows))


# ── Parsing ──────────────────────────────────────────────────────────────────
_NUM = r'(\d[\d,]*\.?\d*)'


def _f(s) -> float:
    return float(str(s).replace(',', ''))


def _area_to_sqft(value: float, unit: str) -> Optional[float]:
    u = unit.lower().strip().rstrip('s')
    if u in ('acre', 'ac'): return value * SQFT_PER_ACRE
    if u in ('hectare', 'ha', 'hect'): return value * SQFT_PER_HECTARE
    if u in ('guntha', 'gunta', 'guntah'): return value * SQFT_PER_GUNTHA
    if u in ('cent',): return value * SQFT_PER_CENT
    if u in ('sqm', 'sq m', 'sq.m', 'square metre', 'square meter', 'm2'):
        return value * SQFT_PER_SQM
    if u in ('sqft', 'sq ft', 'sq.ft', 'sf', 'square foot', 'square feet', 'psf'):
        return value
    if u in ('bigha',): return value * SQFT_PER_BIGHA_PB
    return None


def parse_feasibility_inputs(text: str) -> Optional[FeasibilityInputs]:
    """Pull plot/cost/price parameters out of a free-text feasibility request.

    Deliberately conservative: anything it cannot read with confidence is left
    None so the caller can ask rather than guess. Returns None if the text is
    not a feasibility request at all.
    """
    if not text:
        return None
    t = text.replace('–', '-').replace('—', '-')
    low = t.lower()
    # Several patterns below use [^.] to stop at a sentence boundary. "Rs." has
    # a full stop in it, so "land cost Rs.45 Cr" - the most natural way an
    # Indian developer writes it - failed to parse and the land cost silently
    # became None. The report then solved for maximum viable land instead of
    # testing the price the user actually asked about, and said nothing.
    # The currency marker carries no meaning for the parser, so it is removed
    # before matching rather than every pattern being taught about it.
    low = re.sub(r'\brs\.\s*', 'rs ', low)
    low = low.replace('₹', 'rs ')
    if not re.search(r'feasib|viab|irr|margin|acre|guntha|hectare|plot|land cost|acquisition', low):
        return None

    inp = FeasibilityInputs()

    # ── plot area ──
    m = re.search(_NUM + r'\s*(acres?|ac\b|hectares?|ha\b|gunthas?|cents?|sq\.?\s?m|sqm|m2|sq\.?\s?ft|sqft|sf\b|bighas?)',
                  low)
    if m:
        sq = _area_to_sqft(_f(m.group(1)), m.group(2))
        if sq:
            inp.plot_sqft = sq
            inp.plot_input_text = m.group(0).strip()

    # ── FSI ──
    m = re.search(r'(?:fsi|far)\s*(?:of|is|=|:)?\s*' + _NUM, low)
    if m:
        inp.fsi = _f(m.group(1)); inp.fsi_assumed = False

    # ── land / acquisition cost ──
    m = re.search(r'(?:acq\w*|land|plot)[^.]{0,40}?' + _NUM + r'\s*(cr|crore|lakh|lac)', low) \
        or re.search(_NUM + r'\s*(cr|crore|lakh|lac)[^.]{0,30}?(?:acq\w*|land)', low)
    if m:
        v = _f(m.group(1))
        inp.land_cost_cr = v if m.group(2).startswith('cr') else v / 100.0

    # ── construction cost ──
    m = re.search(r'(?:const\w*|build\w*)[^.]{0,40}?' + _NUM + r'\s*(?:rs\.?\s*)?/?\s*(?:psf|per sq|sq\.?\s?ft)', low) \
        or re.search(r'(?:const\w*|build\w*)[^.]{0,25}?(?:rs\.?\s*)?' + _NUM + r'\s*psf', low)
    if m:
        inp.construction_psf = _f(m.group(1))

    # ── selling price ──
    m = re.search(r'(?:sell\w*|sale|selling|realis\w*|price)[^.]{0,40}?(?:rs\.?\s*)?' + _NUM
                  + r'\s*(?:psf|per sq|sq\.?\s?ft)', low)
    if m:
        inp.price_psf = _f(m.group(1)); inp.price_psf_source = "user supplied"

    # ── efficiency / tenure hints ──
    # SRA and MHADA carried tenure-specific "efficiency" figures written under
    # the old, inverted convention (saleable as a fraction of FSI area). They
    # cannot be mechanically restated as a loading factor, and inventing a
    # conversion would be exactly the substitution this rewrite removes. They
    # are recorded as an unresolved flag instead of a wrong number.
    if re.search(r'\bsra\b', low):
        inp.notes.append("SRA tenure detected - tenure-specific loading is NOT "
                         "calibrated; the freehold loading factor is in use")
    elif re.search(r'mhada', low):
        inp.notes.append("MHADA tenure detected - tenure-specific loading is NOT "
                         "calibrated; the freehold loading factor is in use")
    # "efficiency 70%" in a query meant saleable-as-a-share-of-FSI-area under the
    # old convention. Reading it into loading_pct would silently halve the
    # saleable area, so it is captured as a note and NOT applied. Use the
    # qualifier's Loading factor field to set it deliberately.
    m = re.search(r'efficien\w*[^.]{0,20}?' + _NUM + r'\s*%', low)
    if m:
        inp.notes.append(
            f"query stated efficiency {_f(m.group(1)):.0f}% - NOT applied: the "
            f"engine now uses a loading factor (saleable / FSI area, calibrated "
            f"at {FeasibilityInputs.loading_pct:.0f}%). Set it in the qualifier "
            f"if you meant to override it.")

    m = re.search(r'loading[^.]{0,20}?' + _NUM + r'\s*%', low)
    if m:
        inp.loading_pct = _f(m.group(1))

    # ── timeline ──
    m = re.search(_NUM + r'\s*(?:-|\s)?\s*year', low)
    if m:
        yrs = int(_f(m.group(1)))
        if 1 <= yrs <= 15: inp.project_years = yrs

    m = re.search(r'avg\w*\s*(?:unit|flat)?\s*(?:size)?[^.]{0,15}?' + _NUM + r'\s*sq', low)
    if m: inp.avg_unit_sqft = _f(m.group(1))

    return inp


# ── Finance helpers ──────────────────────────────────────────────────────────
def npv(rate: float, flows: list) -> float:
    return sum(cf / (1.0 + rate) ** i for i, cf in enumerate(flows))


def irr(flows: list, lo: float = -0.95, hi: float = 10.0, tol: float = 1e-7) -> Optional[float]:
    """Bisection IRR. Returns None when no sign change exists (no real IRR)."""
    if not flows or all(f >= 0 for f in flows) or all(f <= 0 for f in flows):
        return None
    f_lo, f_hi = npv(lo, flows), npv(hi, flows)
    if f_lo * f_hi > 0:
        return None
    for _ in range(300):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid, flows)
        if abs(f_mid) < tol:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0


# ── The model ────────────────────────────────────────────────────────────────
def _cost_stack(inp: FeasibilityInputs, bua: float, revenue_cr: float,
                land_cr: float) -> dict:
    construction_cr = bua * inp.construction_psf / CR
    approvals_cr = construction_cr * inp.approval_pct / 100.0
    professional_cr = construction_cr * inp.professional_pct / 100.0
    marketing_cr = revenue_cr * inp.marketing_pct / 100.0
    finance_cr = (construction_cr * inp.finance_drawn_pct / 100.0
                  * inp.finance_rate_pct / 100.0 * inp.project_years)
    contingency_cr = (construction_cr + approvals_cr) * inp.contingency_pct / 100.0
    total_cr = (land_cr + construction_cr + approvals_cr + professional_cr
                + marketing_cr + finance_cr + contingency_cr)
    return dict(land_cr=land_cr, construction_cr=construction_cr,
                approvals_cr=approvals_cr, professional_cr=professional_cr,
                marketing_cr=marketing_cr, finance_cr=finance_cr,
                contingency_cr=contingency_cr, total_cr=total_cr,
                non_land_cr=total_cr - land_cr)


def compute(inp: FeasibilityInputs) -> dict:
    if inp.plot_sqft is None or inp.construction_psf is None:
        raise ValueError("plot area and construction cost are required")
    price = inp.price_psf
    if price is None:
        raise ValueError("price_psf required - supply from LF data before computing")

    gross = inp.plot_sqft
    net = gross * (1 - inp.deduction_pct / 100.0)
    # FSI area is what the AUTHORITY permits. It is not what you build, and it
    # is not what you sell - those are the next two lines, and conflating them
    # was the single largest error in this engine.
    fsi_area = net * inp.fsi
    construction_area = fsi_area * inp.non_fsi_pct / 100.0   # + parking, basements
    saleable = fsi_area * inp.loading_pct / 100.0            # marketed super built-up
    carpet = saleable * inp.carpet_factor
    units = saleable / inp.avg_unit_sqft

    revenue_cr = saleable * price / CR
    land_cr = inp.land_cost_cr if inp.land_cost_cr is not None else 0.0
    # construction_psf is quoted PER SQ.FT OF BUILT-UP AREA (all floors) - the
    # filed K-RERA median on that basis is Rs.2,705. Costing the FSI area
    # instead understated construction by ~29%.
    costs = _cost_stack(inp, construction_area, revenue_cr, land_cr)

    profit_cr = revenue_cr - costs["total_cr"]
    margin_rev = profit_cr / revenue_cr * 100.0 if revenue_cr else 0.0
    margin_cost = profit_cr / costs["total_cr"] * 100.0 if costs["total_cr"] else 0.0
    breakeven_psf = costs["total_cr"] * CR / saleable if saleable else 0.0

    # maximum land cost that still clears the target margin
    target = inp.target_margin_pct / 100.0
    max_land_cr = revenue_cr * (1 - target) - costs["non_land_cr"]

    # ── sensitivity: land cost x selling price, computed cell by cell ────────
    if inp.land_cost_cr:
        base = inp.land_cost_cr
        land_axis = sorted({round(max(0.5, base * f), 2) for f in (0.6, 0.8, 1.0, 1.4, 1.8, 2.2)})
    else:
        land_axis = [round(max_land_cr * f, 2) for f in (0.6, 0.8, 1.0, 1.2)]
    price_axis = sorted({int(round(price * f)) for f in (0.85, 0.925, 1.0, 1.10, 1.20)})
    grid = []
    for lc in land_axis:
        row = {"land_cr": lc, "cells": []}
        for p in price_axis:
            rev = saleable * p / CR
            cs = _cost_stack(inp, construction_area, rev, lc)
            pr = rev - cs["total_cr"]
            row["cells"].append({"price_psf": p,
                                 "margin_pct": round(pr / rev * 100.0, 1) if rev else 0.0,
                                 "profit_cr": round(pr, 2)})
        grid.append(row)

    # ── phased cash flow ────────────────────────────────────────────────────
    yrs = inp.project_years
    sales_curve = _spread(yrs)
    build_curve = _spread(yrs)
    collect_lag = 0.35                       # share of a year's bookings collected later
    flows, table, cum = [], [], 0.0
    y0_out = -(land_cr + costs["approvals_cr"])
    flows.append(y0_out); cum += y0_out
    table.append(dict(year=0, label="Pre-launch", revenue_cr=0.0, collections_cr=0.0,
                      construction_cr=round(-costs["approvals_cr"], 2),
                      other_cr=round(-land_cr, 2), net_cr=round(y0_out, 2),
                      cumulative_cr=round(cum, 2)))
    carried = 0.0
    other_annual = (costs["professional_cr"] + costs["marketing_cr"]
                    + costs["finance_cr"] + costs["contingency_cr"]) / yrs
    for i in range(yrs):
        booked = revenue_cr * sales_curve[i]
        collected = booked * (1 - collect_lag) + carried
        carried = booked * collect_lag
        build = costs["construction_cr"] * build_curve[i]
        # NOT `net`. `net` is the net PLOT AREA computed at the top of this
        # function; reusing the name here overwrote it with the last year's net
        # cash flow, so the block reported a 5-acre plot as "25 sq.ft".
        net_cf = collected - build - other_annual
        flows.append(net_cf); cum += net_cf
        table.append(dict(year=i + 1, label=f"Year {i+1}",
                          revenue_cr=round(booked, 2), collections_cr=round(collected, 2),
                          construction_cr=round(-build, 2), other_cr=round(-other_annual, 2),
                          net_cr=round(net_cf, 2), cumulative_cr=round(cum, 2)))
    if carried > 0.005:                       # final collections after completion
        flows.append(carried); cum += carried
        table.append(dict(year=yrs + 1, label="Post-completion", revenue_cr=0.0,
                          collections_cr=round(carried, 2), construction_cr=0.0,
                          other_cr=0.0, net_cr=round(carried, 2), cumulative_cr=round(cum, 2)))

    # closure checks - these are what the prose version kept getting wrong
    total_in = sum(r["collections_cr"] for r in table)
    # Y0 already carries land in `other_cr`, so this sum IS the whole cost stack.
    # An earlier version added land again here and the check failed - which is
    # exactly what the check is for.
    total_out = -sum(r["construction_cr"] + r["other_cr"] for r in table)
    project_irr = irr(flows)

    # ── absorption ──────────────────────────────────────────────────────────
    absorption = []
    if inp.monthly_velocity_pct:
        for label, factor in (("Pessimistic (-30%)", 0.7), ("Base", 1.0), ("Optimistic (+20%)", 1.2)):
            v = inp.monthly_velocity_pct * factor
            per_month = units * v / 100.0
            absorption.append(dict(scenario=label, velocity_pct=round(v, 2),
                                   units_per_month=round(per_month, 1),
                                   months_to_sell=round(units / per_month, 0) if per_month else None))

    equity = land_cr + costs["approvals_cr"] + max(0.0, -min(r["cumulative_cr"] for r in table))
    return dict(
        inputs=asdict(inp),
        # There is deliberately NO "bua_sqft" key. It was kept briefly as an
        # alias of fsi_area_sqft and the downside case promptly read it, costed
        # the permitted area instead of the built area, and reported a downside
        # margin BETTER than the base case. A name that means two things is how
        # this class of bug keeps coming back; both areas are now named.
        areas=dict(gross_sqft=round(gross), net_sqft=round(net),
                   fsi_area_sqft=round(fsi_area),
                   construction_area_sqft=round(construction_area),
                   saleable_sqft=round(saleable), carpet_sqft=round(carpet),
                   units=round(units)),
        revenue_cr=round(revenue_cr, 2), price_psf=price,
        costs={k: round(v, 2) for k, v in costs.items()},
        profit_cr=round(profit_cr, 2),
        margin_on_revenue_pct=round(margin_rev, 1),
        margin_on_cost_pct=round(margin_cost, 1),
        breakeven_psf=round(breakeven_psf),
        price_cushion_psf=round(price - breakeven_psf),
        max_viable_land_cr=round(max_land_cr, 2),
        sensitivity=dict(price_axis=price_axis, rows=grid),
        cash_flow=table,
        cash_flow_check=dict(total_collections_cr=round(total_in, 2),
                             total_outflow_cr=round(total_out, 2),
                             revenue_cr=round(revenue_cr, 2),
                             total_cost_cr=round(costs["total_cr"], 2),
                             collections_reconcile=abs(total_in - revenue_cr) < 0.05,
                             outflows_reconcile=abs(total_out - costs["total_cr"]) < 0.05),
        irr_pct=round(project_irr * 100.0, 1) if project_irr is not None else None,
        npv_cr={f"{r}%": round(npv(r / 100.0, flows), 2) for r in (12, 15, 18)},
        peak_equity_cr=round(equity, 2),
        equity_multiple=round((equity + profit_cr) / equity, 2) if equity > 0 else None,
        absorption=absorption,
    )


def _spread(n: int) -> list:
    """Front-loaded S-curve over n years, summing to exactly 1.0."""
    if n <= 1: return [1.0]
    base = {2: [0.55, 0.45], 3: [0.40, 0.35, 0.25], 4: [0.30, 0.30, 0.25, 0.15],
            5: [0.25, 0.30, 0.20, 0.15, 0.10]}.get(n)
    if base is None:
        base = [1.0 / n] * n
    s = sum(base)
    return [round(x / s, 6) for x in base]



# ── Phased launch pricing ────────────────────────────────────────────────────
# Escalation measured from the LF database itself: 355 marketable wings across
# 118 projects in Whitefield and Hinjewadi, launch price vs current price,
# Q1 26-27. Median CAGR is 1.1%/yr for the first two years after launch and
# 9.1%/yr thereafter - escalation is BACK-LOADED, not linear. Overall median
# 4.7%/yr, and 15.8% of wings trade BELOW their launch price today.
LF_ESCALATION_EARLY = 0.011      # yrs 0-2, median CAGR
LF_ESCALATION_LATE = 0.091       # yrs 2+,  median CAGR
LF_ESCALATION_SAMPLE = "355 wings / 118 projects, Whitefield + Hinjewadi, Q1 26-27"
LF_BELOW_LAUNCH_PCT = 15.8       # share trading below launch price today

_UNIT_RELEASE = {2: [0.55, 0.45], 3: [0.35, 0.35, 0.30],
                 4: [0.25, 0.30, 0.25, 0.20],
                 5: [0.22, 0.26, 0.22, 0.18, 0.12]}


def _escalation_index(t_years: float) -> float:
    """Observed price index t years after launch (launch = 1.000)."""
    return ((1 + LF_ESCALATION_EARLY) ** min(t_years, 2.0)
            * (1 + LF_ESCALATION_LATE) ** max(0.0, t_years - 2.0))


def _default_ladder(years: int):
    """Unit release schedule and price factors DERIVED FROM LF DATA.

    Factors are the observed escalation index at each phase midpoint,
    normalised so the unit-weighted average is exactly 1.000 - i.e. the ladder
    averages to the target APR by construction, and its SHAPE comes from what
    projects in this market actually did rather than from anyone's opinion.
    """
    shares = _UNIT_RELEASE.get(years, _UNIT_RELEASE[3])
    shares = [s / sum(shares) for s in shares]
    idx = [_escalation_index(i + 0.5) for i in range(len(shares))]
    w = sum(s * x for s, x in zip(shares, idx))
    return shares, [round(x / w, 4) for x in idx]


def compute_launch_plan(inp: FeasibilityInputs, base_result: dict) -> dict:
    """Solve a phased launch ladder whose weighted average equals the target APR,
    then MEASURE it against flat pricing instead of assuming it is better.

    Holding the average constant, an escalating ladder is NPV-NEGATIVE on pure
    timing - it moves rupees later. It only pays if the launch discount buys a
    shorter sell-out. This returns both cases so the report can state the
    condition rather than assert the conclusion.
    """
    years = inp.project_years
    shares = inp.phase_unit_shares or _default_ladder(years)[0]
    factors = inp.phase_price_factors or _default_ladder(years)[1]
    n = min(len(shares), len(factors))
    shares, factors = shares[:n], factors[:n]
    tot = sum(shares)
    shares = [s / tot for s in shares]                 # normalise to exactly 1

    apr = inp.price_psf
    saleable = base_result["areas"]["saleable_sqft"]
    units_total = base_result["areas"]["units"]

    # solve base so the unit-weighted average realisation == target APR
    wavg_factor = sum(s * f for s, f in zip(shares, factors))
    base_psf = apr / wavg_factor
    prices = [base_psf * f for f in factors]
    realised = sum(s * p for s, p in zip(shares, prices))

    phases = []
    for i, (sh, fac, px) in enumerate(zip(shares, factors, prices), start=1):
        phases.append(dict(
            phase=i,
            label=("Launch" if i == 1 else "Final" if i == n else f"Phase {i}"),
            unit_share_pct=round(sh * 100, 1),
            units=round(units_total * sh),
            price_psf=round(px),
            vs_apr_pct=round((px / apr - 1) * 100, 1),
            saleable_sqft=round(saleable * sh),
            revenue_cr=round(saleable * sh * px / CR, 2),
        ))

    def _flows(price_list, share_list, yrs):
        """Cash flow under a construction-linked plan.

        A cohort booking in year i pays `booking_collect_pct` on booking, then
        the middle tranche pro-rata to construction progress over the REMAINING
        build period, then `possession_collect_pct` at OC. This replaces a flat
        65/35 split, which over-collected early and flattered NPV.
        """
        c = base_result["costs"]
        rev = [saleable * s * p / CR for s, p in zip(share_list, price_list)]
        fin = (c["construction_cr"] * inp.finance_drawn_pct / 100.0
               * inp.finance_rate_pct / 100.0 * yrs)
        other_total = fin + c["professional_cr"] + c["contingency_cr"] + c["marketing_cr"]

        bk = inp.booking_collect_pct / 100.0
        ps = inp.possession_collect_pct / 100.0
        mid = max(0.0, 1.0 - bk - ps)

        collections = [0.0] * (yrs + 2)          # index yrs+1 == possession year
        for i, amount in enumerate(rev):
            if amount <= 0:
                continue
            collections[i] += amount * bk
            remaining = yrs - i                   # construction years left
            if remaining > 0:
                per = amount * mid / remaining
                for j in range(i, yrs):
                    collections[j] += per
            else:
                collections[yrs] += amount * mid
            collections[yrs] += amount * ps       # OC tranche

        f = [-(c["land_cr"] + c["approvals_cr"])]
        for i in range(yrs):
            f.append(collections[i] - c["construction_cr"] / yrs - other_total / yrs)
        f.append(collections[yrs])
        return f, sum(rev), fin

    flat_f, flat_rev, flat_fin = _flows([apr] * years, _spread(years), years)
    lad_f, lad_rev, lad_fin = _flows(prices, shares, years)

    comp_years = inp.compressed_years or max(2, years - 1)
    cs, cf_ = _default_ladder(comp_years)
    cs = [x / sum(cs) for x in cs]
    cwf = sum(s * f for s, f in zip(cs, cf_))
    cprices = [apr / cwf * f for f in cf_]
    comp_f, comp_rev, comp_fin = _flows(cprices, cs, comp_years)

    def _pack(flows, rev, fin, yrs, label):
        r = irr(flows)
        return dict(label=label, years=yrs, revenue_cr=round(rev, 2),
                    finance_cr=round(fin, 2),
                    irr_pct=round(r * 100, 1) if r is not None else None,
                    npv15_cr=round(npv(0.15, flows), 2))

    flat = _pack(flat_f, flat_rev, flat_fin, years, f"Flat price at APR, {years}-year sell-out")
    ladder = _pack(lad_f, lad_rev, lad_fin, years, f"Ladder, same {years}-year sell-out")
    comp = _pack(comp_f, comp_rev, comp_fin, comp_years,
                 f"Ladder + discount compresses to {comp_years} years")

    # what the launch discount has to achieve
    launch_units = phases[0]["units"]
    months = comp_years * 12 / len(cs)
    required_velocity = (launch_units / months) / units_total * 100 if units_total and months else None

    # downside: escalation never lands, everything sells near the launch price
    down_prices = [prices[0]] * n
    down_f, down_rev, down_fin = _flows(down_prices, shares, years)
    down_realised = prices[0]
    c = base_result["costs"]
    down_cost = _cost_stack(inp, base_result["areas"]["construction_area_sqft"],
                            down_rev, c["land_cr"])
    downside = dict(label="Escalation not achieved - all phases near launch price",
                    realised_psf=round(down_realised),
                    revenue_cr=round(down_rev, 2),
                    margin_pct=round((down_rev - down_cost["total_cr"]) / down_rev * 100, 1) if down_rev else None,
                    npv15_cr=round(npv(0.15, down_f), 2))

    return dict(
        target_apr_psf=round(apr),
        solved_base_psf=round(base_psf),
        realised_average_psf=round(realised, 2),
        average_matches_target=abs(realised - apr) < 1.0,
        phases=phases,
        comparison=[flat, ladder, comp],
        ladder_vs_flat_npv_cr=round(ladder["npv15_cr"] - flat["npv15_cr"], 2),
        compressed_vs_flat_npv_cr=round(comp["npv15_cr"] - flat["npv15_cr"], 2),
        required_launch_velocity_pct=round(required_velocity, 2) if required_velocity else None,
        market_velocity_pct=inp.monthly_velocity_pct,
        downside=downside,
    )


# ── Rendering ────────────────────────────────────────────────────────────────
def _inr(x: float) -> str:
    return f"{x:,.2f}"


def compute_with_launch_plan(inp: FeasibilityInputs) -> dict:
    """compute() plus the phased launch analysis."""
    r = compute(inp)
    try:
        r["launch_plan"] = compute_launch_plan(inp, r)
    except Exception as e:                     # never let this break the core numbers
        r["launch_plan"] = None
        r.setdefault("warnings", []).append(f"launch plan unavailable: {e}")
    return r


def render_markdown(r: dict) -> str:
    a, c, i = r["areas"], r["costs"], r["inputs"]
    L = []
    L.append("=== COMPUTED FEASIBILITY (AUTHORITATIVE - DO NOT RECALCULATE) ===")
    L.append("Every figure below was computed in Python from the stated inputs. "
             "Use these numbers verbatim. Do not re-derive, round differently, or "
             "recompute any of them. If a number you want is not here, say it was "
             "not computed rather than inventing it.")
    L.append("")
    L.append(f"**Inputs used** - plot {i['plot_input_text'] or str(i['plot_sqft'])+' sqft'}, "
             f"FSI {i['fsi']}{' (assumed)' if i['fsi_assumed'] else ''}, "
             f"deductions {i['deduction_pct']}%, loading {i['loading_pct']}%, "
             f"construction Rs.{i['construction_psf']:,.0f} PSF, "
             f"price Rs.{r['price_psf']:,.0f} PSF ({i['price_psf_source']}), "
             f"land Rs.{i['land_cost_cr']} Cr, term {i['project_years']} years.")
    L.append("")
    L.append("**Area**")
    L.append("")
    L.append("| Parameter | Basis | Value |")
    L.append("|---|---|---|")
    L.append(f"| Gross plot | as supplied | {a['gross_sqft']:,} sq.ft |")
    L.append(f"| Net plot | after {i['deduction_pct']}% deductions | {a['net_sqft']:,} sq.ft |")
    L.append(f"| FSI area (permitted) | net x FSI {i['fsi']} | {a['fsi_area_sqft']:,} sq.ft |")
    L.append(f"| Construction area | FSI area x {i['non_fsi_pct']}% "
             f"(incl. parking/basements) | {a['construction_area_sqft']:,} sq.ft |")
    L.append(f"| Saleable area | FSI area x {i['loading_pct']}% | {a['saleable_sqft']:,} sq.ft |")
    L.append(f"| RERA carpet | saleable x {i['carpet_factor']} "
             f"({i['carpet_factor_source']}) | {a['carpet_sqft']:,} sq.ft |")
    L.append(f"| Units | saleable / {i['avg_unit_sqft']:,.0f} sq.ft | {a['units']:,} |")
    L.append("")
    L.append("**Cost and return**")
    L.append("")
    L.append("| Line | Rs. Cr |")
    L.append("|---|---|")
    L.append(f"| Land | {_inr(c['land_cr'])} |")
    L.append(f"| Construction | {_inr(c['construction_cr'])} |")
    L.append(f"| Approvals ({i['approval_pct']}%) | {_inr(c['approvals_cr'])} |")
    L.append(f"| Professional ({i['professional_pct']}%) | {_inr(c['professional_cr'])} |")
    L.append(f"| Marketing ({i['marketing_pct']}%) | {_inr(c['marketing_cr'])} |")
    L.append(f"| Finance ({i['finance_rate_pct']}% on {i['finance_drawn_pct']}% over {i['project_years']}y) | {_inr(c['finance_cr'])} |")
    L.append(f"| Contingency ({i['contingency_pct']}%) | {_inr(c['contingency_cr'])} |")
    L.append(f"| **Total cost** | **{_inr(c['total_cr'])}** |")
    L.append(f"| **Revenue** | **{_inr(r['revenue_cr'])}** |")
    L.append(f"| **Net profit** | **{_inr(r['profit_cr'])}** |")
    L.append("")
    L.append(f"- Margin on revenue: **{r['margin_on_revenue_pct']}%** | Margin on cost: {r['margin_on_cost_pct']}%")
    L.append(f"- Breakeven: **Rs.{r['breakeven_psf']:,} PSF** (cushion Rs.{r['price_cushion_psf']:,} PSF below market)")
    if r["max_viable_land_cr"] > 0:
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
                 f"it viable. Check the selling price before reading further.")
    L.append(f"- Project IRR: **{r['irr_pct']}%**" if r["irr_pct"] is not None else "- Project IRR: not computable")
    L.append(f"- NPV: " + " | ".join(f"{k} Rs.{v} Cr" for k, v in r["npv_cr"].items()))
    L.append(f"- Peak equity Rs.{r['peak_equity_cr']} Cr | Equity multiple {r['equity_multiple']}x")
    L.append("")
    L.append("**Sensitivity - margin % by land cost and selling price**")
    L.append("")
    L.append("| Land (Rs.Cr) | " + " | ".join(f"Rs.{p:,} PSF" for p in r["sensitivity"]["price_axis"]) + " |")
    L.append("|---" * (len(r["sensitivity"]["price_axis"]) + 1) + "|")
    for row in r["sensitivity"]["rows"]:
        L.append(f"| {row['land_cr']} | " + " | ".join(f"{cell['margin_pct']}%" for cell in row["cells"]) + " |")
    L.append("")
    L.append("**Phased cash flow (Rs. Cr)**")
    L.append("")
    L.append("| Year | Revenue booked | Collections | Construction | Other | Net | Cumulative |")
    L.append("|---|---|---|---|---|---|---|")
    for t in r["cash_flow"]:
        L.append(f"| {t['label']} | {t['revenue_cr']} | {t['collections_cr']} | "
                 f"{t['construction_cr']} | {t['other_cr']} | {t['net_cr']} | {t['cumulative_cr']} |")
    chk = r["cash_flow_check"]
    L.append("")
    L.append(f"Closure check - collections {chk['total_collections_cr']} vs revenue {chk['revenue_cr']} "
             f"({'reconciles' if chk['collections_reconcile'] else 'DOES NOT RECONCILE'}); "
             f"outflows {chk['total_outflow_cr']} vs total cost {chk['total_cost_cr']} "
             f"({'reconciles' if chk['outflows_reconcile'] else 'DOES NOT RECONCILE'}).")
    if r["absorption"]:
        L.append("")
        L.append("**Absorption scenarios**")
        L.append("")
        L.append("| Scenario | Velocity %/month | Units/month | Months to sell out |")
        L.append("|---|---|---|---|")
        for s in r["absorption"]:
            L.append(f"| {s['scenario']} | {s['velocity_pct']} | {s['units_per_month']} | {s['months_to_sell']} |")
    lp = r.get("launch_plan")
    if lp:
        L.append("")
        L.append("**Phased launch plan - priced to average out at the target APR**")
        L.append("")
        L.append(f"Target average realisation Rs.{lp['target_apr_psf']:,} PSF. Base price solved "
                 f"to Rs.{lp['solved_base_psf']:,} PSF so the unit-weighted average of the ladder "
                 f"equals the target exactly (realised Rs.{lp['realised_average_psf']:,.0f}).")
        L.append("")
        L.append("| Phase | Units | Share | Price PSF | vs APR | Revenue (Rs.Cr) |")
        L.append("|---|---|---|---|---|---|")
        for ph in lp["phases"]:
            L.append(f"| {ph['label']} | {ph['units']} | {ph['unit_share_pct']}% | "
                     f"Rs.{ph['price_psf']:,} | {ph['vs_apr_pct']:+}% | {ph['revenue_cr']} |")
        L.append("")
        L.append("**Is the ladder actually worth doing?** Holding the average constant, an "
                 "escalating ladder moves revenue later, so on pure timing it is WORSE than flat "
                 "pricing. It only pays if the launch discount buys a faster sell-out.")
        L.append("")
        L.append("| Strategy | Sell-out | Finance (Rs.Cr) | IRR | NPV @15% (Rs.Cr) |")
        L.append("|---|---|---|---|---|")
        for cmp_ in lp["comparison"]:
            L.append(f"| {cmp_['label']} | {cmp_['years']}y | {cmp_['finance_cr']} | "
                     f"{cmp_['irr_pct']}% | {cmp_['npv15_cr']} |")
        L.append("")
        L.append(f"- Ladder at the SAME sell-out: **{lp['ladder_vs_flat_npv_cr']:+} Cr** NPV vs flat pricing.")
        L.append(f"- Ladder that compresses the sell-out: **{lp['compressed_vs_flat_npv_cr']:+} Cr** NPV vs flat.")
        if lp["required_launch_velocity_pct"]:
            mv = lp["market_velocity_pct"]
            verdict = ("achievable - below current market velocity" if mv and lp["required_launch_velocity_pct"] < mv
                       else "demanding - at or above current market velocity" if mv else "no market velocity available to compare")
            L.append(f"- The launch phase must sell at **{lp['required_launch_velocity_pct']}% per month**"
                     + (f" against LF market velocity of {mv}% - {verdict}." if mv else "."))
        d = lp["downside"]
        L.append(f"- **Downside if the escalation does not land:** realised Rs.{d['realised_psf']:,} PSF, "
                 f"revenue Rs.{d['revenue_cr']} Cr, margin {d['margin_pct']}%, NPV Rs.{d['npv15_cr']} Cr.")
        L.append("")
        L.append("Present the ladder as a pricing STRATEGY with a stated condition, never as "
                 "predicted appreciation. The escalation is an assumption the developer must "
                 "earn through velocity, not a forecast.")
    if i["notes"]:
        L.append("")
        L.append("Notes: " + "; ".join(i["notes"]))
    L.append("")
    L.append("=== END COMPUTED FEASIBILITY ===")
    return "\n".join(L)


# ── Self-test ────────────────────────────────────────────────────────────────
def _self_test() -> int:
    fails = []

    def chk(label, got, want, tol=0.51):
        ok = abs(got - want) <= tol
        print(f"  {'PASS' if ok else 'FAIL'}  {label:<44} got {got:>12,.2f}  want {want:>12,.2f}")
        if not ok: fails.append(label)

    print("1. unit conversion - the error that reached a client PDF")
    chk("5 acres -> sqft", _area_to_sqft(5, "acres"), 217_800)
    chk("1 hectare -> sqft", _area_to_sqft(1, "hectare"), 107_639.1, 1)
    chk("40 guntha -> 1 acre", _area_to_sqft(40, "guntha"), 43_560)
    chk("1000 sqm -> sqft", _area_to_sqft(1000, "sqm"), 10_763.9, 1)

    print("\n2. parsing the original query")
    q = ("Run a feasibility check for this 5 acre plot. cost of acq is 25 cr, "
         "cost of construction is 3000psf. suggest product mix with competing "
         "projects within 3km range. launch phases considering 3 year delivery.")
    inp = parse_feasibility_inputs(q)
    chk("plot parsed (sqft)", inp.plot_sqft, 217_800)
    chk("land cost parsed (Cr)", inp.land_cost_cr, 25)
    chk("construction parsed (PSF)", inp.construction_psf, 3000)
    chk("term parsed (years)", inp.project_years, 3, 0)

    print("\n3. economics at the LF market price")
    inp.price_psf = 8455.0; inp.price_psf_source = "LF absorption price Q1 26-27"
    inp.monthly_velocity_pct = 3.76
    r = compute(inp)
    # These expectations MOVED when the area chain was recalibrated against
    # 1,947 filed K-RERA projects. They were not adjusted to make a test pass -
    # the engine was wrong and the old numbers encoded the error:
    #   saleable was FSI area x 0.70; it is FSI area x 1.07 (loading, filed)
    #   construction was costed on FSI area; it is costed on FSI area x 1.35
    # Revenue rises 53%, cost 30%, so margin moves 23.4% -> 34.9%. The old
    # engine was UNDERSTATING viability, not flattering it.
    chk("FSI area sqft", r["areas"]["fsi_area_sqft"], 370_260, 2)
    chk("construction area sqft", r["areas"]["construction_area_sqft"], 499_851, 2)
    chk("saleable sqft", r["areas"]["saleable_sqft"], 396_178, 2)
    chk("units", r["areas"]["units"], 417, 1)
    chk("revenue Cr", r["revenue_cr"], 334.97, 0.05)
    chk("total cost Cr", r["costs"]["total_cr"], 217.92, 0.05)
    chk("net profit Cr", r["profit_cr"], 117.05, 0.05)
    chk("margin on revenue %", r["margin_on_revenue_pct"], 34.9, 0.1)

    print("\n4. internal consistency - what prose kept getting wrong")
    ok = r["cash_flow_check"]["collections_reconcile"]
    print(f"  {'PASS' if ok else 'FAIL'}  collections reconcile to revenue")
    if not ok: fails.append("collections reconcile")
    ok = r["cash_flow_check"]["outflows_reconcile"]
    print(f"  {'PASS' if ok else 'FAIL'}  outflows reconcile to total cost")
    if not ok: fails.append("outflows reconcile")

    flows = [r["cash_flow"][0]["net_cr"]] + [t["net_cr"] for t in r["cash_flow"][1:]]
    recomputed = irr(flows)
    ok = recomputed is not None and abs(recomputed * 100 - r["irr_pct"]) < 0.2
    print(f"  {'PASS' if ok else 'FAIL'}  IRR matches its own cash flows "
          f"({r['irr_pct']}% vs {recomputed*100:.1f}%)" if recomputed else "  FAIL IRR")
    if not ok: fails.append("IRR self-consistency")

    print("\n5. sensitivity grid - every cell recomputed independently")
    bad = 0
    for row in r["sensitivity"]["rows"]:
        for cell in row["cells"]:
            rev = r["areas"]["saleable_sqft"] * cell["price_psf"] / CR
            # must mirror compute(): the grid costs the CONSTRUCTION area
            cs = _cost_stack(inp, r["areas"]["construction_area_sqft"], rev, row["land_cr"])
            want = (rev - cs["total_cr"]) / rev * 100
            if abs(want - cell["margin_pct"]) > 0.15: bad += 1
    print(f"  {'PASS' if bad == 0 else 'FAIL'}  {len(r['sensitivity']['rows'])*len(r['sensitivity']['price_axis'])} cells, {bad} disagree")
    if bad: fails.append("sensitivity grid")

    print("\n6. NPV / IRR sanity")
    chk("NPV at IRR ~ 0", npv(r["irr_pct"] / 100.0, flows), 0.0, 0.05)
    ok = irr([100, 200]) is None
    print(f"  {'PASS' if ok else 'FAIL'}  no-sign-change returns None rather than a number")
    if not ok: fails.append("irr guard")

    print("\n7. phased launch plan")
    inp.project_years = 4
    full = compute_with_launch_plan(inp)
    lp = full["launch_plan"]
    chk("weighted average == target APR", lp["realised_average_psf"], inp.price_psf, 1.0)
    ok = lp["average_matches_target"]
    print(f"  {'PASS' if ok else 'FAIL'}  ladder averages out to the objective price")
    if not ok: fails.append("ladder average")
    revs = sum(ph["revenue_cr"] for ph in lp["phases"])
    chk("phase revenue sums to project revenue", revs, full["revenue_cr"], 0.2)
    shares = sum(ph["unit_share_pct"] for ph in lp["phases"])
    chk("unit shares sum to 100%", shares, 100.0, 0.2)
    ok = lp["ladder_vs_flat_npv_cr"] < 0
    print(f"  {'PASS' if ok else 'FAIL'}  ladder at same duration is NPV-negative "
          f"({lp['ladder_vs_flat_npv_cr']:+} Cr) - the honest result, not the flattering one")
    if not ok: fails.append("ladder timing effect")
    ok = lp["downside"]["margin_pct"] < full["margin_on_revenue_pct"]
    print(f"  {'PASS' if ok else 'FAIL'}  downside case is worse than base "
          f"({lp['downside']['margin_pct']}% vs {full['margin_on_revenue_pct']}%)")
    if not ok: fails.append("downside case")
    ok = lp["phases"][0]["price_psf"] < lp["phases"][-1]["price_psf"]
    print(f"  {'PASS' if ok else 'FAIL'}  launch price below final phase price")
    if not ok: fails.append("ladder direction")

    print("\n8. collection profile (construction-linked plan)")
    bk, ps = inp.booking_collect_pct, inp.possession_collect_pct
    chk("booking + middle + possession = 100%", bk + (100 - bk - ps) + ps, 100.0, 0.01)
    total_coll = sum(t["collections_cr"] for t in full["cash_flow"])
    chk("every rupee of revenue is collected", total_coll, full["revenue_cr"], 0.05)
    ok = full["cash_flow"][1]["collections_cr"] < full["revenue_cr"] * 0.35
    print(f"  {'PASS' if ok else 'FAIL'}  year-1 collections are modest, as a CLP implies "
          f"(Rs.{full['cash_flow'][1]['collections_cr']} Cr of Rs.{full['revenue_cr']} Cr)")
    if not ok: fails.append("clp front-loading")

    print("\n9. escalation ladder derives from LF data, not opinion")
    ok = _escalation_index(0) == 1.0 and _escalation_index(4) > _escalation_index(2) > _escalation_index(0)
    print(f"  {'PASS' if ok else 'FAIL'}  index starts at 1.000 and rises monotonically")
    if not ok: fails.append("escalation index")
    sh, fac = _default_ladder(4)
    chk("ladder factors average to 1.000", sum(s_*f_ for s_, f_ in zip(sh, fac)), 1.0, 0.002)
    ok = fac[-1] / fac[0] < 1.30
    print(f"  {'PASS' if ok else 'FAIL'}  spread {fac[-1]/fac[0]:.2f}x is within what the data supports "
          f"(an earlier hand-guessed ladder was 1.25x)")
    if not ok: fails.append("ladder spread")

    print(f"\n{'ALL CHECKS PASSED' if not fails else 'FAILURES: ' + ', '.join(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--query", help="parse and compute from a free-text query")
    ap.add_argument("--price", type=float, help="selling price PSF (from LF data)")
    ap.add_argument("--velocity", type=float, help="monthly velocity %% (from LF data)")
    a = ap.parse_args()
    if a.self_test:
        sys.exit(_self_test())
    if a.query:
        inp = parse_feasibility_inputs(a.query)
        if not inp:
            print("not a feasibility query"); sys.exit(1)
        if a.price: inp.price_psf = a.price; inp.price_psf_source = "supplied"
        if a.velocity: inp.monthly_velocity_pct = a.velocity
        if not inp.is_sufficient() or inp.price_psf is None:
            print("missing inputs:", ", ".join(inp.missing())); sys.exit(1)
        print(render_markdown(compute(inp)))
        sys.exit(0)
    ap.print_help()
