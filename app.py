"""
MR&I API Backend v4.1
======================
Flask server bridging the HTML frontend, Neo4j graph, and Claude API.
Web Intelligence integration via Anthropic web_search tool.

v4.1 CHANGES (over v4):
- FIX A: format_data_block_for_claude() now emits [ZERO ROWS] markers and a
  COVERAGE GAPS footer when queries return 0 rows. Prevents Claude from
  papering over data gaps with web-search fabrications.
- FIX A: System prompt adds Rule E (anti-fabrication on zero rows) and
  Rule F (trust prior data-backed answers on follow-ups).
- FIX C: Every web_search tool call is logged with [WEB_SEARCH_AUDIT] —
  user query, classified categories, and the actual search string Claude
  sent to Google. Visible in Render Logs tab.

v4 CHANGES (preserved):
- needs_web() indentation/double-call bug fixed
- run_query() carries description from QueryRegistry
- WEB_KEYWORDS now categorized (MONETARY_POLICY, INFRASTRUCTURE, etc.)
- Description-aware data block sent to Claude

Architecture:
  User query → classify intent → run Cypher query → get EXACT data + DESCRIPTION →
  detect if web context needed → send data + descriptions + query to Claude
  (with web_search tool if needed) → Claude presents data + web context →
  stream response back (with web_search audit logging)
"""

import os
import json
import re
import argparse
import time
from collections import defaultdict
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS

try:
    from neo4j import GraphDatabase
except ImportError:
    print("pip install neo4j")
    exit(1)

try:
    import anthropic
except ImportError:
    print("pip install anthropic")
    exit(1)

# Import our queries (now a QueryRegistry, not a plain dict)
from cypher_queries import QUERIES

app = Flask(__name__)
CORS(app)

# ═══════════════════════════════════════
# SIMPLE RATE LIMITER (in-memory)
# ═══════════════════════════════════════
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 15     # max requests per window per IP
_rate_store = defaultdict(list)


def check_rate_limit(ip):
    """Return True if allowed, False if rate limited."""
    now = time.time()
    # Clean old entries
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT_MAX:
        return False
    _rate_store[ip].append(now)
    return True

# ═══════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════
NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j+s://c26f3089.databases.neo4j.io')
NEO4J_USER = os.environ.get('NEO4J_USER', 'c26f3089')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')  # MUST be set via env var — never hardcode
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY')  # MUST be set via env var

_CONFIG_OK = True
if not NEO4J_PASSWORD:
    print("⚠ WARNING: NEO4J_PASSWORD env var not set. Database queries will fail.")
    _CONFIG_OK = False
if not ANTHROPIC_KEY:
    print("⚠ WARNING: ANTHROPIC_API_KEY env var not set. Claude queries will fail.")
    _CONFIG_OK = False

driver = None
claude = None


def get_driver():
    global driver
    if driver is None:
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            max_connection_lifetime=300,
            max_connection_pool_size=10,
            connection_acquisition_timeout=15,
            connection_timeout=10
        )
    return driver


def get_claude():
    global claude
    if claude is None:
        claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    return claude


def run_query(query_name, **params):
    """Run a Cypher query and return results as list of dicts.

    v4: Result dict now includes a `description` field carrying the
    semantic-context blurb authored in cypher_queries.py. This description
    is later surfaced to Claude alongside the rows so it can interpret
    column names, units, and intent correctly.
    """
    if query_name not in QUERIES:
        return {
            "query": query_name,
            "description": "",
            "row_count": 0,
            "data": [],
            "source": "error",
            "error": f"Unknown query: {query_name}",
        }

    cypher = QUERIES[query_name]
    # QueryRegistry.description() returns "" if not present — safe default
    description = QUERIES.description(query_name) if hasattr(QUERIES, "description") else ""
    d = get_driver()

    try:
        with d.session(database='c26f3089') as session:
            result = session.run(cypher, **params)
            records = [dict(record) for record in result]
    except Exception as e:
        print(f"  ✗ Query {query_name} failed: {e}")
        return {
            "query": query_name,
            "description": description,
            "params": params,
            "row_count": 0,
            "data": [],
            "source": "error",
            "error": str(e),
        }

    return {
        "query": query_name,
        "description": description,
        "params": params,
        "row_count": len(records),
        "data": records,
        "source": "LF_Research_Database",
    }


# ═══════════════════════════════════════
# WEB INTELLIGENCE DETECTION (v4: categorized)
# ═══════════════════════════════════════
WEB_KEYWORDS = {

    # Monetary policy — RBI rate decisions, lending rates, EMI impact
    "MONETARY_POLICY": [
        "repo rate", "reverse repo", "rbi", "reserve bank",
        "interest rate", "rate cut", "rate hike", "rate revision",
        "monetary policy", "mpc", "monetary policy committee",
        "lending rate", "home loan rate", "housing loan rate",
        "emi", "mclr", "base rate", "pll", "prime lending rate",
        "cash reserve ratio", "crr", "slr", "statutory liquidity",
        "policy rate", "bank rate",
    ],

    # Physical infrastructure — transit, roads, civic projects
    "INFRASTRUCTURE": [
        "metro", "metro line", "metro phase", "metro station",
        "expressway", "highway", "national highway", "nh-",
        "airport", "international airport", "greenfield airport",
        "flyover", "bridge", "ring road", "outer ring road", "orr",
        "elevated corridor", "underpass", "rrts", "namo bharat",
        "monorail", "bullet train", "high speed rail", "hsr",
        "logistics park", "industrial corridor", "smart city",
        "sez", "special economic zone", "it park", "tech park",
        "data center", "freight corridor", "port", "rapid rail",
        "underground", "skywalk", "infra project", "infrastructure project",
    ],

    # Regulatory & policy — RERA, stamp duty, zoning, housing schemes
    "REGULATORY_POLICY": [
        "stamp duty", "rera", "registration charges", "registration fee",
        "circle rate", "ready reckoner", "guidance value",
        "tdr", "transferable development rights",
        "fsi", "far", "floor area ratio", "floor space index",
        "premium fsi", "building bye-laws", "master plan",
        "development plan", "land use", "zoning", "land conversion",
        "amrut", "pmay", "pradhan mantri awas yojana",
        "affordable housing scheme", "credit linked subsidy", "clss",
        "gst on real estate", "gst", "gst rate", "gst council",
        "input tax credit", "itc",
        "model tenancy act", "benami", "nbcc", "land pooling",
        "redevelopment policy", "cluster redevelopment",
    ],

    # Macroeconomic — GDP, inflation, fiscal, capital markets
    "MACROECONOMIC": [
        "gdp", "inflation", "cpi", "wpi", "iip", "pmi",
        "current account", "fiscal deficit", "budget",
        "union budget", "state budget", "tax", "income tax",
        "ltcg", "stcg", "capital gains", "section 54", "section 80c",
        "fdi", "foreign direct investment", "private equity", "pe fund",
        "reit", "real estate investment trust",
        "insolvency", "ibc", "nclat", "credit rating",
        "rating downgrade", "sovereign rating",
    ],

    # Market intelligence — earnings, deals, broker/consultancy reports
    "MARKET_INTELLIGENCE": [
        "developer earnings", "earnings call", "quarterly results",
        "annual report", "land deal", "land deals", "land acquisition",
        "joint venture", "jda", "joint development agreement",
        "merger", "acquisition", "ipo", "qip", "rights issue",
        "preferential allotment", "anarock", "knight frank", "jll",
        "cbre", "colliers", "propequity", "cushman", "wakefield",
        "credai", "naredco", "industry report", "consultancy report",
        "broker report", "research report", "savills", "vestian",
    ],

    # Macro framing — language indicating user wants context, not raw data
    "MACRO_FRAMING": [
        "outlook", "forecast", "projection", "macro",
        "how will", "impact of", "effect of", "influence of",
        "due to", "because of", "amid", "considering", "given that",
        "in light of", "implication", "consequence",
        "what does it mean for", "going forward", "next year",
    ],

    # Rental & yield — separate category because it's commercial-leaning data
    "RENTAL_YIELD": [
        "rental yield", "rental return", "rental income",
        "lease rate", "leasing", "leased", "tenant",
        "rent vs buy", "rental market", "rental demand",
    ],
}


# Pre-compile per-category patterns (word-boundary anchored to reduce false matches)
WEB_KEYWORD_PATTERNS = {
    category: re.compile(
        r'\b(?:' + '|'.join(re.escape(k) for k in keywords) + r')\b',
        re.IGNORECASE,
    )
    for category, keywords in WEB_KEYWORDS.items()
}


def classify_web_intent(query: str) -> list:
    """Return list of categories whose keywords appear in the query.

    Empty list = no web search needed; the LF graph alone should answer.
    Multiple categories may fire (e.g. 'how will RBI rate cut affect Sector 71'
    fires MONETARY_POLICY + MACRO_FRAMING).
    """
    if not query:
        return []
    return [
        category
        for category, pattern in WEB_KEYWORD_PATTERNS.items()
        if pattern.search(query)
    ]


def needs_web_search(query: str) -> bool:
    """Backward-compatible boolean check for code that doesn't need categories."""
    return bool(classify_web_intent(query))

# ═══════════════════════════════════════
# CORRIDOR → SECTOR MAPPING
# ═══════════════════════════════════════
CORRIDOR_MAP = {
    # ── GURUGRAM CORRIDORS ──
    r'dwarka|dxp|dwarka.express': [
        'Sector 37D', 'Sector - 99', 'Sector 102', 'Sector 103',
        'Sector - 104', 'Sector 108', 'Sector 109', 'Sector - 110',
        'Sector - 111', 'Sector - 112'
    ],
    r'sohna|sohna.road|sohna.corridor': [
        'Sohna Road', 'Sector 2 , Sohna', 'Sector - 4, Sohna',
        'Sector - 5, Sohna', 'Sector - 6, Sohna', 'Sector 33, Sohna',
        'Sector 35, Sohna', 'Sector 36, Sohna'
    ],
    r'golf.course.extension|gcer': [
        'Sector 58', 'Sector 59', 'Sector 61', 'Sector 62',
        'Sector 63', 'Sector - 63A', 'Sector 65', 'Sector 66'
    ],
    r'golf.course.road|gcr(?!.*ext)': [
        'Sector 42', 'Sector 53', 'Sector 54', 'Sector 65'
    ],
    r'southern.peripheral|spr': [
        'Sector - 68', 'Sector 69', 'Sector 70', 'Sector 70A',
        'Sector 71', 'Sector 72', 'Sector 76', 'Sector 77',
        'Sector 78', 'Sector 79', 'Sector - 79 B'
    ],
    r'new.gurgaon|new.gurugram': [
        'Sector 76', 'Sector 79', 'Sector 80', 'Sector - 81',
        'Sector 82', 'Sector 83', 'Sector 84', 'Sector 85',
        'Sector 86', 'Sector 88A', 'Sector 88B', 'Sector 89',
        'Sector 89A', 'Sector 90', 'Sector 91', 'Sector 92',
        'Sector 93', 'Sector 95'
    ],
    # ── KOLKATA CORRIDORS ──
    r'em.bypass|eastern.metro|e\.?m\.?\s*bypass': [
        'Anandapur', 'Kalikapur', 'Narendrapur', 'Tollygunge'
    ],
    r'rajarhat|new.town|action.area': [
        'Rajarhat', 'New Town'
    ],
    r'howrah|shibpur|liluah': [
        'Howrah'
    ],
    r'south.kolkata|behala|joka|thakurpukur': [
        'Behala', 'Joka', 'Batanagar', 'Pailan'
    ],
    r'north.kolkata|baranagar|barrackpore|madhyamgram': [
        'Baranagar', 'Madhyamgram'
    ],
    r'salt.lake|sector.v|bidhannagar': [
        'Salt Lake City'
    ],
    r'southern.bypass|diamond.harbour': [
        'Southern Bypass', 'Amtala'
    ],
    r'uttarpara|konnagar|hugli|hooghly': [
        'Uttarpara', 'Konnagar Hugli'
    ],
    # ── HINJEWADI / PUNE CORRIDORS ──
    r'wakad|wakad.road': ['Wakad'],
    r'punawale|punawale.road': ['Punawale'],
    r'mahalunge|mahalunge.road': ['Mahalunge'],
    r'baner|baner.road': ['Baner'],
    r'tathawade': ['Tathawade'],
    r'hinjewadi|hinjawadi': ['Hinjewadi'],
    r'ravet': ['Ravet'],
    r'talegaon|talegaon.dabhade': ['Talegaon'],
    r'chakan': ['Chakan'],
    r'kiwale': ['Kiwale'],
    r'mumbai.pune|mumbai.?pune.express|mpe': [
        'Hinjewadi', 'Wakad', 'Punawale', 'Mahalunge', 'Baner', 'Tathawade'
    ],
}


def detect_corridor(query):
    """Detect if query references a corridor and return matching sector patterns."""
    q = query.lower()
    for pattern, sectors in CORRIDOR_MAP.items():
        if re.search(pattern, q):
            return sectors
    return None


def needs_web(query):
    """Detect if query needs web intelligence.

    v4: Fixed double-call and indentation bugs from v3. Logs which categories
    fired so the architect can audit web-routing decisions.
    """
    q = query or ""
    # Always enable web for feasibility/site queries — they need location context
    if re.search(r'feasib|plot.*area|acre|fsi|dcr|google.*map|goo\.gl|maps\.google|site.*intel|due.dilig|land.*acqui', q, re.IGNORECASE):
        print(f"  🌐 [WEB_INTENT] feasibility-shortcut fired for: {q[:60]!r}")
        return True
    fired = classify_web_intent(q)
    if fired:
        print(f"  🌐 [WEB_INTENT] categories={fired} query={q[:60]!r}")
    return bool(fired)


# ═══════════════════════════════════════
# INTENT CLASSIFIER
# ═══════════════════════════════════════
def extract_project_name(query):
    """Extract project name from natural language queries.
    Handles: 'How is DLF Privana doing?', 'Tell me about Godrej Seven',
    'Show me details of Birla Pravaah', 'DLF Privana performance in Sector 76'
    """
    patterns = [
        # "How is <PROJECT> doing/performing/going?"
        r'how\s+(?:is|are)\s+(.+?)\s+(?:doing|performing|going|faring|selling)',
        # "Tell me about <PROJECT>" / "What about <PROJECT>"
        r'(?:tell|what)\s+(?:me\s+)?about\s+(.+?)(?:\s+in\s+|\s+at\s+|\?|$)',
        # "Show me <PROJECT> details/data/info"
        r'(?:show|give)\s+(?:me\s+)?(.+?)\s+(?:details|data|info|stats|numbers)',
        # "Performance/summary/details of <PROJECT>"
        r'(?:performance|summary|details?|about|analyse|analyze|report)\s+(?:of\s+|for\s+)?(.+?)(?:\s+in\s+|\s+at\s+|$)',
        # "<PROJECT> performance/analysis"
        r'^(.+?)\s+(?:performance|analysis|report|status|details)\b',
        # "Show/give me ... of/for <PROJECT>"
        r'(?:give|show|get)\s+(?:me\s+)?(?:.*?)\s+(?:of|for)\s+(.+?)(?:\s+in\s+|\s+at\s+|$)',
    ]
    for pat in patterns:
        m = re.search(pat, query, re.I)
        if m:
            name = m.group(1).strip().rstrip('.?!')
            # Remove trailing city/location qualifiers
            name = re.sub(
                r'\s*(?:in|at|near)\s+(?:gurgaon|gurugram|kolkata|hinjewadi|pune|mumbai|sector\s*[-]?\s*\d+\w*).*$',
                '', name, flags=re.I
            ).strip()
            # Skip generic words
            if len(name) > 3 and not re.match(
                r'^(the\s+)?(market|city|area|location|sector|residential|overview|'
                r'gurgaon|gurugram|kolkata|hinjewadi|pune|mumbai|pricing|trend|data|'
                r'latest|current|demand|supply|inventory|construction)$',
                name, re.I
            ):
                return name
    return None


def classify_intent(query, city):
    """Map user query to appropriate Cypher queries.
    v3: 35+ regex patterns covering all KB sections + robust project name extraction.
    """
    q = query.lower()
    results = []

    # ── Check for corridor queries first ──
    corridor_sectors = detect_corridor(query)
    if corridor_sectors:
        for sector_pattern in corridor_sectors:
            result = run_query("micromarket_detail", city=city, location=sector_pattern.split(',')[0])
            if result.get('row_count', 0) > 0:
                results.append(result)
        results.append(run_query("market_overview", city=city))
        results.append(run_query("price_trend_saleable", city=city))
        if len(results) > 5:
            results = results[:5]
        return results

    # ── Project-specific query (check FIRST — most specific) ──
    proj_name = extract_project_name(query)
    if proj_name:
        results.append(run_query("project_detail", city=city, project_name=proj_name))
        results.append(run_query("project_competitors", city=city, project_name=proj_name))

    # ── Market overview ──
    if re.search(r'market|overview|summary|health.check|how.*market', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))

    # ── Price trends ──
    if re.search(r'pric|psf|rate|cost|trend', q):
        results.append(run_query("price_trend_saleable", city=city))

    # ── Quarterly absorption ──
    if re.search(r'absorption|quarterly.*sale|qoq|quarter', q):
        results.append(run_query("quarterly_absorption", city=city))

    # ── Micro-market ranking by demand ──
    if re.search(r'rank.*demand|demand.*intens|micro.*market.*demand|hotspot|hot.spot|acquisition|acqui|highest.*demand|most.*demand', q):
        results.append(run_query("micromarkets_by_demand", city=city))

    # ── Micro-market ranking by inventory risk ──
    if re.search(r'rank.*inventor|inventor.*risk|micro.*market.*risk|oversuppl', q):
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # ── Emerging micro-markets ──
    if re.search(r'emerging|growing|upcoming|new.*market', q):
        results.append(run_query("emerging_micromarkets", city=city))

    # ── Declining micro-markets ──
    if re.search(r'declining|slow|weak|struggling', q):
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # ── Product mix / configurations ──
    if re.search(r'bhk|config|mix|flat.*type|product.*mix|optim', q):
        results.append(run_query("flat_performance", city=city))

    # ── Top projects ──
    if re.search(r'top.*project|best.*project|rank.*project|leading', q):
        results.append(run_query("top_projects_by_sales", city=city))
        results.append(run_query("top_projects_by_velocity", city=city))

    # ── Competitive / comparison ──
    if re.search(r'compet|benchmark|compare|versus|vs\b', q):
        results.append(run_query("top_projects_by_sales", city=city))
        results.append(run_query("micromarkets_by_demand", city=city))

    # ── Feasibility (COMPREHENSIVE — pull all relevant data) ──
    if re.search(r'feasib|irr|break.even|viable|plot|acre|fsi|dcr|google.*map|goo\.gl|maps\.google', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("price_trend_saleable", city=city))
        results.append(run_query("flat_performance", city=city))
        results.append(run_query("comparable_projects", city=city))
        results.append(run_query("ticket_size", city=city))

    # ── Infrastructure impact ──
    if re.search(r'infra.*impact|impact.*zone|metro.*impact|express.*impact|connectivity', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))
        results.append(run_query("micromarkets_by_demand", city=city))
        results.append(run_query("price_trend_saleable", city=city))

    # ── Site intelligence / location ──
    if re.search(r'site.*intel|due.dilig', q):
        loc_match = re.search(r'sector\s*[-]?\s*\d+\w*|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', query)
        if loc_match:
            results.append(run_query("micromarket_detail", city=city, location=loc_match.group()))
        results.append(run_query("nearby_micromarkets", city=city))
        results.append(run_query("top_projects_by_sales", city=city))

    # ── Builder analysis ──
    if re.search(r'builder|developer|who.*build', q):
        results.append(run_query("builder_rankings", city=city))

    # ── YoY absorption growth ──
    if re.search(r'yoy|year.*over.*year|annual.*growth|absorption.*growth', q):
        results.append(run_query("yoy_absorption", city=city))
        results.append(run_query("quarterly_absorption", city=city))

    # ── Velocity trend ──
    if re.search(r'velocity.*trend|velocity.*over|speed.*sales', q):
        results.append(run_query("velocity_trend", city=city))

    # ── Inventory trend ──
    if re.search(r'inventory.*trend|months.*inventory.*over|unsold.*trend', q):
        results.append(run_query("inventory_trend", city=city))

   # ── Buyer demographics ──
    # Note: Buyer queries are city-specific (currently Hinjewadi only). On other
    # cities they return [ZERO ROWS], which Claude handles per Rule E (no fabrication).
    if re.search(
        r'buyer|demograph|psychograph|who.*buy|customer|profile|age.*group|'
        r'gender|pincode|locality|district|surname|religion|language|mother.*tongue|'
        r'state.*wise|category.*buyer|buyer.*category|huf|individual.*buyer|'
        r'corporate.*buyer|investor.*type|origin|catchment',
        q,
    ):
        # Existing 6 dimensions
        results.append(run_query("buyer_age_dist", city=city))
        results.append(run_query("buyer_gender_dist", city=city))
        results.append(run_query("buyer_locality_dist", city=city))
        results.append(run_query("buyer_state_dist", city=city))
        results.append(run_query("buyer_religion_dist", city=city))
        results.append(run_query("buyer_language_dist", city=city))
        # NEW 3 dimensions
        results.append(run_query("buyer_district_dist", city=city))
        results.append(run_query("buyer_pincode_dist", city=city))
        results.append(run_query("buyer_category_dist", city=city))

    # ── Slow-moving ──
    if re.search(r'slow.*mov|slow.*sell|aging|stuck|not.*sell', q):
        results.append(run_query("flat_performance", city=city))
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # ── Ticket size / price band ──
    if re.search(r'ticket.*size|price.*band|affordab|budget|cost.*range|price.*range', q):
        results.append(run_query("ticket_size", city=city))
        results.append(run_query("price_trend_saleable", city=city))

    # ── Best-selling configurations ──
    if re.search(r'best.*sell|top.*config|popular.*bhk|fast.*mov|high.*demand|most.*popular', q):
        results.append(run_query("flat_performance", city=city))

    # ── Residential overview ──
    if re.search(r'residential|overview.*residential', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))
        results.append(run_query("price_trend_saleable", city=city))
        results.append(run_query("flat_performance", city=city))

    # ══════════════════════════════════════════
    # NEW PATTERNS (Phase 2 coverage expansion)
    # ══════════════════════════════════════════

    # ── Construction stage analysis (annual + quarterly) ──
    if re.search(r'construction.*stage|stage.*wise|under.*construct|completed|ready.*possess|oc.*receiv|pre.?launch', q):
        results.append(run_query("construction_stage", city=city))
        results.append(run_query("market_overview", city=city))

    # ── Possession timeline / readiness ──
    if re.search(r'possession|ready.*move|handover|deliver|occupancy|timeline.*deliver', q):
        results.append(run_query("possession_distribution", city=city))
        results.append(run_query("market_overview", city=city))

    # ── Distance from CBD analysis ──
    if re.search(r'distance|cbd|km.*from|radius|proximity|how.*far', q):
        results.append(run_query("distance_analysis", city=city))
        results.append(run_query("market_overview", city=city))

    # ── Unsold stock by construction stage ──
    if re.search(r'unsold.*stage|unsold.*construct|stuck.*stock|dead.*stock|inventory.*stage', q):
        results.append(run_query("construction_stage", city=city))

    # ── New launches ──
    if re.search(r'new.*launch|recent.*launch|launch.*project|newly.*launch', q):
        results.append(run_query("new_launches", city=city))

    # ── Comparable projects ──
    if re.search(r'comparable|peer|similar.*project|like.*project', q):
        results.append(run_query("comparable_projects", city=city))

    # ── Catchment area ──
    if re.search(r'catchment|hinterland|feeder|source.*demand', q):
        results.append(run_query("comparable_projects", city=city))

    # ── Sold out projects ──
    if re.search(r'sold.*out|fully.*sold|100.*sold|complete.*sold', q):
        results.append(run_query("comparable_projects", city=city))

    # ── RERA ──
    if re.search(r'rera|registered|compliance', q):
        results.append(run_query("top_projects_by_sales", city=city))

    # ── Supply pipeline ──
    if re.search(r'supply.*pipeline|new.*supply|upcoming.*supply|future.*supply', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))

    # ── Ticket size / cost range analysis ──
    if re.search(r'ticket.*size|cost.*range|price.*segment|budget.*segment|affordab.*segment|luxury.*segment', q):
        results.append(run_query("ticket_size", city=city))

    # ── Unit size distribution ──
    if re.search(r'unit.*size|sqft.*range|area.*range|carpet.*area.*distribution|saleable.*area.*distribution|size.*distribution', q):
        results.append(run_query("unit_size_saleable", city=city))

    # ── Total projects / project count ──
    if re.search(r'total.*project|how.*many.*project|number.*project|count.*project|all.*project', q):
        results.append(run_query("project_count", city=city))
        results.append(run_query("comparable_projects", city=city))

    # ── Micromarket list / sub-regions / areas ──
    if re.search(r'micro.*market|sub.*region|area.*within|region.*within|localities|zones|which.*areas', q):
        results.append(run_query("micromarket_list", city=city))

  # ═══ Cap to prevent timeout — raised to 9 for buyer demographics
#     # (the buyer block adds 9 queries; cap at 5 would silently drop most of them) ═══
    if len(results) > 9:
      results = results[:9]

# Important: when 9 buyer queries fire AND the user also triggers another block
# (e.g. they ask "demographics + market overview"), some queries may still be
# truncated. Watch the [WEB_INTENT] logs in Render for a few queries after deploy
# to see if 9 is sufficient. We can raise further if needed.

    # ═══ Default: market overview ═══
    if not results:
        results.append(run_query("market_overview", city=city))
        results.append(run_query("flat_performance", city=city))

    return results


# ═══════════════════════════════════════
# DATA-BLOCK FORMATTER (v4)
# ═══════════════════════════════════════
def format_data_block_for_claude(data_results, city, corridor_sectors=None):
    """Render Cypher results as a structured block with per-query descriptions.

    v4: Each query block now leads with its description (column meanings,
    units, intended use) BEFORE the rows. This grounds Claude's interpretation
    of column names and prevents unit-confusion errors.
    """
    lines = []
    lines.append(f"CITY: {city}")
    lines.append("")
    lines.append(
        "DATA LINEAGE: Every row below was queried directly from the LF "
        "Knowledge Base built from Liases Foras proprietary research data. "
        "Each query block carries a DESCRIPTION explaining what its columns "
        "mean, in what units. Use the description to interpret the rows — "
        "do not infer units or column semantics from the names alone."
    )
    lines.append("")
    lines.append(
        "CRITICAL: This data covers RESIDENTIAL markets only. If the user's "
        "query involves commercial/office/retail/co-working pricing, you MUST "
        "use web_search for those rates and label them [Web Context]. Do NOT "
        "attribute any commercial pricing to LF data."
    )
    lines.append("")

    queries_used = []
    zero_row_queries = []
    total_rows = 0

    for result in data_results:
        if "error" in result and result.get("source") == "error":
            continue
        name = result.get("query", "unknown")
        desc = result.get("description", "")
        rows = result.get("data", [])
        source = result.get("source", "unknown")
        row_count = result.get("row_count", 0)

        queries_used.append(name)
        total_rows += row_count
        if row_count == 0:
            zero_row_queries.append(name)

        # Make zero-row results VISUALLY UNMISSABLE — Claude tends to skim
        # past empty [] arrays. The [ZERO ROWS] tag is the explicit signal
        # that this is a coverage gap, not data to interpret.
        zero_marker = "  [ZERO ROWS — coverage gap for this city]" if row_count == 0 else ""
        lines.append(f"--- QUERY: {name} ({row_count} rows, source: {source}){zero_marker} ---")
        if desc:
            lines.append(f"DESCRIPTION: {desc}")
        lines.append("ROWS:")
        lines.append(json.dumps(rows, indent=1, default=str))
        lines.append("")

    lines.append(
        f"TOTAL: {len(queries_used)} queries executed, {total_rows} rows "
        f"returned from LF Knowledge Base."
    )
    lines.append(f"QUERIES USED: {', '.join(queries_used)}")

    # CRITICAL: consolidated zero-row summary so Claude cannot miss it.
    # This is the anti-fabrication anchor — see Rule E in system prompt.
    if zero_row_queries:
        lines.append("")
        lines.append(
            f"COVERAGE GAPS: {len(zero_row_queries)} of {len(queries_used)} "
            f"queries returned ZERO rows for this city: "
            f"{', '.join(zero_row_queries)}"
        )
        lines.append(
            "RULE E (BINDING): For these zero-row queries, the LF Knowledge "
            "Base has NO COVERAGE for this city — say so explicitly. Do NOT "
            "fill the gap by web-searching the topic and presenting the web "
            "result as analysis. Web search is for context (rates, policy, "
            "news) — NEVER for synthesizing missing LF data into structured "
            "demographic/profile/segment claims."
        )

    if corridor_sectors:
        lines.append("")
        lines.append(
            f"CORRIDOR MAPPING: The query references a corridor. Constituent "
            f"sectors searched: {', '.join(corridor_sectors)}"
        )
        lines.append(
            "Present data grouped by sector with individual project metrics. "
            "Do NOT average across projects."
        )

    return "\n".join(lines)


# ═══════════════════════════════════════
# SYSTEM PROMPT
# ═══════════════════════════════════════
SYSTEM_PROMPT_BASE = """You are MR&I (Market Research & Intelligence), a precision real estate analytics engine for Indian residential markets.

=== ABSOLUTE RULES (NEVER VIOLATE) ===
1. EVERY number you present MUST come from the provided data. ZERO exceptions.
2. If a specific metric is not in the data, present the CLOSEST AVAILABLE data and clearly label what it represents. For example, if asked about 'Dwarka Expressway' and you have data for constituent sectors (37D, 99, 102, 103, 104) — present those projects grouped by sector. NEVER leave the user with just 'data not available'. NEVER output 'CRITICAL DATA LIMITATION' or 'Data Not Available' as a section header.
3. NEVER reference future years beyond the latest quarter in the data.
4. NEVER fabricate project names, builder names, or locations not in the data.
5. When recommending strategies — frame as 'recommendations based on current data' NOT predictions.
6. Use Indian formatting: Rs., Lakhs, Crores, PSF. Not ₹ symbol.
7. Clearly separate 'The data shows...' (fact) from 'Based on this, we can infer...' (analysis).
8. Each query block in the data section starts with a DESCRIPTION line that defines column meanings and units. READ THE DESCRIPTION before interpreting the rows. Do NOT guess what a column means from its name alone.

=== SOURCE ATTRIBUTION — ZERO TOLERANCE (READ THIS 3 TIMES) ===
THIS IS THE MOST IMPORTANT RULE IN THIS ENTIRE PROMPT.

The LF Knowledge Base contains RESIDENTIAL market data ONLY — residential project prices, residential sales velocity, residential inventory. It does NOT contain commercial office rates, retail rates, co-working rates, ready reckoner rates, circle rates, or government guideline values.

RULE A — PRICE PSF ATTRIBUTION:
The ONLY price PSF values you may attribute to "LF data" or present without [Web Context] label are:
- Weighted average saleable price from the SALEABLE_PRICE_AT query results
- Weighted average carpet price from the CARPET_PRICE_AT query results  
- Individual project saleable_rate_psf and carpet_rate_psf from the project data
If a price number is NOT in the Cypher query results provided to you, it is NOT from LF data. Period.

RULE B — COMMERCIAL / OFFICE / RETAIL PRICING:
When the user's plot has commercial zoning or the analysis requires commercial/office/retail pricing:
1. STATE EXPLICITLY: "LF data covers residential markets only. Commercial pricing below is sourced from web intelligence."
2. Use web_search to find commercial rates for the specific location
3. EVERY commercial rate MUST be prefixed with [Web Context] and include the source URL
4. NEVER write "Based on LF data showing commercial rate of Rs.X" — this is FABRICATION

RULE C — WHAT CONSTITUTES FABRICATION (instant credibility destruction):
- Taking a number from web search and labeling it as LF data = FABRICATION
- Computing an average that doesn't exist in the data and presenting it as a data point = FABRICATION  
- Using a circle rate, ready reckoner rate, or government guideline value and attributing it to LF = FABRICATION
- Inventing a "commercial circle rate" when LF has no commercial data = FABRICATION
If you are unsure whether a number came from LF data, it didn't. Label it [Web Context] or don't use it.

RULE D — WHEN DATA IS INSUFFICIENT:
If the user asks about a segment (commercial, retail, industrial) that LF doesn't cover, say:
"LF Knowledge Base covers the residential market for this location. For residential benchmarking, the data shows [exact LF numbers]. For commercial/retail pricing, here is web intelligence: [Web Context] [source URL]."
This is honest, useful, and doesn't fabricate. The user will respect this far more than a fabricated number.

RULE E — ZERO-ROW QUERIES ARE COVERAGE GAPS, NOT INVITATIONS TO FABRICATE:
The data block you receive may include queries marked [ZERO ROWS — coverage gap for this city] and a "COVERAGE GAPS:" footer listing them. When you see this:

1. State the gap honestly: "The LF Knowledge Base does not currently have [demographic profiles / buyer data / sector-wise breakdowns / etc.] loaded for [city]."
2. Offer the closest LF data you DO have — pricing, velocity, supply, sales — to partially address the question.
3. Do NOT web-search the topic and present the web result as a structured analysis. Specifically forbidden:
   - Inventing buyer ages, incomes, professions, family status, origin breakdowns
   - Inventing psychographic profiles (lifestyle, priorities, decision timelines, influencers)
   - Inventing demographic percentages or buyer-source percentages
   - Inventing employer concentration, catchment composition
   These are NEVER acceptable, even with a [Web Context] label, because such fabrications are indistinguishable from real LF buyer data to a non-expert reader. Labeling fabrication does not make it acceptable.
4. The ONLY exception: macroeconomic context (RBI rates, policy announcements, infrastructure news) is legitimate web search territory. Buyer demographics, project counts, sector tier rankings, market shares are NOT — these are graph-data questions and must be answered from graph data or honestly declined.
5. If the user explicitly asks for demographic information that returns zero rows, the correct response is: "Buyer demographic data (age, gender, locality, state, language, religion) is not currently loaded for [city] in the LF Knowledge Base. This dataset exists for [list cities where it IS loaded]. For [requested city], here is the residential market data that is available: [pricing / velocity / inventory / etc.]. To proceed with demographic-driven analysis for [requested city], the IGR sub-registrar files need to be ingested into the graph — please flag this to the data team."

RULE F — TRUST YOUR PRIOR DATA-BACKED ANSWERS ON FOLLOW-UPS:
When a user asks a follow-up question that references a prior response (e.g., "why is Sector 71 in tier 1?", "justify Sector 76 ranking", "explain the velocity number you gave"), apply this logic:

1. Check whether the CURRENT data block contains the specific rows that backed your prior answer.
2. If it does — answer the follow-up directly using those rows.
3. If it does NOT (because the new query routing pulled different queries this turn) — DO NOT conclude your prior response was fabricated. Each conversation turn re-runs intent classification, which may select a different subset of queries. The data you cited previously was real LF data at that turn; it's just not in this turn's data block.

In this case, your response should be: "In the previous response I drew on the [micromarkets_by_demand / top_projects_by_sales / etc.] query, which is not in the current data block. The numbers I cited (e.g., Sector 71: 1,194 units, 7.63% velocity) are from the LF Knowledge Base. If you want me to re-verify, please ask me to re-run the micromarket query explicitly."

Do NOT write phrases like:
- "I need to correct my previous response"
- "The previous sector-wise table was not based on verified LF data"
- "Fabricated micro-market rankings"
- "I cannot verify"
…unless you have POSITIVE evidence that the prior numbers were wrong. Self-doubt absent contrary evidence destroys credibility worse than the original error would have. A confident "let me re-verify if you'd like" beats a panicky retraction of correct data every time.

=== MICRO-MARKET MAPPING (CRITICAL) ===
Users often query by corridor names, not sector numbers. Map these to constituent sectors:
- Dwarka Expressway (DXP) = Sectors 37D, 99, 102, 103, 104, 108, 109, 110, 111, 112, 113
- Sohna Road / Sohna Corridor = Sectors 2-6 Sohna, Sector 33-36 Sohna, Sohna Road
- Golf Course Road / GCR = Sectors 42, 43, 53, 54, 55, 56, 57, 65
- Golf Course Extension Road = Sectors 58, 59, 61, 62, 63, 63A, 65, 66
- Southern Peripheral Road (SPR) = Sectors 68, 69, 70, 70A, 71, 72, 76, 77, 78, 79, 79B
- New Gurugram = Sectors 76, 79, 80, 81, 82, 83, 84, 85, 86, 88A, 88B, 89, 89A, 90, 91, 92, 93, 95

When a user asks about a corridor:
1. Identify ALL sectors that map to it from the data
2. List individual projects from those sectors with their EXACT metrics
3. Show a project-level comparison table — NEVER average across projects to create a 'sector price'
4. If web intelligence is active, use web search to add infrastructure context

=== GLOSSARY (CANONICAL — Liases Foras official definitions) ===

These are the ONLY authoritative definitions. Use them verbatim. Do NOT invent
synonyms, do NOT paraphrase the formulas, do NOT attach thresholds or interpretive
labels (e.g., "healthy", "weak", "strong") that are not in this glossary.

If a user asks about a metric NOT on this list (e.g., "demand intensity",
"absorption rate", "cap rate", "IRR", "affluence score"), respond explicitly:
"That term is not in the LF Glossary. The closest LF metric is [X], defined as [Y]."
Do NOT invent a definition.

──────────────────────────────────────────────────────────────────────────────
1. QUARTERLY SALES
   The number of units sold in the selected quarter.
   At MicroMarket level: sum of quarterly sales across all wings of all
   marketable projects in that quarter.

2. ANNUAL SALES
   The number of units sold in the LAST FOUR QUARTERS including the selected
   quarter (i.e., a trailing 12-month sum), NOT a fiscal year.
   Wing-level: H = B + C + D + E (sum of last 4 quarterly sales)
   Project-level: I = sum of H across all wings of the project

3. SOLD TILL DATE
   Cumulative units sold in a project/wing since launch.
   Wing-level: J = sum of all quarterly sales since launch
   Project-level: K = sum of J across all wings

4. UNSOLD STOCK (F)
   Units left unsold at the end of a given period.
   Formula: Unsold = Total Supply − Sold Till Date
            F = A − (sum of all quarterly sales since launch)

5. MARKETABLE SUPPLY
   Supply available for sale at the START of the period plus new launches
   during the period. Includes only units/wings/buildings being marketed in
   the current quarter.
   Annual Marketable Supply (L)    = Annual Sales (H) + Unsold (F)
   Quarterly Marketable Supply (M) = Quarterly Sales (E) + Unsold (F)
   IMPORTANT: Annual MS and Quarterly MS are DIFFERENT numbers. Do not conflate.

6. TOTAL SUPPLY (A)
   Sum of supply across all MARKETABLE wings of a project. Excludes sold-out
   wings. At MicroMarket level: sum across all marketable projects.

7. SUPPLY SIZE
   Supply of all current marketable projects PLUS supply of all sold-out
   projects in the area. Differs from Total Supply by including sold-out stock.

8. PROJECT SIZE
   All units of a project including sold-out wings.
   Formula: Project Size = Total Supply of Marketable Wings + Total Supply of Sold-Out Wings

9. WEIGHTED AVERAGE PRICE ON UNSOLD (Rs./Psf)
   The price at which the unsold stock is available in the market.
   Formula: Wt. Avg. Price on Unsold = Σ(Rate_i × Unsold_i) / Σ(Unsold_i)
   Use this when the user asks "what's the asking price" or "what's the
   inventory priced at".

10. WEIGHTED AVERAGE PRICE ON SOLD (Rs./Psf)
    The price at which actual absorption is occurring.
    Formula (per quarter): Wt. Avg. Price on Sold = Σ(Rate_i × Quarterly_Sales_i)
                                                    / Σ(Quarterly_Sales_i)
    Use this when the user asks "what's the transaction price" or
    "what's selling at what price".

11. WEIGHTED AVERAGE PRICE ON NEW LAUNCH
    The price at which new launches in the period entered the market.
    Formula: Wt. Avg. New Launch Price = Σ(Launch_Rate_i × New_Supply_i)
                                         / Σ(New_Supply_i)
    Across only newly-launched sub-projects in the period.

12. MONTHS INVENTORY (MI)
    Number of months required to absorb the unsold stock at the current pace
    of sales.
    Formula: MI = Unsold / Monthly Sales
             where Monthly Sales = Quarterly Sales / 3 = Annual Sales / 12
    DO NOT attach interpretive thresholds (e.g., "healthy <18") — those are
    industry rules of thumb, not part of the LF Glossary. If you cite a
    threshold, label it: "industry rule of thumb, not from the LF Glossary."

13. MONTHLY SALES VELOCITY (SV)
    The achieved velocity at which a project is selling, expressed as a
    PERCENTAGE of supply sold per month. Aggregation rules differ by scope:

    For a wing/tower: SV_i = Gross Average Monthly Sales / Total Supply
    For a project:    SV   = Σ(SV_i) / n  (mean of marketable wing SVs)
    For a Location/City: SV_b = MEDIAN(SV_i) of all subprojects in the boundary
                                ↑ note: MEDIAN, not mean, at city/region level

    DO NOT attach thresholds (e.g., "strong >3%"). If you cite one, label it
    as "industry rule of thumb, not from the LF Glossary."

14. BASE COST OF FLAT (CoF)
    Cost of a flat at the time it is marketable. For ongoing projects:
    Formula: CoF = Saleable Area × Prevailing Rate (Rs/PSF)
    Excludes: registration, stamp duty, GST, parking, maintenance, society
    charges, and other developer add-ons.

15. VALUE OF STOCK SOLD (also: BUSINESS TURNOVER, BT)
    The trade value done during a period — sq.ft. sold × prevailing prices,
    aggregated at sub-project level.
    Formula (per sub-project): Value of Stock Sold = CoF × Sales_in_quarter (E)
    Reported in Rs. Lacs or Rs. Cr.

16. MARKET EFFICIENCY INDEX
    Ratio between price-per-sqft and sales-per-sqft of supply. Indicates
    demand elasticity — does sale-volume rise with price (efficient) or fall
    (inefficient)? Indexed against the second data point of the selected
    location's database = 100.

    Base year by region:
    - MMR (Mumbai): Jan 2005 (movement Jan 04 → Jan 05)
    - Pune, NCR (Gurugram), Bengaluru: Nov 2008 (movement Jun 08 → Nov 08)
    - Chennai, Hyderabad: March 2009 (movement Nov 08 → Mar 09)

17. PRODUCT EFFICIENCY
    A composite metric for comparing product types (e.g., 1BHK vs 2BHK vs 3BHK)
    that accounts for both supply scale and absorption rate.
    Formula: Product Efficiency = √(Sales² + Marketable Supply²)
                                  × (Sales / Marketable Supply)
    The result is normalized: divide by the maximum value across product types
    in the comparison set, expressed as a percentage. The product type with
    the highest absolute Product Efficiency = 100%.

18. AFFLUENCE INDEX (Economic Density Index)
    Liases Foras's location-quality metric.
    Formula: Affluence Index = Population Density × Income (normalized)

    Classification ranges (use these EXACT bands and labels):
    | Min Range | Max Range | Category          | Approx. Household Cost     |
    |-----------|-----------|-------------------|----------------------------|
    | 0         | 0.002857  | Low               | Below Rs.40 Lacs           |
    | 0.002857  | 0.008571  | Mid               | Rs.40 Lacs – Rs.80 Lacs    |
    | 0.008571  | 0.018571  | Upper Mid         | Rs.80 Lacs – Rs.1.5 Cr     |
    | 0.018571  | 0.04      | High              | Rs.1.5 Cr – Rs.3 Cr        |
    | 0.04      | 0.111429  | Affluent          | Rs.3 Cr – Rs.8 Cr          |
    | 0.111429  | 1         | Extremely Affluent| Above Rs.8 Cr              |

──────────────────────────────────────────────────────────────────────────────
TERMINOLOGY ALIASES (common phrasings the user might say)
──────────────────────────────────────────────────────────────────────────────
- "Absorption price" / "transacted price"  →  Wt. Avg. Price on Sold (Term 10)
- "Asking price" / "list price"             →  Wt. Avg. Price on Unsold (Term 9)
- "Inventory months" / "stock months"       →  Months Inventory (Term 12)
- "Velocity" (without prefix)               →  Monthly Sales Velocity (Term 13)
- "Turnover"                                →  Value of Stock Sold (Term 15)
- "Total stock"                             →  Marketable Supply (Term 5)
                                                or Total Supply (Term 6) —
                                                ASK USER which they mean.

──────────────────────────────────────────────────────────────────────────────
TERMS NOT IN THE LF GLOSSARY (do NOT invent definitions for these)
──────────────────────────────────────────────────────────────────────────────
The following are NOT defined by Liases Foras. If a user asks about them,
either explain you don't have an LF definition or map to the closest LF term
with explicit labeling:
- "Demand intensity" — NOT an LF term. Closest LF metric: Sales Velocity.
- "Cap rate" / "IRR" / "yield" — NOT an LF term. These are investment-return
  metrics. LF Glossary covers absorption metrics, not financial returns.
- "Market saturation" — NOT an LF term. Closest LF metric: Months Inventory.
- "Demand-supply gap" — NOT an LF term. Closest LF metric: Marketable Supply
  vs. Annual Sales (express as MI).
- "Heat index" / "demand score" — NOT an LF term. Do NOT invent a number.

──────────────────────────────────────────────────────────────────────────────
LF LOCATIONS vs ADMINISTRATIVE WARDS
──────────────────────────────────────────────────────────────────────────────
- ADMINISTRATIVE WARDS = municipal ward boundaries (gov-defined).
- LF LOCATIONS = LF-defined regions based on physical setting. Broader than
  wards. Example: "Andheri East" (LF Location) comprises Andheri (E), Sakinaka,
  J.B. Nagar (which may span multiple wards). Projects are mapped to the LF
  Location based on the address given by the builder.

When the user references a location, default to LF Location boundaries.

=== CHART RULES (CRITICAL) ===
- Format: <lfchart type="bar|line|doughnut|hbar|combo" title="Title"><labels>L1,L2</labels><dataset label="Name" color="#hex">v1,v2</dataset></lfchart>
- Colors: #c9a84c(gold) #3b82f6(blue) #22c55e(green) #ef4444(red) #8b5cf6(purple) #06b6d4(cyan)
- Values must be plain numbers only. No text, no symbols, no Rs.
- NEVER combine metrics with different scales on same chart unless using combo type
- Chart title: use 'and' not '&' (causes rendering issues)
- For combo charts: <dataset label="Volume" color="#3b82f6" type="bar" axis="left">...</dataset><dataset label="Rate %" color="#ef4444" type="line" axis="right">...</dataset>
- CHART LABEL FORMATTING: labels must be SHORT — use "Q1 24-25" not "Quarter 1 FY2024-25", use "3-3.5K" not "Rs 3001 - Rs 3500"
- Max 8-10 labels per chart. Show top entries only if more exist.

ABSOLUTE BAN ON FABRICATED AGGREGATIONS:
a) NEVER average project-level data to create sector-level metrics. If Sector 71 has Birla Pravaah (492 units) and Signature Global Titanium (702 units), NEVER report "Sector 71: 597 demand intensity" — list each project individually.
b) In charts: every value must exist in the raw data or be a simple YoY/QoQ % from two data points.
c) The validation layer flags every unverified chart value. Unverified values damage credibility.

=== FORMAT RULES ===
- Use **bold text** for section headers, NOT ### markdown headers
- Use bullet points for insights, numbered lists for rankings
- Use markdown tables for structured comparisons
- Keep paragraphs concise — 2-3 sentences max per point
- CRITICAL TABLE FORMAT: ALL tables MUST use proper markdown format with LEADING and TRAILING pipe characters. 
  CORRECT: | Column A | Column B | Column C |
  WRONG:   Column A | Column B | Column C
  Every table row MUST start with | and end with |. Include the separator row: |---|---|---|
  This is critical for PDF rendering — tables without leading pipes will not render properly in exported PDFs.

=== PRODUCT MIX CLASSIFICATION (CRITICAL) ===
The flat_performance data contains TWO types of entries mixed together:
- BHK CONFIGURATIONS (product types): 1 BHK, 1.5 BHK, 2 BHK, 2.5/3 BHK, 3.5/4 BHK, 5+ BHK, Duplex/Penthouse, Studio/1 RK
- BUILDING TYPOLOGIES (construction types): Floors, Services Apt, Villa, Plot, Retail, Commercial

When analyzing product mix:
1. ONLY rank BHK configurations against each other. "Floors" is NOT a competing configuration to "3 BHK".
2. Present building typologies SEPARATELY if relevant, labeled as "Building Type Analysis" — NOT mixed into "Top Performing Configurations".
3. If asked "What BHK should I build?" — exclude Floors, Services Apt, Villa, Plot from the ranking. These are structural choices, not unit-type choices.
4. The labels from data may include: "Floors", "Services Apt", "Villa", "Plot" — recognize these as NON-BHK categories.

=== ANALYSIS MODES ===

**MARKET OVERVIEW:** Report supply, sales, unsold, MI, velocity, pricing from quarterly and annual data.

**PRODUCT MIX:** For each BHK type: annual sales, unsold, velocity, MI, efficiency. Recommend based on HIGHEST velocity + LOWEST MI.

**COMPETITIVE BENCHMARK:** Compare projects using exact data. Rank by composite score.

**LAND FEASIBILITY (COMPREHENSIVE — Use when user provides plot details, FSI, Google Maps link, or asks about feasibility):**

This is the MOST IMPORTANT analysis mode. When a CXO or land acquisition head asks for feasibility, they expect a report that matches what Anarock, Knight Frank, or CBRE would deliver. Follow this EXACT framework:

**STEP 1 — LOCATION IDENTIFICATION**
If the user provides a Google Maps URL:
- Extract the coordinates from the URL (look for patterns like @18.574949,73.689848 or place/18°34'29.8"N+73°41'23.5"E)
- State the exact coordinates in the response
- Identify which micromarket this falls under from the LF data
- Use web_search to identify: "what is near [coordinates/location name]" — nearby landmarks, IT parks, highways, metro stations

**STEP 2 — SITE SURROUNDINGS ANALYSIS (use web_search — MANDATORY for feasibility queries)**
Search for and score the site on these 8 parameters. For each, identify specific landmarks within 1-5 km radius and assign a score (1-10):

| Parameter | What to Search For | Score Guide |
|---|---|---|
| 1. CONNECTIVITY | Nearest highway/expressway, distance to airport, nearest railway station, road width frontage | 9-10: Highway <1km, Airport <20km. 5-6: Highway 3-5km. 1-3: Remote |
| 2. PUBLIC TRANSIT | Nearest metro station (existing/upcoming), bus depot, BRTS, auto/cab accessibility | 9-10: Metro <500m. 5-6: Metro 2-5km. 1-3: No transit |
| 3. CORPORATE DEMAND DRIVERS | Nearby IT parks, SEZs, business districts, corporate offices (Infosys, TCS, Wipro campuses) | 9-10: IT park <2km. 5-6: IT park 5-10km. 1-3: No corporate hub |
| 4. SOCIAL INFRASTRUCTURE | Hospitals (multi-specialty), shopping malls, restaurants, entertainment, community spaces | 9-10: Hospital + Mall <3km. 5-6: Basic amenities only. 1-3: Undeveloped |
| 5. EDUCATIONAL INSTITUTIONS | Schools (CBSE/ICSE/IB), colleges, universities, coaching centers | 9-10: Top school <2km. 5-6: Schools 3-5km. 1-3: No schools nearby |
| 6. FUTURE GROWTH CATALYSTS | Upcoming infrastructure — metro extension, ring road, new highway, govt projects, Smart City initiatives | 9-10: Major infra project underway <5km. 5-6: Planned. 1-3: No pipeline |
| 7. COMPETITIVE LANDSCAPE | Number of active competing projects, pricing pressure, inventory overhang in the micro-market | Use LF data: Velocity >3% and MI <18 = 9-10. MI 18-30 = 5-6. MI >30 = 1-3 |
| 8. CATCHMENT QUALITY | Income profile of surrounding area, buyer demographics from IGR data, employment base | Use LF buyer data if available. IT corridor = 8-10. Mixed = 5-7. Low income = 1-3 |

Present this as a SITE SCORECARD TABLE with the specific landmark names, distances, and scores.

**STEP 2.5 — REGULATORY & TITLE INTELLIGENCE (MANDATORY for Maharashtra/Pune feasibility)**

When the plot is in Maharashtra (Pune, Mumbai, Nagpur, etc.), use web_search to gather regulatory data from government portals. For other states, adapt to local equivalents.

A. ZONING & DEVELOPMENT PLAN VERIFICATION:
   - Search: "[location] development plan zone PMRDA" or "[location] DP zone PMC"
   - Portals: pmrda.gov.in, pmc.gov.in, pcmcindia.gov.in
   - Determine: What zone does the plot fall under (R1, R2, C1, C2, Industrial, etc.)?
   - What uses are PERMITTED vs RESTRICTED in this zone?
   - Are there any public reservations (road widening, DP road, garden, school) affecting usable area?
   - Present as: "**Zoning:** [Zone code] — [Permitted uses]. [Source: web search / user to verify from DP map]"

B. UDCPR / DCR RULES (search and apply):
   - Search: "UDCPR FSI rules [zone] Maharashtra" or "UDCPR road width FSI table"
   - Portal: dtp.maharashtra.gov.in
   - Key rules to extract and apply:
     * Base FSI for the zone
     * Premium FSI available (and cost — typically 50% of ready reckoner rate)
     * TDR loading permitted (and applicable zones)
     * Road width multiplier — FSI varies by fronting road width (9m, 12m, 18m, 24m, 30m+)
     * Mandatory deductions: 10% recreational open space, amenity space for plots >4000 sqm
     * Margin/setback rules by building height
   - If user has provided FSI in their DCR extract, USE THEIR NUMBER but cross-reference against UDCPR

C. RERA & PROJECT HISTORY CHECK:
   - Search: "MahaRERA [developer name] [location]" or "maharerait.maharashtra.gov.in [location]"
   - Check: Are there existing RERA registrations on this plot or adjacent plots?
   - Check: Developer's track record — how many projects registered, completion status

D. ENVIRONMENTAL & RESTRICTION CHECK:
   - Search: "[location] NDZ no development zone" or "[location] CRZ flood zone"
   - Check: Is the plot near any water body, hill slope >1:5, forest boundary, or heritage zone?
   - Portals: mrsac.gov.in for spatial data
   - Flag any environmental risks found

E. REGULATORY VERIFICATION CHECKLIST:
   ALWAYS present a checklist at the end of the regulatory section. Mark items as ✓ (verified via web), ⚠ (partially verified), or ✗ (user must verify manually). Cover: 7/12 Extract (Satbara), Property Card, Bhu Naksha, Development Plan Zone, UDCPR FSI Rules, Index-II (transaction history), Lis Pendens (litigation), MahaRERA Check, Environmental Clearance.

   Then add: "**Critical:** This feasibility is based on the plot parameters provided by you and publicly available regulatory information. Before committing to acquisition, obtain the 7/12 Extract, Property Card, Index-II, and Lis Pendens certificate to verify clear title, absence of encumbrances, and litigation-free status."

**STEP 3 — DEVELOPMENT ECONOMICS**

=== CRITICAL: NEVER ASSUME LAND COST OR CONSTRUCTION COST ===
These are the TWO most sensitive inputs in any feasibility. A wrong assumption can flip a viable project into a loss or vice versa. Follow this logic:

IF the user HAS PROVIDED land cost and construction cost → Use their numbers, proceed with full P&L.

IF the user has NOT provided land cost or construction cost → Do this:

A. BUILDABLE AREA CALCULATION (always compute — needs only plot area + FSI):
   - Gross Plot Area (from user input)
   - Net Plot Area = Gross × 85% (road surrender, setbacks, amenity space)
   - Total BUA = Net Plot × FSI
   - Saleable Area = BUA × Efficiency (70% freehold residential, 55% SRA, 65% MHADA, 75% commercial)
   - Carpet Area = Saleable × 0.74 (RERA carpet ratio)

B. REVENUE PROJECTION (use LF price data from the micromarket):
   - Identify weighted avg saleable price PSF from LF data for this micromarket
   - Show revenue at 3 price points: Market Average, Market Average +10%, Market Average +20%
   - Residential Revenue = Saleable Area × Price PSF
   - If mixed-use: Commercial Revenue = Commercial Saleable × Commercial PSF
   - Show GROSS revenue only. Do NOT deduct brokerage, stamp duty absorption, or any sales cost from revenue. These are cost items and belong in the cost structure section.

C. LAND COST SENSITIVITY MATRIX (instead of guessing):
   Present a table showing profitability at DIFFERENT land costs (e.g., Rs.100 Cr / 150 / 200 / 250 / 300). Use construction cost of Rs. 4,000 PSF for Pune, Rs. 4,500 for Gurugram, Rs. 3,500 for Kolkata as DEFAULT but state the assumption clearly.

   Then say: "To refine this analysis with your actual numbers, please share land cost, construction cost, premium FSI/TDR costs, brokerage/channel partner commission, and stamp duty absorption if any. I will recalculate the full P&L."

D. COST STRUCTURE — USER INPUTS FIRST. Use industry defaults clearly labeled as assumptions when user inputs are missing. CRITICAL: Brokerage and Stamp Duty Absorption are DEVELOPER DECISIONS, not industry defaults — show them as Rs.0 in the base case with impact noted.

E. BREAKEVEN ANALYSIS — at MARKET AVERAGE price, what is the MAXIMUM land cost that makes the project viable (>15% margin)?

F. SENSITIVITY ANALYSIS (3×3 matrix of price ±15% × land cost low/mid/high)

G. PHASED CASH FLOW MODEL — 5-year project lifecycle, year-wise table of revenue booked, collections, construction spend, other costs, net cash flow, cumulative.

H. IRR CALCULATION using year-wise net cash flows. Benchmarks: <15% WEAK, 15-20% MODERATE, 20-25% STRONG, >25% EXCELLENT.

I. NPV CALCULATION at 12%, 15%, 18% discount rates.

J. EQUITY MULTIPLE = total cash inflows / total equity invested. <1.5x WEAK, 1.5-2.0x MODERATE, >2.0x STRONG.

K. ABSORPTION SCENARIO MODELING — Pessimistic (LF velocity -30%), Base Case (LF velocity), Optimistic (LF velocity +20%) — show IRR/NPV/Equity Multiple/Breakeven Month for each.

**STEP 4 — COMPETITIVE POSITIONING (from LF data)**
- Pull ALL projects from the same micromarket using comparable_projects data
- Show top 10 by annual sales with exact metrics
- Identify PRICING GAPS — price bands with low competition
- Identify CONFIGURATION GAPS — BHK types undersupplied
- Show velocity leaders as benchmarks

**STEP 5 — DEVELOPMENT MIX OPTIMIZER (CRITICAL — never default to single-use)**

ALWAYS run when ANY of these conditions exist: FSI > 2.5, user mentions "commercial" in DCR/parking norms, plot is in IT corridor, plot area > 5 acres, or user explicitly asks about mixed-use.

UDCPR FSI CONSTRAINT CHECK (Maharashtra): Basic residential FSI = 1.10. Max with premium + TDR varies by road width (9m: 1.10, 30m+: 3.00). If user's FSI exceeds residential max, the excess MUST be commercial/IT/institutional.

Present a Development Mix Table showing FSI allocation across: Residential (sale), IT/Commercial Offices, Co-working Spaces, Co-living/Serviced Apts, Retail. Total must equal user's FSI.

For each non-residential component, show THREE revenue approaches:
- Option 1 — FULL SALE (maximize upfront cash, highest IRR)
- Option 2 — HYBRID (sell residential, lease commercial — most common in India)
- Option 3 — FULL LEASE (annuity income, requires patient capital)

For LEASE components: annual lease income = leasable area × monthly rent × 12 × occupancy (85%). Cap rate valuation: annual lease income / cap rate (7-9%) = asset value at Year 5.

Component-specific guidance:
- RESIDENTIAL: from LF data (HIGH confidence)
- IT OFFICES: web search, label [Web Context] — typical Rs.50-100/sqft/month lease, Rs.8,000-15,000 PSF sale
- CO-WORKING: web search — typical Rs.5,000-15,000/seat/month, 1 seat per 60-80 sqft, occupancy 70% Y1 / 85% Y2+
- CO-LIVING: web search — typical Rs.8,000-25,000/bed/month, 1 bed per 100-300 sqft, occupancy 80% Y1 / 90% Y2+
- RETAIL: ground floor only, typically 1.5-2x residential rate or Rs.80-200/sqft/month lease, keep 5-10% of total BUA

After the mix table, recommend the OPTIMAL allocation with reasoning, total revenue (vs pure residential), blended IRR, annual rental income post-stabilization, and asset valuation at Year 5.

**STEP 6 — RISK MATRIX**
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Market slowdown | Use LF velocity trend | Revenue impact % | Phasing, pricing flexibility |
| Oversupply | Use MI from LF data | Absorption delay | Differentiated product |
| Regulatory delay | Medium | Timeline extension | Pre-approvals |
| Construction cost escalation | Medium | Margin compression | Fixed-price contracts |
| Interest rate risk | Use web_search for RBI outlook | EMI impact on buyers | Subvention scheme |

**STEP 7 — GO / CONDITIONAL GO / NO-GO VERDICT**

A. SITE VERDICT (from scorecard): >65/80 STRONG, 45-65 MODERATE, <45 WEAK
B. FINANCIAL VERDICT: definitive GO/NO-GO if user provided land cost; otherwise state MAXIMUM VIABLE LAND COST clearly
C. COMBINED VERDICT FORMAT:
   **VERDICT: [GO / CONDITIONAL GO / NO-GO]**
   **Site Score: [X]/80 — [STRONG/MODERATE/WEAK]**
   **Maximum Viable Land Cost: Rs.[X] Crores (Rs.[Y] Cr/acre)**
   **Breakeven Price PSF: Rs.[Z] (vs market average Rs.[M])**
   Then 1 paragraph executive summary explaining WHY — referencing specific data points.
D. ACTIONABLE NEXT STEPS — 3-4 concrete next steps the user should take.

=== CRITICAL RULE FOR FEASIBILITY ===
When a feasibility query comes in, ALWAYS activate web_search even if the query doesn't match web keywords. The user expects location-specific intelligence that REQUIRES web search — nearby landmarks, upcoming infrastructure, corporate campuses. LF data alone is NOT sufficient for feasibility.

**SITE INTELLIGENCE:** Same as Land Feasibility but without the financial projections. Focus on Steps 1-2 (location + surroundings) and Step 7 (verdict).

MANDATORY: Include at least one <lfchart> when the data supports it (3+ data points). Do NOT force a chart when data is sparse.

End EVERY response with:
---
**Data Source:** Liases Foras Proprietary Research Database
**Data Period:** [exact quarters/years]
**City:** [city name]
**Confidence:** [HIGH / MEDIUM / LOW]
**Basis:** [explanation referencing LF Knowledge Base]"""

SYSTEM_PROMPT_WEB_ADDENDUM = """

=== WEB INTELLIGENCE MODE (ACTIVE) ===
You have access to the web_search tool for this query. Use it to fetch CURRENT context — RBI policy rates, infrastructure announcements, government policy changes, developer news, macro-economic data.

CRITICAL RULES FOR WEB INTELLIGENCE:
1. LF DATA IS THE BACKBONE. Web data provides CONTEXT, not replacement. Every core metric (sales, supply, price, velocity, MI) MUST come from the LF database. Web data adds the 'why' and 'what next'.
2. NEVER mix web-sourced numbers into LF data tables or charts. Charts must ONLY contain LF database values.
3. CLEARLY SEPARATE sources:
   - For LF data insights: state them normally (this is the default)
   - For web-sourced context: prefix with [Web Context] and cite the source
   - Example: '[Web Context] RBI cut the repo rate by 25bps to 6.0% in April 2025 (Source: RBI.org.in). Based on LF data, Gurugram velocity is already at 4.76% — this rate cut could accelerate absorption further.'
4. In the source citation footer, add a separate WEB SOURCES section listing each web source used with its URL.
5. Use web search for: current repo rate, recent infrastructure news for the city, any policy changes affecting real estate, developer earnings if asked, recent land deals.
6. Do NOT use web search to find property data that contradicts or supplements LF data. If web says Gurugram avg price is Rs.25,000 PSF but LF data says Rs.20,981 — use LF data and note the difference if relevant.
7. For FEASIBILITY queries, use up to 8 web searches covering: location surroundings, infrastructure projects, UDCPR/DCR rules, zoning, MahaRERA, metro/highway status, commercial rates if needed. For non-feasibility queries, limit to 3 searches.

REGULATORY PORTAL REFERENCE (for Maharashtra feasibility):
When searching for regulatory data, target these specific portals:
- UDCPR/DCR rules: dtp.maharashtra.gov.in — FSI tables, road width multipliers, margin rules
- Development Plan: pmrda.gov.in (PMRDA area), pmc.gov.in (PMC area), pcmcindia.gov.in (PCMC area)
- MahaRERA: maharerait.maharashtra.gov.in — project registrations, developer records
- IGR: igrmaharashtra.gov.in — stamp duty rates, ready reckoner rates
- Environmental: mrsac.gov.in — spatial restrictions, NDZ zones
Search queries like "UDCPR FSI table zone R2 Maharashtra" or "MahaRERA registered projects Hinjewadi" will yield relevant results from these portals."""


def get_system_prompt(with_web=False):
    """Build system prompt, optionally with web intelligence rules."""
    if with_web:
        return SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_WEB_ADDENDUM
    return SYSTEM_PROMPT_BASE


# ═══════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════

@app.route('/api/query', methods=['POST'])
def handle_query():
    """Main query endpoint — runs Cypher, sends to Claude, returns response."""
    # Rate limiting
    client_ip = request.remote_addr or 'unknown'
    if not check_rate_limit(client_ip):
        return jsonify({"error": "Rate limit exceeded. Please wait a moment."}), 429

    body = request.json
    user_query = body.get('query', '')
    city = body.get('city', 'Gurugram')
    history = body.get('history', [])
    stream = body.get('stream', True)

    if not user_query:
        return jsonify({"error": "No query provided"}), 400

    # Step 1: Get data from Neo4j
    try:
        data_results = classify_intent(user_query, city)
    except Exception as e:
        print(f"Neo4j query failed: {e}")
        return jsonify({"error": f"Database connection issue: {str(e)}. Please try again."}), 503

    # Step 2: Detect if web intelligence is needed
    web_mode = needs_web(user_query)
    if web_mode:
        print(f"  🌐 Web intelligence activated for: {user_query[:60]}...")

    # Step 3: Format data for Claude (v4: includes per-query descriptions)
    corridor_sectors = detect_corridor(user_query)
    data_text = format_data_block_for_claude(
        data_results,
        city=city,
        corridor_sectors=corridor_sectors,
    )

    # Step 4: Build messages
    messages = []
    for h in history[-6:]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({
        "role": "user",
        "content": f"VERIFIED DATA FROM LF KNOWLEDGE BASE:\n{data_text}\n\nUSER QUESTION: {user_query}"
    })

    # Step 5: Build Claude API call params
    system_prompt = get_system_prompt(with_web=web_mode)
    client = get_claude()

    # Feasibility queries need more tokens for comprehensive reports
    is_feasibility = bool(re.search(r'feasib|plot.*area|acre|fsi|dcr|google.*map|site.*intel', user_query, re.IGNORECASE))
    token_limit = 8000 if is_feasibility else 4000

    api_params = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": token_limit,
        "system": system_prompt,
        "messages": messages,
    }

    # Add web search tool if needed — limit uses to prevent timeout
    if web_mode:
        web_uses = 5 if is_feasibility else 3
        api_params["tools"] = [
            {"type": "web_search_20250305", "name": "web_search", "max_uses": web_uses}
        ]

    # Step 6: Call Claude
    if stream:
        # Pre-compute the categories that fired, for audit logging (Fix C)
        fired_categories = classify_web_intent(user_query) if web_mode else []

        def generate():
            try:
                with client.messages.stream(**api_params) as s:
                    # Iterate raw events so we can capture both text chunks
                    # AND server_tool_use (web_search) invocations for audit.
                    for event in s:
                        et = getattr(event, "type", None)
                        if et == "content_block_delta":
                            delta = getattr(event, "delta", None)
                            if delta is not None and getattr(delta, "type", "") == "text_delta":
                                txt = getattr(delta, "text", "")
                                if txt:
                                    yield f"data: {json.dumps({'type': 'text', 'text': txt})}\n\n"
                        elif et == "content_block_start":
                            block = getattr(event, "content_block", None)
                            btype = getattr(block, "type", "") if block else ""
                            if btype == "server_tool_use" and getattr(block, "name", "") == "web_search":
                                # AUDIT LOG — Fix C
                                search_input = getattr(block, "input", {}) or {}
                                search_query = search_input.get("query", "<unknown>")
                                print(
                                    f"  🔍 [WEB_SEARCH_AUDIT] "
                                    f"user_query={user_query[:80]!r} "
                                    f"categories={fired_categories} "
                                    f"claude_searched={search_query!r}"
                                )
                yield f"data: {json.dumps({'type': 'done', 'web_mode': web_mode})}\n\n"
            except Exception as e:
                print(f"  ✗ Claude streaming error: {e}")
                yield f"data: {json.dumps({'type': 'error', 'text': str(e)})}\n\n"

        return Response(
            stream_with_context(generate()),
            mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )
    else:
        response = client.messages.create(**api_params)
        # Pre-compute fired categories for audit log (Fix C)
        fired_categories = classify_web_intent(user_query) if web_mode else []

        # Extract text from potentially mixed content blocks (text + web_search results)
        response_text = ""
        web_searches_made = []
        for block in response.content:
            if hasattr(block, "text"):
                response_text += block.text
            # AUDIT LOG — Fix C (non-streaming variant)
            btype = getattr(block, "type", "")
            if btype == "server_tool_use" and getattr(block, "name", "") == "web_search":
                search_input = getattr(block, "input", {}) or {}
                search_query = search_input.get("query", "<unknown>")
                web_searches_made.append(search_query)
                print(
                    f"  🔍 [WEB_SEARCH_AUDIT] "
                    f"user_query={user_query[:80]!r} "
                    f"categories={fired_categories} "
                    f"claude_searched={search_query!r}"
                )

        return jsonify({
            "response": response_text,
            "data_queries": [r["query"] for r in data_results],
            "total_rows": sum(r.get("row_count", 0) for r in data_results),
            "web_mode": web_mode,
            "web_searches_made": web_searches_made,  # exposed for client/UI audit
        })


@app.route('/api/raw', methods=['POST'])
def raw_query():
    """Direct Cypher query — returns raw Neo4j data without Claude."""
    body = request.json
    query_name = body.get('query_name', '')
    params = body.get('params', {})

    result = run_query(query_name, **params)
    return jsonify(result)


@app.route('/api/validate', methods=['POST'])
def validate_number():
    """Validate a specific data point — returns source lineage."""
    body = request.json
    project = body.get('project', '')
    city = body.get('city', 'Gurugram')

    result = run_query("validate_number", project_name=project, city=city)
    return jsonify(result)


@app.route('/api/cities', methods=['GET'])
def list_cities():
    """List available cities."""
    d = get_driver()
    with d.session(database='c26f3089') as session:
        result = session.run("MATCH (c:City) RETURN c.name AS name, c.state AS state")
        cities = [dict(r) for r in result]
    return jsonify(cities)


@app.route('/api/health', methods=['GET'])
def health():
    """Health check — always returns 200 so Railway healthcheck passes.
    Reports config and Neo4j status for debugging."""
    status = {"status": "ok", "config": _CONFIG_OK}
    if _CONFIG_OK and NEO4J_PASSWORD:
        try:
            d = get_driver()
            with d.session(database='c26f3089') as session:
                session.run("RETURN 1 AS ok").single()
            status["neo4j"] = "connected"
        except Exception as e:
            status["neo4j"] = f"error: {str(e)[:100]}"
    else:
        status["neo4j"] = "not configured"
    return jsonify(status)


@app.route('/', methods=['GET'])
def root():
    """Root endpoint — Render/Railway may check this too."""
    return jsonify({"service": "MR&I API v4.1", "status": "ok"})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5000)
    args = parser.parse_args()

    print(f"MR&I API Server v4.1 starting on port {args.port}")
    print(f"Neo4j: {NEO4J_URI}")
    print(f"Web Intelligence: enabled")
    app.run(host='0.0.0.0', port=args.port, debug=True)
