"""
MR&I API Backend v3
====================
Flask server bridging the HTML frontend, Neo4j graph, and Claude API.
Web Intelligence integration via Anthropic web_search tool.
v3: Fixed intent classifier (35+ patterns), 10 new Cypher queries,
    project name extraction, rate limiting, credential security.

Architecture:
  User query → classify intent → run Cypher query → get EXACT data →
  detect if web context needed → send data + query to Claude (with web_search tool if needed) →
  Claude presents data + web context → stream response back
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

# Import our queries
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
    """Run a Cypher query and return results as list of dicts."""
    if query_name not in QUERIES:
        return {"error": f"Unknown query: {query_name}"}

    cypher = QUERIES[query_name]
    d = get_driver()

    try:
        with d.session(database='c26f3089') as session:
            result = session.run(cypher, **params)
            records = [dict(record) for record in result]
    except Exception as e:
        print(f"  ✗ Query {query_name} failed: {e}")
        return {"error": str(e), "query": query_name, "row_count": 0, "data": [], "source": "error"}

    return {
        "query": query_name,
        "params": params,
        "row_count": len(records),
        "data": records,
        "source": "LF_Research_Database"
    }


# ═══════════════════════════════════════
# WEB INTELLIGENCE DETECTION
# ═══════════════════════════════════════
WEB_KEYWORDS = re.compile(
    r'repo.rate|rbi|interest.rate|infra|metro|express|highway|airport|policy|'
    r'stamp.duty|rera|government|budget|gdp|inflation|macro|economic|news|recent|'
    r'current.market|trend.2026|trend.2025|regulation|nirmala|fm |union.budget|'
    r'pmay|pradhan|rate.cut|rate.hike|what.if|impact.of|how.will|forecast|predict|'
    r'outlook|developer.*earn|earnings.call|land.deal|acquisition.*land|'
    r'rental.yield|rental|rent.trend|connectivity|dwarka|appreciation|capital.gain|'
    r'corridor|upcoming|under.construction|completion|timeline',
    re.IGNORECASE
)

# ═══════════════════════════════════════
# CORRIDOR → SECTOR MAPPING
# ═══════════════════════════════════════
CORRIDOR_MAP = {
    # ── GURGAON CORRIDORS ──
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
    r'new.gurgaon': [
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
    """Detect if query needs web intelligence."""
    q = query or ""
    # Always enable web for feasibility/site queries — they need location context
    if re.search(r'feasib|plot.*area|acre|fsi|dcr|google.*map|goo\.gl|maps\.google|site.*intel|due.dilig|land.*acqui', q, re.IGNORECASE):
        return True
    return bool(WEB_KEYWORDS.search(q))


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
    if re.search(r'buyer|demograph|who.*buy|customer|profile|age.*group|gender|pincode|locality|surname|religion', q):
        results.append(run_query("buyer_age_dist", city=city))
        results.append(run_query("buyer_gender_dist", city=city))
        results.append(run_query("buyer_locality_dist", city=city))
        results.append(run_query("buyer_state_dist", city=city))
        results.append(run_query("buyer_religion_dist", city=city))

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

    # ═══ Cap at 5 queries to prevent timeout ═══
    if len(results) > 5:
        results = results[:5]

    # ═══ Default: market overview ═══
    if not results:
        results.append(run_query("market_overview", city=city))
        results.append(run_query("flat_performance", city=city))

    return results


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

=== MICRO-MARKET MAPPING (CRITICAL) ===
Users often query by corridor names, not sector numbers. Map these to constituent sectors:
- Dwarka Expressway (DXP) = Sectors 37D, 99, 102, 103, 104, 108, 109, 110, 111, 112, 113
- Sohna Road / Sohna Corridor = Sectors 2-6 Sohna, Sector 33-36 Sohna, Sohna Road
- Golf Course Road / GCR = Sectors 42, 43, 53, 54, 55, 56, 57, 65
- Golf Course Extension Road = Sectors 58, 59, 61, 62, 63, 63A, 65, 66
- Southern Peripheral Road (SPR) = Sectors 68, 69, 70, 70A, 71, 72, 76, 77, 78, 79, 79B
- New Gurgaon = Sectors 76, 79, 80, 81, 82, 83, 84, 85, 86, 88A, 88B, 89, 89A, 90, 91, 92, 93, 95

When a user asks about a corridor:
1. Identify ALL sectors that map to it from the data
2. List individual projects from those sectors with their EXACT metrics
3. Show a project-level comparison table — NEVER average across projects to create a 'sector price'
4. If web intelligence is active, use web search to add infrastructure context

=== GLOSSARY (use these exact definitions) ===
- Marketable Supply = Sales + Unsold (total active stock)
- Months Inventory (MI) = Unsold / Monthly Sales. HEALTHY: <18. MODERATE: 18-24. OVERSUPPLIED: >24
- Sales Velocity = % of supply sold per month. STRONG: >3%. MODERATE: 2-3%. WEAK: <2%
- Absorption Price = Weighted avg price of actually transacted units
- Product Efficiency = Sales-to-supply ratio (higher = better selling product)

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

**STEP 3 — DEVELOPMENT ECONOMICS (from LF data)**
Calculate all of these — NEVER skip any line item:

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
   - Net Revenue = Gross × 0.95 (5% brokerage + stamp duty absorption)

C. LAND COST SENSITIVITY MATRIX (instead of guessing):
   Present this table showing profitability at DIFFERENT land costs:
   
   | Land Cost (Rs. Cr) | Per Acre (Rs. Cr) | Construction Cost | Total Cost | Revenue | Margin | Verdict |
   |---|---|---|---|---|---|---|
   | 100 | 7.1 | [calc] | [calc] | [calc] | [calc] | GO/NO-GO |
   | 150 | 10.7 | [calc] | [calc] | [calc] | [calc] | GO/NO-GO |
   | 200 | 14.2 | [calc] | [calc] | [calc] | [calc] | GO/NO-GO |
   | 250 | 17.8 | [calc] | [calc] | [calc] | [calc] | GO/NO-GO |
   | 300 | 21.4 | [calc] | [calc] | [calc] | [calc] | GO/NO-GO |

   Use construction cost of Rs. 4,000 PSF for Pune, Rs. 4,500 for Gurgaon, Rs. 3,500 for Kolkata as DEFAULT but state the assumption clearly.
   
   Then say: "To refine this analysis with your actual land acquisition cost and construction estimates, please share:
   1. Land cost (total Rs. Crores or per-acre rate)
   2. Expected construction cost (Rs. PSF on BUA)
   3. Any premium FSI or TDR costs applicable
   I will recalculate the full P&L with your exact numbers."

D. COST STRUCTURE — USER INPUTS FIRST:

   Check if the user has provided ANY of these. If yes, use their numbers. If no, use defaults BUT clearly label them as assumptions:

   | Cost Component | User Provided? | Default Assumption | Notes |
   |---|---|---|---|
   | Land Cost | MUST ASK if not provided | Show sensitivity matrix | Most critical input |
   | Construction Cost PSF | Use if provided | Rs.4,000 Pune, Rs.4,500 Gurgaon, Rs.3,500 Kolkata | Varies by developer — Godrej/Lodha: Rs.5,500+, Local: Rs.3,500 |
   | Approvals & Statutory % | Use if provided | 10% of construction | Varies by state — MH higher than HR |
   | Marketing & Sales % | Use if provided | 4% of revenue | Brand developers spend less (2-3%), new entrants more (5-6%) |
   | Finance Cost % | Use if provided | 13% on 60% of project cost | Depends on debt-equity ratio, developer's borrowing rate |
   | Contingency % | Use if provided | 5% of total cost | Standard buffer |

   ALWAYS present the defaults table with a note:
   "**Assumptions Used:** The above uses industry-standard cost assumptions for [city]. Your actual costs may differ significantly. To get a precise P&L, share your:
   1. Land acquisition cost (Rs. Crores or per-acre)
   2. Construction cost (Rs. PSF on BUA) — this varies from Rs.3,500 to Rs.6,000+ depending on specifications
   3. Your target debt-equity ratio and borrowing rate
   4. Any premium FSI / TDR / fungible FSI costs
   5. Your marketing budget assumption (% of revenue)
   
   I will instantly recalculate with your exact numbers."

   If this is a FOLLOW-UP message where the user is providing their cost inputs after seeing the initial analysis, recalculate the FULL P&L with their numbers — no need to show the sensitivity matrix again. Just show the definitive P&L and updated GO/NO-GO verdict.

E. BREAKEVEN ANALYSIS:
   - At the MARKET AVERAGE price, what is the MAXIMUM land cost that makes the project viable (>15% margin)?
   - At the user's TARGET price (if mentioned), what is the breakeven land cost?
   - This is the MOST useful number for a land acquisition head — it tells them their walk-away price.

F. SENSITIVITY ANALYSIS (MANDATORY — present as table):
   Show a 3×3 matrix combining price scenarios AND land cost scenarios:
   
   | | Land @ Rs.100 Cr | Land @ Rs.175 Cr | Land @ Rs.250 Cr |
   |---|---|---|---|
   | Price Rs.7,000 PSF (-15%) | Margin % | Margin % | Margin % |
   | Price Rs.8,200 PSF (Base) | Margin % | Margin % | Margin % |
   | Price Rs.9,500 PSF (+15%) | Margin % | Margin % | Margin % |
   
   This 3×3 matrix lets the CXO immediately see which price-land combinations work.

**STEP 4 — COMPETITIVE POSITIONING (from LF data)**
- Pull ALL projects from the same micromarket using the comparable_projects data
- Show top 10 by annual sales with their exact metrics
- Identify PRICING GAPS — price bands with low competition
- Identify CONFIGURATION GAPS — BHK types undersupplied
- Show velocity leaders as benchmarks for what sells

**STEP 5 — DEVELOPMENT SCENARIO ANALYSIS (MANDATORY — always show multiple options)**

NEVER recommend only one development type. Always present at least 2 scenarios so the CXO can compare:

**SCENARIO A — PURE RESIDENTIAL (use LF data for pricing):**
- Revenue PSF: Use LF weighted average saleable price (from SALEABLE_PRICE_AT data)
- Revenue calculation using LF residential pricing — this is the MOST reliable scenario because LF data directly supports it
- Configuration mix from flat_performance data
- Absorption rate from LF velocity data
- Label: "Pricing based on LF Research Database — HIGH confidence"

**SCENARIO B — MIXED-USE (Residential + Commercial):**
- Residential component: Use LF data pricing (state the exact PSF from data)
- Commercial component: Use web_search for local commercial rates — MUST label [Web Context] with source
- Split assumption: State the assumed residential-commercial ratio (e.g., 70-30 or 60-40)
- Label residential numbers as "LF Data" and commercial numbers as "[Web Context]"

**SCENARIO C — PURE COMMERCIAL (only if plot zoning demands it):**
- ALL pricing from web_search — label EVERY number [Web Context]
- State explicitly: "LF database does not cover commercial real estate. All commercial pricing below is from web intelligence."
- Include office, co-working, retail sub-segments with separate PSF
- Label: "Pricing based on Web Intelligence — MODERATE confidence, verify with local brokers"

For EACH scenario, show: Revenue, Cost, Margin, Breakeven Land Cost, and a GO/NO-GO.
This lets the CXO see which development type maximizes returns for their specific land cost.

**CONFIGURATION RECOMMENDATION (from LF residential data):**
- Recommended BHK mix (from flat_performance — which BHK has highest velocity)
- Recommended ticket size (from ticket_size data — which price band has best absorption)
- Recommended unit sizes (from unit_size data)
- Phasing strategy (Phase 1 launch size, based on market absorption rate)
- Positioning: Where should this project sit vs competitors

**STEP 6 — RISK MATRIX**
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Market slowdown | Use LF velocity trend | Revenue impact % | Phasing, pricing flexibility |
| Oversupply | Use MI from LF data | Absorption delay | Differentiated product |
| Regulatory delay | Medium | Timeline extension | Pre-approvals |
| Construction cost escalation | Medium | Margin compression | Fixed-price contracts |
| Interest rate risk | Use web_search for RBI outlook | EMI impact on buyers | Subvention scheme |

**STEP 7 — GO / CONDITIONAL GO / NO-GO VERDICT**

The verdict must consider BOTH the site scorecard AND the financial viability:

A. SITE VERDICT (from scorecard):
- Total Score > 65/80: STRONG SITE — Location fundamentals support development
- Total Score 45-65/80: MODERATE SITE — Some parameters need improvement
- Total Score < 45/80: WEAK SITE — Location risks dominate

B. FINANCIAL VERDICT (from land cost sensitivity):
- If user provided land cost: Give definitive GO/NO-GO based on margin
- If user did NOT provide land cost: State the MAXIMUM VIABLE LAND COST clearly
  Example: "At current market rates of Rs.8,200 PSF, the project is viable if land cost stays below Rs.150 Crores (Rs.10.7 Cr/acre). Above Rs.200 Crores, the project enters loss territory."

C. COMBINED VERDICT FORMAT:
  **VERDICT: [GO / CONDITIONAL GO / NO-GO]**
  **Site Score: [X]/80 — [STRONG/MODERATE/WEAK]**
  **Maximum Viable Land Cost: Rs.[X] Crores (Rs.[Y] Cr/acre)**
  **Breakeven Price PSF: Rs.[Z] (vs market average Rs.[M])**

  Then 1 paragraph executive summary explaining WHY — referencing specific data points.

D. ACTIONABLE NEXT STEPS (always include):
  Present 3-4 concrete next steps the user should take, e.g.:
  - "Negotiate land at Rs.[X] Cr/acre or below for 18%+ margin"
  - "Commission a detailed soil/geotechnical survey for the plot"
  - "Verify Metro Line 3 station proximity — confirm walking distance"
  - "Share your land cost and construction estimates for a refined P&L"

=== CRITICAL RULE FOR FEASIBILITY ===
When a feasibility query comes in, ALWAYS activate web_search even if the query doesn't match web keywords.
The user expects location-specific intelligence that REQUIRES web search — nearby landmarks, upcoming infrastructure, corporate campuses. LF data alone is NOT sufficient for feasibility. The combination of LF market data + web location intelligence is what makes this analysis valuable.

**SITE INTELLIGENCE:** Same as Land Feasibility but without the financial projections. Focus on Steps 1-2 (location + surroundings) and Step 7 (verdict). Used for quick site assessments without full financial modeling.

MANDATORY: Include at least one <lfchart> when the data supports it (3+ data points). Do NOT force a chart when data is sparse (single project lookup, yes/no answers). If only 1-2 data points exist, use a markdown table instead.

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
   - Example: '[Web Context] RBI cut the repo rate by 25bps to 6.0% in April 2025 (Source: RBI.org.in). Based on LF data, Gurgaon velocity is already at 4.76% — this rate cut could accelerate absorption further.'
4. In the source citation footer, add a separate WEB SOURCES section listing each web source used with its URL.
5. Use web search for: current repo rate, recent infrastructure news for the city, any policy changes affecting real estate, developer earnings if asked, recent land deals.
6. Do NOT use web search to find property data that contradicts or supplements LF data. If web says Gurgaon avg price is Rs.25,000 PSF but LF data says Rs.20,981 — use LF data and note the difference if relevant.
7. Maximum 3 web searches per response. Be targeted."""


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
    city = body.get('city', 'Gurgaon')
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

    # Step 3: Format data for Claude
    data_text = f"CITY: {city}\n\n"
    data_text += "DATA LINEAGE: Every row below was queried directly from the LF Knowledge Base built from Liases Foras proprietary research data. Row counts and query names are provided for traceability.\n"
    data_text += "CRITICAL: This data covers RESIDENTIAL markets only. If the user's query involves commercial/office/retail/co-working pricing, you MUST use web_search for those rates and label them [Web Context]. Do NOT attribute any commercial pricing to LF data.\n\n"
    queries_used = []
    total_rows = 0
    for result in data_results:
        if "error" in result:
            continue
        queries_used.append(result['query'])
        total_rows += result['row_count']
        data_text += f"--- {result['query']} ({result['row_count']} rows, source: {result['source']}) ---\n"
        data_text += json.dumps(result['data'], indent=1, default=str)
        data_text += "\n\n"
    data_text += f"TOTAL: {len(queries_used)} queries executed, {total_rows} rows returned from LF Knowledge Base.\n"
    data_text += f"QUERIES USED: {', '.join(queries_used)}\n"

    # Detect corridor context and add mapping hint
    corridor_sectors = detect_corridor(user_query)
    if corridor_sectors:
        corridor_name = user_query  # Will be parsed by Claude from context
        data_text += f"\nCORRIDOR MAPPING: The query references a corridor. Constituent sectors searched: {', '.join(corridor_sectors)}\n"
        data_text += "Present data grouped by sector with individual project metrics. Do NOT average across projects.\n"

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

    # Add web search tool if needed
    if web_mode:
        api_params["tools"] = [
            {"type": "web_search_20250305", "name": "web_search", "max_uses": 5}
        ]

    # Step 6: Call Claude
    if stream:
        def generate():
            try:
                with client.messages.stream(**api_params) as s:
                    for text in s.text_stream:
                        yield f"data: {json.dumps({'type': 'text', 'text': text})}\n\n"
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
        # Extract text from potentially mixed content blocks (text + web_search results)
        response_text = ""
        for block in response.content:
            if hasattr(block, 'text'):
                response_text += block.text

        return jsonify({
            "response": response_text,
            "data_queries": [r["query"] for r in data_results],
            "total_rows": sum(r["row_count"] for r in data_results),
            "web_mode": web_mode,
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
    city = body.get('city', 'Gurgaon')

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
    """Root endpoint — Railway may check this too."""
    return jsonify({"service": "MR&I API v3", "status": "ok"})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5000)
    args = parser.parse_args()

    print(f"MR&I API Server v3 starting on port {args.port}")
    print(f"Neo4j: {NEO4J_URI}")
    print(f"Web Intelligence: enabled")
    app.run(host='0.0.0.0', port=args.port, debug=True)
