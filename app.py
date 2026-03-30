"""
MR&I API Backend
=================
Flask server bridging the HTML frontend, Neo4j graph, and Claude API.

Architecture:
  User query → classify intent → run Cypher query → get EXACT data → 
  send data + query to Claude → Claude presents (does NOT interpret raw JSON)

Usage:
  python api_server.py --neo4j-uri bolt://localhost:7687 --neo4j-password <pwd> --anthropic-key <key>

  Or with env vars:
  export NEO4J_URI=bolt://localhost:7687
  export NEO4J_PASSWORD=your_password  
  export ANTHROPIC_API_KEY=sk-ant-...
  python api_server.py
"""

import os
import json
import re
import argparse
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
# CONFIG
# ═══════════════════════════════════════
NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j+s://c26f3089.databases.neo4j.io')
NEO4J_USER = os.environ.get('NEO4J_USER', 'c26f3089')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'X_EaaI8F3BXe3YGBP8k9jAJTN28W_QvnGSgCjvELaTY')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')  # Pass via --anthropic-key or env var

driver = None
claude = None


def get_driver():
    global driver
    if driver is None:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
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

    with d.session(database='c26f3089') as session:
        result = session.run(cypher, **params)
        records = [dict(record) for record in result]

    # Attach data lineage
    return {
        "query": query_name,
        "params": params,
        "row_count": len(records),
        "data": records,
        "source": "LF_database_via_neo4j"
    }


# ═══════════════════════════════════════
# INTENT CLASSIFIER
# ═══════════════════════════════════════
def classify_intent(query, city):
    """Map user query to appropriate Cypher queries."""
    q = query.lower()
    results = []

    # Market overview
    if re.search(r'market|overview|summary|health.check|how.*market', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))

    # Price trends
    if re.search(r'pric|psf|rate|cost|trend', q):
        results.append(run_query("price_trend_saleable", city=city))

    # Quarterly absorption
    if re.search(r'absorption|quarterly.*sale|qoq|quarter', q):
        results.append(run_query("quarterly_absorption", city=city))

    # Micro-market ranking by demand
    if re.search(r'rank.*demand|demand.*intens|micro.*market.*demand', q):
        results.append(run_query("micromarkets_by_demand", city=city))

    # Micro-market ranking by inventory risk
    if re.search(r'rank.*inventor|inventor.*risk|micro.*market.*risk', q):
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # Emerging micro-markets
    if re.search(r'emerging|growing|upcoming|new.*market', q):
        results.append(run_query("emerging_micromarkets", city=city))

    # Declining micro-markets
    if re.search(r'declining|slow|weak|struggling', q):
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # Product mix / configurations
    if re.search(r'bhk|config|mix|flat|product.*mix|optim', q):
        results.append(run_query("flat_performance", city=city))

    # Top projects
    if re.search(r'top.*project|best.*project|rank.*project|leading', q):
        results.append(run_query("top_projects_by_sales", city=city))
        results.append(run_query("top_projects_by_velocity", city=city))

    # Competitive / specific project
    if re.search(r'compet|benchmark|compare|versus|vs', q):
        results.append(run_query("top_projects_by_sales", city=city))
        results.append(run_query("micromarkets_by_demand", city=city))

    # Feasibility
    if re.search(r'feasib|irr|break.even|viable|plot|acre|fsi', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("price_trend_saleable", city=city))
        results.append(run_query("flat_performance", city=city))

    # Site intelligence / location
    if re.search(r'site.*intel|location|sector|due.dilig', q):
        # Try to extract location name
        loc_match = re.search(r'sector\s*[-]?\s*\d+\w*|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', query)
        if loc_match:
            loc = loc_match.group()
            results.append(run_query("micromarket_detail", city=city, location=loc))
        results.append(run_query("nearby_micromarkets", city=city))
        results.append(run_query("top_projects_by_sales", city=city))

    # Builder analysis
    if re.search(r'builder|developer|who.*build', q):
        results.append(run_query("builder_rankings", city=city))

    # YoY absorption growth
    if re.search(r'yoy|year.*over.*year|annual.*growth|absorption.*growth', q):
        results.append(run_query("yoy_absorption", city=city))
        results.append(run_query("quarterly_absorption", city=city))

    # Velocity trend
    if re.search(r'velocity.*trend|velocity.*over|speed.*sales', q):
        results.append(run_query("velocity_trend", city=city))

    # Inventory trend
    if re.search(r'inventory.*trend|months.*inventory.*over|unsold.*trend', q):
        results.append(run_query("inventory_trend", city=city))

    # Buyer demographics
    if re.search(r'buyer|demograph|who.*buy|customer|profile|age.*group|gender|pincode|locality|surname|religion', q):
        results.append(run_query("buyer_age_dist", city=city))
        results.append(run_query("buyer_gender_dist", city=city))
        results.append(run_query("buyer_locality_dist", city=city))
        results.append(run_query("buyer_state_dist", city=city))
        results.append(run_query("buyer_religion_dist", city=city))

    # Slow-moving / declining
    if re.search(r'slow.*mov|slow.*sell|aging|stuck|not.*sell', q):
        results.append(run_query("flat_performance", city=city))
        results.append(run_query("micromarkets_by_inventory_risk", city=city))

    # Ticket size
    if re.search(r'ticket.*size|price.*band|affordab|budget|cost.*range', q):
        results.append(run_query("flat_performance", city=city))
        results.append(run_query("price_trend_saleable", city=city))

    # Best-selling configurations
    if re.search(r'best.*sell|top.*config|popular.*bhk|fast.*mov|high.*demand', q):
        results.append(run_query("flat_performance", city=city))

    # Residential overview (matches the chip exactly)
    if re.search(r'residential|overview.*residential', q):
        results.append(run_query("market_overview", city=city))
        results.append(run_query("annual_overview", city=city))
        results.append(run_query("price_trend_saleable", city=city))
        results.append(run_query("flat_performance", city=city))

    # Default: market overview
    if not results:
        results.append(run_query("market_overview", city=city))
        results.append(run_query("flat_performance", city=city))

    return results


# ═══════════════════════════════════════
# SYSTEM PROMPT
# ═══════════════════════════════════════
SYSTEM_PROMPT = """You are MR&I (Market Research & Intelligence), a precision real estate analytics engine.

ABSOLUTE RULES:
1. Every number you present MUST come from the data provided below. Zero exceptions.
2. If a number is not in the data, say "data not available" — NEVER estimate or approximate.
3. Use Indian formatting: ₹, Lakhs, Crores, PSF.
4. Every data point has a source tag (query name + row). Cite these when presenting.
5. For location analysis beyond what's in the data, you may add general knowledge context but CLEARLY label it as "General context (not from LF data):"

DATA LINEAGE:
The data below was queried directly from a Neo4j knowledge graph built from Liases Foras proprietary research data. Each result set shows:
- query: the Cypher query name that produced it
- row_count: exact number of records
- data: the actual records
- source: always "LF_database_via_neo4j"

CHART FORMAT:
<lfchart type="bar|line|doughnut|hbar" title="Title">
<labels>L1,L2</labels>
<dataset label="Name" color="#hex">v1,v2</dataset>
</lfchart>
Colors: #c9a84c #3b82f6 #22c55e #ef4444 #8b5cf6 #06b6d4
Values must be plain numbers only.

FORMAT: **bold** key numbers (use bold headers not ### markdown headers), markdown tables, bullet points. Always include at least one chart.

CONFIDENCE FRAMEWORK (apply dynamically to every response):
- HIGH: Every number comes directly from the Neo4j query results. You are reporting/ranking/comparing explicit data.
- MEDIUM: Base numbers from Neo4j + your calculations/projections/analytical frameworks on top.
- LOW: Significant reasoning beyond the data (predictions, qualitative assessments).
- DATA NOT AVAILABLE: The query asks about something not in the LF database. Known gaps: tower-level data, floor premiums, marketing spend, sales channels, discount tracking, pre-launch pricing. NEVER fabricate.

TRANSPARENCY: Separate 'The data shows...' (direct) from 'Based on this, we can infer...' (derived).

End EVERY response with:
---
**Data Source:** Liases Foras Proprietary Research Database
**Data Period:** [exact quarters/years referenced]
**Confidence:** [HIGH/MEDIUM/LOW]
**Basis:** [1-2 line explanation]"""


# ═══════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════

@app.route('/api/query', methods=['POST'])
def handle_query():
    """Main query endpoint — runs Cypher, sends to Claude, returns response."""
    body = request.json
    user_query = body.get('query', '')
    city = body.get('city', 'Gurgaon')
    history = body.get('history', [])
    stream = body.get('stream', True)

    if not user_query:
        return jsonify({"error": "No query provided"}), 400

    # Step 1: Get data from Neo4j
    data_results = classify_intent(user_query, city)

    # Step 2: Format data for Claude
    data_text = f"CITY: {city}\n\n"
    data_text += "DATA LINEAGE: Every row below was queried directly from the Neo4j knowledge graph built from Liases Foras proprietary Excel data. Row counts and query names are provided for traceability.\n\n"
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
    data_text += f"TOTAL: {len(queries_used)} queries executed, {total_rows} rows returned from Neo4j.\n"
    data_text += f"QUERIES USED: {', '.join(queries_used)}\n"

    # Step 3: Build messages
    messages = []
    for h in history[-6:]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({
        "role": "user",
        "content": f"VERIFIED DATA FROM NEO4J:\n{data_text}\n\nUSER QUESTION: {user_query}"
    })

    # Step 4: Call Claude
    client = get_claude()

    if stream:
        def generate():
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                system=SYSTEM_PROMPT,
                messages=messages,
            ) as s:
                for text in s.text_stream:
                    yield f"data: {json.dumps({'type': 'text', 'text': text})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

        return Response(
            stream_with_context(generate()),
            mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )
    else:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=messages,
        )
        return jsonify({
            "response": response.content[0].text,
            "data_queries": [r["query"] for r in data_results],
            "total_rows": sum(r["row_count"] for r in data_results),
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
    """Health check."""
    try:
        d = get_driver()
        with d.session(database='c26f3089') as session:
            result = session.run("MATCH (n) RETURN count(n) AS nodes")
            count = result.single()["nodes"]
        return jsonify({"status": "ok", "nodes": count})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    print(f"MR&I API Server starting on port {port}")
    print(f"Neo4j: {NEO4J_URI}")
    app.run(host='0.0.0.0', port=port, debug=debug)
