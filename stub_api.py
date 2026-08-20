#!/usr/bin/env python3
"""
stub_api.py — a fake mri-api that reproduces the exact failure and the exact fix,
so the frontend can be tested without Neo4j or an Anthropic key.

It serves BOTH transports:
  /api/query                 SSE, but severed at GATEWAY_SECS mid-sentence -
                             the production bug, reproduced.
  /api/query/async + result  submit/poll, no gateway involved.

and the patched page itself, so a browser can drive the whole thing.

    python stub_api.py --html mri_v3.html --port 8899
"""
import argparse
import json
import os
import threading
import time
import uuid

from flask import Flask, Response, jsonify, request, send_file, stream_with_context
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

GATEWAY_SECS = float(os.environ.get("STUB_GATEWAY_SECS", "6"))
GEN_SECS = float(os.environ.get("STUB_GEN_SECS", "18"))
HTML_PATH = None

# A report shaped like the real one: verdict, tables, and a closing footer that
# only exists if generation actually finished.
SECTIONS = [
    "# HINJEWADI FEASIBILITY REPORT\n\n",
    "## STEP 0 - EXECUTIVE VERDICT\n\n> VERDICT: NO-GO at Rs.25 Cr land cost\n\n",
    "| Parameter | Value |\n|---|---|\n| Site Score | 58/80 |\n| Project IRR | -29.3% |\n\n",
    "## STEP 1 - SITE\n\nPlot resolved to Hinjawadi, Mulshi.\n\n",
    "## STEP 2 - MARKET\n\n| Metric | Value |\n|---|---|\n| Supply | 6,843 |\n| Unsold | 6,031 |\n\n",
    "## STEP 3 - PRICING\n\nAbsorption price Rs.8,455 PSF.\n\n",
    "## STEP 4 - COMPETITION\n\nSeventeen projects within 3 km.\n\n",
    "## STEP 5 - PRODUCT MIX\n\n| Config | Units |\n|---|---|\n| 1 BHK | 55 |\n| 2 BHK | 136 |\n\n",
    "## STEP 6 - PHASED LAUNCH\n\n| Phase | Units | PSF |\n|---|---|---|\n| Launch | 96 | 8,000 |\n\n",
    "## STEP 7 - CASH FLOW\n\n| Year | Net |\n|---|---|\n| Y1 | -44 |\n| Y2 | +18 |\n\n",
    "## STEP 8 - SENSITIVITY\n\nBreak-even at Rs.10,200 PSF.\n\n",
    "## STEP 9 - RISKS\n\nAbsorption risk is the binding constraint.\n\n",
    "## STEP 10 - RECOMMENDATION\n\nRenegotiate land to Rs.14 Cr or walk.\n\n",
    "---\nData Source: Liases Foras RESSEX | Basis: Q1 26-27 | Confidence: High\n",
]

_JOBS, _LOCK = {}, threading.Lock()


def _emit():
    """Yield the report in small pieces over GEN_SECS."""
    per = GEN_SECS / len(SECTIONS)
    for s in SECTIONS:
        time.sleep(per)
        yield s


@app.route("/api/query", methods=["POST"])
def sync_query():
    """The broken transport: the gateway severs this mid-report."""
    def gen():
        t0 = time.time()
        for s in _emit():
            if time.time() - t0 > GATEWAY_SECS:
                return                      # severed. no 'done', no marker.
            yield f"data: {json.dumps({'type': 'text', 'text': s})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'city': 'Hinjewadi', 'total_rows': 412})}\n\n"
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no"})


def _worker(job_id):
    try:
        for s in _emit():
            with _LOCK:
                j = _JOBS.get(job_id)
                if j is None:
                    return
                j["text"] += s
        with _LOCK:
            j = _JOBS.get(job_id)
            if j:
                j["status"] = "done"
                j["meta"] = {"type": "done", "city": "Hinjewadi", "total_rows": 412,
                             "data_through": "Q1 26-27"}
    except Exception as e:
        with _LOCK:
            if job_id in _JOBS:
                _JOBS[job_id].update(status="error", error=str(e))


@app.route("/api/query/async", methods=["POST"])
def start():
    body = request.json or {}
    if not body.get("query"):
        return jsonify({"error": "No query provided"}), 400
    jid = uuid.uuid4().hex
    with _LOCK:
        _JOBS[jid] = {"status": "running", "text": "", "meta": None,
                      "error": None, "partial": False, "created": time.time()}
    threading.Thread(target=_worker, args=(jid,), daemon=True).start()
    return jsonify({"job_id": jid}), 202


@app.route("/api/query/result/<jid>", methods=["GET"])
def poll(jid):
    try:
        cur = max(0, int(request.args.get("cursor", 0)))
    except (TypeError, ValueError):
        cur = 0
    with _LOCK:
        j = _JOBS.get(jid)
        if j is None:
            return jsonify({"error": "Unknown or expired job id"}), 404
        return jsonify({"status": j["status"], "delta": j["text"][cur:],
                        "cursor": len(j["text"]), "done_meta": j["meta"],
                        "error": j["error"], "partial": j["partial"],
                        "elapsed_secs": round(time.time() - j["created"], 1)})


@app.route("/")
def page():
    return send_file(HTML_PATH)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True)
    ap.add_argument("--port", type=int, default=8899)
    a = ap.parse_args()
    HTML_PATH = os.path.abspath(a.html)
    app.run(port=a.port, threaded=True)
