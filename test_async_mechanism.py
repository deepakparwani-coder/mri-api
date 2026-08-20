#!/usr/bin/env python3
"""
test_async_mechanism.py — prove the async layer works before it goes anywhere near
production.

Three things are being tested, none of which are obvious enough to take on faith:

  1. A worker thread can re-enter a Flask view through test_request_context and
     consume the generator that stream_with_context() wraps. If this does not
     work, the whole design collapses and no amount of endpoint code saves it.
  2. A generation that runs far longer than any gateway would allow (simulated
     here at 8s with a 2s "gateway") completes in full through polling.
  3. The mid-stream deadline actually fires mid-generation - the specific hole
     in the previous patch - and produces a marked, non-severed result.

The real app.py is not importable here (Neo4j, Anthropic, the KB), so this
mirrors its structure exactly: same test_request_context re-entry, same SSE
parsing, same job dict, same deadline placement.
"""
import json
import threading
import time
import uuid

from flask import Flask, Response, jsonify, request, stream_with_context

app = Flask(__name__)

INTERNAL_IP = "__mri_internal__"
_rate_hits = []


def check_rate_limit(ip):
    if ip == INTERNAL_IP:
        return True
    _rate_hits.append(ip)
    return True


GEN_BUDGET_SECS = 2.0                     # stands in for the gateway limit
_BUDGET = threading.local()


def _budget_secs():
    return getattr(_BUDGET, "value", None) or GEN_BUDGET_SECS


def _budget_left(t0):
    return _budget_secs() - (time.time() - t0)


DEADLINE_MARKER = "\n\n**REPORT TRUNCATED - TIME LIMIT**\n"

# 80 chunks x 0.1s = 8s of "generation" - four times the 2s budget.
CHUNKS = [f"sentence {i} of the report. " for i in range(80)]


@app.route("/api/query", methods=["POST"])
def handle_query():
    if not check_rate_limit(request.remote_addr or "unknown"):
        return jsonify({"error": "rate limited"}), 429
    body = request.json or {}
    if not body.get("query"):
        return jsonify({"error": "No query provided"}), 400

    def generate():
        t0 = time.time()
        deadline_hit = False
        full = ""
        for c in CHUNKS:
            if _budget_left(t0) <= 0:
                deadline_hit = True
                break
            time.sleep(0.1)
            full += c
            yield f"data: {json.dumps({'type': 'text', 'text': c})}\n\n"
        if deadline_hit:
            yield f"data: {json.dumps({'type': 'text', 'text': DEADLINE_MARKER})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'chars': len(full)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


# ── the async layer, copied in shape from patch_async.py ───────────────────
_JOBS, _JOBS_LOCK = {}, threading.Lock()
ASYNC_BUDGET_SECS = 600.0


def _job_set(job_id, **f):
    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if j is None:
            return False
        j.update(f)
        j["updated"] = time.time()
        return True


def _run_job(job_id, payload):
    _BUDGET.value = ASYNC_BUDGET_SECS
    try:
        payload = dict(payload)
        payload["stream"] = True
        with app.test_request_context("/api/query", json=payload,
                                      environ_base={"REMOTE_ADDR": INTERNAL_IP}):
            rv = handle_query()
            if isinstance(rv, tuple):
                _job_set(job_id, status="error",
                         error=(rv[0].get_json() or {}).get("error", "err"))
                return
            buf = ""
            for raw in rv.response:
                buf += raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
                lines = buf.split("\n")
                buf = lines.pop()
                for line in lines:
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    ev = json.loads(line[5:].strip())
                    with _JOBS_LOCK:
                        j = _JOBS.get(job_id)
                        if j is None:
                            return
                        if ev.get("type") == "text":
                            j["text"] += ev.get("text", "")
                        elif ev.get("type") == "done":
                            j["meta"] = ev
                            j["status"] = "done"
                        j["updated"] = time.time()
        with _JOBS_LOCK:
            j = _JOBS.get(job_id)
            if j and j["status"] == "running":
                j["status"] = "done"
                j["partial"] = True
    except Exception as e:
        _job_set(job_id, status="error", error=f"{type(e).__name__}: {e}")


@app.route("/api/query/async", methods=["POST"])
def start_query_job():
    body = request.json or {}
    if not body.get("query"):
        return jsonify({"error": "No query provided"}), 400
    job_id = uuid.uuid4().hex
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "text": "", "meta": None,
                         "error": None, "partial": False,
                         "created": time.time(), "updated": time.time()}
    threading.Thread(target=_run_job, args=(job_id, body), daemon=True).start()
    return jsonify({"job_id": job_id}), 202


@app.route("/api/query/result/<job_id>", methods=["GET"])
def poll_query_job(job_id):
    try:
        cursor = max(0, int(request.args.get("cursor", 0)))
    except (TypeError, ValueError):
        cursor = 0
    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if j is None:
            return jsonify({"error": "Unknown or expired job id"}), 404
        text, status, meta = j["text"], j["status"], j["meta"]
        err, partial = j["error"], j["partial"]
    return jsonify({"status": status, "delta": text[cursor:], "cursor": len(text),
                    "done_meta": meta, "error": err, "partial": partial})


# ── tests ──────────────────────────────────────────────────────────────────
FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


def main():
    c = app.test_client()
    expected = "".join(CHUNKS)

    print("\n1. SYNCHRONOUS PATH — what happens today")
    print("   2s budget, 8s of generation. This is the failure being fixed.")
    t0 = time.time()
    r = c.post("/api/query", json={"query": "feasibility"})
    body = r.get_data(as_text=True)
    got = "".join(json.loads(l[6:])["text"]
                  for l in body.splitlines()
                  if l.startswith("data: ") and json.loads(l[6:]).get("type") == "text")
    secs = time.time() - t0
    check("deadline fires mid-generation", secs < 4.0, f"stopped at {secs:.1f}s, not 8s")
    check("output is marked, not severed", DEADLINE_MARKER.strip() in got)
    check("a 'done' event is still sent", '"type": "done"' in body or '"type":"done"' in body)
    check("partial text is real text", len(got) > 100, f"{len(got)} chars of {len(expected)}")

    print("\n2. ASYNC PATH — the durable fix")
    print("   Same 8s generation, 600s worker budget, polled.")
    _rate_hits.clear()          # only the async submission should register below
    t0 = time.time()
    r = c.post("/api/query/async", json={"query": "feasibility"})
    check("submit returns immediately", time.time() - t0 < 0.5,
          f"{(time.time() - t0) * 1000:.0f}ms")
    check("submit returns 202", r.status_code == 202)
    job = r.get_json()["job_id"]

    text, cursor, polls, longest = "", 0, 0, 0.0
    while polls < 200:
        p0 = time.time()
        pr = c.get(f"/api/query/result/{job}?cursor={cursor}").get_json()
        longest = max(longest, time.time() - p0)
        polls += 1
        text += pr["delta"]
        cursor = pr["cursor"]
        if pr["status"] in ("done", "error"):
            break
        time.sleep(0.15)
    total = time.time() - t0

    check("job completes", pr["status"] == "done", f"status={pr['status']}")
    check("full text recovered", text == expected,
          f"{len(text)} chars, expected {len(expected)}")
    check("no truncation marker", DEADLINE_MARKER.strip() not in text)
    check("ran past the gateway limit", total > GEN_BUDGET_SECS * 2,
          f"{total:.1f}s total vs {GEN_BUDGET_SECS}s gateway")
    check("every poll is fast", longest < 0.5, f"slowest poll {longest * 1000:.0f}ms")
    check("progressive delivery", polls > 3, f"{polls} polls, text arrived in pieces")
    check("done_meta delivered", pr["done_meta"] is not None)
    check("not flagged partial", pr["partial"] is False)

    print("\n3. EDGE CASES")
    check("unknown job id -> 404",
          c.get("/api/query/result/nosuchjob").status_code == 404)
    check("empty query rejected",
          c.post("/api/query/async", json={"query": ""}).status_code == 400)
    check("worker bypassed the rate limiter", _rate_hits == [],
          "internal re-entry was not counted against the caller")

    r2 = c.post("/api/query/async", json={"query": "x"})
    j2 = r2.get_json()["job_id"]
    p = c.get(f"/api/query/result/{j2}?cursor=abc").get_json()
    check("bad cursor does not 500", "status" in p)

    print("\n" + "=" * 62)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
