#!/usr/bin/env python3
"""
patch_async.py — two fixes for the report that keeps ending mid-sentence.

FIX 1: THE HOLE IN THE PREVIOUS DEADLINE PATCH
----------------------------------------------
The deadline I added was only checked BETWEEN continuations:

    while True:
        with client.messages.stream(**_params) as s:
            for event in s:            <-- no clock here
                ...
        _left = _budget_left(_t0)      <-- only here

The dominant failure is a SINGLE call that runs ~300s. It never reaches the
check, so the budget never fires, the gateway severs the socket at ~120s, and
no marker is ever appended. The patch therefore fixed nothing for the case it
was written for. This adds the check inside the event loop, so generation
always stops on its own terms, inside the gateway window, with an honest
marker instead of a severed sentence.

FIX 2: ASYNC GENERATION — the durable fix
-----------------------------------------
A full report needs ~300s at the measured ~29 tok/s. The platform allows ~120s.
No amount of prompt tightening makes that inequality reliably true; it only
makes it true on short queries. So take generation off the request path:

    POST /api/query/async          -> {"job_id": "..."} immediately (202)
    GET  /api/query/result/<id>?cursor=N
                                   -> {"status", "delta", "cursor", "done_meta"}

The worker thread re-enters the EXISTING handle_query() through a synthetic
request context and consumes its SSE generator internally. Nothing about the
query pipeline is duplicated — same Neo4j calls, same pin resolution, same
feasibility engine, same prompt. Only the transport changes. Each poll is a
sub-second request, so no single HTTP call is ever near the gateway limit, and
the generation budget rises to 600s.

The frontend keeps its progressive display: it appends each poll's delta the
same way it appended stream chunks.

    python patch_async.py /path/to/mri-api/app.py

DEPLOYMENT NOTE: jobs live in the worker's memory. Run ONE worker:
    gunicorn app:app --workers 1 --threads 8 --timeout 900
With several workers a poll can land on a process that never saw the job.
"""
import shutil
import sys
from pathlib import Path

# ── 1. sentinel + rate-limit bypass for the internal re-entry ───────────────
OLD_RATE = '''def check_rate_limit(ip):
    """Return True if allowed, False if rate limited."""
    now = time.time()'''

NEW_RATE = '''# The async worker re-enters handle_query() through a synthetic request whose
# remote_addr is this sentinel. The real caller was already rate-limited when it
# submitted the job; counting it twice would throttle every second report.
INTERNAL_IP = "__mri_internal__"


def check_rate_limit(ip):
    """Return True if allowed, False if rate limited."""
    if ip == INTERNAL_IP:
        return True
    now = time.time()'''

# ── 2. per-thread budget override ──────────────────────────────────────────
OLD_BUDGET = '''def _budget_left(t0):
    return GEN_BUDGET_SECS - (_time.time() - t0)'''

NEW_BUDGET = '''import threading as _threading

# A request served on the HTTP path has ~120s before the gateway cuts it. A
# request served by the async worker has no gateway in front of it at all, so
# it gets a much larger budget. Same code, different ceiling, chosen per thread.
_BUDGET = _threading.local()


def _budget_secs():
    return getattr(_BUDGET, "value", None) or GEN_BUDGET_SECS


def _budget_left(t0):
    return _budget_secs() - (_time.time() - t0)'''

# ── 3. THE REAL FIX: check the clock inside the event loop ─────────────────
OLD_LOOP = '''                while True:
                    _chunk = ""
                    with client.messages.stream(**_params) as s:
                        # Iterate raw events so we can capture both text chunks
                        # AND server_tool_use (web_search) invocations for audit.
                        for event in s:
                            et = getattr(event, "type", None)
                            if et == "content_block_delta":
                                delta = getattr(event, "delta", None)
                                if delta is not None and getattr(delta, "type", "") == "text_delta":
                                    txt = getattr(delta, "text", "")
                                    if txt:
                                        _chunk += txt
                                        yield f"data: {json.dumps({'type': 'text', 'text': txt})}\\n\\n"'''

NEW_LOOP = '''                _deadline_hit = False
                while True:
                    _chunk = ""
                    with client.messages.stream(**_params) as s:
                        # Iterate raw events so we can capture both text chunks
                        # AND server_tool_use (web_search) invocations for audit.
                        for event in s:
                            # THE CLOCK BELONGS HERE. Checking it only between
                            # continuations meant one long call could run past
                            # the gateway limit untouched - which is exactly
                            # what was happening: ~300s of generation against a
                            # ~120s cap, severed mid-word, no marker, every time.
                            if _budget_left(_t0) <= 0:
                                _deadline_hit = True
                                break
                            et = getattr(event, "type", None)
                            if et == "content_block_delta":
                                delta = getattr(event, "delta", None)
                                if delta is not None and getattr(delta, "type", "") == "text_delta":
                                    txt = getattr(delta, "text", "")
                                    if txt:
                                        _chunk += txt
                                        yield f"data: {json.dumps({'type': 'text', 'text': txt})}\\n\\n"'''

OLD_FINAL = '''                        _final = s.get_final_message()
                    _full += _chunk
                    _stop = getattr(_final, "stop_reason", None)'''

NEW_FINAL = '''                        if not _deadline_hit:
                            _final = s.get_final_message()
                    _full += _chunk
                    if _deadline_hit:
                        # Stop cleanly on our own terms rather than waiting to be
                        # cut. _truncated stays True so a marker is appended and
                        # the 'done' event still reaches the client.
                        print(f"  [DEADLINE] {_budget_secs():.0f}s budget spent mid-generation "
                              f"after {len(_full):,} chars - closing cleanly")
                        break
                    _stop = getattr(_final, "stop_reason", None)'''

# ── 4. the async job layer ─────────────────────────────────────────────────
ASYNC_BLOCK = '''
# ── Asynchronous generation ────────────────────────────────────────────────
# Reports need ~300s; the platform gateway allows ~120s. Rather than keep
# shrinking the report to fit a limit it will never reliably fit, generation
# moves off the request path: submit a job, poll for the result. Each poll is a
# sub-second request, so no single call goes anywhere near the gateway limit.
#
# The worker does NOT reimplement the pipeline. It re-enters handle_query()
# through a synthetic request context and consumes the SSE generator that
# function already returns, so Neo4j retrieval, pin resolution, the feasibility
# engine and the prompt are byte-for-byte the ones the sync path uses.
import uuid as _uuid

_JOBS = {}
_JOBS_LOCK = _threading.Lock()
JOB_TTL_SECS = float(os.environ.get("MRI_JOB_TTL_SECS", "1800"))
ASYNC_BUDGET_SECS = float(os.environ.get("MRI_ASYNC_BUDGET_SECS", "600"))


def _job_gc_locked():
    now = _time.time()
    for k in [k for k, v in _JOBS.items() if now - v["updated"] > JOB_TTL_SECS]:
        _JOBS.pop(k, None)


def _job_set(job_id, **fields):
    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if j is None:
            return False
        j.update(fields)
        j["updated"] = _time.time()
        return True


def _run_job(job_id, payload):
    _BUDGET.value = ASYNC_BUDGET_SECS
    t0 = _time.time()
    try:
        payload = dict(payload)
        payload["stream"] = True
        with app.test_request_context("/api/query", json=payload,
                                      environ_base={"REMOTE_ADDR": INTERNAL_IP}):
            rv = handle_query()
            if isinstance(rv, tuple):          # (jsonify(...), status) error path
                try:
                    msg = (rv[0].get_json() or {}).get("error", f"HTTP {rv[1]}")
                except Exception:
                    msg = f"HTTP {rv[1]}"
                _job_set(job_id, status="error", error=msg)
                return
            buf = ""
            for raw in rv.response:
                buf += raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
                lines = buf.split("\\n")
                buf = lines.pop()
                for line in lines:
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue
                    et = ev.get("type")
                    with _JOBS_LOCK:
                        j = _JOBS.get(job_id)
                        if j is None:
                            return                      # expired or cancelled
                        if et == "text":
                            j["text"] += ev.get("text", "")
                        elif et == "done":
                            j["meta"] = ev
                            j["status"] = "done"
                        elif et == "error":
                            j["error"] = ev.get("text")
                            j["status"] = "error"
                        j["updated"] = _time.time()
        with _JOBS_LOCK:
            j = _JOBS.get(job_id)
            if j and j["status"] == "running":
                # Generator finished without a 'done' event. Not fatal - the text
                # collected so far is real - but say so rather than hanging.
                j["status"] = "done"
                j["partial"] = True
                j["updated"] = _time.time()
        print(f"  [ASYNC] job {job_id[:8]} finished in {_time.time() - t0:.0f}s, "
              f"{len(_JOBS.get(job_id, {}).get('text', '')):,} chars")
    except Exception as e:
        print(f"  ✗ [ASYNC] job {job_id[:8]} failed after {_time.time() - t0:.0f}s: {e}")
        _job_set(job_id, status="error", error=f"{type(e).__name__}: {e}")


@app.route('/api/query/async', methods=['POST'])
def start_query_job():
    """Submit a query. Returns a job id immediately; poll for the result."""
    client_ip = request.remote_addr or 'unknown'
    if not check_rate_limit(client_ip):
        return jsonify({"error": "Rate limit exceeded. Please wait a moment."}), 429

    body = request.json or {}
    if not body.get('query'):
        return jsonify({"error": "No query provided"}), 400

    job_id = _uuid.uuid4().hex
    with _JOBS_LOCK:
        _job_gc_locked()
        _JOBS[job_id] = {"status": "running", "text": "", "meta": None,
                         "error": None, "partial": False,
                         "created": _time.time(), "updated": _time.time()}
    _threading.Thread(target=_run_job, args=(job_id, body), daemon=True).start()
    print(f"  [ASYNC] job {job_id[:8]} started: {body.get('query', '')[:70]!r}")
    return jsonify({"job_id": job_id,
                    "poll": f"/api/query/result/{job_id}",
                    "budget_secs": ASYNC_BUDGET_SECS}), 202


@app.route('/api/query/result/<job_id>', methods=['GET'])
def poll_query_job(job_id):
    """Return everything generated since ?cursor=N. Cheap; poll about once a second."""
    try:
        cursor = max(0, int(request.args.get("cursor", 0)))
    except (TypeError, ValueError):
        cursor = 0

    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if j is None:
            return jsonify({
                "error": "Unknown or expired job id.",
                "hint": ("If the server runs more than one gunicorn worker, the poll "
                         "can land on a process that never saw this job. Start it with "
                         "--workers 1 --threads 8."),
            }), 404
        text, status = j["text"], j["status"]
        meta, err, partial = j["meta"], j["error"], j["partial"]
        elapsed = _time.time() - j["created"]

    return jsonify({
        "status": status,                      # running | done | error
        "delta": text[cursor:] if cursor <= len(text) else "",
        "cursor": len(text),
        "elapsed_secs": round(elapsed, 1),
        "partial": partial,
        "done_meta": meta,
        "error": err,
    })


@app.route('/api/query/cancel/<job_id>', methods=['POST'])
def cancel_query_job(job_id):
    """Drop a job. The worker notices the entry is gone and stops."""
    with _JOBS_LOCK:
        existed = _JOBS.pop(job_id, None) is not None
    return jsonify({"cancelled": existed})

'''


def main(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    orig = src
    applied, missing = [], []

    def sub(old, new, label, required=True):
        nonlocal src
        if new.split("\n")[0] in src and label.startswith("async"):
            missing.append(f"{label} (already present)")
            return
        if old in src:
            src = src.replace(old, new, 1)
            applied.append(label)
        else:
            (missing if required else applied).append(label + " (not found)")

    if "INTERNAL_IP" in src:
        missing.append("rate-limit bypass (already present)")
    else:
        sub(OLD_RATE, NEW_RATE, "rate-limit bypass for internal re-entry")

    if "_budget_secs" in src:
        missing.append("per-thread budget (already present)")
    else:
        sub(OLD_BUDGET, NEW_BUDGET, "per-thread generation budget")

    if "_deadline_hit" in src:
        missing.append("mid-stream deadline (already present)")
    else:
        sub(OLD_LOOP, NEW_LOOP, "MID-STREAM DEADLINE CHECK (the real fix)")
        sub(OLD_FINAL, NEW_FINAL, "clean close when the deadline fires")

    anchor = "@app.route('/api/raw', methods=['POST'])"
    if "/api/query/async" in src:
        missing.append("async endpoints (already present)")
    elif anchor in src:
        src = src.replace(anchor, ASYNC_BLOCK.lstrip("\n") + "\n" + anchor, 1)
        applied.append("async job endpoints (submit / poll / cancel)")
    else:
        missing.append("async endpoints (anchor not found)")

    for a in applied:
        print(f"  applied: {a}")
    for m in missing:
        print(f"  skipped: {m}")

    if src == orig:
        print("\nnothing changed")
        return 1

    backup = path.with_suffix(path.suffix + ".pre_async")
    shutil.copy2(path, backup)
    path.write_text(src, encoding="utf-8")
    print(f"\nwritten {path}   (backup: {backup.name})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(Path(sys.argv[1])))
