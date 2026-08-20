#!/usr/bin/env python3
"""
diagnose_truncation.py — find out WHERE a feasibility report is being cut off.

Reports keep ending mid-sentence at roughly 3,000-6,000 output tokens, with no
citation footer and — importantly — without the INCOMPLETE REPORT marker that
app.py is supposed to append when it gives up. That absence is the clue: if the
server had detected truncation it would have said so. So either the detection
is not running, or the text is being lost AFTER the server produced it.

Those two causes need completely different fixes, and guessing between them has
already cost several rounds. This script separates them by running the SAME
query twice - once streaming, once not - and comparing.

    NON-STREAM complete + STREAM short  -> the SSE connection is being severed
                                           in transport (proxy / ngrok / gateway
                                           idle timeout). Nothing in the model
                                           layer will fix it.
    BOTH short                          -> generation really is stopping; the
                                           continuation loop is not running or
                                           not deployed.
    BOTH complete                       -> the loss is in the browser or the PDF
                                           export, not the server.

Usage:
    python diagnose_truncation.py https://your-host

    # optional: your own query and city
    python diagnose_truncation.py https://your-host --city Hinjewadi \\
        --query "Run a feasibility check for this 5 acre plot..."
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

DEFAULT_QUERY = (
    "Run a feasibility check for this 5 acre plot. cost of acq is 25 cr, "
    "cost of construction is 3000psf. suggest product mix with competing "
    "projects within 3km range. launch phases considering 3 year delivery."
)

# A finished report must contain the citation footer the system prompt mandates.
COMPLETION_MARKERS = ("Data Source:", "Confidence:", "Basis:")


def _post(url, payload, timeout, stream=False):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Accept": "*/*"})
    t0 = time.time()
    chunks, nbytes = [], 0
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status = r.status
            if stream:
                for raw in r:
                    nbytes += len(raw)
                    line = raw.decode("utf-8", "replace").strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue
                    if ev.get("type") == "text":
                        chunks.append(ev.get("text", ""))
                    elif ev.get("type") == "done":
                        chunks.append("\x00DONE\x00")
                    elif ev.get("type") == "error":
                        chunks.append(f"\x00ERROR:{ev.get('text')}\x00")
            else:
                body = r.read().decode("utf-8", "replace")
                nbytes = len(body)
                chunks.append(json.loads(body).get("response", ""))
        return dict(ok=True, status=status, text="".join(chunks),
                    bytes=nbytes, secs=round(time.time() - t0, 1))
    except Exception as e:
        return dict(ok=False, status=None, text="".join(chunks), bytes=nbytes,
                    secs=round(time.time() - t0, 1), error=f"{type(e).__name__}: {e}")


def _describe(label, r):
    text = r["text"].replace("\x00DONE\x00", "")
    saw_done = "\x00DONE\x00" in r["text"]
    err = [c for c in r["text"].split("\x00") if c.startswith("ERROR:")]
    approx_tokens = len(text) // 4
    complete = all(m in text for m in COMPLETION_MARKERS)
    marked = "INCOMPLETE REPORT" in text

    print(f"\n── {label} ─────────────────────────────────────────────")
    print(f"   transport      : {'ok' if r['ok'] else 'FAILED - ' + r.get('error', '')}")
    print(f"   elapsed        : {r['secs']}s")
    print(f"   text received  : {len(text):,} chars  (~{approx_tokens:,} output tokens)")
    if saw_done:
        print("   stream 'done'  : received — the server finished its side")
    elif label.startswith("STREAM"):
        print("   stream 'done'  : NOT received — the stream ended early")
    if err:
        print(f"   server error   : {err[0][6:]}")
    print(f"   citation footer: {'present — report is complete' if complete else 'MISSING — report is incomplete'}")
    print(f"   incomplete flag: {'server marked it truncated' if marked else 'not marked'}")
    if text:
        tail = " ".join(text[-160:].split())
        print(f"   ends with      : ...{tail}")
    return dict(chars=len(text), complete=complete, marked=marked, done=saw_done)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host", help="base URL, e.g. https://abc123.ngrok-free.app")
    ap.add_argument("--city", default="Hinjewadi")
    ap.add_argument("--query", default=DEFAULT_QUERY)
    ap.add_argument("--timeout", type=int, default=600)
    a = ap.parse_args()

    host = a.host.rstrip("/")
    for suffix in ("/app", "/api/query", "/api"):
        if host.endswith(suffix):
            host = host[: -len(suffix)]
            print(f"note: stripped '{suffix}' from the host you gave me")
    url = host + "/api/query"
    print(f"endpoint : {url}")
    print(f"city     : {a.city}")
    print(f"query    : {a.query[:88]}...")
    print("\nRunning the same query twice. This takes a few minutes.")

    ns = _post(url, {"query": a.query, "city": a.city, "history": [], "stream": False},
               a.timeout, stream=False)
    n = _describe("NON-STREAMING", ns)

    st = _post(url, {"query": a.query, "city": a.city, "history": [], "stream": True},
               a.timeout, stream=True)
    s = _describe("STREAMING", st)

    print("\n" + "=" * 66)
    print("VERDICT")
    print("=" * 66)

    # A transport failure is NOT a truncation finding. Earlier this printed
    # "generation is stopping early" after two 404s, which is exactly the kind
    # of confident-but-wrong output this whole exercise is meant to prevent.
    if not ns["ok"] or not st["ok"]:
        print("  The request did not reach the API, so nothing can be concluded")
        print("  about truncation yet.")
        for lbl, r in (("non-streaming", ns), ("streaming", st)):
            if not r["ok"]:
                print(f"    {lbl}: {r.get('error')}")
        print()
        print(f"  Tried: {url}")
        print("  Check the endpoint. Pass the API ROOT, not the /app page:")
        print("     python diagnose_truncation.py https://mrisq-api.onrender.com")
        print("  Confirm it answers at all:")
        print("     curl https://mrisq-api.onrender.com/api/health")
        return 1

    # A gateway timeout has a signature that completeness alone cannot see:
    # both calls ending at the same wall-clock, and the stream never delivering
    # its 'done' event. Check that BEFORE concluding anything about the model.
    same_clock = abs(ns["secs"] - st["secs"]) < 5 and st["secs"] > 45
    if same_clock and not s["done"]:
        print(f"  Both calls ended at ~{st['secs']:.0f}s and the stream never sent 'done'.")
        print("  That is a GATEWAY REQUEST TIMEOUT, not the model stopping.")
        print()
        print("  Two different code paths cannot coincidentally stop at the same")
        print("  wall-clock second. The platform is cutting the request while the")
        print("  server is still generating - which is also why no INCOMPLETE")
        print("  marker arrives: the server never reaches the line that adds it.")
        print()
        print(f"  Throughput was ~{(s['chars']/4)/max(st['secs'],1):.0f} output tokens/sec.")
        print(f"  A full ~9,000-token report needs ~{9000/max((s['chars']/4)/max(st['secs'],1),1):.0f}s;")
        print(f"  the platform allows ~{st['secs']:.0f}s.")
        print()
        print("  Fix: make the report fit the budget (fewer web searches, shorter")
        print("  framework, verdict first), or move generation off the request")
        print("  path entirely - submit a job, poll for the result.")
        return 0

    if n["complete"] and not s["complete"]:
        print("  The non-streaming response is COMPLETE, the streamed one is not.")
        print(f"  Streaming lost {n['chars'] - s['chars']:,} characters.")
        print()
        print("  -> The model and the server are fine. The SSE connection is being")
        print("     severed in transport - almost always a proxy or tunnel idle")
        print("     timeout (ngrok free tier caps long-lived streams; nginx and")
        print("     Cloudflare buffer or drop SSE unless configured for it).")
        print()
        print("  Fix in this order:")
        print("     1. Serve the app over a normal HTTPS host rather than a tunnel,")
        print("        or use a paid tunnel with no stream cap.")
        print("     2. If nginx sits in front: proxy_buffering off; proxy_read_timeout 600s;")
        print("        and keep the X-Accel-Buffering: no header app.py already sends.")
        print("     3. Interim mitigation: set stream:false in the frontend for")
        print("        feasibility queries. Slower to first token, but complete.")
    elif not n["complete"] and not s["complete"]:
        print("  BOTH are incomplete - generation really is stopping early.")
        if not n["marked"]:
            print()
            print("  The server did NOT mark it truncated, so the continuation loop")
            print("  is not running. Check that the patched app.py is deployed and")
            print("  look for these lines in the server log:")
            print("     [INCOMPLETE] stop_reason=... continuation 1/4")
            print("     [DIAG-8] FEASIBILITY_CALC: ...")
            print("  If neither appears, the running app.py is the unpatched one.")
        else:
            print()
            print("  The server DID mark it truncated, so the loop ran and still could")
            print("  not finish. Raise MRI_MAX_TOKENS_FEASIBILITY and MRI_MAX_CONTINUATIONS.")
    elif n["complete"] and s["complete"]:
        print("  BOTH responses are complete. The server is producing a full report.")
        print("  The loss is downstream - in the browser's accumulation of the")
        print("  stream, or in the PDF export. Compare what is on screen against")
        print("  the exported PDF for the same answer.")
    else:
        print("  The streamed response is complete but the non-streamed one is not,")
        print("  which is unusual. Send me both outputs above.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
