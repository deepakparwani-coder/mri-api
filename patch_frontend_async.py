#!/usr/bin/env python3
"""
patch_frontend_async.py — make the UI submit-and-poll instead of holding one
long HTTP request open.

The report was being cut off because a single request cannot outlive the
platform gateway (~120s) while a full feasibility report needs ~300s. The
backend now offers submit/poll endpoints; this teaches the frontend to use
them.

Behaviour:
  - Tries POST /api/query/async first.
  - If that endpoint is not there (404/405 - i.e. the backend has not been
    redeployed yet), it silently falls back to the existing SSE path, so this
    file is safe to publish before the API is updated.
  - While polling it appends each delta exactly as it appended stream chunks,
    so the report still appears progressively. Same renderer, same chart
    handling, same trust footer, same validation badge.
  - Shows an elapsed-seconds hint, because a 4-minute report with a silent UI
    reads as a hang.

    python patch_frontend_async.py mri_v3.html [-o out.html]
"""
import argparse
import shutil
import sys
from pathlib import Path

OPEN_ANCHOR = '    fetch(API_BASE + "/api/query", {'
OPEN_NEW = '''    // The synchronous streaming path, kept intact as a fallback for servers
    // that do not yet expose /api/query/async.
    function runStreamingQuery() {
    fetch(API_BASE + "/api/query", {'''

TAIL_ANCHOR = '''    });
    return; // Skip the direct Anthropic path below
  }'''

TAIL_NEW = '''    });
    }

    // ═══ ASYNC PATH — submit a job, poll for the result ═══
    // A single HTTP request cannot outlive the platform gateway (~120s), and a
    // full feasibility report takes ~300s to generate. Every poll below is a
    // sub-second request, so the gateway is never the constraint.
    function runAsyncQuery() {
      var fullText = "", doneMeta = null, msgEl = null, bubble = null;
      var firstToken = true, cursor = 0, jobId = null, failures = 0;
      var t0 = Date.now();
      var MAX_WAIT_MS = 15 * 60 * 1000;

      function paint(isFinal) {
        if (!bubble) return;
        if (isFinal) {
          bubble.innerHTML = md(fullText) + buildTrustFooter(doneMeta, fullText);
          var warnings = validateResponse(fullText, _pendingCharts.slice());
          bubble.innerHTML += validationBadge(warnings);
          rcharts(bubble);
        } else {
          var display = fullText;
          var oc = display.lastIndexOf("<lfchart");
          var cc2 = display.lastIndexOf("</lfchart>");
          if (oc > cc2) display = display.substring(0, oc) + "\\n\\n_Generating chart..._";
          display = maskIncompleteTable(display);
          bubble.innerHTML = md(display);
          scr();
        }
      }

      function finish() {
        if (fullText && bubble) paint(true);
        handleDone(fullText || "No response.");
      }

      function poll() {
        if (Date.now() - t0 > MAX_WAIT_MS) {
          if (fullText && bubble) { finish(); } else { handleError("The report is taking unusually long. Please retry."); }
          return;
        }
        fetch(API_BASE + "/api/query/result/" + jobId + "?cursor=" + cursor)
          .then(function(r) {
            if (r.status === 404) throw new Error("The report job expired on the server. Please run the query again.");
            return r.json();
          })
          .then(function(p) {
            failures = 0;
            if (p.delta) {
              if (firstToken) {
                rtyp();
                msgEl = ab("b", "", true);
                bubble = msgEl.querySelector(".bbl");
                firstToken = false;
              }
              fullText += p.delta;
              paint(false);
            }
            if (typeof p.cursor === "number") cursor = p.cursor;

            if (p.status === "error") {
              if (fullText && bubble) { finish(); }
              else { handleError(p.error || "Generation failed on the server."); }
              return;
            }
            if (p.status === "done") {
              doneMeta = p.done_meta;
              finish();
              return;
            }
            var secs = Math.round((Date.now() - t0) / 1000);
            hint(firstToken ? ("Reading the graph and computing feasibility... " + secs + "s")
                            : ("Writing report... " + secs + "s"));
            setTimeout(poll, 1000);
          })
          .catch(function(err) {
            // A dropped poll is not a dropped report - the server keeps
            // generating. Retry a few times before giving up on it.
            if (++failures > 4) {
              if (fullText && bubble) { finish(); }
              else { handleError("Lost contact with the server: " + (err.message || "connection error")); }
              return;
            }
            setTimeout(poll, 2000);
          });
      }

      fetch(API_BASE + "/api/query/async", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          query: q || "Analyse the attached data",
          city: CC,
          history: hist.slice(-8)
        })
      }).then(function(r) {
        // Backend not redeployed yet - use the old path rather than failing.
        if (r.status === 404 || r.status === 405) { runStreamingQuery(); return null; }
        if (!r.ok) {
          return r.json().then(function(e) { throw new Error(e.error || "API server error: HTTP " + r.status); });
        }
        return r.json();
      }).then(function(j) {
        if (!j) return;
        jobId = j.job_id;
        hint("Reading the graph and computing feasibility...");
        poll();
      }).catch(function(e) {
        rtyp();
        var msg = e.message || "Cannot connect to API server";
        ab("b", '<span style="color:#DC2626;font-weight:600">' + esc(msg) + '</span><br><span style="color:#8C8578;font-size:12px">API server not reachable. Please retry in a moment, or contact support if this persists.<br><button onclick="USE_BACKEND=false;retryLast()" style="margin-top:8px;background:linear-gradient(135deg,#C9A84C,#B8963E);color:#fff;border:none;border-radius:6px;padding:8px 18px;font-family:inherit;font-size:12px;font-weight:600;cursor:pointer">Switch to Direct Mode</button></span>', true);
        busy=false;sbtn.disabled=false;
      });
    }

    if (window.USE_ASYNC === false) { runStreamingQuery(); } else { runAsyncQuery(); }
    return; // Skip the direct Anthropic path below
  }'''


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("html")
    ap.add_argument("-o", "--out")
    a = ap.parse_args()

    path = Path(a.html)
    src = path.read_text(encoding="utf-8")

    if "runAsyncQuery" in src:
        print("  ! already patched")
        return 1
    for label, anchor in (("open", OPEN_ANCHOR), ("tail", TAIL_ANCHOR)):
        n = src.count(anchor)
        if n != 1:
            print(f"  ! {label} anchor found {n} times, expected 1")
            return 1

    src = src.replace(OPEN_ANCHOR, OPEN_NEW, 1)
    src = src.replace(TAIL_ANCHOR, TAIL_NEW, 1)
    print("  wrapped the existing streaming path as runStreamingQuery()")
    print("  added runAsyncQuery() with polling, progressive paint and fallback")

    out = Path(a.out) if a.out else path
    if out == path:
        shutil.copy2(path, path.with_suffix(path.suffix + ".pre_async"))
    out.write_text(src, encoding="utf-8")
    print(f"\nwritten {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
