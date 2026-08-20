#!/usr/bin/env python3
"""
patch_deadline.py — make feasibility reports finish inside the platform timeout.

THE FINDING
-----------
Both a streaming and a non-streaming call to /api/query died at exactly 121.3
seconds. Non-streaming returned HTTP 500 with no body; streaming delivered
14,080 characters and was then severed mid-sentence, never reaching its 'done'
event. Identical wall-clock, two different code paths: that is a gateway
request timeout (Render caps at ~120s), not the model stopping.

This also explains why no INCOMPLETE marker ever appeared. The server was still
generating when the socket was cut, so it never got to append one.

And it means my earlier continuation loop was actively harmful here - every
retry adds another round trip to a budget that has already been spent.

Measured throughput was ~29 output tokens/sec. A full 11-step feasibility report
runs ~9,000 tokens, so it needs roughly 300 seconds. There are 120. The report
must get smaller or faster; there is no third option inside one HTTP request.

WHAT THIS PATCH DOES
--------------------
1. DEADLINE AWARENESS. A wall-clock budget (default 100s, under the 120s cap).
   Continuations only start if there is time for them. When the budget is spent
   the stream closes cleanly with an honest marker instead of being severed
   mid-word.

2. CUTS THE BIGGEST TIME SINK. Feasibility queries allowed up to 5 server-side
   web searches. Each costs several seconds of the budget. Reduced to 2, and
   the model is told to spend them on zoning and infrastructure only.

3. TIGHTENS THE REPORT TO FIT. The framework asked for 11 steps of narrative
   that cannot be generated in 120s. The system prompt now instructs a compact
   report - verdict first, tables over prose, no restating of computed figures -
   with the detail available on a follow-up question.

The durable fix is asynchronous generation (submit -> poll), which removes the
HTTP timeout from the picture entirely. See README for that design; it needs a
frontend change so it is not included here.

    python patch_deadline.py /path/to/mri-api/app.py
"""
import shutil
import sys
from pathlib import Path

HELPER = '''

# ── Generation deadline ─────────────────────────────────────────────────────
# Render (and most PaaS gateways) terminate a request at ~120s. Measured:
# both streaming and non-streaming calls died at exactly 121.3s. Generation
# must therefore finish inside a budget, and finish CLEANLY when it cannot.
import time as _time

GEN_BUDGET_SECS = float(os.environ.get("MRI_GEN_BUDGET_SECS", "100"))

DEADLINE_MARKER = (
    "\\n\\n---\\n\\n**REPORT TRUNCATED - TIME LIMIT**\\n\\n"
    "Generation reached the server's time budget before the report finished. "
    "The verdict and economics above are complete and computed; the remaining "
    "sections were not written. Ask for any specific section as a follow-up "
    "question and it will be produced on its own.\\n"
)


def _budget_left(t0):
    return GEN_BUDGET_SECS - (_time.time() - t0)

'''

INJECT_STREAM = '''        def generate():
            try:
                _t0 = _time.time()
                # Loop so a max_tokens/pause_turn stop resumes instead of ending
                # the report mid-sentence - but only while there is time left.
                _params = api_params
                _full = ""
                _round = 0
                _truncated = True
                while True:
                    _chunk = ""
                    with client.messages.stream(**_params) as s:'''

def main(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    orig = src

    # ── 1. helper ───────────────────────────────────────────────────────────
    anchor = "@app.route('/api/raw', methods=['POST'])"
    if "GEN_BUDGET_SECS" in src:
        print("  ! deadline helper already present")
    elif anchor not in src:
        print("  ! anchor not found")
        return 1
    else:
        src = src.replace(anchor, HELPER.lstrip("\n") + "\n" + anchor, 1)
        print("  added: wall-clock generation budget")

    # ── 2. streaming loop respects the deadline ────────────────────────────
    old = '''        def generate():
            try:
                # Loop so a max_tokens stop resumes instead of ending the report
                # mid-sentence. Text streams to the client as it arrives; the
                # continuation simply keeps the same stream going.
                _params = api_params
                _full = ""
                _round = 0
                _truncated = True
                while True:
                    _chunk = ""
                    with client.messages.stream(**_params) as s:'''
    if old in src:
        src = src.replace(old, INJECT_STREAM, 1)
        print("  added: deadline clock to the streaming loop")
    else:
        print("  ! streaming loop not in the expected shape (patch order?)")

    old_cont = '''                    _round += 1
                    print(f"  [INCOMPLETE] stop_reason={_stop}, continuation {_round}/{MAX_CONTINUATIONS}")
                    if _round >= MAX_CONTINUATIONS:
                        break
                    _params = _continuation_params(api_params, _full, _stop)'''
    new_cont = '''                    _round += 1
                    _left = _budget_left(_t0)
                    print(f"  [INCOMPLETE] stop_reason={_stop}, continuation {_round}/{MAX_CONTINUATIONS}, "
                          f"{_left:.0f}s of budget left")
                    # A continuation costs at least one more round trip. Starting
                    # one with no budget guarantees the gateway severs it mid-word.
                    if _round >= MAX_CONTINUATIONS or _left < 25:
                        if _left < 25:
                            print("  [DEADLINE] not enough time to continue - closing cleanly")
                        break
                    _params = _continuation_params(api_params, _full, _stop)'''
    if old_cont in src:
        src = src.replace(old_cont, new_cont, 1)
        print("  added: continuations only start when the budget allows")

    old_mark = '''                if _truncated:
                    print("  [INCOMPLETE] still unfinished after continuations - marking")
                    yield f"data: {json.dumps({'type': 'text', 'text': TRUNCATION_MARKER})}\\n\\n"'''
    new_mark = '''                if _truncated:
                    _left = _budget_left(_t0)
                    _mark = DEADLINE_MARKER if _left < 25 else TRUNCATION_MARKER
                    print(f"  [INCOMPLETE] unfinished; {_left:.0f}s budget left - marking")
                    yield f"data: {json.dumps({'type': 'text', 'text': _mark})}\\n\\n"'''
    if old_mark in src:
        src = src.replace(old_mark, new_mark, 1)
        print("  added: honest marker distinguishing time-out from token-limit")

    # ── 3. fewer web searches on the critical path ─────────────────────────
    old_web = "        web_uses = 5 if is_feasibility else 3"
    new_web = ('        # Each server-side search costs several seconds of a ~100s budget.\n'
               '        # Measured throughput is ~29 tok/s; 5 searches can consume half the\n'
               '        # time available and the report never reaches its verdict.\n'
               '        web_uses = int(os.environ.get("MRI_WEB_USES_FEASIBILITY", "2")) \\\n'
               '            if is_feasibility else int(os.environ.get("MRI_WEB_USES", "2"))')
    if old_web in src:
        src = src.replace(old_web, new_web, 1)
        print("  changed: web searches 5/3 -> 2/2 (env-overridable)")

    # ── 4. tell the model to fit the budget ────────────────────────────────
    old_fw = "Follow this EXACT framework:"
    new_fw = """Follow this EXACT framework.

**LENGTH DISCIPLINE (the server has a hard time limit).**
The full report must be generated in under 100 seconds or the connection is cut
mid-sentence and the reader gets nothing after that point. Therefore:
- Lead with STEP 0, the verdict. If only one thing survives, it must be that.
- Prefer tables to prose. Do not restate a number in a sentence that already
  appears in a table.
- Where a COMPUTED FEASIBILITY block is supplied, quote its figures directly.
  Never re-derive or re-explain the arithmetic - it is already correct.
- Keep each step to its essentials. Depth is available on request: end with one
  line offering the sections the reader can ask for in detail.
- Do not pad with generic market commentary the reader did not ask for.

Follow this EXACT framework:"""
    if old_fw in src and "LENGTH DISCIPLINE" not in src:
        src = src.replace(old_fw, new_fw, 1)
        print("  added: length discipline so the report fits the budget")

    if src == orig:
        print("nothing changed")
        return 1
    backup = path.with_suffix(path.suffix + ".pre_deadline_fix")
    shutil.copy2(path, backup)
    path.write_text(src, encoding="utf-8")
    print(f"\nwritten {path}   (backup: {backup.name})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(Path(sys.argv[1])))
