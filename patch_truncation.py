#!/usr/bin/env python3
"""
patch_truncation.py — stop feasibility reports being silently cut off.

THE BUG
-------
`max_tokens` is 8000 for feasibility queries. A full land-feasibility report
(11 sections, several tables, sensitivity matrix, cash flow, IRR/NPV) exceeds
that, so generation stops mid-sentence. Nothing in app.py inspects
`stop_reason`, so a truncated report is indistinguishable from a complete one —
it is returned, rendered and exported to PDF as if finished.

In the 19 Aug Hinjewadi report this cut the "Top 10 Active Competing Projects"
table off after 3 rows, mid-row. The giveaway is not the table though: the
mandatory "Data Source / Data Period / Confidence / Basis" footer that the
system prompt requires at the end of EVERY response was absent. Generation
never reached the end.

THE FIX (three parts)
---------------------
1. Raise the ceiling. 8000 -> 16000 for feasibility, 4000 -> 8000 otherwise,
   both overridable by env var.
2. Auto-continue. On stop_reason == "max_tokens", re-request with the partial
   answer as an assistant turn plus a continue instruction, and concatenate.
   Up to MAX_CONTINUATIONS rounds. Applied to BOTH the streaming and
   non-streaming paths.
3. Fail loudly. If it is still truncated after those rounds, append a visible
   marker so an incomplete report can never be mistaken for a finished one.

Usage:
    python patch_truncation.py /path/to/mri-api/app.py
"""
import re
import shutil
import sys
from pathlib import Path


def patch(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    orig = src

    # ── 1. token ceilings ───────────────────────────────────────────────────
    old = '    token_limit = 8000 if is_feasibility else 4000'
    new = ('    # A full feasibility report runs well past 8000 output tokens; at that\n'
           '    # ceiling it stopped mid-table with no citation footer. Overridable.\n'
           '    token_limit = int(os.environ.get("MRI_MAX_TOKENS_FEASIBILITY", "16000")) \\\n'
           '        if is_feasibility else int(os.environ.get("MRI_MAX_TOKENS", "8000"))')
    if old not in src:
        print("  ! token_limit line not found — already patched?")
    else:
        src = src.replace(old, new, 1)
        print("  fixed: token ceilings 8000/4000 -> 16000/8000 (env-overridable)")

    # ── 2. continuation helper, inserted before the handler ────────────────
    helper = '''

# ── Truncation handling ─────────────────────────────────────────────────────
MAX_CONTINUATIONS = int(os.environ.get("MRI_MAX_CONTINUATIONS", "4"))

CONTINUE_INSTRUCTION = (
    "Your previous message was cut off because it reached the output limit. "
    "Continue from exactly where you stopped. Do not repeat any content already "
    "written, do not re-introduce the report, and do not summarise what came "
    "before. If you were part-way through a table row, finish that row first. "
    "End with the mandatory Data Source / Data Period / City / Confidence / "
    "Basis footer."
)

TRUNCATION_MARKER = (
    "\\n\\n---\\n\\n**INCOMPLETE REPORT — OUTPUT LIMIT REACHED**\\n\\n"
    "This response was cut off after several continuation attempts and is "
    "missing content, including its source-citation footer. Do not treat the "
    "figures above as a complete analysis. Re-run with a narrower question, or "
    "raise MRI_MAX_TOKENS_FEASIBILITY.\\n"
)


def _continuation_params(api_params, text_so_far):
    """Build the follow-up request that resumes a truncated answer."""
    p = dict(api_params)
    # the API rejects an assistant turn with trailing whitespace
    p["messages"] = list(api_params["messages"]) + [
        {"role": "assistant", "content": text_so_far.rstrip()},
        {"role": "user", "content": CONTINUE_INSTRUCTION},
    ]
    return p

'''
    anchor = "@app.route('/api/raw', methods=['POST'])"
    if "MAX_CONTINUATIONS" in src:
        print("  ! continuation helper already present")
    elif anchor not in src:
        print("  ! could not find an anchor for the helper")
        return 1
    else:
        src = src.replace(anchor, helper.lstrip("\n") + "\n" + anchor, 1)
        print("  added: continuation helper + truncation marker")

    # ── 3. streaming path ───────────────────────────────────────────────────
    old_stream = """        def generate():
            try:
                with client.messages.stream(**api_params) as s:"""
    new_stream = """        def generate():
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
                    with client.messages.stream(**_params) as s:"""
    if old_stream not in src:
        print("  ! streaming block not found — already patched?")
    else:
        src = src.replace(old_stream, new_stream, 1)

        # reindent the event loop body by 4 spaces (it moved inside `while`)
        start = src.index(new_stream) + len(new_stream)
        end = src.index("                done_meta = {", start)
        block = src[start:end]
        # accumulate streamed text (do this BEFORE reindenting, so the added
        # line picks up the same shift as everything around it)
        _y = "                                    yield f\"data: {json.dumps({'type': 'text', 'text': txt})}\\n\\n\""
        block = block.replace(_y, "                                    _chunk += txt\n" + _y, 1)
        block = "\n".join(("    " + ln) if ln.strip() else ln for ln in block.split("\n"))
        tail = """                        _final = s.get_final_message()
                    _full += _chunk
                    _stop = getattr(_final, "stop_reason", None)
                    if _stop != "max_tokens":
                        _truncated = False
                        break
                    _round += 1
                    print(f"  [TRUNCATION] stop_reason=max_tokens, continuation {_round}/{MAX_CONTINUATIONS}")
                    if _round >= MAX_CONTINUATIONS:
                        break
                    _params = _continuation_params(api_params, _full)

                if _truncated:
                    print("  [TRUNCATION] still incomplete after continuations - marking")
                    yield f"data: {json.dumps({'type': 'text', 'text': TRUNCATION_MARKER})}\\n\\n"

"""
        src = src[:start] + block + tail + src[end:]
        print("  fixed: streaming path now continues on max_tokens")

    # ── 4. non-streaming path ───────────────────────────────────────────────
    old_ns = "        response = client.messages.create(**api_params)"
    new_ns = """        # Same continuation loop as the streaming path.
        _params = api_params
        _parts = []
        _round = 0
        _truncated = True
        while True:
            response = client.messages.create(**_params)
            _parts.append("".join(b.text for b in response.content if hasattr(b, "text")))
            if getattr(response, "stop_reason", None) != "max_tokens":
                _truncated = False
                break
            _round += 1
            print(f"  [TRUNCATION] stop_reason=max_tokens, continuation {_round}/{MAX_CONTINUATIONS}")
            if _round >= MAX_CONTINUATIONS:
                break
            _params = _continuation_params(api_params, "".join(_parts))
        _continued_text = "".join(_parts) + (TRUNCATION_MARKER if _truncated else "")"""
    if old_ns not in src:
        print("  ! non-streaming create() not found — already patched?")
    else:
        src = src.replace(old_ns, new_ns, 1)
        # use the concatenated text rather than only the last response
        src = src.replace(
            """        response_text = ""
        web_searches_made = []
        for block in response.content:
            if hasattr(block, "text"):
                response_text += block.text""",
            """        response_text = _continued_text
        web_searches_made = []
        for block in response.content:""", 1)
        src = src.replace(
            '            "response": response_text,',
            '            "response": response_text,\n            "truncated": _truncated,', 1)
        print("  fixed: non-streaming path now continues on max_tokens")

    if src == orig:
        print("nothing changed")
        return 1

    backup = path.with_suffix(path.suffix + ".pre_truncation_fix")
    shutil.copy2(path, backup)
    path.write_text(src, encoding="utf-8")
    print(f"\nwritten {path}   (backup: {backup.name})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(patch(Path(sys.argv[1])))
