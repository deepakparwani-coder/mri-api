#!/usr/bin/env python3
"""
patch_llm.py — route app.py's generation through the provider shim.

The client is consolidating on their OpenAI contract. This does NOT hard-swap
the vendor; it moves both behind `llm.py` so the choice is one env var and this
surgery never has to happen again:

    MRI_LLM_PROVIDER = anthropic | openai
    MRI_LLM_MODEL    = model id

Everything that made this week's fixes work is above the shim and untouched:
the wall-clock budget, the continuation loop, the truncation markers, the
web-search audit log, and every validator. The generation loop now consumes
normalised ("text"|"search"|"stop") events instead of Anthropic content blocks.

Stop reasons are normalised, so CONTINUABLE_STOPS becomes ("length", "pause"):
    Anthropic  max_tokens  -> length      OpenAI  incomplete_details.reason
               pause_turn  -> pause                   max_tokens -> length
               other       -> end                 (no pause equivalent)

    python patch_llm.py /path/to/app.py
"""
import shutil
import sys
from pathlib import Path

IMPORT_OLD = """    import anthropic"""
IMPORT_NEW = """    import anthropic          # still used when MRI_LLM_PROVIDER=anthropic
    import llm                # provider shim - see llm.py"""

CLIENT_OLD = """        claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)"""
CLIENT_NEW = """        # The shim owns client construction so the provider can change without
        # touching this file. Kept assigning to `claude` for callers below.
        claude = llm.get_client()"""

# ── build_params ───────────────────────────────────────────────────────────
PARAMS_OLD = '''    api_params = {
        "model": os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6"),
        "max_tokens": token_limit,
        "system": system_prompt,
        "messages": messages,
    }'''
PARAMS_NEW = '''    # Provider-neutral. llm.py translates this into an Anthropic Messages call
    # or an OpenAI Responses call depending on MRI_LLM_PROVIDER.
    api_params = llm.build_params(system=system_prompt, messages=messages,
                                  max_tokens=token_limit, web_uses=0)'''

WEB_OLD = '''        api_params["tools"] = [
            {"type": "web_search_20250305", "name": "web_search", "max_uses": web_uses}
        ]'''
WEB_NEW = '''        api_params["web_uses"] = web_uses'''

# ── streaming loop ─────────────────────────────────────────────────────────
STREAM_OLD = '''                while True:
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
                                        yield f"data: {json.dumps({'type': 'text', 'text': txt})}\\n\\n"
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
                        if not _deadline_hit:
                            _final = s.get_final_message()
                    _full += _chunk
                    if _deadline_hit:'''

STREAM_NEW = '''                while True:
                    _chunk = ""
                    _stop = "end"
                    # Normalised events from the shim: ("text"|"search"|"stop").
                    # Identical handling whichever provider answered.
                    for _kind, _val in llm.stream(_params):
                        # THE CLOCK BELONGS HERE. Checking it only between
                        # continuations meant one long call could run past
                        # the gateway limit untouched - which is exactly
                        # what was happening: ~300s of generation against a
                        # ~120s cap, severed mid-word, no marker, every time.
                        if _budget_left(_t0) <= 0:
                            _deadline_hit = True
                            break
                        if _kind == "text":
                            _chunk += _val
                            yield f"data: {json.dumps({'type': 'text', 'text': _val})}\\n\\n"
                        elif _kind == "search":
                            print(
                                f"  🔍 [WEB_SEARCH_AUDIT] "
                                f"provider={llm.provider()} "
                                f"user_query={user_query[:80]!r} "
                                f"categories={fired_categories} "
                                f"model_searched={_val!r}"
                            )
                        elif _kind == "stop":
                            _stop = _val
                    _full += _chunk
                    if _deadline_hit:'''

STOP_OLD = '''                    _stop = getattr(_final, "stop_reason", None)
                    if _stop not in CONTINUABLE_STOPS:'''
STOP_NEW = '''                    if _stop not in CONTINUABLE_STOPS:'''

# ── non-streaming path ─────────────────────────────────────────────────────
NONSTREAM_OLD = '''        while True:
            response = client.messages.create(**_params)
            _parts.append("".join(b.text for b in response.content if hasattr(b, "text")))
            _stop = getattr(response, "stop_reason", None)
            if _stop not in CONTINUABLE_STOPS:'''
NONSTREAM_NEW = '''        _searches_seen = []
        while True:
            response = llm.complete(_params)
            _parts.append(response["text"])
            _searches_seen.extend(response.get("searches") or [])
            _stop = response["stop"]
            if _stop not in CONTINUABLE_STOPS:'''

AUDIT_OLD = '''        response_text = _continued_text
        web_searches_made = []
        for block in response.content:
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
                )'''
AUDIT_NEW = '''        response_text = _continued_text
        web_searches_made = [q for q in _searches_seen if q]
        for search_query in web_searches_made:
            print(
                f"  🔍 [WEB_SEARCH_AUDIT] "
                f"provider={llm.provider()} "
                f"user_query={user_query[:80]!r} "
                f"categories={fired_categories} "
                f"model_searched={search_query!r}"
            )'''

STOPS_OLD = '''CONTINUABLE_STOPS = ("max_tokens", "pause_turn")'''
STOPS_NEW = '''# Normalised by llm.py, so this vocabulary is provider-independent:
#   Anthropic  max_tokens -> "length" | pause_turn -> "pause"
#   OpenAI     incomplete_details.reason == max_tokens -> "length"
#              (no pause equivalent; that branch simply never fires)
CONTINUABLE_STOPS = ("length", "pause")'''

CONT_OLD = '''def _continuation_params(api_params, text_so_far, stop_reason="max_tokens"):'''
CONT_NEW = '''def _continuation_params(api_params, text_so_far, stop_reason="length"):'''

PAUSE_OLD = '''        {"role": "user", "content": (PAUSE_INSTRUCTION if stop_reason == "pause_turn"'''
PAUSE_NEW = '''        {"role": "user", "content": (PAUSE_INSTRUCTION if stop_reason == "pause"'''

HEALTH_OLD = '''    status = {"status": "ok", "config": _CONFIG_OK,'''
HEALTH_NEW = '''    status = {"status": "ok", "config": _CONFIG_OK,
              "llm_provider": llm.provider(), "llm_model": llm.model_name(),'''

KEYCHECK_OLD = '''if not ANTHROPIC_KEY:'''
KEYCHECK_NEW = '''if os.environ.get("MRI_LLM_PROVIDER", "anthropic").lower() == "openai":
    if not os.environ.get("OPENAI_API_KEY"):
        print("⚠ WARNING: MRI_LLM_PROVIDER=openai but OPENAI_API_KEY is not set.")
elif not ANTHROPIC_KEY:'''

STEPS = [
    ("import the shim", IMPORT_OLD, IMPORT_NEW),
    ("client built by the shim", CLIENT_OLD, CLIENT_NEW),
    ("key check covers both providers", KEYCHECK_OLD, KEYCHECK_NEW),
    ("provider-neutral request params", PARAMS_OLD, PARAMS_NEW),
    ("web search expressed as a count, not a vendor tool", WEB_OLD, WEB_NEW),
    ("streaming loop consumes normalised events", STREAM_OLD, STREAM_NEW),
    ("stop reason comes from the event stream", STOP_OLD, STOP_NEW),
    ("non-streaming path goes through the shim", NONSTREAM_OLD, NONSTREAM_NEW),
    ("non-streaming audit log", AUDIT_OLD, AUDIT_NEW),
    ("continuable stops are normalised", STOPS_OLD, STOPS_NEW),
    ("continuation default", CONT_OLD, CONT_NEW),
    ("pause branch uses the normalised name", PAUSE_OLD, PAUSE_NEW),
    ("health reports provider and model", HEALTH_OLD, HEALTH_NEW),
]


def main(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    if "import llm" in src:
        print("  ! already patched")
        return 1
    for label, old, new in STEPS:
        n = src.count(old)
        if n != 1:
            print(f"  ! anchor for '{label}' found {n} times, expected 1")
            return 1
    for label, old, new in STEPS:
        src = src.replace(old, new, 1)
        print(f"  done: {label}")
    shutil.copy2(path, path.with_suffix(path.suffix + ".pre_llm"))
    path.write_text(src, encoding="utf-8")
    print(f"\n  written {path}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(Path(sys.argv[1])))
