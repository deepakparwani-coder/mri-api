"""
llm.py — one interface, two providers.

The client wants to consolidate on their OpenAI contract. Rather than rewrite
app.py's generation loop for OpenAI and then rewrite it again the next time a
commercial decision changes, the provider sits behind a shim and the switch is
one environment variable:

    MRI_LLM_PROVIDER = anthropic | openai        (default: anthropic)
    MRI_LLM_MODEL    = model id                  (default: per provider)

Everything above this file - the deadline budget, the continuation loop, the
truncation markers, the web-search audit log - is written against the normalised
events below and does not know or care which vendor answered.

NORMALISED EVENTS  (yielded by stream())
    ("text",   str)   incremental output text
    ("search", str)   a server-side web search was issued, with its query
    ("stop",   str)   terminal; one of:
                        "length"  output token ceiling hit    -> continuable
                        "pause"   provider paused the turn    -> continuable
                        "end"     finished normally
                        "error"   see the accompanying message

WHY "pause" SURVIVES THE PORT
    Anthropic returns stop_reason "pause_turn" mid-tool-use and expects the
    caller to resume. OpenAI has no equivalent. The normalised vocabulary keeps
    the concept so app.py's continuation logic is unchanged; on OpenAI it simply
    never fires.

WHAT IS DELIBERATELY NOT ABSTRACTED
    Prompt content. The system prompt carries rules written against observed
    model behaviour ("NO PROSE ARITHMETIC", "ONE VERDICT ONLY", ...). Those are
    instructions to a model, not plumbing, and whether a given model obeys them
    is an empirical question this file cannot answer. Run the fixed query set
    against both and diff.
"""
from __future__ import annotations

import os
import sys

PROVIDER = os.environ.get("MRI_LLM_PROVIDER", "anthropic").strip().lower()

_DEFAULT_MODEL = {
    "anthropic": "claude-sonnet-4-6",
    "openai": "gpt-5",
}


def model_name() -> str:
    explicit = os.environ.get("MRI_LLM_MODEL")
    if explicit:
        return explicit
    if PROVIDER == "anthropic":
        # CLAUDE_MODEL is the pre-shim env var and is still honoured - but ONLY
        # for Anthropic. Reading it under any other provider would carry a
        # Claude model id across the provider boundary and send
        # "claude-sonnet-4-6" to the OpenAI Responses API, which answers 404
        # model_not_found. A value that is valid on one side of a boundary is
        # not a default for the other side.
        legacy = os.environ.get("CLAUDE_MODEL")
        if legacy:
            return legacy
    return _DEFAULT_MODEL.get(PROVIDER, "")


def provider() -> str:
    return PROVIDER


# ── client construction ────────────────────────────────────────────────────
_client = None


def get_client():
    """Build (once) the SDK client for the configured provider."""
    global _client
    if _client is not None:
        return _client
    if PROVIDER == "openai":
        # Configuration is checked BEFORE the SDK is imported. A misconfigured
        # model is a config error, and reporting it as an import error would
        # send whoever reads the log after the wrong problem.
        m = model_name()
        if m.startswith("claude"):
            # Fail here, in one clear line at boot, rather than as a 404 in the
            # middle of a report the user is watching stream.
            raise RuntimeError(
                f"MRI_LLM_PROVIDER=openai but the model resolves to {m!r}. "
                "Unset MRI_LLM_MODEL (or set it to an OpenAI model id).")
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        try:
            from openai import OpenAI
        except ImportError:
            print("pip install openai", file=sys.stderr)
            raise
        _client = OpenAI(api_key=key)
    else:
        try:
            import anthropic
        except ImportError:
            print("pip install anthropic", file=sys.stderr)
            raise
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")
        _client = anthropic.Anthropic(api_key=key)
    return _client


def reset_client():
    """Drop the cached client - used by the tests when swapping providers."""
    global _client
    _client = None


# ── request shaping ────────────────────────────────────────────────────────
def build_params(system: str, messages: list, max_tokens: int,
                 web_uses: int = 0) -> dict:
    """Provider-neutral request description.

    `messages` is the Anthropic shape: [{"role": ..., "content": ...}] where
    content is a string or a list of content blocks. That is the shape app.py
    already builds, so nothing upstream changes.
    """
    return {"system": system, "messages": messages,
            "max_tokens": max_tokens, "web_uses": web_uses}


def _to_anthropic(p: dict) -> dict:
    out = {"model": model_name(), "max_tokens": p["max_tokens"],
           "system": p["system"], "messages": p["messages"]}
    if p.get("web_uses"):
        out["tools"] = [{"type": "web_search_20250305", "name": "web_search",
                         "max_uses": p["web_uses"]}]
    return out


def _flatten(content):
    """OpenAI wants a plain string, or its own typed parts. app.py may pass
    Anthropic content blocks (image / document / text) for uploaded files."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text":
                parts.append(b.get("text", ""))
            elif isinstance(b, dict):
                # Images and PDFs use a different envelope on each provider and
                # are dropped rather than mistranslated - a silently mangled
                # attachment is worse than an absent one.
                parts.append(f"[attachment of type '{b.get('type')}' omitted "
                             f"- not supported on this provider]")
        return "\n".join(p for p in parts if p)
    return str(content)


def _to_openai(p: dict) -> dict:
    msgs = [{"role": "system", "content": p["system"]}]
    for m in p["messages"]:
        msgs.append({"role": m["role"], "content": _flatten(m.get("content"))})
    out = {"model": model_name(), "input": msgs,
           "max_output_tokens": p["max_tokens"]}
    if p.get("web_uses"):
        out["tools"] = [{"type": "web_search"}]
    return out


# ── streaming ──────────────────────────────────────────────────────────────
def stream(p: dict):
    """Yield normalised ("text"|"search"|"stop", value) tuples."""
    if PROVIDER == "openai":
        yield from _stream_openai(p)
    else:
        yield from _stream_anthropic(p)


def _stream_anthropic(p: dict):
    client = get_client()
    stop = "end"
    with client.messages.stream(**_to_anthropic(p)) as s:
        for event in s:
            et = getattr(event, "type", None)
            if et == "content_block_delta":
                d = getattr(event, "delta", None)
                if d is not None and getattr(d, "type", "") == "text_delta":
                    t = getattr(d, "text", "")
                    if t:
                        yield ("text", t)
            elif et == "content_block_start":
                b = getattr(event, "content_block", None)
                if b is not None and getattr(b, "type", "") == "server_tool_use" \
                        and getattr(b, "name", "") == "web_search":
                    q = (getattr(b, "input", {}) or {}).get("query", "<unknown>")
                    yield ("search", q)
        final = s.get_final_message()
    raw = getattr(final, "stop_reason", None)
    stop = {"max_tokens": "length", "pause_turn": "pause"}.get(raw, "end")
    yield ("stop", stop)


def _stream_openai(p: dict):
    """Responses API.

    Text deltas arrive as `response.output_text.delta`. Some SDK/model
    combinations emit `response.content_part.added` carrying a whole part
    instead, so both are handled - guessing one and being wrong would look
    exactly like a model that produced nothing.
    """
    client = get_client()
    stop = "end"
    seen_delta = False
    with client.responses.stream(**_to_openai(p)) as s:
        for event in s:
            et = getattr(event, "type", "") or ""
            if et == "response.output_text.delta":
                t = getattr(event, "delta", "") or ""
                if t:
                    seen_delta = True
                    yield ("text", t)
            elif et == "response.content_part.added" and not seen_delta:
                part = getattr(event, "part", None)
                t = getattr(part, "text", "") if part is not None else ""
                if t:
                    yield ("text", t)
            elif et == "response.output_item.added":
                item = getattr(event, "item", None)
                if item is not None and getattr(item, "type", "") in (
                        "web_search_call", "web_search"):
                    q = ""
                    action = getattr(item, "action", None)
                    if action is not None:
                        q = getattr(action, "query", "") or ""
                    yield ("search", q or "<query not exposed>")
            elif et == "response.incomplete":
                r = getattr(event, "response", None)
                det = getattr(r, "incomplete_details", None) if r is not None else None
                reason = getattr(det, "reason", "") if det is not None else ""
                stop = "length" if reason in ("max_tokens", "max_output_tokens") else "end"
            elif et == "response.failed":
                r = getattr(event, "response", None)
                err = getattr(r, "error", None) if r is not None else None
                msg = getattr(err, "message", "") if err is not None else "response.failed"
                yield ("stop", "error")
                raise RuntimeError(f"OpenAI response failed: {msg}")
    yield ("stop", stop)


# ── non-streaming ──────────────────────────────────────────────────────────
def complete(p: dict) -> dict:
    """Return {"text": str, "stop": str, "searches": [str]}."""
    if PROVIDER == "openai":
        client = get_client()
        r = client.responses.create(**_to_openai(p))
        text = getattr(r, "output_text", "") or ""
        det = getattr(r, "incomplete_details", None)
        reason = getattr(det, "reason", "") if det is not None else ""
        stop = "length" if reason in ("max_tokens", "max_output_tokens") else "end"
        searches = []
        for item in (getattr(r, "output", None) or []):
            if getattr(item, "type", "") in ("web_search_call", "web_search"):
                a = getattr(item, "action", None)
                searches.append(getattr(a, "query", "") if a is not None else "")
        return {"text": text, "stop": stop, "searches": searches}

    client = get_client()
    r = client.messages.create(**_to_anthropic(p))
    text = "".join(b.text for b in r.content if hasattr(b, "text"))
    raw = getattr(r, "stop_reason", None)
    stop = {"max_tokens": "length", "pause_turn": "pause"}.get(raw, "end")
    searches = []
    for b in r.content:
        if getattr(b, "type", "") == "server_tool_use" and getattr(b, "name", "") == "web_search":
            searches.append((getattr(b, "input", {}) or {}).get("query", ""))
    return {"text": text, "stop": stop, "searches": searches}
