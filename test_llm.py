#!/usr/bin/env python3
"""
test_llm.py — prove both providers produce identical normalised events.

Neither SDK is called for real. Fake clients emit the exact event shapes each
vendor documents, and the shim must turn both into the same stream. That is the
only thing app.py depends on, so it is the only thing worth asserting.
"""
import importlib
import os
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
FAILURES = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(label)


def O(**kw):
    """A tiny attribute bag - both SDKs return objects, not dicts."""
    return types.SimpleNamespace(**kw)


# ── fake Anthropic ─────────────────────────────────────────────────────────
class _AStream:
    def __init__(self, events, stop):
        self._events, self._stop = events, stop
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def __iter__(self): return iter(self._events)
    def get_final_message(self): return O(stop_reason=self._stop)


class FakeAnthropic:
    last_request = None
    def __init__(self, stop="end_turn"):
        self._stop = stop
        self.messages = O(stream=self._stream, create=self._create)
    def _events(self):
        return [
            O(type="content_block_start",
              content_block=O(type="server_tool_use", name="web_search",
                              input={"query": "hinjewadi metro line 3"})),
            O(type="content_block_delta", delta=O(type="text_delta", text="Hello ")),
            O(type="content_block_delta", delta=O(type="text_delta", text="world")),
            O(type="content_block_delta", delta=O(type="thinking_delta", thinking="ignore me")),
        ]
    def _stream(self, **kw):
        FakeAnthropic.last_request = kw
        return _AStream(self._events(), self._stop)
    def _create(self, **kw):
        FakeAnthropic.last_request = kw
        return O(stop_reason=self._stop,
                 content=[O(type="server_tool_use", name="web_search",
                            input={"query": "hinjewadi metro line 3"}),
                          O(type="text", text="Hello world")])


# ── fake OpenAI ────────────────────────────────────────────────────────────
class _OStream:
    def __init__(self, events):
        self._events = events
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def __iter__(self): return iter(self._events)


class FakeOpenAI:
    last_request = None
    def __init__(self, incomplete=None):
        self._incomplete = incomplete
        self.responses = O(stream=self._stream, create=self._create)
    def _events(self):
        ev = [
            O(type="response.created"),
            O(type="response.output_item.added",
              item=O(type="web_search_call", action=O(query="hinjewadi metro line 3"))),
            O(type="response.output_text.delta", delta="Hello "),
            O(type="response.output_text.delta", delta="world"),
        ]
        if self._incomplete:
            ev.append(O(type="response.incomplete",
                        response=O(incomplete_details=O(reason=self._incomplete))))
        ev.append(O(type="response.completed"))
        return ev
    def _stream(self, **kw):
        FakeOpenAI.last_request = kw
        return _OStream(self._events())
    def _create(self, **kw):
        FakeOpenAI.last_request = kw
        return O(output_text="Hello world",
                 incomplete_details=O(reason=self._incomplete) if self._incomplete else None,
                 output=[O(type="web_search_call", action=O(query="hinjewadi metro line 3"))])


def load(provider):
    os.environ["MRI_LLM_PROVIDER"] = provider
    os.environ.setdefault("ANTHROPIC_API_KEY", "stub")
    os.environ.setdefault("OPENAI_API_KEY", "stub")
    import llm
    importlib.reload(llm)
    return llm


PARAMS = None


def main() -> int:
    global PARAMS
    llm = load("anthropic")
    PARAMS = llm.build_params(system="SYS", max_tokens=1234, web_uses=2,
                              messages=[{"role": "user", "content": "Q"}])

    print("\n1. THE SAME CALL PRODUCES THE SAME EVENTS ON BOTH PROVIDERS")
    llm = load("anthropic"); llm.reset_client(); llm._client = FakeAnthropic()
    a = list(llm.stream(PARAMS))
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI()
    o = list(llm.stream(PARAMS))
    check("anthropic stream normalises", a == [("search", "hinjewadi metro line 3"),
                                               ("text", "Hello "), ("text", "world"),
                                               ("stop", "end")], str(a))
    check("openai stream normalises identically", o == a, str(o))
    check("text concatenates the same",
          "".join(v for k, v in a if k == "text") == "".join(v for k, v in o if k == "text"))
    check("the web search is surfaced on both",
          [v for k, v in a if k == "search"] == [v for k, v in o if k == "search"])
    check("anthropic thinking deltas are not leaked as text",
          "ignore me" not in "".join(v for k, v in a if k == "text"))

    print("\n2. STOP REASONS MAP ONTO ONE VOCABULARY")
    llm = load("anthropic"); llm.reset_client(); llm._client = FakeAnthropic(stop="max_tokens")
    check("anthropic max_tokens -> length",
          list(llm.stream(PARAMS))[-1] == ("stop", "length"))
    llm.reset_client(); llm._client = FakeAnthropic(stop="pause_turn")
    check("anthropic pause_turn -> pause",
          list(llm.stream(PARAMS))[-1] == ("stop", "pause"))
    llm.reset_client(); llm._client = FakeAnthropic(stop="end_turn")
    check("anthropic end_turn -> end",
          list(llm.stream(PARAMS))[-1] == ("stop", "end"))
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI(incomplete="max_tokens")
    check("openai incomplete(max_tokens) -> length",
          list(llm.stream(PARAMS))[-1] == ("stop", "length"))
    llm.reset_client(); llm._client = FakeOpenAI()
    check("openai completed -> end", list(llm.stream(PARAMS))[-1] == ("stop", "end"))

    print("\n3. app.py's CONTINUATION LOGIC STILL LINES UP")
    app = (Path(__file__).parent / "app.py").read_text(encoding="utf-8")
    check("CONTINUABLE_STOPS uses the normalised names",
          'CONTINUABLE_STOPS = ("length", "pause")' in app)
    check("no vendor stop_reason strings remain in the loop",
          "pause_turn" not in app.split("CONTINUABLE_STOPS")[0])
    check("the pause branch matches the new name", 'stop_reason == "pause"' in app)
    check("no direct SDK calls remain",
          "client.messages.stream" not in app and "client.messages.create" not in app)

    print("\n4. REQUESTS ARE TRANSLATED, NOT PASSED THROUGH")
    llm = load("anthropic"); llm.reset_client(); llm._client = FakeAnthropic()
    list(llm.stream(PARAMS))
    ar = FakeAnthropic.last_request
    check("anthropic gets a top-level system", ar.get("system") == "SYS")
    check("anthropic gets max_tokens", ar.get("max_tokens") == 1234)
    check("anthropic gets its own web tool",
          ar.get("tools", [{}])[0].get("type") == "web_search_20250305")
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI()
    list(llm.stream(PARAMS))
    orq = FakeOpenAI.last_request
    check("openai gets system as a message",
          orq["input"][0] == {"role": "system", "content": "SYS"})
    check("openai gets max_output_tokens", orq.get("max_output_tokens") == 1234)
    check("openai gets no 'max_tokens' key", "max_tokens" not in orq)
    check("openai gets its own web tool", orq.get("tools", [{}])[0].get("type") == "web_search")
    check("the user message survives",
          orq["input"][1] == {"role": "user", "content": "Q"})

    print("\n5. ATTACHMENTS ARE DECLARED, NOT SILENTLY MANGLED")
    p2 = llm.build_params(system="S", max_tokens=10, messages=[{"role": "user", "content": [
        {"type": "image", "source": {"data": "..."}},
        {"type": "text", "text": "describe this"}]}])
    llm.reset_client(); llm._client = FakeOpenAI()
    list(llm.stream(p2))
    body = FakeOpenAI.last_request["input"][1]["content"]
    check("text part survives", "describe this" in body)
    check("the dropped image is stated", "attachment of type 'image' omitted" in body)

    print("\n6. NON-STREAMING MATCHES STREAMING")
    llm = load("anthropic"); llm.reset_client(); llm._client = FakeAnthropic(stop="max_tokens")
    ca = llm.complete(PARAMS)
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI(incomplete="max_tokens")
    co = llm.complete(PARAMS)
    check("same text", ca["text"] == co["text"] == "Hello world")
    check("same stop", ca["stop"] == co["stop"] == "length")
    check("same searches", ca["searches"] == co["searches"] == ["hinjewadi metro line 3"])

    print("\n7. A CLAUDE MODEL ID NEVER CROSSES THE PROVIDER BOUNDARY")
    # Live /api/health on 07-Sep reported llm_model=claude-sonnet-4-6. If that
    # came from a CLAUDE_MODEL env var left over from before the shim, then
    # flipping MRI_LLM_PROVIDER to openai would have sent that id to the
    # Responses API and 404'd - a switch that "deploys fine" and then fails on
    # the first real query.
    os.environ.pop("MRI_LLM_MODEL", None)
    os.environ["CLAUDE_MODEL"] = "claude-sonnet-4-6"
    check("CLAUDE_MODEL is still honoured on anthropic",
          load("anthropic").model_name() == "claude-sonnet-4-6")
    check("CLAUDE_MODEL is ignored on openai",
          load("openai").model_name() == "gpt-5.6-terra", load("openai").model_name())

    os.environ["MRI_LLM_MODEL"] = "claude-sonnet-4-6"
    llm = load("openai"); llm.reset_client()
    try:
        llm.get_client()
        check("a mismatched MRI_LLM_MODEL fails loudly at boot", False, "no error raised")
    except RuntimeError as e:
        check("a mismatched MRI_LLM_MODEL fails loudly at boot", "resolves to" in str(e), str(e))
    except Exception as e:
        check("a mismatched MRI_LLM_MODEL fails loudly at boot", False, repr(e))

    os.environ["MRI_LLM_MODEL"] = "gpt-5.6-luna"
    check("an openai model id still overrides the default",
          load("openai").model_name() == "gpt-5.6-luna")
    os.environ.pop("MRI_LLM_MODEL", None)
    os.environ.pop("CLAUDE_MODEL", None)
    check("with neither set, each provider gets its own default",
          load("openai").model_name() == "gpt-5.6-terra"
          and load("anthropic").model_name() == "claude-sonnet-4-6")

    print("\n8. THE DEFAULT MODEL IS A MODEL THAT EXISTS")
    # "gpt-5" is not in OpenAI's current model list (GPT-6 Astra, GPT-5.6
    # Sol/Terra/Luna). Shipping it as the default would have 404'd on the first
    # query after the switch.
    llm = load("openai")
    check("the openai default is not the retired gpt-5", llm.model_name() != "gpt-5")
    check("the openai default is a current id",
          llm.model_name() in ("gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.6-luna",
                               "gpt-6-astra"), llm.model_name())

    print("\n9. REASONING EFFORT IS TUNABLE, AND OFF BY DEFAULT")
    os.environ.pop("MRI_LLM_REASONING", None)
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI()
    list(llm.stream(PARAMS))
    check("nothing is sent when unset", "reasoning" not in FakeOpenAI.last_request)
    check("health says provider default", llm.reasoning_effort() == "(provider default)")
    os.environ["MRI_LLM_REASONING"] = "low"
    llm = load("openai"); llm.reset_client(); llm._client = FakeOpenAI()
    list(llm.stream(PARAMS))
    check("it reaches the Responses call as an object",
          FakeOpenAI.last_request.get("reasoning") == {"effort": "low"},
          str(FakeOpenAI.last_request.get("reasoning")))
    check("health reports the effort", llm.reasoning_effort() == "low")
    llm = load("anthropic"); llm.reset_client(); llm._client = FakeAnthropic()
    list(llm.stream(PARAMS))
    check("anthropic is never sent a reasoning key",
          "reasoning" not in FakeAnthropic.last_request)
    os.environ.pop("MRI_LLM_REASONING", None)

    print("\n10. AN EMPTY CONTINUABLE ROUND IS NOT CONTINUED")
    # Reasoning tokens are charged against max_output_tokens, so a high effort
    # can burn the whole ceiling and return stop=length with no text. Retrying
    # that four times spends the budget and still ends in a truncation marker.
    check("the empty-round guard exists", "if not _chunk.strip():" in app)
    check("it breaks rather than continuing",
          app.split("if not _chunk.strip():")[1].split("_round += 1")[0].count("break") == 1)
    check("the log names the likely cause", "reasoning tokens" in app)
    check("it is tested before the round counter increments",
          app.index("if not _chunk.strip():") < app.index("_round += 1"))
    check("health reports the effort too", '"llm_reasoning": llm.reasoning_effort()' in app)

    print("\n11. THE REST OF THE APP IS UNTOUCHED")
    for needle, label in (
            ("NO PROSE ARITHMETIC", "no-prose-arithmetic rule"),
            ("NEVER CONVERT A PRICE", "carpet basis rule"),
            ("THE SITE SCORE IS ARITHMETIC", "scorecard rule"),
            ("PROXIMITY CLAIMS MUST COME FROM PROXIMITY DATA", "proximity rule"),
            ("THE EXECUTIVE VERDICT QUOTES", "verdict-quotes rule"),
            ("_quarter_sort_key", "latest-quarter price resolver"),
            ("_wants_carpet_basis", "carpet routing"),
            ("pin_projects", "pin distance query"),
            ("/api/query/async", "async generation"),
            ("build_feasibility_block", "feasibility engine"),
            ("COMPUTED FEASIBILITY: NOT RUN", "visible abstention")):
        check(f"{label} intact", needle in app)
    check("health reports the provider", '"llm_provider": llm.provider()' in app)

    print("\n" + "=" * 66)
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
