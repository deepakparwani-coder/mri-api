#!/usr/bin/env python3
"""
test_frontend_e2e.py — drive the real page in a real browser against a stub API
that reproduces the real bug.

The stub generates an 18-second report and severs its SSE connection at 6
seconds, which is the production failure scaled down (300s report, 120s
gateway). Two pages are then loaded and asked the same question:

    BEFORE  the current mri_v3.html  -> synchronous stream -> should truncate
    AFTER   the patched mri_v3.html  -> submit and poll    -> should complete

"Complete" is judged the way a reader judges it: the closing
'Data Source: ... Confidence: ...' footer is on screen and the last section
heading is present. That is the same check that has failed on every PDF so far.

    python test_frontend_e2e.py
"""
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
STUB = "http://127.0.0.1:8899"
QUERY = ("https://maps.app.goo.gl/RHAhAyRmfEjLZXNJ7 . Run a feasibility check for this "
         "5 acre plot. cost of acq is 25 cr, cost of construction is 3000psf.")


def prepare(src: Path, dst: Path):
    s = src.read_text(encoding="utf-8")
    s2 = re.sub(r'var API_BASE = "[^"]*";', f'var API_BASE = "{STUB}";', s, count=1)
    assert s2 != s, f"could not repoint API_BASE in {src.name}"
    dst.write_text(s2, encoding="utf-8")


def run_case(pw, label, url, wait_secs):
    from playwright.sync_api import TimeoutError as PWTimeout
    browser = pw.chromium.launch()
    page = browser.new_page()
    page.on("dialog", lambda d: d.accept("sk-ant-api03-test"))
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    # Landing screen: pick a city, then enter the app.
    opts = page.eval_on_selector(
        "#lpCity", "el => Array.from(el.options).map(o => o.value)")
    city = "Hinjewadi" if "Hinjewadi" in opts else opts[0]
    page.select_option("#lpCity", city)
    page.click("text=Start Analysis")
    page.wait_for_selector("#qi", state="visible", timeout=15000)
    page.wait_for_timeout(500)

    page.fill("#qi", QUERY)
    page.keyboard.press("Enter")

    t0 = time.time()
    text = ""
    while time.time() - t0 < wait_secs:
        page.wait_for_timeout(1000)
        text = page.inner_text("#chat")
        if "Data Source:" in text:
            break
    elapsed = time.time() - t0

    result = dict(
        label=label,
        elapsed=elapsed,
        chars=len(text),
        has_footer="Data Source:" in text,
        has_last_step="STEP 10" in text,
        has_verdict="EXECUTIVE VERDICT" in text,
        js_errors=errors,
        tail=" ".join(text[-140:].split()),
    )
    browser.close()
    return result


def show(r):
    print(f"\n── {r['label']} ──────────────────────────────")
    print(f"   elapsed        : {r['elapsed']:.0f}s")
    print(f"   text on screen : {r['chars']:,} chars")
    print(f"   verdict shown  : {'yes' if r['has_verdict'] else 'no'}")
    print(f"   last section   : {'STEP 10 present' if r['has_last_step'] else 'MISSING'}")
    print(f"   closing footer : {'present' if r['has_footer'] else 'MISSING - report is cut off'}")
    if r["js_errors"]:
        print(f"   js errors      : {r['js_errors'][:2]}")
    print(f"   ends with      : ...{r['tail']}")


def main() -> int:
    before = HERE / "_before.html"
    after = HERE / "_after.html"
    prepare(Path("/root/dist/mri_v3.html"), before)
    prepare(HERE / "mri_v3.html", after)

    proc = subprocess.Popen(
        [sys.executable, str(HERE / "stub_api.py"), "--html", str(before), "--port", "8899"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2.5)

    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as pw:
            b = run_case(pw, "BEFORE - current build (synchronous stream)", STUB + "/", 30)
            proc.terminate(); proc.wait()
            proc2 = subprocess.Popen(
                [sys.executable, str(HERE / "stub_api.py"), "--html", str(after), "--port", "8899"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2.5)
            a = run_case(pw, "AFTER - patched build (submit and poll)", STUB + "/", 45)
            proc2.terminate(); proc2.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()

    show(b)
    show(a)

    print("\n" + "=" * 62)
    fails = []
    if b["has_footer"]:
        fails.append("the stub did not reproduce the bug - BEFORE completed, so "
                     "the AFTER result proves nothing")
    if not a["has_footer"]:
        fails.append("AFTER is still cut off - the fix does not work")
    if not a["has_last_step"]:
        fails.append("AFTER is missing the final section")
    if a["js_errors"]:
        fails.append(f"AFTER threw javascript errors: {a['js_errors'][:1]}")
    if a["chars"] <= b["chars"]:
        fails.append("AFTER delivered no more text than BEFORE")

    if fails:
        for f in fails:
            print(f"FAIL: {f}")
        return 1
    print(f"PASS: BEFORE truncated at {b['chars']:,} chars with no footer;")
    print(f"      AFTER delivered {a['chars']:,} chars including the closing footer,")
    print(f"      taking {a['elapsed']:.0f}s - well past the {6}s gateway the stub enforces.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
