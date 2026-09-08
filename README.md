# Fix 1 — the progress line said "computing feasibility" over every query

## What was wrong

The line the user watches while a report generates was a string constant in
`mri_v3.html`:

```js
hint("Reading the graph and computing feasibility...");
```

It said that over a carpet-price lookup, a micromarket ranking, everything. The
frontend was asserting what the server was doing instead of asking it.

That is the same fault as reading a price by column position or taking `rows[0]`
of a series: **a claim about the data made without consulting the data.**

## What changed

`describe_intent(query)` in `app.py` is now the single source of truth. It
returns both the flag that sets the token ceiling and the label the user reads,
so those two can never disagree:

```python
{"is_feasibility": True, "label": "Running the feasibility appraisal"}
```

`/api/query/async` returns `status_label` in its 202, and the frontend displays
it verbatim. If it talks to an older backend that does not send one, it shows a
neutral "Reading the graph" — degrading to a vague truth rather than a specific
falsehood.

Labels: feasibility appraisal · carpet-basis series · resolving the pin and
ranking nearby projects · ranking projects by distance · absorption and
inventory · price series · comparing projects · ranking the market · reading the
graph.

## The second bug found while fixing the first

There were **two** feasibility detectors and they had drifted apart:

| pattern | web routing | token ceiling |
|---|---|---|
| `feasib`, `acre`, `fsi`, `google.*map` | ✅ | ✅ |
| `due.dilig`, `land.*acqui`, `goo.gl`, `maps.google` | ✅ | ❌ |

So "due diligence on this land parcel" got web context and then an 8,000-token
ceiling meant for a short answer — a long report guaranteed to stop early. There
is now one `_FEASIBILITY_RE`, the union of both, feeding all three consumers:
web routing, the token ceiling, and the progress label.

## Verify

```
python3 test_intent.py
```

30 checks: every query type gets the right label, no non-feasibility query
claims feasibility, the pattern is defined exactly once, the frontend no longer
carries a hardcoded string, and the two queries the detectors used to disagree
on now resolve identically for both.

`test_llm.py` (56), `test_pinfix.py`, `test_carpet.py`, `test_pricefix.py` and
`test_kpi.js` all re-run green.

## Deploy

- `app.py` → `mri-api`
- `mri_v3.html` → `mri-frontend`

Both, together. `/api/health` will read `"build":"2026-09-08-intentlabel"` — the
marker is bumped, so from here two deployments can be told apart.
