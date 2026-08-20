# Incomplete reports — the actual fix

## What the last patch got wrong

The deadline I shipped in `timeout_fix.zip` was checked in only one place: **between
continuations**.

```python
while True:
    with client.messages.stream(**params) as s:
        for event in s:          # <- no clock here
            ...
    left = _budget_left(t0)      # <- only here
```

The dominant failure is a **single** call that runs ~300 seconds. It never reaches
the second `while` iteration, so the budget never fires, the gateway severs the
socket at ~120s, and no marker is appended. The patch therefore did nothing for
the case it was written for. Your 20 August report is the proof: eight pages,
cut mid-table at `Year 1 ~75 ~48 -44`, no truncation marker, no citation footer.

Both fixes below are in `app.py`.

## Fix 1 — the clock goes inside the event loop

Generation now stops on its own terms, inside the gateway window, and appends an
honest marker instead of being cut mid-word. This turns a silent severed report
into a short but *complete-looking and self-declaring* one.

That is damage limitation, not a cure. Fix 2 is the cure.

## Fix 2 — generation moves off the request path

```
POST /api/query/async              -> 202 {"job_id": "..."}          (returns in ms)
GET  /api/query/result/<id>?cursor=N
                                   -> {"status","delta","cursor","done_meta"}
POST /api/query/cancel/<id>        -> {"cancelled": true}
```

Each poll is a sub-second request, so no single HTTP call is ever near the gateway
limit and the generation budget rises from 100s to 600s.

The worker does **not** reimplement anything. It re-enters the existing
`handle_query()` through a synthetic request context and consumes the SSE
generator that function already returns. Neo4j retrieval, pin resolution, the
feasibility engine and the system prompt are byte-for-byte the ones the
synchronous path uses. Only the transport changed.

The frontend appends each poll's delta exactly as it appended stream chunks, so
the report still appears progressively, with an elapsed-seconds hint — a
four-minute report behind a silent UI reads as a hang.

## Deploy

**1. API repo (`mri-api`)** — replace `app.py` with `app.py` from this archive.
It supersedes `timeout_fix.zip`; you do not need to apply that one first.

**Run one worker.** Jobs live in the worker's memory, so a poll that lands on a
different process will 404:

```
gunicorn app:app --workers 1 --threads 8 --timeout 900
```

On Render this is the **Start Command** field. If you leave the default multi-worker
command, the poll endpoint returns a 404 whose body says exactly this — it fails
loudly rather than mysteriously.

**2. Frontend repo (`mri-frontend`)** — replace `mri_v3.html`.

Order does not matter. The page tries `/api/query/async` first and falls back to
the old streaming path on a 404, so it is safe to publish before the API is
redeployed, and the API is harmless before the page is.

## Environment variables

| Variable | Default | What it does |
|---|---|---|
| `MRI_ASYNC_BUDGET_SECS` | `600` | Generation budget for async jobs |
| `MRI_GEN_BUDGET_SECS` | `100` | Budget for the synchronous fallback path |
| `MRI_JOB_TTL_SECS` | `1800` | How long a finished job stays pollable |
| `MRI_WEB_USES_FEASIBILITY` | `2` | Server-side web searches on a feasibility query |

With async in place the web-search cuts and the length-discipline prompt from the
previous patch are no longer load-bearing. If you want the long-form report back,
raise `MRI_WEB_USES_FEASIBILITY` to 4 — there is time for it now.

## What was verified before this was sent

`test_async_mechanism.py` — 17 checks, all passing. Proves a worker thread can
consume a `stream_with_context` generator through `test_request_context`; that a
generation four times longer than the gateway completes in full through polling;
that the mid-stream deadline fires *during* generation; that an unknown job id
404s, a bad cursor does not 500, and the internal re-entry does not consume the
caller's rate limit.

`test_frontend_e2e.py` — drives both builds in headless Chromium against
`stub_api.py`, which reproduces the production failure at 1/25 scale (an 18s
report severed at 6s):

```
BEFORE  1,316 chars, cut at "STEP 1 - SITE", no closing footer
AFTER   1,944 chars, STEP 10 present, closing footer present, 19s elapsed
```

Run either with `python <file>` from this directory.

## Known limits

- **One worker only.** A multi-process deployment needs Redis for the job store.
  Say the word if you want that; it is a small change to `_JOBS`.
- **Jobs are lost on restart.** A redeploy mid-generation loses in-flight
  reports. The UI surfaces this as "the report job expired — run the query again"
  rather than hanging.
- **The synchronous path still exists** and still has a 100s ceiling. It is only
  reached if the async endpoint is missing.
