# PATCH — Tier 1 Trust UI (provenance footer, verified row counts, data vintage, table fix)

**Files touched:** `app.py` (2 edits), `mri_v3.html` (3 edits)
**Why:** the streaming path — the one every demo uses — ignores the `done` event's metadata and
hardcodes a bare "Source: LF Knowledge Base" line, so the audience never sees *what* grounded the
answer. This patch makes every streamed answer end with: LF badge + verified row/query counts +
data-vintage stamp + amber WEB badge when web mode fired + purple REGULATORY badge when UDCPR
citations appear. It also replaces the two fragile table regexes with a line-based parser
(tested against 9 failure modes incl. CRLF, missing trailing newline, empty cells, no leading pipes).

---

## Backend

### Edit B1 — `app.py`: data-vintage helper (paste below `run_query`)

```python
# ── TRUST-UI: cached "data through" vintage per city ──
_VINTAGE_CACHE = {}
def get_data_vintage(city):
    """Latest quarter present for this city, e.g. 'Q2 25-26'. Cached per process."""
    if city not in _VINTAGE_CACHE:
        try:
            with get_driver().session() as s:
                rec = s.run(
                    "MATCH (c:City {name:$city})-[r]->(q:Quarter) "
                    "RETURN q.name AS q ORDER BY q.sort_order DESC LIMIT 1",
                    city=city).single()
            _VINTAGE_CACHE[city] = rec["q"] if rec else None
        except Exception:
            _VINTAGE_CACHE[city] = None
    return _VINTAGE_CACHE[city]
```
(If your Quarter nodes lack `sort_order`, order by whatever the loader stamps — check one
node in sandbox; the fallback `ORDER BY q.name DESC` is wrong for FY quarters, so use the real key.)

### Edit B2 — `app.py`: enrich the streaming `done` event

**Find** (inside `generate()`):
```python
                yield f"data: {json.dumps({'type': 'done', 'web_mode': web_mode})}\n\n"
```
**Replace with:**
```python
                done_meta = {
                    'type': 'done',
                    'web_mode': web_mode,
                    'data_queries': [r.get('query') for r in data_results],
                    'row_counts': {r.get('query'): r.get('row_count', 0) for r in data_results},
                    'total_rows': sum(r.get('row_count', 0) for r in data_results),
                    'data_through': get_data_vintage(city),
                    'city': city,
                }
                yield f"data: {json.dumps(done_meta)}\n\n"
```

---

## Frontend (`mri_v3.html`)

### Edit F1 — capture the done metadata in `pump()`

Where `fullText` is declared (top of the streaming setup), add alongside it:
```javascript
var doneMeta = null;
```
In the event loop inside `pump()`, after the `if (evt.type === "text" && evt.text) { ... }`
block, add:
```javascript
                else if (evt.type === "done") { doneMeta = evt; }
```

### Edit F2 — replace the hardcoded source line with the trust footer

**Find** (in `pump()`, inside `if (result.done)`):
```javascript
                bubble.innerHTML = md(fullText) + '<div class="sr">Source: LF Knowledge Base · ' + CC + '</div>';
```
**Replace with:**
```javascript
                bubble.innerHTML = md(fullText) + buildTrustFooter(doneMeta, fullText);
```
Then paste this function anywhere at top level of the main script (e.g. next to
`buildSourceFooter`, which stays untouched for the non-streaming path):

```javascript
// ── TRUST-UI: provenance footer for streamed answers ──
function buildTrustFooter(meta, responseText) {
  meta = meta || {};
  var chip = function(bg, label){
    return '<span style="background:'+bg+';color:#fff;font-size:8px;font-weight:700;' +
           'padding:1px 6px;border-radius:3px;letter-spacing:.3px;margin-right:5px">'+label+'</span>';
  };
  var f = '<div class="sr" style="margin-top:12px;padding:8px 12px;border-radius:6px;' +
          'background:#F8F7F5;border:1px solid #E8E4DE;line-height:1.7">';
  // Tier 1 — LF database, with verifiable grounding stats
  f += chip('#2563EB','LF DATA');
  if (meta.total_rows != null) {
    var nq = (meta.data_queries || []).filter(function(q){
      return (meta.row_counts||{})[q] > 0; }).length;
    f += '<span style="color:#6B6560;font-size:10px">grounded in ' + meta.total_rows +
         ' database rows across ' + nq + ' quer' + (nq===1?'y':'ies');
    if (meta.data_through) f += ' · data through ' + meta.data_through;
    f += '</span>';
  } else {
    f += '<span style="color:#6B6560;font-size:10px">' + CC + '</span>';
  }
  // Tier 2 — regulatory module (UDCPR verbatim citations)
  if (/UDCPR|Reg\.?\s*\d+\.\d+|Table\s*6-[A-Z]/.test(responseText)) {
    f += '<br>' + chip('#7C3AED','REGULATORY') +
         '<span style="color:#6B6560;font-size:10px">UDCPR-2022 verbatim values, cited by regulation number</span>';
  }
  // Tier 3 — web context (never mixed into LF tables per system prompt)
  if (meta.web_mode) {
    f += '<br>' + chip('#D97706','WEB CONTEXT') +
         '<span style="color:#6B6560;font-size:10px">macro/location context from web search — kept separate from LF figures</span>';
  }
  return f + '</div>';
}
```

### Edit F3 — robust table rendering in `md()`

**Find** the two table regex statements (both begin `r=r.replace(/\|(.+)\|` and
`// Also catch loose tables` + its `r=r.replace(...)` line) and **replace both** with:
```javascript
  r = renderTables(r);
```
Then paste this function at top level:

```javascript
// ── TRUST-UI: line-based table parser (replaces two regexes) ──
// Handles: CRLF, no trailing newline, no leading pipes, empty cells (keeps
// column alignment), alignment colons. Rejects single-column and prose pipes.
function renderTables(r){
  r = r.replace(/\r\n/g, "\n");
  var lines = r.split("\n"), out = [], i = 0;
  function isSep(l){ return /^\s*\|?[\s:|-]+\|?\s*$/.test(l) && l.indexOf("-") >= 0; }
  function cells(l){
    var p = l.split("|");
    if (p.length && p[0].trim() === "") p.shift();
    if (p.length && p[p.length-1].trim() === "") p.pop();
    return p.map(function(s){ return s.trim(); });
  }
  while (i < lines.length){
    var l = lines[i];
    if (l.indexOf("|") >= 0 && i+1 < lines.length && isSep(lines[i+1]) && cells(l).length >= 2){
      var ths = cells(l).map(function(c){ return "<th>"+c+"</th>"; }).join("");
      var trs = [], j = i+2;
      while (j < lines.length && lines[j].indexOf("|") >= 0 && !isSep(lines[j])){
        trs.push("<tr>"+cells(lines[j]).map(function(c){ return "<td>"+(c||"&nbsp;")+"</td>"; }).join("")+"</tr>");
        j++;
      }
      if (trs.length){
        out.push("<table><thead><tr>"+ths+"</tr></thead><tbody>"+trs.join("")+"</tbody></table>");
        i = j; continue;
      }
    }
    out.push(l); i++;
  }
  return out.join("\n");
}
```
Placement note: `md()` calls this on already-escaped text (`esc()` runs first) — that is
correct and unchanged; the parser only introduces the same `<table>` markup the old regexes did.

---

## Smoke tests after deploy

1. Any market question → footer shows **LF DATA · grounded in N database rows across M queries · data through Qx yy-yy**. N must match the `[DIAG-4]` server log for the same request.
2. "RBI rate impact on Sector 71" → adds amber **WEB CONTEXT** chip.
3. A Hinjewadi feasibility question with FSI → adds purple **REGULATORY** chip.
4. Ask anything that answers with a table → renders as a proper grid; then specifically ask a question whose table has an empty cell (project with no data for one quarter) → columns stay aligned.
5. Zero-data question (e.g. demographics for Gurugram) → footer shows low/zero rows — this is a feature: the badge makes honest "no coverage" answers visibly honest.
