"""
AI agent for the dashboard. Streams via SSE.

Tools available to the model:
  - query_database(sql) — read-only SELECT against the `probate` Postgres DB.
    Hard caps: SELECT only, LIMIT enforced (default 200, max 1000), 10s timeout,
    read-only transaction, ~30 result columns/row max returned to the model.

Architecture: manual agentic loop with streaming. We stream text deltas to the
browser as they arrive; when the model emits a tool_use block we execute the
SQL, surface a `tool` event so the UI can show "🔎 querying...", then continue
the loop until stop_reason='end_turn' or we hit MAX_ITERATIONS.

The system prompt (model-stable) is cached with `cache_control: ephemeral` so
follow-up turns cost ~10% input on the prefix.
"""
import json
import os
import re
import time

import anthropic
import psycopg2
import psycopg2.extras
from flask import Blueprint, Response, jsonify, request, stream_with_context

chat_bp = Blueprint("chat", __name__)

MODEL = "claude-opus-4-7"
MAX_ITERATIONS = 6           # tool-use → result → text rounds
DEFAULT_ROW_LIMIT = 200
HARD_ROW_LIMIT = 1000
STATEMENT_TIMEOUT_MS = 10_000

# ── SQL safety ─────────────────────────────────────────────────────────────
# Word-boundary regex; ignores keywords inside strings only loosely (we also
# wrap every query in a read-only transaction so writes can't land regardless).
DENY_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|"
    r"vacuum|reindex|copy|comment|do|call|rollback|commit|begin|"
    r"savepoint|set|reset|notify|listen|unlisten|cluster|lock)\b",
    re.IGNORECASE,
)
SELECT_RE = re.compile(r"^\s*(with\s+.*?\)\s*select|select)\b", re.IGNORECASE | re.DOTALL)
LIMIT_RE = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)


def _sanitize_sql(sql: str):
    """Return (sql, error). Caller bails if error is set."""
    if not sql or not isinstance(sql, str):
        return None, "sql must be a non-empty string"
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        return None, "sql is empty"
    if ";" in sql:
        return None, "only a single statement is allowed (no ';' chaining)"
    if not SELECT_RE.match(sql):
        return None, "only SELECT (or WITH … SELECT) statements are allowed"
    if DENY_KEYWORDS.search(sql):
        return None, "query contains a disallowed keyword (DDL/DML/transaction control)"
    # Enforce LIMIT
    m = LIMIT_RE.search(sql)
    if m:
        n = int(m.group(1))
        if n > HARD_ROW_LIMIT:
            sql = LIMIT_RE.sub(f"LIMIT {HARD_ROW_LIMIT}", sql, count=1)
    else:
        sql = f"{sql} LIMIT {DEFAULT_ROW_LIMIT}"
    return sql, None


def _run_query(sql: str):
    """Execute sanitized SQL in a read-only txn. Return (rows, columns, error)."""
    sql, err = _sanitize_sql(sql)
    if err:
        return None, None, err
    # Dedicated knob — prefers Neon (NEON_DB), then PROBATE_DATABASE_URL, then local.
    # Don't reuse DATABASE_URL, which is the app's own (different) DB for leads/campaigns/etc.
    dsn = (
        os.getenv("NEON_DB")
        or os.getenv("PROBATE_DATABASE_URL")
        or "postgresql://localhost:5432/probate"
    )
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        conn.set_session(readonly=True, autocommit=False)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}")
                t0 = time.monotonic()
                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
                cols = [d.name for d in (cur.description or [])]
                ms = int((time.monotonic() - t0) * 1000)
                # Coerce non-JSON-serializable types
                clean = []
                for r in rows:
                    row = {}
                    for k, v in r.items():
                        if hasattr(v, "isoformat"):
                            row[k] = v.isoformat()
                        elif isinstance(v, (bytes, bytearray)):
                            row[k] = "<bytes>"
                        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
                            try:
                                row[k] = float(v)
                            except Exception:
                                row[k] = str(v)
                        else:
                            row[k] = v
                    clean.append(row)
                return {"rows": clean, "columns": cols, "row_count": len(clean), "elapsed_ms": ms, "sql_executed": sql}, cols, None
        finally:
            conn.close()
    except psycopg2.errors.QueryCanceled:
        return None, None, f"query exceeded {STATEMENT_TIMEOUT_MS}ms statement timeout"
    except Exception as e:
        return None, None, f"{type(e).__name__}: {str(e)[:300]}"


# ── System prompt (cached) ────────────────────────────────────────────────
SYSTEM_PROMPT = """You are the AI agent for "Market Intel" — a real-estate dashboard for a direct-mail company that buys distressed houses for cash.

Your job: explain the dashboard, answer questions about it, and — when the user asks something that requires live data — query the Postgres database to get exact answers. Do NOT fabricate numbers. If a question needs live data, use the `query_database` tool.

## When to use the tool

Use `query_database` when the user asks anything that requires current, specific, row-level facts — counts, sums, breakdowns, lookups by ZIP/county/state, sample listings, "show me top X", "how many", "what's the breakdown of Y in ZIP Z", etc.

Do NOT use it for:
- Conceptual questions ("what is the Golden Score?") — you already know these from below.
- Map metrics (Buy/Exit/Golden, ZHVI, DOM, Heat) — those come from Realtor/Zillow/Redfin static feeds, not the DB.

## Database schema — Postgres `foreclosure_records`

The connection is already pointed at the right database. **Refer to the table as just `foreclosure_records` (unqualified) — do NOT prefix it with `probate.`** (schema is `public`).


```
foreclosure_records (882,188 rows; ~821,508 with status='active')
  id              BIGINT
  source          VARCHAR    -- 'foreclosure_com' (866k) or 'auction_com' (16k)
  listing_id      VARCHAR
  street          TEXT
  city            TEXT
  state           CHAR(2)
  zip             VARCHAR    -- 5-digit, always populated for f-com; needed for joining to map
  county          TEXT       -- ONLY set for auction_com (NULL for foreclosure_com)
  full_address    TEXT
  latitude        NUMERIC
  longitude       NUMERIC
  bedrooms        NUMERIC
  bathrooms       NUMERIC
  square_footage  INTEGER
  year_built      SMALLINT
  property_type   TEXT
  auction_date    DATE
  listed_on       DATE
  status          TEXT       -- 'active' (821k), 'inactive' (44k), 'SALE_PENDING', etc.
  estimated_value NUMERIC
  starting_bid    NUMERIC
  details_url     TEXT
  classification  TEXT       -- see buckets below
  scraped_at      TIMESTAMPTZ
```

### Classification values (use exact strings — case sensitive)

**auction_com** (all are mailable auctions):
  TRUSTEE (12,202), DAY_1_REO (2,516), REO (1,086), PRIVATE_SELLER (274), REDEMPTION (151), PRIVATE_SELLER_INSPECTION (50)

**foreclosure_com** — mailable buckets we keep:
  'Tax Lien' (282k), 'Chapter 13 Filed' (248k), 'Chapter 7 Filed' (93k),
  'Auction' (53k), 'Foreclosure' (16k), 'Short Sale' (3k), 'Chapter 11 Filed' (1k),
  'Chapter 12 Filed', 'Bankruptcy', 'Chapter 15 Filed'

**foreclosure_com — DROP, not mailable:**
  'Rent to Own', 'city-owned', 'HUD', 'VA', 'REO' (listing tag, ≠ a buyable REO),
  'fixer-upper', 'Deal', 'One Hundred Down', 'Redemption'

### Mail-target bucket helpers (use these in WHERE clauses)

```sql
-- All mailable rows
(source='auction_com' AND classification IN ('TRUSTEE','DAY_1_REO','REO','PRIVATE_SELLER','PRIVATE_SELLER_INSPECTION'))
OR
(source='foreclosure_com' AND classification IN
    ('Auction','Foreclosure','Tax Lien','Short Sale',
     'Chapter 7 Filed','Chapter 11 Filed','Chapter 12 Filed','Chapter 13 Filed','Chapter 15 Filed','Bankruptcy'))

-- Auctions only (the strongest mail target)
(source='auction_com' AND classification IN ('TRUSTEE','DAY_1_REO','REO','PRIVATE_SELLER','PRIVATE_SELLER_INSPECTION'))
OR (source='foreclosure_com' AND classification = 'Auction')
```

## SQL constraints

- The tool is **read-only**: SELECT only, no DDL/DML/transaction control. Single statement per call. LIMIT is forced (max 1000, default 200) — request what you need; the tool will append LIMIT if missing.
- Statement timeout is 10s; write queries that scan with predicates (`WHERE status='active' AND zip='90210'`), don't `SELECT *` from the whole table.
- foreclosure_com rows have `county` NULL — never filter on `county` for them; filter on `zip` instead.
- For county-scoped questions, **call `zips_in_county(fips)` first** to get the authoritative ZIP list, then write a SQL with `zip IN (...)`. Don't guess ZIPs from training data.
- Use COUNT(*), GROUP BY, aggregates, and ORDER BY freely.
- Quote case-sensitive classification strings exactly: `classification = 'Tax Lien'` (not `'tax lien'`).
- After running a query, summarize the result for the user in 2-4 sentences. Don't dump every row — pick highlights and reason about them.

## The dashboard (for conceptual answers)

Drill chain: USA → State → County → ZIP grid (double-click a county or click "ZIP View ›").

**Three composite scores (0–100, all in `county-heatmap.json` and `zip-heatmap.json`):**
- Golden Zone = √(Buy × Exit). Both must be strong — asymmetric markets get punished.
- Buy Opportunity — Price Reduction % (primary), YoY price trend, stale inventory ratio.
- Exit Speed — DOM (primary, lower=faster), Pending Ratio, Sale-to-List ratio.

**Mail Targets layer (★)** — `mail_score = log10(weighted_mailable_count + 1) × (exit_score/100)^0.6 × 100`. Weights: auctions 1.0, foreclosures 1.2, tax liens 0.5, bankruptcies 0.4, short sales 0.6.

**Layers**: Golden, Buy×Exit Matrix, Exit Speed, Buy Opportunity, Home Value (Zillow ZHVI), Days on Market, Price Drops %, Income (Census), Pre-Foreclosures, Auctions (count), All Listings, **Mail Targets ★**.

**Sources**: Realtor.com Research, Zillow Research (ZHVI + Heat Index), Redfin Data Center, Census ACS, FHFA HPI. The internal `probate.foreclosure_records` is the source for Mail Targets and the only thing your tool can query.

## "Is this county/state worth the hassle?" — synthesis rubric

When the user asks whether a region is worth pursuing, **don't just look at one metric**. Pull both the market stack (`get_county_metrics` / `get_zip_metrics` / `state_top_counties`) AND the live distress data (`query_database` after `zips_in_county` if needed). Then judge against this rubric:

| Factor | Strong (✅) | Marginal | Avoid (❌) |
|---|---|---|---|
| Exit Speed | ≥ 65 | 50–65 | < 45 |
| Market Heat (Zillow) | ≥ 60 (Seller market) | 35–60 | < 35 (Strong Buyer) |
| Mail Targets total (active mailable) | ≥ 1,000 | 200–1,000 | < 100 (no scale) |
| Auction count (next 60 days) | ≥ 50 | 10–50 | < 5 |
| DOM | ≤ 45 days | 45–75 | > 90 (slow exit) |
| Sale-to-list | ≥ 99% | 96–99% | < 95% (bidders flat) |

**Verdict structure** (always include all three):
1. **Worth it / Marginal / Skip** — one-word call.
2. **Why** — name the 2–3 metrics that drove the call. Cite actual numbers.
3. **Tactic** — if Worth it: which buckets to mail (auctions vs tax-lien vs Ch13) and over what date window. If Marginal: what to test first. If Skip: what would change your mind.

Example shape:
> **Camden NJ — Worth it.** Exit Speed 78, Heat 71 (Seller market), 17,499 mailable records with 199 auctions in next 60 days. Lead with auction-window mail (next 60d) → fall back to Ch13 filings as a slower-funnel pool. Skip pure tax-lien blasts here unless ROI per piece is dialed in — too much volume to mail blind.

When asked about a STATE: call `state_top_counties` with `metric=mail_score` (or `g` for raw market quality). Report the top 5–10, identify which clusters carry the state's mail value, and tell the user where to focus rather than blanket-mailing.

## How to behave

- Concise. 2–4 sentences for most answers; expand only if the user asks for depth.
- Use the user's `[CONTEXT FROM CURRENT VIEW]` (selected county, layer, ZIP) to ground answers.
- For data questions: form a tight SQL query, run it, then explain the result. Don't recite the SQL unless the user asks.
- For score/concept questions: answer from the knowledge above; don't query the DB.
- Never fabricate numbers. If unsure, query."""


TOOLS = [
    {
        "name": "query_database",
        "description": (
            "Execute a read-only SELECT against the probate Postgres database "
            "(table `foreclosure_records`). Returns the result rows as JSON. "
            "Single statement only; LIMIT is enforced (max 1000); 10s timeout. "
            "Use specific WHERE predicates — do not scan the whole 882k-row table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A single SELECT statement. No semicolons, no DDL/DML."
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "zips_in_county",
        "description": (
            "Return the authoritative list of ZIP codes that belong to a county, by FIPS code. "
            "Use this BEFORE writing a SQL query that needs to scope to a county — foreclosure_com "
            "rows have county=NULL, so you must filter by zip IN (...). Source: Census ZCTA polygons "
            "intersected with county polygons (built into the dashboard)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fips": {"type": "string", "description": "5-digit county FIPS code (e.g., '34007' for Camden NJ)"},
            },
            "required": ["fips"],
        },
    },
    {
        "name": "get_county_metrics",
        "description": (
            "Return the full Realtor/Zillow/Redfin market metric stack for a county PLUS the "
            "Mail Targets aggregates from our DB. Includes: Golden/Buy/Exit scores, ZHVI home value + YoY, "
            "Zillow Market Heat Index, Realtor DOM/pending ratio/price drops/active listings, "
            "Redfin sale price/sale-to-list, and mail_score/mail_total/bucket counts. "
            "Use this whenever you need the market context for a county — don't ask the user, look it up."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fips": {"type": "string", "description": "5-digit FIPS code"},
            },
            "required": ["fips"],
        },
    },
    {
        "name": "get_zip_metrics",
        "description": (
            "Same as get_county_metrics but for a ZIP code — returns Zillow ZHVI, Heat Index, "
            "Realtor DOM/pending/price drops at ZIP level, and the composite scores. "
            "ZIPs may have sparser data than counties (Realtor only covers ~28k of ~33k ZIPs)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "zip": {"type": "string", "description": "5-digit ZIP code"},
            },
            "required": ["zip"],
        },
    },
    {
        "name": "state_top_counties",
        "description": (
            "Return the top N counties in a state ranked by a chosen metric. Useful for "
            "answering 'which counties in [state] are worth pursuing'. Available metrics: "
            "g (Golden), bs (Buy), es (Exit), mail_score, mail_total, mail_auc (auction count), "
            "v (home value), pr (price drops %), d (DOM), zh (Zillow Heat Index)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {"type": "string", "description": "2-letter state code (e.g., 'NJ', 'CA')"},
                "metric": {"type": "string", "description": "Metric key to rank by", "default": "g"},
                "limit": {"type": "integer", "description": "Max counties to return (default 20)", "default": 20},
            },
            "required": ["state"],
        },
    },
]


_static_cache = {}

def _load_static(name):
    if name not in _static_cache:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "data", name)
        try:
            with open(path) as f:
                _static_cache[name] = json.load(f)
        except Exception:
            _static_cache[name] = {}
    return _static_cache[name]


def _zips_in_county(fips):
    fips = str(fips).zfill(5)
    zips = _load_static("zip-by-county.json").get(fips, [])
    return {"fips": fips, "zip_count": len(zips), "zips": zips}


# Friendly label map for the compact heatmap fields
HEATMAP_FIELD_LABELS = {
    "g":   "golden_score", "bs": "buy_score", "es": "exit_score",
    "v":   "zillow_zhvi_home_value", "vy": "home_value_yoy_pct",
    "d":   "median_days_on_market",  "pr": "price_drops_pct",
    "ppr": "pending_ratio_pct",      "lp": "median_list_price",
    "sp":  "median_sale_price",      "a":  "active_listings",
    "zh":  "zillow_heat_index",      "zhc":"market_classification",
    "name": "county_name",            "sc": "state_code",
    "fc_ct": "scraped_pre_foreclosures",
    "au_ct": "scraped_auctions",
    "tot_ct": "scraped_total_listings",
    "mail_score": "mail_targets_score",
    "mail_total": "mail_targets_total_records",
    "mail_auc":   "mail_buc_auctions",
    "mail_fc":    "mail_buc_foreclosures",
    "mail_tl":    "mail_buc_tax_liens",
    "mail_bk":    "mail_buc_bankruptcies",
    "mail_ss":    "mail_buc_short_sales",
}

def _expand_metrics(d):
    """Replace short keys with friendly names for clearer reasoning by the model."""
    return {HEATMAP_FIELD_LABELS.get(k, k): v for k, v in d.items()}


def _get_county_metrics(fips):
    fips = str(fips).zfill(5)
    h = _load_static("county-heatmap.json")
    d = h.get(fips)
    if not d:
        return {"error": f"No county-level data for FIPS {fips}"}
    return {"fips": fips, "metrics": _expand_metrics(d)}


def _get_zip_metrics(zip5):
    z = str(zip5).zfill(5)
    h = _load_static("zip-heatmap.json")
    d = h.get(z)
    if not d:
        return {"error": f"No ZIP-level data for {z}"}
    return {"zip": z, "metrics": _expand_metrics(d)}


# State abbr → FIPS prefix
_STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19",
    "KS":"20","KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27",
    "MS":"28","MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35",
    "NY":"36","NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44",
    "SC":"45","SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53",
    "WV":"54","WI":"55","WY":"56",
}

def _state_top_counties(state, metric="g", limit=20):
    state = (state or "").upper()
    prefix = _STATE_FIPS.get(state)
    if not prefix:
        return {"error": f"Unknown state code: {state}"}
    if not isinstance(limit, int) or limit < 1: limit = 20
    if limit > 100: limit = 100
    h = _load_static("county-heatmap.json")
    in_state = [(f, d) for f, d in h.items() if f.startswith(prefix)]
    in_state.sort(key=lambda x: (x[1].get(metric) or 0), reverse=True)
    rows = []
    for f, d in in_state[:limit]:
        row = {"fips": f, "county": d.get("name"), "state": d.get("sc")}
        row.update(_expand_metrics({k: d.get(k) for k in (
            "g","bs","es","mail_score","mail_total","mail_auc","mail_fc","mail_tl","mail_bk",
            "v","vy","d","pr","ppr","zh","zhc","a"
        ) if k in d}))
        rows.append(row)
    return {"state": state, "metric": metric, "ranked_by": HEATMAP_FIELD_LABELS.get(metric, metric),
            "count": len(rows), "counties": rows}


def _client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    return anthropic.Anthropic(api_key=api_key)


def _format_context(ctx):
    if not ctx:
        return None
    lines = ["Current view state:"]
    if ctx.get("view"):  lines.append(f"- map mode: {ctx['view']}")
    if ctx.get("layer"): lines.append(f"- active layer: {ctx['layer']}")
    if ctx.get("fips"):  lines.append(f"- selected county FIPS: {ctx['fips']} ({ctx.get('county_name','')}, {ctx.get('state','')})")
    if ctx.get("zip"):   lines.append(f"- selected ZIP: {ctx['zip']}")
    if ctx.get("metrics"):
        lines.append(f"- visible metrics: {json.dumps(ctx['metrics'])}")
    return "\n".join(lines) if len(lines) > 1 else None


def _sse(obj):
    return "data: " + json.dumps(obj) + "\n\n"


@chat_bp.route("/api/chat", methods=["POST"])
def chat():
    payload = request.get_json(silent=True) or {}
    user_messages = payload.get("messages") or []
    ctx = payload.get("context") or {}

    if not user_messages or not isinstance(user_messages, list):
        return jsonify({"error": "messages array required"}), 400

    try:
        client = _client()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500

    # Build initial messages: optional context preamble + user history
    context_text = _format_context(ctx)
    messages = []
    if context_text:
        messages.append({"role": "user", "content": f"[CONTEXT FROM CURRENT VIEW]\n{context_text}"})
        messages.append({"role": "assistant", "content": "Got it — I'll keep that in mind."})
    for m in user_messages:
        if m.get("role") in ("user", "assistant") and m.get("content"):
            messages.append({"role": m["role"], "content": m["content"]})

    def generate():
        try:
            input_total = output_total = cache_read = 0

            for iteration in range(MAX_ITERATIONS):
                with client.messages.stream(
                    model=MODEL,
                    max_tokens=2048,
                    system=[{
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    tools=TOOLS,
                    messages=messages,
                    thinking={"type": "adaptive"},
                    output_config={"effort": "medium"},
                ) as stream:
                    for event in stream:
                        if event.type == "content_block_delta":
                            d = event.delta
                            if getattr(d, "type", None) == "text_delta":
                                yield _sse({"delta": d.text})

                    final = stream.get_final_message()

                input_total += final.usage.input_tokens
                output_total += final.usage.output_tokens
                cache_read += getattr(final.usage, "cache_read_input_tokens", 0) or 0

                # Append assistant turn (preserve thinking + tool_use blocks)
                messages.append({"role": "assistant", "content": final.content})

                if final.stop_reason != "tool_use":
                    yield _sse({"done": True, "usage": {
                        "input": input_total, "output": output_total,
                        "cache_read": cache_read, "iterations": iteration + 1,
                    }})
                    return

                # Execute every tool_use block in the response
                tool_results = []
                for block in final.content:
                    if getattr(block, "type", None) != "tool_use":
                        continue
                    if block.name == "query_database":
                        sql = (block.input or {}).get("sql", "")
                        yield _sse({"tool": "query_database", "sql": sql, "status": "running"})
                        result, _, err = _run_query(sql)
                        if err:
                            yield _sse({"tool": "query_database", "status": "error", "error": err})
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": f"Query error: {err}",
                                "is_error": True,
                            })
                        else:
                            yield _sse({
                                "tool": "query_database", "status": "ok",
                                "row_count": result["row_count"],
                                "elapsed_ms": result["elapsed_ms"],
                            })
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": json.dumps(result, default=str),
                            })
                    elif block.name == "zips_in_county":
                        fips = (block.input or {}).get("fips", "")
                        yield _sse({"tool": "zips_in_county", "fips": fips, "status": "running"})
                        result = _zips_in_county(fips)
                        yield _sse({"tool": "zips_in_county", "status": "ok", "row_count": result["zip_count"]})
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result),
                        })
                    elif block.name == "get_county_metrics":
                        fips = (block.input or {}).get("fips", "")
                        yield _sse({"tool": "get_county_metrics", "fips": fips, "status": "running"})
                        result = _get_county_metrics(fips)
                        yield _sse({"tool": "get_county_metrics", "status": "error" if result.get("error") else "ok",
                                    "row_count": 0 if result.get("error") else 1,
                                    "error": result.get("error")})
                        tool_results.append({
                            "type": "tool_result", "tool_use_id": block.id,
                            "content": json.dumps(result),
                            **({"is_error": True} if result.get("error") else {}),
                        })
                    elif block.name == "get_zip_metrics":
                        z = (block.input or {}).get("zip", "")
                        yield _sse({"tool": "get_zip_metrics", "zip": z, "status": "running"})
                        result = _get_zip_metrics(z)
                        yield _sse({"tool": "get_zip_metrics", "status": "error" if result.get("error") else "ok",
                                    "row_count": 0 if result.get("error") else 1,
                                    "error": result.get("error")})
                        tool_results.append({
                            "type": "tool_result", "tool_use_id": block.id,
                            "content": json.dumps(result),
                            **({"is_error": True} if result.get("error") else {}),
                        })
                    elif block.name == "state_top_counties":
                        args = block.input or {}
                        st = args.get("state", "")
                        metric = args.get("metric", "g")
                        limit = args.get("limit", 20)
                        yield _sse({"tool": "state_top_counties", "state": st, "metric": metric, "status": "running"})
                        result = _state_top_counties(st, metric, limit)
                        yield _sse({"tool": "state_top_counties",
                                    "status": "error" if result.get("error") else "ok",
                                    "row_count": 0 if result.get("error") else result.get("count", 0),
                                    "error": result.get("error")})
                        tool_results.append({
                            "type": "tool_result", "tool_use_id": block.id,
                            "content": json.dumps(result),
                            **({"is_error": True} if result.get("error") else {}),
                        })
                    else:
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": f"Unknown tool: {block.name}",
                            "is_error": True,
                        })

                messages.append({"role": "user", "content": tool_results})

            # Hit max iterations without end_turn
            yield _sse({"done": True, "warning": f"Reached {MAX_ITERATIONS} iterations without final answer", "usage": {
                "input": input_total, "output": output_total, "cache_read": cache_read,
            }})
        except anthropic.APIError as e:
            msg = getattr(e, "message", None) or str(e)
            yield _sse({"error": f"Claude API error: {msg}"})
        except Exception as e:
            yield _sse({"error": f"{type(e).__name__}: {str(e)[:300]}"})

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@chat_bp.route("/api/chat/health")
def chat_health():
    return jsonify({"ok": bool(os.getenv("ANTHROPIC_API_KEY")), "model": MODEL, "tools": [t["name"] for t in TOOLS]})
