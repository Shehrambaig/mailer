"""
Live Realtor data explorer endpoints.

Two grains, both LEFT JOIN inventory ↔ hotness on the natural key for the
latest available month:
  - /api/explore/county   (3,108 rows max)
  - /api/explore/zip      (28,153 rows max)

Each endpoint supports:
  ?q=<text>           global search (county_name / zip_name / county_fips /
                      postal_code, case-insensitive)
  ?sort=<col>         column key to sort by (whitelisted from COLUMN_META)
  ?dir=asc|desc       default desc
  ?offset=N&limit=N   pagination — default offset=0, limit=100
  ?format=csv         streamed CSV (no pagination)
  ?cols=a,b,c         CSV: column subset to include
  ?f.<col>.<op>=<v>   per-column filter — repeatable, AND-combined.
                      Operators by type:
                        numeric  : gte gt lte lt eq ne pct
                        text     : contains starts equals
                        multiselect : in   (comma-separated values)
                      `pct` resolves to a percentile threshold via
                      PERCENTILE_CONT against all rows of the latest month.
                      Values: t10 / t25 (top N% of distribution) or
                              b10 / b25 (bottom N%).

Returns JSON:
  { "rows": [{...}, ...],
    "total": N,                  // rows matching all filters
    "columns": [...],            // column metadata
    "applied_filters": [...]     // resolved filter list (with thresholds for `pct`)
  }

Distinct values for multi-select dropdowns:
  GET /api/explore/<grain>/distinct/<col>
"""
import csv
import io
import os
import re
from typing import Iterable

import psycopg2
import psycopg2.extras
from flask import Blueprint, Response, jsonify, request, stream_with_context

explore_bp = Blueprint("explore", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Column metadata — one definition per grain. `source` is just informational.
# `type` drives client-side formatting (currency / percent / integer / text).
# `default` rows are visible by default; everything else hides behind the
# Show/Hide Columns toggle.
# ─────────────────────────────────────────────────────────────────────────────

# Column types:
#   text           — render as string
#   integer        — render as integer with thousands separators
#   currency       — render as $ with thousands separators
#   ratio          — value × 100 with % suffix and 2 decimals (Realtor's _yy / _mm fields)
#   percent_share  — already-percent value (e.g. price_reduced_share is 0..1) → render as %
#   number         — float with 1-2 decimals

COUNTY_COLUMNS = [
    # default visible
    {"key":"county_fips",            "label":"FIPS",                  "type":"text",      "default":True,  "source":"inv"},
    {"key":"county_name",            "label":"County",                "type":"text",      "default":True,  "source":"inv"},
    {"key":"median_listing_price",   "label":"Median List Price",     "type":"currency",  "default":True,  "source":"inv"},
    {"key":"active_listing_count",   "label":"Active Listings",       "type":"integer",   "default":True,  "source":"inv"},
    {"key":"median_days_on_market",  "label":"DOM",                   "type":"integer",   "default":True,  "source":"inv"},
    {"key":"new_listing_count",      "label":"New Listings",          "type":"integer",   "default":True,  "source":"inv"},
    {"key":"pending_ratio",          "label":"Pending Ratio",         "type":"percent_share", "default":True, "source":"inv"},
    {"key":"foreclosure_count",      "label":"Foreclosures",          "type":"integer",   "default":True,  "source":"fc"},

    # extra inventory cols
    {"key":"median_listing_price_mm","label":"List Price MoM",        "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_listing_price_yy","label":"List Price YoY",        "type":"ratio",     "default":False, "source":"inv"},
    {"key":"active_listing_count_mm","label":"Active Listings MoM",   "type":"ratio",     "default":False, "source":"inv"},
    {"key":"active_listing_count_yy","label":"Active Listings YoY",   "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_days_on_market_mm","label":"DOM MoM",              "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_days_on_market_yy","label":"DOM YoY",              "type":"ratio",     "default":False, "source":"inv"},
    {"key":"new_listing_count_mm",   "label":"New Listings MoM",      "type":"ratio",     "default":False, "source":"inv"},
    {"key":"new_listing_count_yy",   "label":"New Listings YoY",      "type":"ratio",     "default":False, "source":"inv"},
    {"key":"price_increased_count",  "label":"Price Increases #",     "type":"integer",   "default":False, "source":"inv"},
    {"key":"price_increased_share",  "label":"Price Increases %",     "type":"percent_share","default":False,"source":"inv"},
    {"key":"price_reduced_count",    "label":"Price Drops #",         "type":"integer",   "default":False, "source":"inv"},
    {"key":"price_reduced_share",    "label":"Price Drops %",         "type":"percent_share","default":False,"source":"inv"},
    {"key":"pending_listing_count",  "label":"Pending Listings",      "type":"integer",   "default":False, "source":"inv"},
    {"key":"median_listing_price_per_square_foot",
                                     "label":"$ / SqFt",              "type":"currency",  "default":False, "source":"inv"},
    {"key":"median_square_feet",     "label":"Median SqFt",           "type":"integer",   "default":False, "source":"inv"},
    {"key":"average_listing_price",  "label":"Avg List Price",        "type":"currency",  "default":False, "source":"inv"},
    {"key":"total_listing_count",    "label":"Total Listings",        "type":"integer",   "default":False, "source":"inv"},
    {"key":"pending_ratio_mm",       "label":"Pending Ratio MoM",     "type":"ratio",     "default":False, "source":"inv"},
    {"key":"pending_ratio_yy",       "label":"Pending Ratio YoY",     "type":"ratio",     "default":False, "source":"inv"},
    {"key":"quality_flag",           "label":"Quality Flag",          "type":"integer",   "default":False, "source":"inv"},

    # hotness (LEFT JOIN — may be NULL)
    {"key":"hotness_score",          "label":"Hotness Score",         "type":"number",    "default":False, "source":"hot"},
    {"key":"hotness_rank",           "label":"Hotness Rank",          "type":"integer",   "default":False, "source":"hot"},
    {"key":"supply_score",           "label":"Supply Score",          "type":"number",    "default":False, "source":"hot"},
    {"key":"demand_score",           "label":"Demand Score",          "type":"number",    "default":False, "source":"hot"},
    {"key":"median_dom_vs_us",       "label":"DOM vs US",             "type":"ratio",     "default":False, "source":"hot"},
    {"key":"page_view_count_per_property_mm",
                                     "label":"Page Views MoM",        "type":"ratio",     "default":False, "source":"hot"},
    {"key":"page_view_count_per_property_yy",
                                     "label":"Page Views YoY",        "type":"ratio",     "default":False, "source":"hot"},
    {"key":"median_listing_price_vs_us",
                                     "label":"List Price vs US",      "type":"ratio",     "default":False, "source":"hot"},
    {"key":"cbsa_code",              "label":"CBSA Code",             "type":"text",      "default":False, "source":"hot"},
    {"key":"cbsa_title",             "label":"CBSA",                  "type":"text",      "default":False, "source":"hot"},
]


ZIP_COLUMNS = [
    {"key":"postal_code",            "label":"ZIP",                   "type":"text",      "default":True,  "source":"inv"},
    {"key":"zip_name",               "label":"City, State",           "type":"text",      "default":True,  "source":"inv"},
    {"key":"median_listing_price",   "label":"Median List Price",     "type":"currency",  "default":True,  "source":"inv"},
    {"key":"active_listing_count",   "label":"Active Listings",       "type":"integer",   "default":True,  "source":"inv"},
    {"key":"median_days_on_market",  "label":"DOM",                   "type":"integer",   "default":True,  "source":"inv"},
    {"key":"new_listing_count",      "label":"New Listings",          "type":"integer",   "default":True,  "source":"inv"},
    {"key":"pending_ratio",          "label":"Pending Ratio",         "type":"percent_share","default":True, "source":"inv"},
    {"key":"foreclosure_count",      "label":"Foreclosures",          "type":"integer",   "default":True,  "source":"fc"},

    {"key":"median_listing_price_mm","label":"List Price MoM",        "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_listing_price_yy","label":"List Price YoY",        "type":"ratio",     "default":False, "source":"inv"},
    {"key":"active_listing_count_mm","label":"Active Listings MoM",   "type":"ratio",     "default":False, "source":"inv"},
    {"key":"active_listing_count_yy","label":"Active Listings YoY",   "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_days_on_market_mm","label":"DOM MoM",              "type":"ratio",     "default":False, "source":"inv"},
    {"key":"median_days_on_market_yy","label":"DOM YoY",              "type":"ratio",     "default":False, "source":"inv"},
    {"key":"new_listing_count_mm",   "label":"New Listings MoM",      "type":"ratio",     "default":False, "source":"inv"},
    {"key":"new_listing_count_yy",   "label":"New Listings YoY",      "type":"ratio",     "default":False, "source":"inv"},
    {"key":"price_increased_count",  "label":"Price Increases #",     "type":"integer",   "default":False, "source":"inv"},
    {"key":"price_increased_share",  "label":"Price Increases %",     "type":"percent_share","default":False,"source":"inv"},
    {"key":"price_reduced_count",    "label":"Price Drops #",         "type":"integer",   "default":False, "source":"inv"},
    {"key":"price_reduced_share",    "label":"Price Drops %",         "type":"percent_share","default":False,"source":"inv"},
    {"key":"pending_listing_count",  "label":"Pending Listings",      "type":"integer",   "default":False, "source":"inv"},
    {"key":"median_listing_price_per_square_foot",
                                     "label":"$ / SqFt",              "type":"currency",  "default":False, "source":"inv"},
    {"key":"median_square_feet",     "label":"Median SqFt",           "type":"integer",   "default":False, "source":"inv"},
    {"key":"average_listing_price",  "label":"Avg List Price",        "type":"currency",  "default":False, "source":"inv"},
    {"key":"total_listing_count",    "label":"Total Listings",        "type":"integer",   "default":False, "source":"inv"},
    {"key":"pending_ratio_mm",       "label":"Pending Ratio MoM",     "type":"ratio",     "default":False, "source":"inv"},
    {"key":"pending_ratio_yy",       "label":"Pending Ratio YoY",     "type":"ratio",     "default":False, "source":"inv"},
    {"key":"quality_flag",           "label":"Quality Flag",          "type":"integer",   "default":False, "source":"inv"},

    {"key":"hotness_score",          "label":"Hotness Score",         "type":"number",    "default":False, "source":"hot"},
    {"key":"hotness_rank",           "label":"Hotness Rank",          "type":"integer",   "default":False, "source":"hot"},
    {"key":"supply_score",           "label":"Supply Score",          "type":"number",    "default":False, "source":"hot"},
    {"key":"demand_score",           "label":"Demand Score",          "type":"number",    "default":False, "source":"hot"},
    {"key":"median_dom_vs_us",       "label":"DOM vs US",             "type":"ratio",     "default":False, "source":"hot"},
    {"key":"page_view_count_per_property_mm",
                                     "label":"Page Views MoM",        "type":"ratio",     "default":False, "source":"hot"},
    {"key":"page_view_count_per_property_yy",
                                     "label":"Page Views YoY",        "type":"ratio",     "default":False, "source":"hot"},
    {"key":"median_listing_price_vs_us",
                                     "label":"List Price vs US",      "type":"ratio",     "default":False, "source":"hot"},
]


GRAIN_CONFIG = {
    "county": {
        "columns":      COUNTY_COLUMNS,
        "inv_table":    "realtor_inventory_county",
        "hot_table":    "realtor_hotness_county",
        "fc_table":     "foreclosure_counts_by_county",   # precomputed, see scripts/refresh_foreclosure_counts.py
        "fc_key":       "county_fips",
        "key":          "county_fips",
        "search_cols":  ["county_fips", "county_name"],
        "default_sort": "active_listing_count",
    },
    "zip": {
        "columns":      ZIP_COLUMNS,
        "inv_table":    "realtor_inventory_zip",
        "hot_table":    "realtor_hotness_zip",
        "fc_table":     "foreclosure_counts_by_zip",
        "fc_key":       "postal_code",
        "key":          "postal_code",
        "search_cols":  ["postal_code", "zip_name"],
        "default_sort": "active_listing_count",
    },
}


def _conn():
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    c = psycopg2.connect(dsn, connect_timeout=5)
    c.set_session(readonly=True)
    return c


# ─────────────────────────────────────────────────────────────────────────────
# Per-column filter parsing — request.args parses to "f.<col>.<op>=<value>"
# ─────────────────────────────────────────────────────────────────────────────

NUMERIC_TYPES = {"integer", "currency", "ratio", "percent_share", "number"}
TEXT_TYPES    = {"text"}

NUMERIC_OPS = {"gte", "gt", "lte", "lt", "eq", "ne"}
NUMERIC_OP_SQL = {"gte": ">=", "gt": ">", "lte": "<=", "lt": "<", "eq": "=", "ne": "<>"}
TEXT_OPS      = {"contains", "starts", "equals"}
ALL_OPS       = NUMERIC_OPS | TEXT_OPS | {"pct", "in"}

PCT_VALUES = {"t10": 0.90, "t25": 0.75, "b25": 0.25, "b10": 0.10}
PCT_DIR    = {"t10": ">=", "t25": ">=", "b25": "<=", "b10": "<="}


def _col_meta(grain, key):
    for c in GRAIN_CONFIG[grain]["columns"]:
        if c["key"] == key:
            return c
    return None


def _parse_filters(grain, args):
    """Return ([validated_filters], [errors]).
    Each validated_filter is dict {col, op, value, sql_qualifier}."""
    out = []
    errors = []
    for raw_key, raw_val in args.items(multi=True):
        if not raw_key.startswith("f."):
            continue
        # f.<col>.<op>
        parts = raw_key.split(".", 2)
        if len(parts) != 3:
            errors.append(f"bad filter key {raw_key!r}")
            continue
        _, col, op = parts
        meta = _col_meta(grain, col)
        if not meta:
            errors.append(f"unknown column {col!r}")
            continue
        if op not in ALL_OPS:
            errors.append(f"unknown op {op!r}")
            continue

        ctype = meta["type"]
        # Validate op vs type
        if op in NUMERIC_OPS and ctype not in NUMERIC_TYPES:
            errors.append(f"{col} ({ctype}): op {op} only valid for numeric"); continue
        if op in TEXT_OPS and ctype not in TEXT_TYPES:
            errors.append(f"{col} ({ctype}): op {op} only valid for text"); continue
        if op == "pct" and ctype not in NUMERIC_TYPES:
            errors.append(f"{col} ({ctype}): pct only valid for numeric"); continue
        if op == "pct" and raw_val not in PCT_VALUES:
            errors.append(f"pct value must be one of {sorted(PCT_VALUES)}"); continue

        # For numeric: parse value to float; for ratio/percent_share, accept
        # raw percent (25 → 0.25) since the popover shows percent inputs.
        if op in NUMERIC_OPS:
            try:
                v = float(raw_val)
            except (TypeError, ValueError):
                errors.append(f"{col}: {op} requires a number, got {raw_val!r}"); continue
            if ctype in ("ratio", "percent_share") and abs(v) > 5:
                # Heuristic: user typed a percent (e.g. "25" for 25%).
                # If absolute value is suspiciously large (>5), treat as percent and divide.
                v = v / 100.0
        elif op == "in":
            v = [s for s in raw_val.split(",") if s.strip()]
            if not v:
                continue
        else:
            v = str(raw_val)

        out.append({
            "col": col,
            "op":  op,
            "value": v,
            "raw":  raw_val,
            "type": ctype,
            "source": meta["source"],
        })
    return out, errors


def _resolve_pct_thresholds(grain, filters):
    """Replace any 'pct' filter with a 'gte'/'lte' filter using a single
    multi-PERCENTILE_CONT query against all rows of the latest month."""
    pct_filters = [f for f in filters if f["op"] == "pct"]
    if not pct_filters:
        return filters

    cfg = GRAIN_CONFIG[grain]
    inv_t, hot_t, key = cfg["inv_table"], cfg["hot_table"], cfg["key"]

    parts = []
    for i, f in enumerate(pct_filters):
        pct = PCT_VALUES[f["value"]]
        qual = "i" if f["source"] == "inv" else ("h" if f["source"] == "hot" else None)
        if qual is None:
            # foreclosure_count — pull from the precomputed table inline
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY COALESCE(fc.fc_count, 0)) "
                f"FROM {inv_t} i "
                f"LEFT JOIN {cfg['fc_table']} fc ON fc.{cfg['fc_key']} = i.{key} "
                f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})"
            ))
        else:
            join = ""
            if qual == "h":
                join = (f"LEFT JOIN {hot_t} h ON h.{key} = i.{key} "
                        f"AND h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t})")
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY {qual}.{f['col']}) "
                f"FROM {inv_t} i {join} "
                f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})"
            ))

    sql = "SELECT " + ", ".join(f"({s}) AS {n}" for n, s in parts)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    finally:
        conn.close()

    out = [f for f in filters if f["op"] != "pct"]
    for i, f in enumerate(pct_filters):
        thr = row[i]
        if thr is None:
            continue
        thr = float(thr)
        new_op = "gte" if PCT_DIR[f["value"]] == ">=" else "lte"
        out.append({**f, "op": new_op, "value": thr,
                    "pct_resolved_from": f["value"], "pct_threshold": thr})
    return out


def _filter_to_sql(grain, f, qualifier):
    """Translate one validated filter to (where_clause, params)."""
    col = f["col"]
    op  = f["op"]
    val = f["value"]
    ctype = f["type"]
    src   = f["source"]

    if src == "fc":
        ref = "COALESCE(fc.fc_count, 0)"
    else:
        ref = f"{qualifier}.{col}"

    if op in NUMERIC_OPS:
        return f"{ref} {NUMERIC_OP_SQL[op]} %s", [val]
    if op == "contains":
        return f"{ref}::text ILIKE %s", [f"%{val}%"]
    if op == "starts":
        return f"{ref}::text ILIKE %s", [f"{val}%"]
    if op == "equals":
        return f"LOWER({ref}::text) = LOWER(%s)", [val]
    if op == "in":
        if not val:
            return None, []
        ph = ", ".join(["%s"] * len(val))
        return f"{ref}::text IN ({ph})", list(val)
    return None, []


def _build_query(grain: str, q: str, sort: str, sort_dir: str, want_count: bool, filters=None):
    """Return (sql, params, cols_in_order) for the given grain."""
    filters = filters or []
    cfg = GRAIN_CONFIG[grain]
    cols     = cfg["columns"]
    key      = cfg["key"]
    inv_t    = cfg["inv_table"]
    hot_t    = cfg["hot_table"]
    fc_t     = cfg["fc_table"]
    fc_key   = cfg["fc_key"]
    search_cols = cfg["search_cols"]

    inv_cols = [c["key"] for c in cols if c["source"] == "inv" and c["key"] != key]
    hot_cols = [c["key"] for c in cols if c["source"] == "hot"]
    fc_cols  = [c["key"] for c in cols if c["source"] == "fc"]

    # SELECT list — qualify each column to avoid ambiguity (some keys overlap,
    # e.g. median_days_on_market exists in both inventory and hotness).
    select_parts = [f"i.{key} AS {key}"]
    for k in inv_cols:
        select_parts.append(f"i.{k} AS {k}")
    for k in hot_cols:
        select_parts.append(f"h.{k} AS {k}")
    # Foreclosure count comes from a precomputed lookup table; default 0 when
    # the geo has no scraped records.
    if "foreclosure_count" in fc_cols:
        select_parts.append("COALESCE(fc.fc_count, 0) AS foreclosure_count")

    # WHERE clause — pin to latest month and apply optional global search.
    where = [
        f"i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})",
    ]
    params = []
    if q and q.strip():
        like = f"%{q.strip()}%"
        ors = " OR ".join([f"i.{c}::text ILIKE %s" for c in search_cols])
        where.append(f"({ors})")
        params.extend([like] * len(search_cols))

    # Per-column filters — each translates to a clause referencing the
    # appropriate table alias.
    for f in filters:
        qual = "i" if f["source"] == "inv" else ("h" if f["source"] == "hot" else "fc")
        clause, fparams = _filter_to_sql(grain, f, qual)
        if clause:
            where.append(clause)
            params.extend(fparams)

    base_from = (
        f"FROM {inv_t} i "
        f"LEFT JOIN {hot_t} h ON h.{key} = i.{key} "
        f"  AND h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t}) "
        f"LEFT JOIN {fc_t} fc ON fc.{fc_key} = i.{key} "
        f"WHERE {' AND '.join(where)}"
    )

    if want_count:
        return f"SELECT COUNT(*) {base_from}", params, []

    # Validate sort column against the whitelist
    valid_sort_keys = {c["key"] for c in cols if c.get("sortable", True)}
    if sort not in valid_sort_keys:
        sort = cfg["default_sort"]
    sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

    # Pick the right SQL alias for ORDER BY based on which table the column came from
    if sort == "foreclosure_count":
        order_sort = "COALESCE(fc.fc_count, 0)"
    elif sort in (inv_cols + [key]):
        order_sort = f"i.{sort}"
    else:
        order_sort = f"h.{sort}"
    order_sql = f"ORDER BY {order_sort} {sort_dir} NULLS LAST, i.{key} ASC"

    cols_in_order = [key] + inv_cols + hot_cols + fc_cols
    sql = f"SELECT {', '.join(select_parts)} {base_from} {order_sql}"
    return sql, params, cols_in_order


def _fmt_for_csv(val):
    if val is None:
        return ""
    return str(val)


def _stream_csv(grain: str, q: str, sort: str, sort_dir: str, visible_cols: Iterable[str], filters=None):
    cfg = GRAIN_CONFIG[grain]
    visible_cols = list(visible_cols) if visible_cols else [c["key"] for c in cfg["columns"]]
    label_by_key = {c["key"]: c["label"] for c in cfg["columns"]}

    sql, params, _ = _build_query(grain, q, sort, sort_dir, want_count=False, filters=filters or [])

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            colnames = [d.name for d in cur.description]
            idx = {n: i for i, n in enumerate(colnames)}

            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow([label_by_key.get(k, k) for k in visible_cols])
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

            BATCH = 2000
            while True:
                rows = cur.fetchmany(BATCH)
                if not rows:
                    break
                for row in rows:
                    w.writerow([_fmt_for_csv(row[idx[k]]) if k in idx else "" for k in visible_cols])
                yield buf.getvalue()
                buf.seek(0); buf.truncate(0)
    finally:
        conn.close()


@explore_bp.route("/api/explore/<grain>")
def explore(grain):
    if grain not in GRAIN_CONFIG:
        return jsonify({"error": f"unknown grain '{grain}'; expected 'county' or 'zip'"}), 400

    q        = request.args.get("q", "")
    sort     = request.args.get("sort", GRAIN_CONFIG[grain]["default_sort"])
    sort_dir = request.args.get("dir", "desc")
    fmt      = request.args.get("format", "json").lower()

    raw_filters, filter_errors = _parse_filters(grain, request.args)
    filters = _resolve_pct_thresholds(grain, raw_filters)

    if fmt == "csv":
        visible = request.args.get("cols", "")
        visible_cols = [c for c in visible.split(",") if c] or None
        filename = f"realtor_{grain}_{request.args.get('q','all')}.csv"
        return Response(
            stream_with_context(_stream_csv(grain, q, sort, sort_dir, visible_cols, filters=filters)),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-cache",
            },
        )

    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        offset = 0
    try:
        limit = min(500, max(1, int(request.args.get("limit", 100))))
    except ValueError:
        limit = 100

    sql, params, cols_in_order = _build_query(grain, q, sort, sort_dir, want_count=False, filters=filters)
    paged_sql = f"{sql} LIMIT {limit} OFFSET {offset}"

    count_sql, count_params, _ = _build_query(grain, q, sort, sort_dir, want_count=True, filters=filters)

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(count_sql, count_params)
            total = cur.fetchone()[0]
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(paged_sql, params)
            rows = []
            for r in cur.fetchall():
                row = {}
                for k, v in r.items():
                    if v is None:
                        row[k] = None
                    elif hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
                        try:
                            row[k] = float(v)
                        except Exception:
                            row[k] = str(v)
                    else:
                        row[k] = v
                rows.append(row)
    finally:
        conn.close()

    return jsonify({
        "grain":   grain,
        "rows":    rows,
        "total":   int(total),
        "offset":  offset,
        "limit":   limit,
        "sort":    sort,
        "dir":     "asc" if sort_dir.lower() == "asc" else "desc",
        "columns": GRAIN_CONFIG[grain]["columns"],
        "applied_filters": [
            {"col": f["col"], "op": f["op"], "value": f["value"],
             "pct_resolved_from": f.get("pct_resolved_from"),
             "pct_threshold": f.get("pct_threshold")}
            for f in filters
        ],
        "filter_errors": filter_errors,
    })


@explore_bp.route("/api/explore/<grain>/distinct/<col>")
def explore_distinct(grain, col):
    """Distinct values for a column — feeds the multi-select filter dropdown."""
    if grain not in GRAIN_CONFIG:
        return jsonify({"error": f"unknown grain '{grain}'"}), 400
    meta = _col_meta(grain, col)
    if not meta:
        return jsonify({"error": f"unknown column '{col}'"}), 400

    cfg = GRAIN_CONFIG[grain]
    inv_t, hot_t, key = cfg["inv_table"], cfg["hot_table"], cfg["key"]
    src = meta["source"]

    if src == "inv":
        sql = (f"SELECT DISTINCT i.{col} AS v FROM {inv_t} i "
               f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t}) "
               f"AND i.{col} IS NOT NULL ORDER BY v ASC LIMIT 500")
    elif src == "hot":
        sql = (f"SELECT DISTINCT h.{col} AS v FROM {hot_t} h "
               f"WHERE h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t}) "
               f"AND h.{col} IS NOT NULL ORDER BY v ASC LIMIT 500")
    else:
        return jsonify({"error": "distinct values not supported for this column"}), 400

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            values = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"col": col, "values": values, "count": len(values)})
