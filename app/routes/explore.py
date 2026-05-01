"""
Live Realtor data explorer endpoints.

Two grains, both LEFT JOIN inventory ↔ hotness on the natural key for the
latest available month:
  - /api/explore/county   (3,108 rows max)
  - /api/explore/zip      (28,153 rows max)

Each endpoint supports:
  ?q=<text>           global search (matches across county_name / zip_name /
                      county_fips / state / postal_code, case-insensitive)
  ?sort=<col>         column key to sort by (whitelisted from COLUMN_META)
  ?dir=asc|desc       default desc
  ?offset=N&limit=N   pagination — default offset=0, limit=100
  ?format=csv         download response as text/csv (no offset/limit applied)
  ?cols=a,b,c         optional column subset (CSV export uses this for "visible only")

Returns JSON:
  { "rows": [{...}, ...],
    "total": <int>,             // total rows matching the query (after q filter)
    "columns": [                 // column metadata for the frontend
      {"key":"county_fips","label":"FIPS","type":"text","default":true,"sortable":true,"source":"inv"},
      ...
    ]
  }
"""
import csv
import io
import os
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
        "key":          "county_fips",
        "search_cols":  ["county_fips", "county_name"],   # case-insensitive ILIKE
        "default_sort": "active_listing_count",
    },
    "zip": {
        "columns":      ZIP_COLUMNS,
        "inv_table":    "realtor_inventory_zip",
        "hot_table":    "realtor_hotness_zip",
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


def _build_query(grain: str, q: str, sort: str, sort_dir: str, want_count: bool):
    """Return (sql, params, cols_in_order) for the given grain."""
    cfg = GRAIN_CONFIG[grain]
    cols    = cfg["columns"]
    key     = cfg["key"]
    inv_t   = cfg["inv_table"]
    hot_t   = cfg["hot_table"]
    search_cols = cfg["search_cols"]

    inv_cols = [c["key"] for c in cols if c["source"] == "inv" and c["key"] != key]
    hot_cols = [c["key"] for c in cols if c["source"] == "hot"]

    # SELECT list — qualify each column to avoid ambiguity (some keys overlap, e.g.
    # median_days_on_market exists in both inventory and hotness).
    select_parts = [f"i.{key} AS {key}"]
    for k in inv_cols:
        select_parts.append(f"i.{k} AS {k}")
    for k in hot_cols:
        select_parts.append(f"h.{k} AS {k}")

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

    base_from = (
        f"FROM {inv_t} i "
        f"LEFT JOIN {hot_t} h ON h.{key} = i.{key} "
        f"  AND h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t}) "
        f"WHERE {' AND '.join(where)}"
    )

    if want_count:
        return f"SELECT COUNT(*) {base_from}", params, []

    # Validate sort column against the column whitelist
    valid_sort_keys = {c["key"] for c in cols if c.get("sortable", True)}
    if sort not in valid_sort_keys:
        sort = cfg["default_sort"]
    sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"
    sort_qualifier = "i" if sort in (inv_cols + [key]) else "h"
    order_sql = f"ORDER BY {sort_qualifier}.{sort} {sort_dir} NULLS LAST, i.{key} ASC"

    cols_in_order = [key] + inv_cols + hot_cols
    sql = f"SELECT {', '.join(select_parts)} {base_from} {order_sql}"
    return sql, params, cols_in_order


def _fmt_for_csv(val):
    if val is None:
        return ""
    return str(val)


def _stream_csv(grain: str, q: str, sort: str, sort_dir: str, visible_cols: Iterable[str]):
    cfg = GRAIN_CONFIG[grain]
    visible_cols = list(visible_cols) if visible_cols else [c["key"] for c in cfg["columns"]]
    label_by_key = {c["key"]: c["label"] for c in cfg["columns"]}

    sql, params, _ = _build_query(grain, q, sort, sort_dir, want_count=False)

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

    if fmt == "csv":
        visible = request.args.get("cols", "")
        visible_cols = [c for c in visible.split(",") if c] or None
        filename = f"realtor_{grain}_{request.args.get('q','all')}.csv"
        return Response(
            stream_with_context(_stream_csv(grain, q, sort, sort_dir, visible_cols)),
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

    sql, params, cols_in_order = _build_query(grain, q, sort, sort_dir, want_count=False)
    paged_sql = f"{sql} LIMIT {limit} OFFSET {offset}"

    count_sql, count_params, _ = _build_query(grain, q, sort, sort_dir, want_count=True)

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
    })
