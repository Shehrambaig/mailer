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
import ast
import csv
import io
import json
import os
import re
from typing import Iterable

import psycopg2
import psycopg2.extras
from flask import Blueprint, Response, jsonify, request, stream_with_context

explore_bp = Blueprint("explore", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# User-tunable Exit Speed weights.
#
# The dashboard's heatmap uses the full 6-signal multi-source formula in
# app/ingest/market_data.py:compute_exit_score. /explore re-computes the
# score per request from the 3 Realtor signals that exist in Postgres so
# the user can tweak weights live (Redfin/Zillow signals are file-only).
# Defaults renormalize the original {dom: 0.40, pending: 0.30, yoy: 0.03}
# so the three remaining weights still sum to 1.
# ─────────────────────────────────────────────────────────────────────────────
EXIT_WEIGHT_DEFAULTS = {"dom": 0.55, "pending": 0.41, "yoy": 0.04}
EXIT_WEIGHT_KEYS = ("dom", "pending", "yoy")

# (x, y) breakpoints — same as compute_exit_score in app/ingest/market_data.py.
DOM_BREAKPOINTS     = [(15, 100), (30, 70), (60, 30), (90, 10), (150, 0)]
PENDING_BREAKPOINTS = [(0.03, 0), (0.10, 20), (0.25, 65), (0.40, 100)]
YOY_BREAKPOINTS     = [(-0.05, 0), (0, 50), (0.05, 85), (0.10, 100)]


def _piecewise_sql(col_expr: str, points) -> str:
    """SQL CASE that maps `col_expr` to the piecewise-linear curve through `points`.
    Flat extrapolation outside the first/last x value."""
    branches = [f"WHEN ({col_expr}) <= {points[0][0]} THEN {points[0][1]}"]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        slope = (y1 - y0) / (x1 - x0)
        branches.append(
            f"WHEN ({col_expr}) <= {x1} THEN {y0} + (({col_expr}) - {x0}) * {slope}"
        )
    branches.append(f"ELSE {points[-1][1]}")
    return "CASE " + " ".join(branches) + " END"


def _exit_score_sql(weights: dict, qualifier: str = "i") -> str:
    """Build a SQL scalar expression that yields a 0..100 exit score per row,
    using `weights` for {dom, pending, yoy}. Mirrors the gate bonuses and
    coverage check in compute_exit_score()."""
    dom     = f"{qualifier}.median_days_on_market"
    pending = f"{qualifier}.pending_ratio"
    yoy     = f"{qualifier}.median_listing_price_yy"

    dom_n     = _piecewise_sql(dom, DOM_BREAKPOINTS)
    pending_n = _piecewise_sql(pending, PENDING_BREAKPOINTS)
    yoy_n     = _piecewise_sql(yoy, YOY_BREAKPOINTS)

    w_dom     = float(weights.get("dom",     EXIT_WEIGHT_DEFAULTS["dom"]))
    w_pending = float(weights.get("pending", EXIT_WEIGHT_DEFAULTS["pending"]))
    w_yoy     = float(weights.get("yoy",     EXIT_WEIGHT_DEFAULTS["yoy"]))

    # Renormalized weighted sum: if a signal is NULL its weight is excluded
    # from both numerator and denominator.
    num = (
        f"(CASE WHEN {dom} IS NULL THEN 0 ELSE {w_dom}::numeric * ({dom_n}) END) + "
        f"(CASE WHEN {pending} IS NULL THEN 0 ELSE {w_pending}::numeric * ({pending_n}) END) + "
        f"(CASE WHEN {yoy} IS NULL THEN 0 ELSE {w_yoy}::numeric * ({yoy_n}) END)"
    )
    denom = (
        f"(CASE WHEN {dom} IS NULL THEN 0 ELSE {w_dom}::numeric END) + "
        f"(CASE WHEN {pending} IS NULL THEN 0 ELSE {w_pending}::numeric END) + "
        f"(CASE WHEN {yoy} IS NULL THEN 0 ELSE {w_yoy}::numeric END)"
    )
    base = f"(({num}) / NULLIF({denom}, 0))"

    # Gate bonuses (port of compute_exit_score):
    #   dom < 30 AND pending > 0.40 → ×1.15
    #   dom < 60 AND pending > 0.25 → ×1.10
    #   dom > 120                   → ×0.60 (stagnant override)
    bonus = (
        "(CASE "
        f" WHEN {dom} IS NOT NULL AND {pending} IS NOT NULL AND {dom} < 30 AND {pending} > 0.40 THEN 1.15"
        f" WHEN {dom} IS NOT NULL AND {pending} IS NOT NULL AND {dom} < 60 AND {pending} > 0.25 THEN 1.10"
        " ELSE 1.0 END) * "
        f"(CASE WHEN {dom} IS NOT NULL AND {dom} > 120 THEN 0.60 ELSE 1.0 END)"
    )

    # Coverage gate — if denominator < 0.4 we don't trust the score; return 50.
    expr = (
        f"CASE WHEN ({denom}) >= 0.4 "
        f" THEN LEAST(100, GREATEST(0, ROUND(({base}) * ({bonus}))))::int "
        f" ELSE 50 END"
    )
    return expr


# ─────────────────────────────────────────────────────────────────────────────
# Settings persistence — single global row in app_settings, no auth.
# ─────────────────────────────────────────────────────────────────────────────
_SETTINGS_DDL = """
CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _normalize_weights(raw) -> dict:
    """Coerce input → {dom, pending, yoy} clamped ≥ 0 and renormalized to sum 1.
    Falls back to defaults if input is unusable."""
    if not isinstance(raw, dict):
        return dict(EXIT_WEIGHT_DEFAULTS)
    out = {}
    for k in EXIT_WEIGHT_KEYS:
        try:
            v = float(raw.get(k, EXIT_WEIGHT_DEFAULTS[k]))
        except (TypeError, ValueError):
            v = EXIT_WEIGHT_DEFAULTS[k]
        out[k] = max(0.0, v)
    s = sum(out.values())
    if s <= 0:
        return dict(EXIT_WEIGHT_DEFAULTS)
    return {k: v / s for k, v in out.items()}


def _get_exit_weights() -> dict:
    """Read saved weights from app_settings; return defaults if absent / table missing."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT value FROM app_settings WHERE key = 'exit_weights'")
                row = cur.fetchone()
            except psycopg2.Error:
                conn.rollback()
                return dict(EXIT_WEIGHT_DEFAULTS)
        if not row:
            return dict(EXIT_WEIGHT_DEFAULTS)
        return _normalize_weights(row[0])
    finally:
        conn.close()


def _save_exit_weights(weights: dict) -> dict:
    """Upsert weights into app_settings (writable connection). Returns saved value."""
    norm = _normalize_weights(weights)
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(_SETTINGS_DDL)
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES ('exit_weights', %s::jsonb) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                (json.dumps(norm),),
            )
        conn.commit()
    finally:
        conn.close()
    return norm


def _reset_exit_weights() -> dict:
    """Delete saved weights, falling back to defaults."""
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(_SETTINGS_DDL)
            cur.execute("DELETE FROM app_settings WHERE key = 'exit_weights'")
        conn.commit()
    finally:
        conn.close()
    return dict(EXIT_WEIGHT_DEFAULTS)


# ─────────────────────────────────────────────────────────────────────────────
# Custom column — user-defined arithmetic over existing numeric columns.
# Saved globally in app_settings as {"label", "expression"}. Parsed via Python's
# ast module restricted to + - * / unary +/-, numeric literals, and identifiers
# that map to numeric columns in the current grain.
# ─────────────────────────────────────────────────────────────────────────────
_ALLOWED_BINOPS   = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/"}
_ALLOWED_UNARYOPS = {ast.USub: "-", ast.UAdd: "+"}


def _column_sql(grain: str, col_key: str, exit_expr: str = None):
    """SQL ref for a numeric column key in the current grain, or None if unknown
    or non-numeric. Knows about the special source='sc' exit_score override and
    the COALESCE wrap on foreclosure_count."""
    for c in GRAIN_CONFIG[grain]["columns"]:
        if c["key"] != col_key:
            continue
        if c["type"] not in NUMERIC_TYPES:
            return None
        src = c["source"]
        if src == "inv":
            return f"i.{col_key}"
        if src == "hot":
            return f"h.{col_key}"
        if src == "fc" and col_key == "foreclosure_count":
            return "COALESCE(fc.fc_count, 0)"
        if src == "sc":
            if col_key == "exit_score" and exit_expr:
                return f"({exit_expr})"
            return f"sc.{col_key}"
        return None
    return None


def _formula_to_sql(formula: str, grain: str, exit_expr: str = None):
    """Compile a user formula to a safe SQL expression. Returns (sql, refs).
    Raises ValueError on any disallowed construct."""
    if not formula or not formula.strip():
        raise ValueError("formula is empty")
    if len(formula) > 500:
        raise ValueError("formula too long (max 500 chars)")
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"syntax error: {e.msg}")

    refs = []

    def emit(node):
        if isinstance(node, ast.Expression):
            return emit(node.body)
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in _ALLOWED_BINOPS:
                raise ValueError(f"operator {op_type.__name__} is not allowed")
            op = _ALLOWED_BINOPS[op_type]
            left, right = emit(node.left), emit(node.right)
            if op == "/":
                # Cast both sides to numeric (so integer / integer doesn't
                # truncate) and NULLIF the divisor.
                return f"(({left})::numeric / NULLIF(({right})::numeric, 0))"
            return f"(({left}) {op} ({right}))"
        if isinstance(node, ast.UnaryOp):
            op_type = type(node.op)
            if op_type not in _ALLOWED_UNARYOPS:
                raise ValueError("unary operator not allowed")
            sign = _ALLOWED_UNARYOPS[op_type]
            inner = emit(node.operand)
            return f"({sign}({inner}))" if sign == "-" else f"({inner})"
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ValueError(f"only numeric constants allowed, got {type(node.value).__name__}")
            return str(node.value)
        if isinstance(node, ast.Name):
            ref = _column_sql(grain, node.id, exit_expr=exit_expr)
            if ref is None:
                raise ValueError(f"unknown or non-numeric column: {node.id!r}")
            refs.append(node.id)
            return ref
        raise ValueError(f"expression element not allowed: {type(node).__name__}")

    return emit(tree), refs


def _validate_custom_column(payload):
    """Validate {label, expression} for both grains. Returns the cleaned dict
    or raises ValueError."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    label = (payload.get("label") or "").strip()
    expr  = (payload.get("expression") or "").strip()
    if not expr:
        raise ValueError("expression is required")
    if not label:
        label = "Custom"
    if len(label) > 60:
        label = label[:60]
    # Compile against both grains so a user can switch tabs without surprises.
    _formula_to_sql(expr, "county")
    _formula_to_sql(expr, "zip")
    return {"label": label, "expression": expr}


def _get_custom_column():
    """Return saved {label, expression} or None."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT value FROM app_settings WHERE key = 'custom_column'")
                row = cur.fetchone()
            except psycopg2.Error:
                conn.rollback()
                return None
        if not row or not row[0]:
            return None
        v = row[0]
        if not isinstance(v, dict) or not v.get("expression"):
            return None
        return v
    finally:
        conn.close()


def _save_custom_column(payload) -> dict:
    cleaned = _validate_custom_column(payload)
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(_SETTINGS_DDL)
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES ('custom_column', %s::jsonb) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                (json.dumps(cleaned),),
            )
        conn.commit()
    finally:
        conn.close()
    return cleaned


def _reset_custom_column():
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(_SETTINGS_DDL)
            cur.execute("DELETE FROM app_settings WHERE key = 'custom_column'")
        conn.commit()
    finally:
        conn.close()


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
    {"key":"state_code",             "label":"State",                 "type":"text",      "default":True,  "source":"inv"},
    {"key":"median_listing_price",   "label":"Median List Price",     "type":"currency",  "default":True,  "source":"inv"},
    {"key":"active_listing_count",   "label":"Active Listings",       "type":"integer",   "default":True,  "source":"inv"},
    {"key":"median_days_on_market",  "label":"DOM",                   "type":"integer",   "default":True,  "source":"inv"},
    {"key":"new_listing_count",      "label":"New Listings",          "type":"integer",   "default":True,  "source":"inv"},
    {"key":"pending_ratio",          "label":"Pending Ratio",         "type":"percent_share", "default":True, "source":"inv"},
    {"key":"foreclosure_count",      "label":"Foreclosures",          "type":"integer",   "default":True,  "source":"fc"},
    # Derived scores (precomputed in realtor_scores_county)
    {"key":"exit_score",             "label":"Exit Speed",            "type":"integer",   "default":True,  "source":"sc"},
    {"key":"buy_score",              "label":"Buy Score",             "type":"integer",   "default":False, "source":"sc"},
    {"key":"golden_score",           "label":"Golden Zone",           "type":"integer",   "default":False, "source":"sc"},

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
    {"key":"zip_name",               "label":"City",                  "type":"text",      "default":True,  "source":"inv"},
    {"key":"state_code",             "label":"State",                 "type":"text",      "default":True,  "source":"inv"},
    {"key":"median_listing_price",   "label":"Median List Price",     "type":"currency",  "default":True,  "source":"inv"},
    {"key":"active_listing_count",   "label":"Active Listings",       "type":"integer",   "default":True,  "source":"inv"},
    {"key":"median_days_on_market",  "label":"DOM",                   "type":"integer",   "default":True,  "source":"inv"},
    {"key":"new_listing_count",      "label":"New Listings",          "type":"integer",   "default":True,  "source":"inv"},
    {"key":"pending_ratio",          "label":"Pending Ratio",         "type":"percent_share","default":True, "source":"inv"},
    {"key":"foreclosure_count",      "label":"Foreclosures",          "type":"integer",   "default":True,  "source":"fc"},
    {"key":"exit_score",             "label":"Exit Speed",            "type":"integer",   "default":True,  "source":"sc"},
    {"key":"buy_score",              "label":"Buy Score",             "type":"integer",   "default":False, "source":"sc"},
    {"key":"golden_score",           "label":"Golden Zone",           "type":"integer",   "default":False, "source":"sc"},

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
        "fc_table":     "foreclosure_counts_by_county",   # see scripts/refresh_foreclosure_counts.py
        "fc_key":       "county_fips",
        "score_table":  "realtor_scores_county",          # see scripts/refresh_score_tables.py
        "score_key":    "county_fips",
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
        "score_table":  "realtor_scores_zip",
        "score_key":    "postal_code",
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


# Per-grain derived inventory column expressions. The DB stores
# `county_name`/`zip_name` as "<primary>, <state>" (lowercase) — split it so
# the grid can show the geography and state in separate columns.
DERIVED_INV_EXPR = {
    "county": {
        "county_name": "TRIM(SPLIT_PART(i.county_name, ',', 1))",
        "state_code":  "UPPER(TRIM(SPLIT_PART(i.county_name, ',', 2)))",
    },
    "zip": {
        "zip_name":   "TRIM(SPLIT_PART(i.zip_name, ',', 1))",
        "state_code": "UPPER(TRIM(SPLIT_PART(i.zip_name, ',', 2)))",
    },
}


# Foreclosure week-over-week: refresh_foreclosure_counts.py copies the
# current count tables into *_prev before rebuilding them. The explore grid
# left-joins prev to render a Δ vs last week. On a fresh deploy these may
# not exist yet, so create empty stubs once per process.
_PREV_TABLES_READY = False

def _ensure_prev_tables():
    global _PREV_TABLES_READY
    if _PREV_TABLES_READY:
        return
    dsn = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
    if not dsn:
        return
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS foreclosure_counts_by_zip_prev (
                        postal_code text PRIMARY KEY,
                        fc_count    int
                    );
                    CREATE TABLE IF NOT EXISTS foreclosure_counts_by_county_prev (
                        county_fips text PRIMARY KEY,
                        fc_count    int
                    );
                """)
            conn.commit()
        finally:
            conn.close()
        _PREV_TABLES_READY = True
    except Exception:
        # Don't break /explore if we can't create — query will surface the issue.
        pass


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


def _col_meta(grain, key, custom_col_meta=None):
    if custom_col_meta and key == custom_col_meta["key"]:
        return custom_col_meta
    for c in GRAIN_CONFIG[grain]["columns"]:
        if c["key"] == key:
            return c
    return None


def _parse_filters(grain, args, custom_col_meta=None):
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
        meta = _col_meta(grain, col, custom_col_meta=custom_col_meta)
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


def _resolve_pct_thresholds(grain, filters, exit_expr=None, custom_col_sql=None):
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
        src = f["source"]
        if src == "fc":
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY COALESCE(fc.fc_count, 0)) "
                f"FROM {inv_t} i "
                f"LEFT JOIN {cfg['fc_table']} fc ON fc.{cfg['fc_key']} = i.{key} "
                f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})"
            ))
        elif src == "custom" and custom_col_sql:
            # Custom column references multiple sources — walk inventory + LEFT
            # JOIN whichever side tables it might touch. Score table joined for
            # exit_score / buy_score / golden_score cases.
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY ({custom_col_sql})) "
                f"FROM {inv_t} i "
                f"LEFT JOIN {hot_t} h ON h.{key} = i.{key} "
                f"  AND h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t}) "
                f"LEFT JOIN {cfg['fc_table']} fc ON fc.{cfg['fc_key']} = i.{key} "
                f"LEFT JOIN {cfg['score_table']} sc ON sc.{cfg['score_key']} = i.{key} "
                f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})"
            ))
        elif src == "sc" and f["col"] == "exit_score" and exit_expr:
            # exit_score is computed on-the-fly from current weights — percentile
            # has to walk inventory rows, not the precomputed score table.
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY ({exit_expr})) "
                f"FROM {inv_t} i "
                f"WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})"
            ))
        elif src == "sc":
            parts.append((
                f"thr_{i}",
                f"SELECT PERCENTILE_CONT({pct}) WITHIN GROUP "
                f"(ORDER BY sc.{f['col']}) "
                f"FROM {cfg['score_table']} sc "
                f"WHERE sc.{f['col']} IS NOT NULL"
            ))
        else:
            qual = "i" if src == "inv" else "h"
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


# ─────────────────────────────────────────────────────────────────────────────
# Foreclosure-record-level filters (fc.*) — these don't filter rows directly,
# they re-scope which foreclosure_records rows count toward the Foreclosures
# column for each county/ZIP. When any fc.* filter is active we replace the
# precomputed JOIN with a CTE that re-aggregates on the fly.
# ─────────────────────────────────────────────────────────────────────────────

FC_FIELDS = {
    "auction_date":  {"type": "date",   "ops": {"has", "gte", "lte", "eq"}},
    "listing_type":  {"type": "text",   "ops": {"in", "equals", "contains"}},
    "classification":{"type": "text",   "ops": {"in", "equals", "contains"}},
}


def _parse_fc_filters(args):
    """Parse fc.<field>.<op>=<value> records-level filters."""
    out = []
    errors = []
    for raw_key, raw_val in args.items(multi=True):
        if not raw_key.startswith("fc."):
            continue
        parts = raw_key.split(".", 2)
        if len(parts) != 3:
            errors.append(f"bad fc filter key {raw_key!r}"); continue
        _, field, op = parts
        if field not in FC_FIELDS:
            errors.append(f"unknown fc field {field!r}"); continue
        spec = FC_FIELDS[field]
        if op not in spec["ops"]:
            errors.append(f"fc.{field}: op {op!r} not allowed"); continue
        # Normalize values
        if op == "has":
            v = str(raw_val).strip().lower() in ("1", "true", "yes")
        elif op == "in":
            v = [s for s in raw_val.split(",") if s.strip()]
            if not v:
                continue
        else:
            v = str(raw_val).strip()
        out.append({"field": field, "op": op, "value": v, "type": spec["type"]})
    return out, errors


def _build_fc_count_cte(grain: str, fc_filters):
    """Build a CTE that recomputes foreclosure count per geo, using the
    record-level filters. Returns (cte_sql, params) producing a relation
    with columns (postal_code or county_fips, fc_count) joined alias `fc`."""
    cfg = GRAIN_CONFIG[grain]
    where = ["status = 'active'", "zip IS NOT NULL", "zip <> ''"]
    params = []

    for f in fc_filters:
        if f["field"] == "auction_date":
            if f["op"] == "has":
                where.append("auction_date IS NOT NULL" if f["value"] else "auction_date IS NULL")
            elif f["op"] == "gte":
                where.append("auction_date >= %s::date"); params.append(f["value"])
            elif f["op"] == "lte":
                where.append("auction_date <= %s::date"); params.append(f["value"])
            elif f["op"] == "eq":
                where.append("auction_date = %s::date"); params.append(f["value"])
        elif f["field"] in ("listing_type", "classification"):
            col = f["field"]
            if f["op"] == "in":
                ph = ", ".join(["%s"] * len(f["value"]))
                where.append(f"{col} IN ({ph})"); params.extend(f["value"])
            elif f["op"] == "equals":
                where.append(f"LOWER({col}) = LOWER(%s)"); params.append(f["value"])
            elif f["op"] == "contains":
                where.append(f"{col} ILIKE %s"); params.append(f"%{f['value']}%")

    where_sql = " AND ".join(where)
    if grain == "zip":
        cte = (f"fc AS ("
               f"  SELECT LPAD(zip, 5, '0') AS postal_code, COUNT(*)::int AS fc_count "
               f"  FROM foreclosure_records WHERE {where_sql} "
               f"  GROUP BY LPAD(zip, 5, '0') )")
    else:
        # county — first per-zip, then SUM via zip_county_map
        cte = (f"fc_zip AS ("
               f"  SELECT LPAD(zip, 5, '0') AS postal_code, COUNT(*)::int AS fc_count "
               f"  FROM foreclosure_records WHERE {where_sql} "
               f"  GROUP BY LPAD(zip, 5, '0') ),"
               f"fc AS ("
               f"  SELECT zcm.county_fips, SUM(fz.fc_count)::int AS fc_count "
               f"  FROM fc_zip fz JOIN zip_county_map zcm ON zcm.postal_code = fz.postal_code "
               f"  GROUP BY zcm.county_fips )")
    return cte, params


def _filter_to_sql(grain, f, qualifier, exit_expr=None, custom_col_sql=None):
    """Translate one validated filter to (where_clause, params)."""
    col = f["col"]
    op  = f["op"]
    val = f["value"]
    ctype = f["type"]
    src   = f["source"]

    if src == "fc":
        ref = "COALESCE(fc.fc_count, 0)"
    elif src == "custom" and custom_col_sql:
        ref = f"({custom_col_sql})"
    elif src == "sc" and col == "exit_score" and exit_expr:
        ref = f"({exit_expr})"
    elif src == "sc":
        ref = f"sc.{col}"
    else:
        derived = DERIVED_INV_EXPR.get(grain, {}).get(col) if src == "inv" else None
        ref = derived if derived else f"{qualifier}.{col}"

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


def _build_query(grain: str, q: str, sort: str, sort_dir: str, want_count: bool, filters=None, fc_filters=None, exit_expr=None, custom_col_sql=None, custom_col_label=None):
    """Return (sql, params, cols_in_order) for the given grain."""
    filters = filters or []
    fc_filters = fc_filters or []
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
    sc_cols  = [c["key"] for c in cols if c["source"] == "sc"]

    # SELECT list — qualify each column to avoid ambiguity (some keys overlap,
    # e.g. median_days_on_market exists in both inventory and hotness).
    derived = DERIVED_INV_EXPR.get(grain, {})
    select_parts = [f"i.{key} AS {key}"]
    for k in inv_cols:
        expr = derived.get(k, f"i.{k}")
        select_parts.append(f"{expr} AS {k}")
    for k in hot_cols:
        select_parts.append(f"h.{k} AS {k}")
    if "foreclosure_count" in fc_cols:
        select_parts.append("COALESCE(fc.fc_count, 0) AS foreclosure_count")
        select_parts.append("fcp.fc_count AS foreclosure_count_prev")
    for k in sc_cols:
        if k == "exit_score" and exit_expr:
            select_parts.append(f"({exit_expr}) AS {k}")
        else:
            select_parts.append(f"sc.{k} AS {k}")
    if custom_col_sql:
        select_parts.append(f"({custom_col_sql}) AS custom_col")

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
    SRC_QUALIFIER = {"inv": "i", "hot": "h", "fc": "fc", "sc": "sc", "custom": "i"}
    for f in filters:
        qual = SRC_QUALIFIER.get(f["source"], "i")
        clause, fparams = _filter_to_sql(grain, f, qual, exit_expr=exit_expr, custom_col_sql=custom_col_sql)
        if clause:
            where.append(clause)
            params.extend(fparams)

    # When record-level fc.* filters are active, replace the precomputed
    # foreclosure_counts table with a dynamic CTE that re-aggregates per geo.
    fc_prefix = ""
    fc_params_pre = []
    if fc_filters:
        fc_cte, fc_params_pre = _build_fc_count_cte(grain, fc_filters)
        fc_prefix = f"WITH {fc_cte} "
        fc_join_table = "fc"   # CTE alias already named 'fc'
    else:
        fc_join_table = fc_t

    sc_t   = cfg["score_table"]
    sc_key = cfg["score_key"]
    fcp_t  = f"{fc_t}_prev"
    base_from = (
        f"FROM {inv_t} i "
        f"LEFT JOIN {hot_t} h ON h.{key} = i.{key} "
        f"  AND h.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {hot_t}) "
        f"LEFT JOIN {fc_join_table} fc ON fc.{fc_key} = i.{key} "
        f"LEFT JOIN {fcp_t} fcp ON fcp.{fc_key} = i.{key} "
        f"LEFT JOIN {sc_t} sc ON sc.{sc_key} = i.{key} "
        f"WHERE {' AND '.join(where)}"
    )

    if want_count:
        return f"{fc_prefix}SELECT COUNT(*) {base_from}", fc_params_pre + params, []

    # Validate sort column against the whitelist
    valid_sort_keys = {c["key"] for c in cols if c.get("sortable", True)}
    if custom_col_sql:
        valid_sort_keys.add("custom_col")
    if sort not in valid_sort_keys:
        sort = cfg["default_sort"]
    sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

    # Pick the right SQL alias for ORDER BY based on which table the column came from
    if sort == "foreclosure_count":
        order_sort = "COALESCE(fc.fc_count, 0)"
    elif sort == "custom_col" and custom_col_sql:
        order_sort = f"({custom_col_sql})"
    elif sort == "exit_score" and exit_expr:
        order_sort = f"({exit_expr})"
    elif sort in sc_cols:
        order_sort = f"sc.{sort}"
    elif sort in (inv_cols + [key]):
        order_sort = derived.get(sort, f"i.{sort}")
    else:
        order_sort = f"h.{sort}"
    order_sql = f"ORDER BY {order_sort} {sort_dir} NULLS LAST, i.{key} ASC"

    cols_in_order = [key] + inv_cols + hot_cols + fc_cols + sc_cols
    if custom_col_sql:
        cols_in_order.append("custom_col")
    sql = f"{fc_prefix}SELECT {', '.join(select_parts)} {base_from} {order_sql}"
    return sql, fc_params_pre + params, cols_in_order


def _fmt_for_csv(val):
    if val is None:
        return ""
    return str(val)


def _stream_csv(grain: str, q: str, sort: str, sort_dir: str, visible_cols: Iterable[str], filters=None, fc_filters=None, exit_expr=None, custom_col_sql=None, custom_col_label=None):
    cfg = GRAIN_CONFIG[grain]
    base_cols = list(cfg["columns"])
    if custom_col_sql:
        base_cols.append({"key": "custom_col", "label": custom_col_label or "Custom"})
    visible_cols = list(visible_cols) if visible_cols else [c["key"] for c in base_cols]
    label_by_key = {c["key"]: c["label"] for c in base_cols}

    sql, params, _ = _build_query(grain, q, sort, sort_dir, want_count=False, filters=filters or [], fc_filters=fc_filters or [], exit_expr=exit_expr, custom_col_sql=custom_col_sql, custom_col_label=custom_col_label)

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
    _ensure_prev_tables()

    q        = request.args.get("q", "")
    sort     = request.args.get("sort", GRAIN_CONFIG[grain]["default_sort"])
    sort_dir = request.args.get("dir", "desc")
    fmt      = request.args.get("format", "json").lower()

    weights = _get_exit_weights()
    exit_expr = _exit_score_sql(weights, qualifier="i")

    custom_col      = _get_custom_column()
    custom_col_sql  = None
    custom_col_meta = None
    custom_col_err  = None
    if custom_col and custom_col.get("expression"):
        try:
            custom_col_sql, _ = _formula_to_sql(custom_col["expression"], grain, exit_expr=exit_expr)
            custom_col_meta = {
                "key":    "custom_col",
                "label":  custom_col.get("label") or "Custom",
                "type":   "number",
                "default": True,
                "source":  "custom",
                "expression": custom_col["expression"],
                "is_custom":  True,
            }
        except ValueError as e:
            custom_col_err = f"custom column disabled: {e}"

    raw_filters, filter_errors = _parse_filters(grain, request.args, custom_col_meta=custom_col_meta)
    fc_filters,  fc_errors      = _parse_fc_filters(request.args)
    filter_errors.extend(fc_errors)
    if custom_col_err:
        filter_errors.append(custom_col_err)

    filters = _resolve_pct_thresholds(grain, raw_filters, exit_expr=exit_expr, custom_col_sql=custom_col_sql)

    if fmt == "csv":
        visible = request.args.get("cols", "")
        visible_cols = [c for c in visible.split(",") if c] or None
        filename = f"realtor_{grain}_{request.args.get('q','all')}.csv"
        return Response(
            stream_with_context(_stream_csv(grain, q, sort, sort_dir, visible_cols,
                                            filters=filters, fc_filters=fc_filters,
                                            exit_expr=exit_expr,
                                            custom_col_sql=custom_col_sql,
                                            custom_col_label=(custom_col_meta or {}).get("label"))),
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

    sql, params, cols_in_order = _build_query(grain, q, sort, sort_dir, want_count=False, filters=filters, fc_filters=fc_filters, exit_expr=exit_expr, custom_col_sql=custom_col_sql, custom_col_label=(custom_col_meta or {}).get("label"))
    paged_sql = f"{sql} LIMIT {limit} OFFSET {offset}"

    count_sql, count_params, _ = _build_query(grain, q, sort, sort_dir, want_count=True, filters=filters, fc_filters=fc_filters, exit_expr=exit_expr, custom_col_sql=custom_col_sql, custom_col_label=(custom_col_meta or {}).get("label"))

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
        "columns": GRAIN_CONFIG[grain]["columns"] + ([custom_col_meta] if custom_col_meta else []),
        "exit_weights":          weights,
        "exit_weights_defaults": dict(EXIT_WEIGHT_DEFAULTS),
        "custom_column":         custom_col_meta,
        "applied_filters": [
            {"col": f["col"], "op": f["op"], "value": f["value"],
             "pct_resolved_from": f.get("pct_resolved_from"),
             "pct_threshold": f.get("pct_threshold")}
            for f in filters
        ],
        "applied_fc_filters": [
            {"field": f["field"], "op": f["op"], "value": f["value"]}
            for f in fc_filters
        ],
        "filter_errors": filter_errors,
    })


@explore_bp.route("/api/explore/foreclosure-distinct/<field>")
def explore_foreclosure_distinct(field):
    """Distinct values from foreclosure_records for fc.* multi-select filters."""
    if field not in ("listing_type", "classification"):
        return jsonify({"error": f"unsupported field {field!r}"}), 400
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {field}, COUNT(*) AS n FROM foreclosure_records "
                f"WHERE status='active' AND {field} IS NOT NULL "
                f"GROUP BY {field} ORDER BY n DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return jsonify({
        "field":  field,
        "values": [{"value": v, "count": n} for v, n in rows],
        "count":  len(rows),
    })


@explore_bp.route("/api/explore/suggest")
def explore_suggest():
    """Autocomplete suggestions for the /explore search box.

    Returns up to 10 matches against the FIPS / postal_code / name of the
    latest-month inventory rows. Prefix matches rank above substring matches.
    """
    grain = request.args.get("grain", "county")
    if grain not in GRAIN_CONFIG:
        return jsonify({"suggestions": []})
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"suggestions": []})

    cfg = GRAIN_CONFIG[grain]
    inv_t = cfg["inv_table"]
    key   = cfg["key"]
    name_col = "county_name" if grain == "county" else "zip_name"

    like_pre = f"{q}%"
    like_any = f"%{q}%"
    sql = f"""
        SELECT i.{key} AS k, i.{name_col} AS n
        FROM {inv_t} i
        WHERE i.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM {inv_t})
          AND (i.{key}::text ILIKE %s OR i.{name_col} ILIKE %s)
        ORDER BY
          CASE WHEN i.{key}::text ILIKE %s OR i.{name_col} ILIKE %s THEN 0 ELSE 1 END,
          i.{name_col} ASC
        LIMIT 10
    """
    params = [like_any, like_any, like_pre, like_pre]

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    suggestions = []
    for k, name in rows:
        parts = (name or "").split(",", 1)
        primary = parts[0].strip()
        state   = parts[1].strip().upper() if len(parts) == 2 else ""
        suggestions.append({
            "value":   name or k,
            "key":     k,
            "primary": primary,
            "state":   state,
        })
    return jsonify({"suggestions": suggestions})


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


# ─────────────────────────────────────────────────────────────────────────────
# Exit Speed weight settings — global (single-tenant app, no per-user auth).
# ─────────────────────────────────────────────────────────────────────────────
@explore_bp.route("/api/settings/exit-weights", methods=["GET"])
def get_exit_weights():
    return jsonify({
        "weights":  _get_exit_weights(),
        "defaults": dict(EXIT_WEIGHT_DEFAULTS),
        "keys":     list(EXIT_WEIGHT_KEYS),
    })


@explore_bp.route("/api/settings/exit-weights", methods=["PUT"])
def put_exit_weights():
    body = request.get_json(silent=True) or {}
    raw  = body.get("weights", body)
    saved = _save_exit_weights(raw)
    return jsonify({
        "weights":  saved,
        "defaults": dict(EXIT_WEIGHT_DEFAULTS),
        "saved":    True,
    })


@explore_bp.route("/api/settings/exit-weights", methods=["DELETE"])
def delete_exit_weights():
    return jsonify({
        "weights":  _reset_exit_weights(),
        "defaults": dict(EXIT_WEIGHT_DEFAULTS),
        "reset":    True,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Custom column settings.
# ─────────────────────────────────────────────────────────────────────────────
def _numeric_columns_list(grain):
    return [
        {"key": c["key"], "label": c["label"], "type": c["type"], "source": c["source"]}
        for c in GRAIN_CONFIG[grain]["columns"]
        if c["type"] in NUMERIC_TYPES
    ]


@explore_bp.route("/api/settings/custom-column", methods=["GET"])
def get_custom_column():
    return jsonify({
        "custom_column": _get_custom_column(),
        "operands": {
            "county": _numeric_columns_list("county"),
            "zip":    _numeric_columns_list("zip"),
        },
        "operators": ["+", "-", "*", "/"],
    })


@explore_bp.route("/api/settings/custom-column", methods=["PUT"])
def put_custom_column():
    body = request.get_json(silent=True) or {}
    try:
        saved = _save_custom_column(body)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"custom_column": saved, "saved": True})


@explore_bp.route("/api/settings/custom-column", methods=["DELETE"])
def delete_custom_column():
    _reset_custom_column()
    return jsonify({"custom_column": None, "reset": True})
