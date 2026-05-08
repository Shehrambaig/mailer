"""Mailing-export endpoints.

Surfaced under /api/mailing/* :

  GET  /api/mailing/preview            count rows + dedupe stats for the modal
  POST /api/mailing/export             stream xlsx, write a mailing_exports row
                                       + mailing_export_items rows
  GET  /api/mailing/exports            list previous exports
  GET  /api/mailing/exports/<id>/download   re-stream xlsx for a past batch

The preview/export endpoints accept the *same* query-string filter shape as
/api/explore (`fc.<field>.<op>=<v>`, `f.<col>.<op>=<v>`, `fc_net_new=1`, `q`).
The frontend therefore reuses its `buildURL()` to call us — no parallel
filter encoder to maintain.

Dedupe is two-pass:

  1. (listing_id, source) match → drop_listing_id
  2. address_norm match (only counted if step 1 didn't already drop it)
                                 → drop_address

Address normalization in SQL:
    UPPER(REGEXP_REPLACE(COALESCE(street,''), '[^A-Za-z0-9]+', '', 'g'))
    || '|' || LPAD(NULLIF(zip,''), 5, '0')
"""
from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from flask import Blueprint, Response, abort, jsonify, request, stream_with_context

from app.routes.explore import _conn, _parse_fc_filters


def _conn_rw():
    """Writable connection for inserting into mailing_exports / items.

    `_conn()` from explore.py is intentionally read-only.
    """
    dsn = (os.getenv("NEON_DB")
           or os.getenv("PROBATE_DATABASE_URL")
           or "postgresql://localhost:5432/probate")
    return psycopg2.connect(dsn, connect_timeout=10)

mailing_bp = Blueprint("mailing", __name__)


# ── Constants ────────────────────────────────────────────────────────────────

# Inventory columns we will translate from `f.<col>.<op>=v` filters into a
# WHERE clause on realtor_inventory_zip. Every other column-level filter on
# the explore page is silently ignored at row-export time — those tend to be
# scoring/hotness/custom expressions that don't apply at the listing level.
INV_FILTER_COLS = {
    "median_days_on_market", "median_listing_price", "active_listing_count",
    "pending_listing_count", "new_listing_count",
    "price_increased_count", "price_reduced_count",
    "price_increased_share", "price_reduced_share",
    "median_listing_price_per_square_foot", "median_square_feet",
    "average_listing_price", "total_listing_count",
}

NUMERIC_OP_SQL = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "="}

# Foreclosure columns dropped from the export (raw is huge; parties is
# flattened into a human-readable string column instead).
EXPORT_DROP_COLS = {"raw", "parties"}

# SQL expression used everywhere the address fingerprint is needed. Defined
# once so dedupe + insert + match all use the same canonicalization.
ADDR_NORM_SQL = (
    "UPPER(REGEXP_REPLACE(COALESCE(street, ''), '[^A-Za-z0-9]+', '', 'g')) "
    "|| '|' || LPAD(NULLIF(zip, ''), 5, '0')"
)


# ── Filter translation ──────────────────────────────────────────────────────

def _parse_inv_filters(args) -> list[dict]:
    """Pick out f.<inv_col>.<op>=v filters that we know how to translate."""
    out = []
    for raw_key, raw_val in args.items(multi=True):
        if not raw_key.startswith("f."):
            continue
        parts = raw_key.split(".", 2)
        if len(parts) != 3:
            continue
        _, col, op = parts
        if col not in INV_FILTER_COLS:
            continue
        if op not in NUMERIC_OP_SQL:
            continue
        try:
            v = float(raw_val)
        except (TypeError, ValueError):
            continue
        out.append({"col": col, "op": op, "value": v})
    return out


def _build_fc_where(fc_filters, net_new: bool) -> tuple[str, list]:
    """Build the WHERE-on-foreclosure_records clause from explore fc filters.

    Mirrors the logic in explore._build_fc_count_cte but at the row level so
    the export returns full foreclosure rows (not aggregates).
    """
    where = ["fr.status = 'active'", "fr.zip IS NOT NULL", "fr.zip <> ''"]
    params: list = []

    if net_new:
        where.append(
            "fr.first_seen_at >= ("
            "  SELECT MAX(DATE(first_seen_at))::timestamptz"
            "  FROM foreclosure_records"
            ")"
        )

    for f in fc_filters:
        field, op, val = f["field"], f["op"], f["value"]
        if field == "auction_date":
            if op == "has":
                where.append("fr.auction_date IS NOT NULL" if val else "fr.auction_date IS NULL")
            elif op == "gte":
                where.append("fr.auction_date >= %s::date"); params.append(val)
            elif op == "lte":
                where.append("fr.auction_date <= %s::date"); params.append(val)
            elif op == "eq":
                where.append("fr.auction_date = %s::date");  params.append(val)
        elif field in ("listing_type", "classification"):
            if op == "in":
                ph = ", ".join(["%s"] * len(val))
                where.append(f"fr.{field} IN ({ph})"); params.extend(val)
            elif op == "equals":
                where.append(f"LOWER(fr.{field}) = LOWER(%s)"); params.append(val)
            elif op == "contains":
                where.append(f"fr.{field} ILIKE %s"); params.append(f"%{val}%")
    return " AND ".join(where), params


def _build_inv_where(inv_filters) -> tuple[str, list]:
    """Build the WHERE-on-realtor_inventory_zip clause."""
    where = []
    params: list = []
    for f in inv_filters:
        sql_op = NUMERIC_OP_SQL[f["op"]]
        where.append(f"rz.{f['col']} {sql_op} %s"); params.append(f["value"])
    return (" AND ".join(where) if where else "TRUE"), params


def _build_match_sql(fc_filters, inv_filters, net_new: bool, state: str | None,
                     q: str | None) -> tuple[str, list, str]:
    """Return (from_join_where_sql, params, where_only_sql).

    - from_join_where_sql is the FROM/JOIN/WHERE chunk used by SELECT.
    - where_only_sql is just the inner WHERE (handy for COUNT subqueries).
    """
    fc_where, fc_params = _build_fc_where(fc_filters, net_new)
    inv_where, inv_params = _build_inv_where(inv_filters)

    extra_where: list[str] = []
    extra_params: list = []

    if state and len(state) == 2:
        extra_where.append("fr.state = %s")
        extra_params.append(state.upper())

    if q:
        # Search match the explore behavior loosely: any of street / city /
        # county / zip ILIKE the query.
        extra_where.append(
            "(fr.city ILIKE %s OR fr.county ILIKE %s OR fr.state ILIKE %s "
            "OR fr.zip ILIKE %s OR fr.street ILIKE %s)"
        )
        like = f"%{q.strip()}%"
        extra_params.extend([like, like, like, like, like])

    extra_sql = (" AND " + " AND ".join(extra_where)) if extra_where else ""

    join_sql = (
        "FROM foreclosure_records fr "
        "JOIN realtor_inventory_zip rz "
        "  ON rz.postal_code = LPAD(fr.zip, 5, '0') "
        " AND rz.month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM realtor_inventory_zip) "
        f"WHERE {fc_where} AND ({inv_where}){extra_sql}"
    )
    params = fc_params + inv_params + extra_params

    where_only = f"{fc_where} AND ({inv_where}){extra_sql}"
    return join_sql, params, where_only


def _filter_snapshot() -> dict:
    """Capture the request's filter args so we can rebuild the same query later."""
    snap = {k: v for k, v in request.args.items(multi=True)}
    snap.pop("format", None)
    return snap


# ── Preview / export ────────────────────────────────────────────────────────

@mailing_bp.route("/api/mailing/preview", methods=["GET"])
def preview():
    """Count rows + show how many would be dropped due to dedupe."""
    fc_filters, _ = _parse_fc_filters(request.args)
    inv_filters    = _parse_inv_filters(request.args)
    net_new        = str(request.args.get("fc_net_new", "")).lower() in ("1", "true", "yes")
    state          = request.args.get("state") or None
    q              = request.args.get("q")    or None

    join_sql, params, _ = _build_match_sql(fc_filters, inv_filters, net_new, state, q)

    sql = f"""
        WITH matched AS (
            SELECT
                fr.listing_id,
                fr.source,
                {ADDR_NORM_SQL} AS addr_norm
            {join_sql}
        )
        SELECT
            COUNT(*) AS match_total,
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM mailing_export_items mei
                    WHERE mei.listing_id = m.listing_id
                      AND mei.source     = m.source
                )
            ) AS drop_listing_id,
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                          SELECT 1 FROM mailing_export_items mei
                          WHERE mei.listing_id = m.listing_id
                            AND mei.source     = m.source
                      )
                  AND EXISTS (
                          SELECT 1 FROM mailing_export_items mei
                          WHERE mei.address_norm = m.addr_norm
                      )
            ) AS drop_address
        FROM matched m
    """

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            match_total, drop_id, drop_addr = cur.fetchone()
    finally:
        conn.close()

    match_total = int(match_total or 0)
    drop_id     = int(drop_id or 0)
    drop_addr   = int(drop_addr or 0)
    net_count   = match_total - drop_id - drop_addr

    return jsonify({
        "match_total":     match_total,
        "drop_listing_id": drop_id,
        "drop_address":    drop_addr,
        "net_count":       max(net_count, 0),
        "filter_summary":  _summarize(fc_filters, inv_filters, net_new, state, q),
    })


def _summarize(fc_filters, inv_filters, net_new: bool, state, q) -> str:
    """Human-readable one-liner for the modal + the exports list."""
    parts: list[str] = []
    for f in fc_filters:
        v = f["value"]
        if isinstance(v, list): v = ", ".join(v)
        parts.append(f"{f['field']} {f['op']} {v}")
    for f in inv_filters:
        parts.append(f"{f['col']} {f['op']} {f['value']}")
    if net_new: parts.append("net new")
    if state:   parts.append(f"state = {state.upper()}")
    if q:       parts.append(f'q = "{q}"')
    return " · ".join(parts) if parts else "no filters"


# Columns to emit in the xlsx. Order matters — drives the header row.
def _export_columns(cur) -> list[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='foreclosure_records' "
        "ORDER BY ordinal_position"
    )
    # Tolerate both tuple and RealDictCursor rows.
    cols = [(r["column_name"] if isinstance(r, dict) else r[0]) for r in cur.fetchall()]
    return [c for c in cols if c not in EXPORT_DROP_COLS]


def _flatten_parties(p) -> str:
    """parties jsonb → human-readable 'role: name; role: name' string."""
    if not p: return ""
    if isinstance(p, str):
        try: p = json.loads(p)
        except Exception: return p
    if isinstance(p, dict):
        items = [(k, v) for k, v in p.items()]
    elif isinstance(p, list):
        items = []
        for entry in p:
            if isinstance(entry, dict):
                role = entry.get("role") or entry.get("type") or "party"
                name = entry.get("name") or entry.get("full") or entry.get("first", "") + " " + entry.get("last", "")
                items.append((role, str(name).strip()))
            else:
                items.append(("party", str(entry)))
    else:
        return str(p)
    return "; ".join(f"{r}: {n}" for r, n in items if n)


def _stream_xlsx(rows: list[dict], cols: list[str], filename: str) -> Response:
    """Build an xlsx in memory and return a Flask Response that streams it."""
    from openpyxl import Workbook
    from openpyxl.styles import Font

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Mailing")

    header_cols = list(cols)
    if "parties" in header_cols:
        header_cols[header_cols.index("parties")] = "parties_text"
    elif "Parties" not in header_cols:
        # parties dropped by EXPORT_DROP_COLS; we still want the flattened text.
        header_cols.append("parties_text")

    ws.append(header_cols)

    for row in rows:
        out_row = []
        for k in header_cols:
            if k == "parties_text":
                out_row.append(_flatten_parties(row.get("parties")))
                continue
            v = row.get(k)
            if v is None:
                out_row.append(None)
            elif hasattr(v, "isoformat"):
                out_row.append(v.isoformat())
            elif isinstance(v, (dict, list)):
                out_row.append(json.dumps(v, default=str))
            else:
                out_row.append(v)
        ws.append(out_row)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )


@mailing_bp.route("/api/mailing/export", methods=["POST", "GET"])
def export():
    """Run the filter, dedupe, insert a mailing_exports batch, stream xlsx."""
    fc_filters, _ = _parse_fc_filters(request.args)
    inv_filters    = _parse_inv_filters(request.args)
    net_new        = str(request.args.get("fc_net_new", "")).lower() in ("1", "true", "yes")
    state          = request.args.get("state") or None
    q              = request.args.get("q")    or None

    join_sql, params, _ = _build_match_sql(fc_filters, inv_filters, net_new, state, q)

    conn = _conn_rw()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            export_cols = _export_columns(cur)

            # Build SELECT including everything we need to insert into items
            # plus the export columns. Hold the addr_norm for items + drop count
            # bookkeeping.
            select_list = ", ".join([f"fr.{c}" for c in export_cols] + [
                f"({ADDR_NORM_SQL}) AS _addr_norm",
            ])

            sql = f"""
                WITH matched AS (
                    SELECT {select_list}
                    {join_sql}
                ),
                survivors AS (
                    SELECT m.*
                    FROM matched m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mailing_export_items mei
                        WHERE mei.listing_id = m.listing_id
                          AND mei.source     = m.source
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM mailing_export_items mei
                        WHERE mei.address_norm = m._addr_norm
                    )
                ),
                stats AS (
                    SELECT
                        (SELECT COUNT(*) FROM matched)                AS match_total,
                        (SELECT COUNT(*) FROM matched m
                         WHERE EXISTS (SELECT 1 FROM mailing_export_items mei
                                       WHERE mei.listing_id = m.listing_id
                                         AND mei.source     = m.source)) AS drop_listing_id,
                        (SELECT COUNT(*) FROM matched m
                         WHERE NOT EXISTS (SELECT 1 FROM mailing_export_items mei
                                           WHERE mei.listing_id = m.listing_id
                                             AND mei.source     = m.source)
                           AND EXISTS (SELECT 1 FROM mailing_export_items mei
                                       WHERE mei.address_norm = m._addr_norm)) AS drop_address
                )
                SELECT s.* FROM survivors s
            """

            cur.execute(sql, params)
            rows = list(cur.fetchall())

            # Re-issue the stats query because cursor is exhausted on rows.
            cur.execute(f"""
                WITH matched AS (
                    SELECT fr.listing_id, fr.source, ({ADDR_NORM_SQL}) AS _addr_norm
                    {join_sql}
                )
                SELECT
                    (SELECT COUNT(*) FROM matched) AS match_total,
                    (SELECT COUNT(*) FROM matched m
                     WHERE EXISTS (SELECT 1 FROM mailing_export_items mei
                                   WHERE mei.listing_id = m.listing_id
                                     AND mei.source     = m.source)) AS drop_listing_id,
                    (SELECT COUNT(*) FROM matched m
                     WHERE NOT EXISTS (SELECT 1 FROM mailing_export_items mei
                                       WHERE mei.listing_id = m.listing_id
                                         AND mei.source     = m.source)
                       AND EXISTS (SELECT 1 FROM mailing_export_items mei
                                   WHERE mei.address_norm = m._addr_norm)) AS drop_address
            """, params)
            stats = cur.fetchone()
            match_total = int(stats["match_total"] or 0)
            drop_id     = int(stats["drop_listing_id"] or 0)
            drop_addr   = int(stats["drop_address"] or 0)

            row_count = len(rows)
            ts        = datetime.now(timezone.utc)
            filename  = f"mailing-{ts.strftime('%Y-%m-%d-%H%M')}.xlsx"

            with conn.cursor() as wcur:
                wcur.execute("""
                    INSERT INTO mailing_exports
                        (filter_json, match_total, drop_listing_id, drop_address,
                         row_count, filename)
                    VALUES (%s::jsonb, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (json.dumps(_filter_snapshot()), match_total, drop_id, drop_addr,
                      row_count, filename))
                export_id = wcur.fetchone()[0]

                if rows:
                    psycopg2.extras.execute_values(
                        wcur,
                        "INSERT INTO mailing_export_items "
                        "(export_id, listing_id, source, address_norm) VALUES %s "
                        "ON CONFLICT DO NOTHING",
                        [(export_id, r["listing_id"], r["source"], r["_addr_norm"])
                         for r in rows],
                    )
            conn.commit()
    finally:
        conn.close()

    # Strip the bookkeeping column before streaming.
    for r in rows:
        r.pop("_addr_norm", None)

    return _stream_xlsx(rows, export_cols, filename)


# ── Previous exports ────────────────────────────────────────────────────────

@mailing_bp.route("/api/mailing/exports", methods=["GET"])
def list_exports():
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, created_at, filter_json, match_total,
                       drop_listing_id, drop_address, row_count, filename
                FROM mailing_exports
                ORDER BY created_at DESC
                LIMIT 100
            """)
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id":              r["id"],
                    "created_at":      r["created_at"].isoformat() if r["created_at"] else None,
                    "filter_json":     r["filter_json"] or {},
                    "match_total":     r["match_total"],
                    "drop_listing_id": r["drop_listing_id"],
                    "drop_address":    r["drop_address"],
                    "row_count":       r["row_count"],
                    "filename":        r["filename"],
                })
    finally:
        conn.close()
    return jsonify({"exports": rows})


@mailing_bp.route("/api/mailing/exports/<int:export_id>/download", methods=["GET"])
def redownload(export_id: int):
    """Re-run the saved query, restrict to that batch's items, stream xlsx."""
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT filter_json, filename FROM mailing_exports WHERE id = %s",
                (export_id,),
            )
            row = cur.fetchone()
            if not row:
                abort(404)
            filter_json = row["filter_json"] or {}
            filename    = row["filename"] or f"mailing-{export_id}.xlsx"

            # Re-parse the snapshot back into the same shapes as live filters.
            class _Args:
                """Mimic the request.args.items(multi=True) interface for parsers."""
                def __init__(self, mapping):
                    self._items = list(mapping.items())
                def items(self, multi=False):
                    return list(self._items)
                def get(self, k, default=None):
                    for kk, vv in self._items:
                        if kk == k: return vv
                    return default

            args = _Args(filter_json)
            fc_filters, _ = _parse_fc_filters(args)
            inv_filters    = _parse_inv_filters(args)
            net_new        = str(args.get("fc_net_new", "")).lower() in ("1", "true", "yes")
            state          = args.get("state") or None
            q              = args.get("q")    or None

            join_sql, params, _ = _build_match_sql(fc_filters, inv_filters, net_new, state, q)
            export_cols = _export_columns(cur)
            select_list = ", ".join(f"fr.{c}" for c in export_cols)

            cur.execute(f"""
                SELECT {select_list}
                {join_sql}
                  AND EXISTS (
                    SELECT 1 FROM mailing_export_items mei
                    WHERE mei.export_id = %s
                      AND mei.listing_id = fr.listing_id
                      AND mei.source     = fr.source
                  )
            """, params + [export_id])
            rows = list(cur.fetchall())
    finally:
        conn.close()

    return _stream_xlsx(rows, export_cols, filename)
