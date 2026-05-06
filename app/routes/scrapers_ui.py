"""Scrapers status page — reads from `scraper_report_snapshot`.

Endpoints:
  /scrapers/                          page (snapshot summary + per-source report)
  /scrapers/api/all                   JSON list of every snapshot row
  /scrapers/api/<key>                 JSON for one source's snapshot
  /scrapers/<key>/rows                paged + searchable row list (scalar cols)
  /scrapers/<key>/row/<rid>           full row including JSON columns

Snapshot is built by `scripts/build_scraper_report.py` — we only read here.
"""
from __future__ import annotations
import os
from urllib.parse import unquote

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, render_template, request

scrapers_bp = Blueprint("scrapers", __name__)


def _dsn() -> str:
    return (
        os.getenv("NEON_DB")
        or os.getenv("PROBATE_DATABASE_URL")
        or "postgresql://localhost:5432/probate"
    )


def _connect():
    return psycopg2.connect(_dsn(), connect_timeout=8)


def _load_snapshot() -> list[dict]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT to_regclass('public.scraper_report_snapshot') IS NOT NULL AS ok")
            if not cur.fetchone()["ok"]:
                return []
            cur.execute(
                "SELECT data, computed_at FROM scraper_report_snapshot "
                "ORDER BY (data->>'total')::int DESC NULLS LAST"
            )
            out = []
            for row in cur.fetchall():
                d = dict(row["data"])
                d["computed_at"] = row["computed_at"].isoformat() if row["computed_at"] else None
                out.append(d)
            return out


# ─────────────────────────────────────────────────────────────────────────────
# Page

@scrapers_bp.route("/")
def index():
    sources = _load_snapshot()
    total_records  = sum(int(s.get("total") or 0) for s in sources)
    total_active = total_closed = 0
    for s in sources:
        b = s.get("buckets", {}) or {}
        for k in ("active", "active_pending", "active_qualified"):
            total_active += int((b.get(k) or {}).get("count", 0))
        for k in ("closed", "closed_by_doc"):
            total_closed += int((b.get(k) or {}).get("count", 0))
    summary = {
        "total_records":  total_records,
        "total_active":   total_active,
        "total_closed":   total_closed,
        "total_sources":  len(sources),
        "computed_at":    sources[0]["computed_at"] if sources else None,
    }
    return render_template("scrapers/index.html", sources=sources, summary=summary)


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot JSON APIs

@scrapers_bp.route("/api/all")
def api_all():
    return jsonify(_load_snapshot())


@scrapers_bp.route("/api/<key>")
def api_one(key: str):
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT data, computed_at FROM scraper_report_snapshot WHERE code = %s",
                (key,),
            )
            row = cur.fetchone()
    if not row:
        return jsonify({"error": f"unknown source: {key}"}), 404
    d = dict(row["data"])
    d["computed_at"] = row["computed_at"].isoformat() if row["computed_at"] else None
    return jsonify(d)


# ─────────────────────────────────────────────────────────────────────────────
# Row inspector — generic, per-source config

# Each source declares:
#   table        : SQL table name
#   id_col       : primary identifier (used in /row/<id> URLs)
#   list_cols    : ordered list of (sql_expr, alias, label) tuples shown in
#                  the table list. SQL expressions only — no JSON columns.
#   detail_scalar_cols: scalar columns to show in the detail modal header.
#   detail_json_cols  : JSON columns to return verbatim in the detail (the UI
#                       picks them up by name and renders with a custom
#                       visualizer).
#   search_cols  : columns to OR-match against the q= search param (scalar).
#   order_by     : ORDER BY clause for the list query.
_NC_PR_ROLES = (
    "'Applicant','Executor','Co-Executor','Administrator',"
    "'Co-Administrator','Petitioner','Affiant','Administrator CTA',"
    "'Ancillary Executor','Ancillary Administrator',"
    "'Public Administrator','Collector'"
)
_NC_HEIR_ROLES = "'Beneficiary','Next of Kin','Interested Person','Trust','Minor'"

# `decedent_full` is the formatted_name of any party whose roles[] contains
# 'Decedent'. Same pattern for petitioner_full and counts.
_NC_DECEDENT_FULL = (
    "(SELECT p->>'formatted_name' "
    " FROM jsonb_array_elements(parties) p "
    " WHERE jsonb_typeof(p->'roles')='array' AND p->'roles' ? 'Decedent' "
    " LIMIT 1)"
)
_NC_PETITIONER_FULL = (
    "(SELECT p->>'formatted_name' "
    " FROM jsonb_array_elements(parties) p "
    f" WHERE jsonb_typeof(p->'roles')='array' AND (p->'roles' ?| array[{_NC_PR_ROLES}]) "
    " LIMIT 1)"
)
_NC_DECEDENT_CITY_STATE = (
    "(SELECT TRIM(BOTH ', ' FROM CONCAT_WS(', ', a->>'city', a->>'state')) "
    " FROM jsonb_array_elements(parties) p, jsonb_array_elements(p->'addresses') a "
    " WHERE jsonb_typeof(p->'roles')='array' AND p->'roles' ? 'Decedent' "
    " LIMIT 1)"
)
_NC_N_PR = (
    "(SELECT COUNT(*) FROM jsonb_array_elements(parties) p "
    f" WHERE jsonb_typeof(p->'roles')='array' AND (p->'roles' ?| array[{_NC_PR_ROLES}]))"
)
_NC_N_BENEF = (
    "(SELECT COUNT(*) FROM jsonb_array_elements(parties) p "
    f" WHERE jsonb_typeof(p->'roles')='array' AND (p->'roles' ?| array[{_NC_HEIR_ROLES}]))"
)
_NC_N_DOCS = (
    "(CASE WHEN jsonb_typeof(documents)='array' "
    " THEN jsonb_array_length(documents) ELSE 0 END)"
)


SOURCES_INSPECTOR: dict[str, dict] = {
    "new_york": {
        "table": "records",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",                       "case_number",     "Case #"),
            ("county",                            "county",          "County"),
            ("filed_date",                        "filed_date",      "Filed"),
            ("death_date",                        "death_date",      "Died"),
            ("proceeding",                        "proceeding",      "Proceeding"),
            ("decedent->>'full_name'",            "decedent_name",   "Decedent"),
            ("petitioner->>'full_name'",          "petitioner_name", "Petitioner"),
            ("petitioner_active",                 "petitioner_active","Active?"),
            ("petitioner_phone",                  "petitioner_phone","PR phone"),
            ("petitioner_email",                  "petitioner_email","PR email"),
            ("(tiered_extraction IS NOT NULL)",   "has_extraction",  "Extracted"),
            ("jsonb_array_length(COALESCE(tiered_extraction->'persons','[]'::jsonb))",
                                                  "persons_n",       "Ppl"),
            ("jsonb_array_length(COALESCE(tiered_extraction->'real_property','[]'::jsonb))",
                                                  "props_n",         "Prop"),
            ("scrape_timestamp::date::text",      "scraped_on",      "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "court", "county", "filed_date", "death_date",
            "proceeding", "estate_closed",
            "petitioner_phone", "petitioner_email", "petitioner_role",
            "petitioner_active", "petitioner_appointed_date",
            "pdf_url", "pdf_status", "pdf_last_attempted_at", "pdf_error",
            "scrape_timestamp", "tiered_extracted_at", "derived_status",
        ],
        "detail_json_cols": [
            "decedent", "decedent_address",
            "petitioner", "petitioner_address",
            "other_parties",
            "document_names",
            "tiered_extraction",
        ],
        "search_cols": [
            "case_number",
            "county",
            "decedent->>'full_name'",
            "petitioner->>'full_name'",
            "proceeding",
        ],
        "order_by": "scrape_timestamp DESC NULLS LAST",
    },
    "north_carolina": {
        "table": "north_carolina",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",                "case_number",      "Case #"),
            ("county",                     "county",           "County"),
            ("case_type",                  "case_type",        "Type"),
            ("file_date::text",            "file_date",        "Filed"),
            (_NC_DECEDENT_FULL,            "decedent_full",    "Decedent"),
            (_NC_DECEDENT_CITY_STATE,      "decedent_csz",     "Decedent loc"),
            (_NC_PETITIONER_FULL,          "petitioner_full",  "PR (first)"),
            ("derived_status",             "derived_status",   "Status"),
            (_NC_N_PR,                     "n_pr",             "PR"),
            (_NC_N_BENEF,                  "n_benef",          "Benef"),
            (_NC_N_DOCS,                   "n_docs",           "Docs"),
            ("scraped_at::date::text",     "scraped_on",       "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "case_type", "case_type_code",
            "location_name", "county", "style", "file_date",
            "url", "fetch_status", "extraction_status", "extracted_at",
            "scraped_at", "derived_status",
        ],
        "detail_json_cols": [
            "parties",            # the canonical view rendered in the modal
            "documents",
            "raw_case_summary",
            "raw_case_events",
            "raw_disposition_events",
            "pdf_extractions",
        ],
        "search_cols": [
            "case_number", "county", "case_type",
            # parties::text gives a fast ILIKE on the formatted_name string
            "parties::text",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
        # Exclude guardianship rows from the inspector too.
        "where_filter": "derived_status IS DISTINCT FROM 'guardianship'",
    },
}


def _inspector(key: str) -> dict | None:
    return SOURCES_INSPECTOR.get(key)


@scrapers_bp.route("/<key>/rows")
def rows(key: str):
    cfg = _inspector(key)
    if not cfg:
        return jsonify({"error": f"row inspector not configured for: {key}"}), 404

    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit") or 50), 200)
    offset = max(int(request.args.get("offset") or 0), 0)

    clauses, params = [], []
    if q and cfg["search_cols"]:
        or_parts = []
        for col in cfg["search_cols"]:
            or_parts.append(f"COALESCE({col}::text,'') ILIKE %s")
            params.append(f"%{q}%")
        clauses.append("(" + " OR ".join(or_parts) + ")")
    if cfg.get("where_filter"):
        clauses.append(cfg["where_filter"])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    select_parts = [f'{expr} AS "{alias}"' for expr, alias, _ in cfg["list_cols"]]
    select_parts.append(f'{cfg["id_col"]}::text AS "_id"')
    select_sql = ", ".join(select_parts)

    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f'SELECT {select_sql} FROM "{cfg["table"]}" {where} '
                f'ORDER BY {cfg["order_by"]} LIMIT %s OFFSET %s',
                params + [limit, offset],
            )
            data = [dict(r) for r in cur.fetchall()]

            cur.execute(f'SELECT COUNT(*) AS n FROM "{cfg["table"]}" {where}', params)
            total = cur.fetchone()["n"]

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "cols": [{"alias": alias, "label": label} for _, alias, label in cfg["list_cols"]],
        "rows": data,
    })


@scrapers_bp.route("/<key>/row/<path:rid>")
def row_detail(key: str, rid: str):
    cfg = _inspector(key)
    if not cfg:
        return jsonify({"error": f"row inspector not configured for: {key}"}), 404

    rid = unquote(rid)
    cols = list(cfg["detail_scalar_cols"]) + list(cfg["detail_json_cols"])
    cols_sql = ", ".join(f'"{c}"' for c in cols)

    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f'SELECT {cols_sql} FROM "{cfg["table"]}" WHERE {cfg["id_col"]}::text = %s',
                (rid,),
            )
            row = cur.fetchone()
    if not row:
        return jsonify({"error": f"row not found: {rid}"}), 404

    out = {k: row[k] for k in row.keys()}
    return jsonify(out)
