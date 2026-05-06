"""Scrapers status page — reads from `scraper_report_snapshot`.

Endpoints:
  /scrapers/                              page (snapshot summary + per-source report)
  /scrapers/api/all                       JSON list of every snapshot row (drives the page)
  /scrapers/api/<key>                     JSON for one source's snapshot
  /scrapers/ny/cases                      paged NY case list (search by name / case_number)
  /scrapers/ny/case/<case_number>         tiered_extraction for one NY case (drill-in)

Snapshot is built by `scripts/build_scraper_report.py` — we only read here.
Foreclosure/auction sources are intentionally not on this page anymore.
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
    """Return all snapshot rows ordered by total descending."""
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
    total_active   = sum((s.get("buckets", {}).get("active", {}) or {}).get("count", 0) for s in sources)
    total_active  += sum((s.get("buckets", {}).get("active_pending", {}) or {}).get("count", 0) for s in sources)
    total_active  += sum((s.get("buckets", {}).get("active_qualified", {}) or {}).get("count", 0) for s in sources)
    total_closed   = sum((s.get("buckets", {}).get("closed", {}) or {}).get("count", 0) for s in sources)
    total_closed  += sum((s.get("buckets", {}).get("closed_by_doc", {}) or {}).get("count", 0) for s in sources)
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
# NY drill-in

@scrapers_bp.route("/ny/cases")
def ny_cases():
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit") or 50), 200)
    offset = max(int(request.args.get("offset") or 0), 0)

    where = []
    params: list = []
    if q:
        where.append(
            "(case_number ILIKE %s "
            " OR COALESCE(decedent->>'full_name','') ILIKE %s "
            " OR COALESCE(petitioner->>'full_name','') ILIKE %s)"
        )
        params.extend([f"%{q}%"] * 3)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    case_number,
                    county,
                    filed_date,
                    death_date,
                    proceeding,
                    petitioner_active,
                    decedent->>'full_name'     AS decedent_name,
                    petitioner->>'full_name'   AS petitioner_name,
                    (tiered_extraction IS NOT NULL) AS has_extraction,
                    jsonb_array_length(COALESCE(tiered_extraction->'persons','[]'::jsonb)) AS persons_n,
                    jsonb_array_length(COALESCE(tiered_extraction->'real_property','[]'::jsonb)) AS props_n
                FROM records
                {where_sql}
                ORDER BY scrape_timestamp DESC NULLS LAST
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            rows = [dict(r) for r in cur.fetchall()]

            cur.execute(f"SELECT COUNT(*) FROM records {where_sql}", params)
            total = cur.fetchone()["count"]

    return jsonify({"total": total, "limit": limit, "offset": offset, "cases": rows})


@scrapers_bp.route("/ny/case/<path:case_number>")
def ny_case_detail(case_number: str):
    case_number = unquote(case_number)
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    case_number, court, county, filed_date, death_date, proceeding,
                    decedent, decedent_address,
                    petitioner, petitioner_address, petitioner_phone, petitioner_email,
                    petitioner_role, petitioner_active, petitioner_appointed_date,
                    other_parties,
                    document_names, pdf_url,
                    tiered_extraction, tiered_extracted_at
                FROM records
                WHERE case_number = %s
                """,
                (case_number,),
            )
            row = cur.fetchone()
    if not row:
        return jsonify({"error": f"unknown case: {case_number}"}), 404
    return jsonify(dict(row))
