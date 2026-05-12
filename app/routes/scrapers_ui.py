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
import json
import os
from pathlib import Path
from urllib.parse import unquote

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, render_template, request

scrapers_bp = Blueprint("scrapers", __name__)

_SEARCH_METHODS_PATH = (
    Path(__file__).resolve().parents[1]
    / "static" / "data" / "scraper_search_methods.json"
)


def _load_search_methods() -> dict:
    try:
        with _SEARCH_METHODS_PATH.open() as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


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
    search_methods = _load_search_methods()
    for s in sources:
        sm = search_methods.get(s.get("key")) or {}
        if sm:
            s["search"] = sm

    # Interleave a synthetic "AI extraction" card right after each AI-capable
    # scraper card. Cards are only emitted when the source actually has rows
    # with AI extraction, so /scrapers stays compact for scrapers that haven't
    # been run through extraction yet.
    interleaved: list[dict] = []
    for s in sources:
        interleaved.append(s)
        bk = s.get("key")
        if bk in _AI_CAPABLE_KEYS:
            ai = _compute_ai_card(bk)
            if ai:
                interleaved.append(ai)
    sources = interleaved

    total_records  = sum(int(s.get("total") or 0) for s in sources if not s.get("is_ai_card"))
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
        "total_sources":  sum(1 for s in sources if not s.get("is_ai_card")),
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
            "pdfs_synced_at",
            "scraped_at", "derived_status",
        ],
        "detail_json_cols": [
            "parties",            # the canonical view rendered in the modal
            "documents",
            "raw_case_summary",
            "raw_case_events",
            "raw_disposition_events",
            "pdf_extractions",
            "tiered_extraction",
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
    "georgia_probate_records": {
        "table": "georgia",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",         "case_number",     "Case #"),
            ("county",              "county",          "County"),
            ("court_name",          "court_name",      "Court"),
            ("file_date::text",     "file_date",       "Filed"),
            ("death_date::text",    "death_date",      "DOD"),
            ("decedent_full",       "decedent_full",   "Decedent"),
            ("CONCAT_WS(', ', NULLIF(decedent_city,''), NULLIF(decedent_state,''))",
                                    "decedent_csz",    "Decedent loc"),
            ("status",              "status",          "Source"),
            ("derived_status",      "derived_status",  "Derived"),
            ("(CASE WHEN jsonb_typeof(parties)='array' THEN jsonb_array_length(parties) ELSE 0 END)",
                                    "n_parties",       "Parties"),
            ("(CASE WHEN jsonb_typeof(documents)='array' THEN jsonb_array_length(documents) ELSE 0 END)",
                                    "n_docs",          "Docs"),
            ("scraped_at::date::text", "scraped_on",   "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "record_id", "county", "court_name", "url",
            "file_date", "death_date",
            "decedent_full", "decedent_first", "decedent_middle",
            "decedent_last", "decedent_suffix",
            "decedent_street", "decedent_city", "decedent_state", "decedent_zip",
            "fetch_status", "scraped_at", "status", "derived_status",
            "extracted_at", "extraction_status", "pdfs_synced_at",
        ],
        "detail_json_cols": [
            "parties", "documents", "raw_detail",
            "pdf_extractions", "tiered_extraction",
        ],
        "search_cols": [
            "case_number", "county", "court_name",
            "decedent_full", "decedent_first", "decedent_last",
            "status",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "gwinnett": {
        "table": "gwinnett",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",         "case_number",     "Case #"),
            ("location_name",       "location_name",   "Court"),
            ("file_date::text",     "file_date",       "Filed"),
            ("decedent_full",       "decedent_full",   "Decedent"),
            ("CONCAT_WS(', ', NULLIF(decedent_city,''), NULLIF(decedent_state,''))",
                                    "decedent_csz",    "Decedent loc"),
            ("case_status",         "case_status",     "Source status"),
            ("derived_status",      "derived_status",  "Derived"),
            ("(CASE WHEN jsonb_typeof(parties)='array' THEN jsonb_array_length(parties) ELSE 0 END)",
                                    "n_parties",       "Parties"),
            ("(CASE WHEN jsonb_typeof(events)='array' THEN jsonb_array_length(events) ELSE 0 END)",
                                    "n_events",        "Events"),
            ("scraped_at::date::text", "scraped_on",   "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "case_type", "case_type_code", "case_status",
            "case_status_code", "location_name", "style", "file_date",
            "record_id", "url", "court", "judicial_officer",
            "decedent_full", "decedent_first", "decedent_middle", "decedent_last",
            "decedent_suffix", "decedent_street", "decedent_city",
            "decedent_state", "decedent_zip",
            "fetch_status", "scraped_at", "derived_status",
        ],
        "detail_json_cols": ["parties", "events", "raw_search"],
        "search_cols": [
            "case_number", "decedent_full", "case_status", "judicial_officer",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "ga_cobb": {
        "table": "ga_probate_cases",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",         "case_number",     "Case #"),
            ("filed_date::text",    "filed_date",      "Filed"),
            ("death_date::text",    "death_date",      "Died"),
            ("decedent_full",       "decedent_full",   "Decedent"),
            ("TRIM(BOTH ' ' FROM CONCAT_WS(' ', petitioner_first, petitioner_middle, petitioner_last))",
                                    "petitioner_full", "PR (executor)"),
            ("derived_status",      "derived_status",  "Status"),
            ("(CASE WHEN jsonb_typeof(parties)='array' THEN jsonb_array_length(parties) ELSE 0 END)",
                                    "n_parties",       "Parties"),
            ("(CASE WHEN jsonb_typeof(pdf_refs)='array' THEN jsonb_array_length(pdf_refs) ELSE 0 END)",
                                    "n_pdfs",          "PDFs"),
            ("scraped_at::date::text", "scraped_on",   "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "county", "filed_date", "last_updated", "url",
            "decedent_full", "decedent_first", "decedent_middle", "decedent_last", "decedent_suffix",
            "decedent_street", "decedent_city", "decedent_state", "decedent_zip", "death_date",
            "petitioner_first", "petitioner_middle", "petitioner_last", "petitioner_suffix",
            "petitioner_street", "petitioner_city", "petitioner_state", "petitioner_zip",
            "petitioner_phone", "petitioner_email",
            "pdf_extracted_at", "scraped_at", "derived_status",
            "extracted_at", "extraction_status", "pdfs_synced_at",
        ],
        "detail_json_cols": [
            "parties", "pdf_refs", "raw",
            "pdf_extractions", "tiered_extraction",
        ],
        "search_cols": [
            "case_number", "decedent_full",
            "petitioner_first", "petitioner_last",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
        "where_filter": "county = 'Cobb'",
    },
    "ga_rockdale": {
        "table": "ga_probate_cases",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",         "case_number",     "Case #"),
            ("filed_date::text",    "filed_date",      "Filed"),
            ("decedent_full",       "decedent_full",   "Decedent"),
            ("TRIM(BOTH ' ' FROM CONCAT_WS(' ', petitioner_first, petitioner_middle, petitioner_last))",
                                    "petitioner_full", "PR"),
            ("derived_status",      "derived_status",  "Status"),
            ("(CASE WHEN jsonb_typeof(pdf_refs)='array' THEN jsonb_array_length(pdf_refs) ELSE 0 END)",
                                    "n_pdfs",          "PDFs"),
            ("scraped_at::date::text", "scraped_on",   "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "county", "filed_date", "last_updated", "url",
            "decedent_full", "decedent_first", "decedent_middle", "decedent_last", "decedent_suffix",
            "decedent_street", "decedent_city", "decedent_state", "decedent_zip", "death_date",
            "petitioner_first", "petitioner_middle", "petitioner_last", "petitioner_suffix",
            "petitioner_street", "petitioner_city", "petitioner_state", "petitioner_zip",
            "petitioner_phone", "petitioner_email",
            "pdf_extracted_at", "scraped_at", "derived_status",
            "extracted_at", "extraction_status", "pdfs_synced_at",
        ],
        "detail_json_cols": [
            "parties", "pdf_refs", "raw",
            "pdf_extractions", "tiered_extraction",
        ],
        "search_cols": [
            "case_number", "decedent_full",
            "petitioner_first", "petitioner_last",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
        "where_filter": "county = 'Rockdale'",
    },
    "indiana": {
        "table": "indiana",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",         "case_number",     "Case #"),
            ("county",              "county",          "County"),
            ("case_type",           "case_type",       "Type"),
            ("file_date",           "file_date",       "Filed"),
            ("decedent_full",       "decedent_full",   "Decedent"),
            ("decedent_dod",        "decedent_dod",    "DOD"),
            ("rep_full",            "rep_full",        "PR"),
            ("rep_role",            "rep_role",        "PR role"),
            ("rep_address_raw",     "rep_address_raw", "PR address"),
            ("case_status",         "case_status",     "Source"),
            ("derived_status",      "derived_status",  "Derived"),
            ("scraped_at::date::text", "scraped_on",   "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "case_key", "case_token", "case_type",
            "case_status", "county", "county_code", "file_date", "url",
            "decedent_full", "decedent_first", "decedent_middle",
            "decedent_last", "decedent_suffix", "decedent_dod",
            "decedent_street", "decedent_city", "decedent_state",
            "decedent_zip", "decedent_address_raw",
            "will", "will_date", "probate_date",
            "rep_full", "rep_first", "rep_middle", "rep_last",
            "rep_suffix", "rep_role",
            "rep_street", "rep_city", "rep_state", "rep_zip",
            "rep_address_raw",
            "fetch_status", "last_fetch_error", "last_fetch_error_at",
            "fetch_attempts", "scraped_at", "derived_status",
        ],
        # parties_json is TEXT but we still want it in the modal payload —
        # we'll parse it client-side.
        "detail_json_cols": ["parties_json", "raw_detail_json"],
        "search_cols": [
            "case_number", "county", "decedent_full", "rep_full",
            "case_type",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "wisconsin": {
        "table": "wisconsin",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",      "case_number",     "Case #"),
            ("county",           "county",          "County"),
            ("case_type",        "case_type",       "Type"),
            ("file_date::text",  "file_date",       "Filed"),
            ("TRIM(BOTH ' ' FROM CONCAT_WS(' ', decedent_first, decedent_middle, decedent_last, decedent_suffix))",
                                 "decedent_full",   "Decedent"),
            ("decedent_dob",     "decedent_dob",    "DOB"),
            ("(CASE WHEN jsonb_typeof(parties)='array' THEN jsonb_array_length(parties) ELSE 0 END)",
                                 "n_parties",       "Parties"),
            ("case_status",      "case_status",     "Status"),
            ("fetch_status",     "fetch_status",    "Detail fetch"),
            ("scraped_at::date::text", "scraped_on","Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "case_type", "case_type_code", "county_no",
            "county", "caption", "case_status", "file_date", "url",
            "decedent_first", "decedent_middle", "decedent_last",
            "decedent_suffix", "decedent_dob", "decedent_dob_sealed",
            "decedent_street", "decedent_city", "decedent_state", "decedent_zip",
            "fetch_status", "last_fetch_error", "last_fetch_error_at",
            "fetch_attempts", "scraped_at",
        ],
        "detail_json_cols": ["parties", "raw_search", "raw_case_detail"],
        "search_cols": [
            "case_number", "county", "caption",
            "decedent_first", "decedent_last",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "connecticut": {
        "table": "ct_probate_cases",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",          "case_number",      "Case #"),
            ("county",               "county",           "County"),
            ("court_name",           "court_name",       "Court / district"),
            ("case_type_code",       "case_type_code",   "Type"),
            ("filed_date::text",     "filed_date",       "Filed"),
            ("decedent_full",        "decedent_full",    "Decedent"),
            ("petitioner_full",      "petitioner_full",  "PR"),
            ("CONCAT_WS(', ', NULLIF(petitioner_city,''), NULLIF(petitioner_state,''))",
                                     "petitioner_csz",   "PR loc"),
            ("(CASE WHEN jsonb_typeof(parties)='array' THEN jsonb_array_length(parties) ELSE 0 END)",
                                     "n_parties",        "Parties"),
            ("scraped_at::date::text","scraped_on",      "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "case_id", "district_num", "county", "court_name",
            "case_type_code", "filed_date", "url",
            "decedent_first", "decedent_middle", "decedent_last", "decedent_full",
            "petitioner_first", "petitioner_middle", "petitioner_last", "petitioner_full",
            "petitioner_street", "petitioner_city", "petitioner_state", "petitioner_zip",
            "petitioner_phone",
            "scraped_at",
        ],
        "detail_json_cols": ["parties", "raw"],
        "search_cols": [
            "case_number", "county", "court_name", "case_type_code",
            "decedent_full", "petitioner_full",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "oklahoma": {
        "table": "ok_probate_cases",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",                                                      "case_number",     "Case #"),
            ("county",                                                           "county",          "County"),
            ("filed_date::text",                                                 "filed_date",      "Filed"),
            ("closed_date::text",                                                "closed_date",     "Closed"),
            ("TRIM(BOTH ' ' FROM CONCAT_WS(' ', decedent_first, decedent_middle, decedent_last))",
                                                                                 "decedent_full",   "Decedent"),
            ("TRIM(BOTH ' ' FROM CONCAT_WS(' ', petitioner_first, petitioner_middle, petitioner_last))",
                                                                                 "petitioner_full", "PR"),
            ("derived_status",                                                   "derived_status",  "Status"),
            ("(CASE WHEN jsonb_typeof(pdf_urls)='array' THEN jsonb_array_length(pdf_urls) ELSE 0 END)",
                                                                                 "n_pdfs",          "PDFs"),
            ("(pdf_extracted_at IS NOT NULL)",                                   "pdf_extracted",   "Extr"),
            ("scraped_at::date::text",                                           "scraped_on",      "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "county", "filed_date", "closed_date", "url",
            "decedent_first", "decedent_middle", "decedent_last",
            "decedent_street", "decedent_city", "decedent_state", "decedent_zip",
            "petitioner_first", "petitioner_middle", "petitioner_last",
            "petitioner_street", "petitioner_city", "petitioner_state", "petitioner_zip",
            "petitioner_phone", "petitioner_email",
            "pdf_extracted_at", "scraped_at", "derived_status",
            "extracted_at", "extraction_status", "pdfs_synced_at",
        ],
        "detail_json_cols": [
            "parties", "pdf_urls", "raw",
            "pdf_extractions", "tiered_extraction",
        ],
        "search_cols": [
            "case_number", "county",
            "decedent_first", "decedent_middle", "decedent_last",
            "petitioner_first", "petitioner_middle", "petitioner_last",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
    "maryland": {
        "table": "maryland",
        "id_col": "case_number",
        "list_cols": [
            ("case_number",   "case_number",     "Case #"),
            ("county",        "county",          "County"),
            ("case_type",     "case_type",       "Type"),
            ("file_date",     "file_date",       "Filed"),
            ("date_opened",   "date_opened",     "Opened"),
            ("date_closed",   "date_closed",     "Closed"),
            ("decedent_full", "decedent_full",   "Decedent"),
            ("decedent_dod",  "decedent_dod",    "DOD"),
            ("rep_full",      "rep_full",        "PR"),
            ("rep_address_raw", "rep_address_raw", "PR address"),
            ("case_status",   "case_status",     "Status"),
            ("will",          "will",            "Will"),
            ("scraped_at::date::text", "scraped_on", "Scraped"),
        ],
        "detail_scalar_cols": [
            "case_number", "record_id", "case_type", "county",
            "case_status", "file_date", "date_opened", "date_closed",
            "reference", "url",
            "decedent_full", "decedent_first", "decedent_middle",
            "decedent_last", "decedent_suffix", "decedent_dod",
            "aliases", "will", "will_date", "probate_date",
            "rep_full", "rep_first", "rep_middle", "rep_last",
            "rep_suffix", "rep_street", "rep_city", "rep_state",
            "rep_zip", "rep_address_raw",
            "fetch_status", "last_fetch_error", "last_fetch_error_at",
            "fetch_attempts", "scraped_at",
        ],
        # raw_detail_html is huge (~20KB) — keep it out of the modal payload
        # by default. Operators can hit the portal URL directly.
        "detail_json_cols": [],
        "search_cols": [
            "case_number", "county", "decedent_full", "rep_full",
            "aliases", "case_type",
        ],
        "order_by": "scraped_at DESC NULLS LAST",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# AI-extraction sibling inspectors
#
# For every scraper that has Anthropic-extracted rows, a synthetic key
# "<key>__ai" inspector is auto-generated from the base inspector by:
#   - AND-ing a "AI extraction is present" predicate into the WHERE clause
#   - swapping the list_cols for AI-focused columns (extracted_at, model,
#     #persons, #real_property, tokens)
# detail_scalar_cols / detail_json_cols are inherited as-is, so the row modal
# receives the same payload and the JS renderer decides what to highlight.

_AI_CAPABLE_KEYS = (
    "new_york",
    "north_carolina",
    "ga_cobb",
    "ga_rockdale",
    "oklahoma",
    "georgia_probate_records",
)

# Extraction-presence predicate: a row counts as "AI-extracted" only when the
# extractor actually produced output. We deliberately exclude rows where
# `tiered_extraction` exists but AI was disabled at extraction time — these
# show up as `model='(ai-disabled)'` with 0 input/output tokens, and they
# would otherwise clutter the AI EXTRACTED card with rows that have nothing
# in any column (the symptom in img_24.png).
_AI_PRESENT_SQL = (
    "("
    "  COALESCE((tiered_extraction->'totals'->>'input_tokens')::int, 0) > 0"
    "  OR COALESCE((tiered_extraction->'totals'->>'output_tokens')::int, 0) > 0"
    "  OR (jsonb_typeof(pdf_extractions) = 'array' AND jsonb_array_length(pdf_extractions) > 0)"
    ")"
)

# Per-row doc count — prefer the per-PDF array length when the source has it,
# otherwise fall back to `tiered_extraction.totals.docs_processed` (NY's
# consolidated rollup, which has no pdf_extractions[]).
_AI_N_DOCS_SQL = (
    "CASE "
    "  WHEN jsonb_typeof(pdf_extractions) = 'array' AND jsonb_array_length(pdf_extractions) > 0 "
    "       THEN jsonb_array_length(pdf_extractions) "
    "  ELSE COALESCE((tiered_extraction->'totals'->>'docs_processed')::int, 0) "
    "END"
)

# Persons extracted — prefer the consolidated rollup (`tiered_extraction.persons`).
# When only per-PDF extractions are available (NC/OK/GPR/Cobb-without-rollup),
# sum persons across *every* element so multi-doc cases aren't undercounted —
# the previous code only read `pdf_extractions[0]`.
_AI_N_PERSONS_SQL = (
    "CASE "
    "  WHEN jsonb_typeof(tiered_extraction->'persons') = 'array' "
    "       THEN jsonb_array_length(tiered_extraction->'persons') "
    "  ELSE COALESCE("
    "       (SELECT SUM(jsonb_array_length(COALESCE(e->'data'->'persons','[]'::jsonb)))::int"
    "          FROM jsonb_array_elements(COALESCE(pdf_extractions,'[]'::jsonb)) e), 0) "
    "END"
)

_AI_N_PROPERTY_SQL = (
    "CASE "
    "  WHEN jsonb_typeof(tiered_extraction->'real_property') = 'array' "
    "       THEN jsonb_array_length(tiered_extraction->'real_property') "
    "  ELSE COALESCE("
    "       (SELECT SUM(jsonb_array_length(COALESCE(e->'data'->'real_property','[]'::jsonb)))::int"
    "          FROM jsonb_array_elements(COALESCE(pdf_extractions,'[]'::jsonb)) e), 0) "
    "END"
)

# Latest extraction timestamp / model — column layout differs per table.
# `records` (NY) only has `tiered_extracted_at` populated; every other AI-
# capable table uses `extracted_at`. Model lives in
# `tiered_extraction.extraction_log[0].model` on NY but in
# `pdf_extractions[0].model` everywhere else.
_AI_EXPR_BY_TABLE = {
    # NY — consolidated extraction only
    "records": {
        "extracted_at":  "tiered_extracted_at",
        "model":         "(tiered_extraction->'extraction_log'->0->>'model')",
    },
}
_AI_EXPR_DEFAULT = {
    "extracted_at":  "extracted_at",
    "model":         "(pdf_extractions->0->>'model')",
}

def _ai_expr(table: str, key: str) -> str:
    return _AI_EXPR_BY_TABLE.get(table, _AI_EXPR_DEFAULT).get(
        key, _AI_EXPR_DEFAULT[key]
    )


def _build_ai_inspector(base_key: str) -> dict | None:
    base = SOURCES_INSPECTOR.get(base_key)
    if not base:
        return None

    ts_expr    = _ai_expr(base["table"], "extracted_at")
    model_expr = _ai_expr(base["table"], "model")

    list_cols_base = [
        ("case_number",          "case_number",      "Case #"),
        (f"{ts_expr}::date::text", "extracted_on",   "Extracted"),
        (_AI_N_DOCS_SQL,         "n_docs",           "Docs"),
        (_AI_N_PERSONS_SQL,      "n_persons",        "Ppl"),
        (_AI_N_PROPERTY_SQL,     "n_property",       "Prop"),
        (model_expr,             "model",            "Model"),
    ]

    # Find the "case label" col already in the base so we can keep at least one
    # human-readable label up front. We always include case_number as col 0,
    # then prepend a "Decedent" label sourced the same way the base does.
    label_col = None
    for expr, alias, lbl in base["list_cols"]:
        if alias in ("decedent_full", "decedent_name"):
            label_col = (expr, alias, "Decedent")
            break

    list_cols = [list_cols_base[0]]  # case_number
    if label_col:
        list_cols.append(label_col)
    list_cols += list_cols_base[1:]

    # Compose WHERE: AI present + structural base filters only (e.g. Cobb /
    # Rockdale split by county). We deliberately drop status-derived filters
    # like `derived_status IS DISTINCT FROM 'guardianship'`, because:
    #   1. derived_status doesn't exist on every dev/local schema, and
    #   2. AI extraction implies non-guardianship by design — operators don't
    #      run extraction on those rows.
    where_parts = [_AI_PRESENT_SQL]
    base_where = base.get("where_filter") or ""
    if base_where and "derived_status" not in base_where:
        where_parts.append(base_where)
    where_filter = " AND ".join(where_parts)

    return {
        "table":              base["table"],
        "id_col":             base["id_col"],
        "list_cols":          list_cols,
        "detail_scalar_cols": base["detail_scalar_cols"],
        "detail_json_cols":   base["detail_json_cols"],
        "search_cols":        base["search_cols"],
        "order_by":           f"{ts_expr} DESC NULLS LAST",
        "where_filter":       where_filter,
        "ai_parent":          base_key,  # marker so the frontend can theme it
        "ai_ts_expr":         ts_expr,
        "ai_model_expr":      model_expr,
    }


for _bk in _AI_CAPABLE_KEYS:
    SOURCES_INSPECTOR[f"{_bk}__ai"] = _build_ai_inspector(_bk)


def _compute_ai_card(base_key: str) -> dict | None:
    """Return a synthetic snapshot-card dict for the AI-extracted sibling of
    `base_key`, or None if the source has no AI-extracted rows yet."""
    cfg = SOURCES_INSPECTOR.get(f"{base_key}__ai")
    if not cfg:
        return None
    where_sql = f"WHERE {cfg['where_filter']}"
    # Tokens deliberately omitted — the upstream extractor only records the
    # uncached delta (~2 input tokens/doc on every source), which is
    # misleading. Output_tokens is honest but on its own is more noise than
    # signal at the card level, so we drop both until the pipeline records
    # true input including cache reads.
    sql = (
        f"SELECT "
        f"  COUNT(*) AS n, "
        f"  MAX({cfg['ai_ts_expr']}) AS latest, "
        f"  MAX({cfg['ai_model_expr']}) AS model "
        f'FROM "{cfg["table"]}" {where_sql}'
    )
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                r = cur.fetchone()
    except Exception:
        return None
    if not r or not r.get("n"):
        return None
    return {
        "key":          f"{base_key}__ai",
        "is_ai_card":   True,
        "ai_parent":    base_key,
        "name":         "AI extraction",
        "total":        int(r["n"]),
        "latest":       r["latest"].isoformat() if r["latest"] else None,
        "model":        r["model"],
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


_TABLE_COLUMNS_CACHE: dict[str, set[str]] = {}


def _existing_columns(cur, table: str) -> set[str]:
    """Return the set of columns actually present on `table`. Cached for the
    process lifetime — schema doesn't change at runtime, and this lets the
    inspector tolerate local-vs-Neon drift (e.g. `derived_status` exists on
    Neon but not on every local schema) instead of 500-ing on the modal."""
    if table not in _TABLE_COLUMNS_CACHE:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (table,),
        )
        _TABLE_COLUMNS_CACHE[table] = {r["column_name"] for r in cur.fetchall()}
    return _TABLE_COLUMNS_CACHE[table]


@scrapers_bp.route("/<key>/row/<path:rid>")
def row_detail(key: str, rid: str):
    cfg = _inspector(key)
    if not cfg:
        return jsonify({"error": f"row inspector not configured for: {key}"}), 404

    rid = unquote(rid)
    requested = list(cfg["detail_scalar_cols"]) + list(cfg["detail_json_cols"])

    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            present = _existing_columns(cur, cfg["table"])
            cols = [c for c in requested if c in present]
            missing = [c for c in requested if c not in present]
            cols_sql = ", ".join(f'"{c}"' for c in cols)
            cur.execute(
                f'SELECT {cols_sql} FROM "{cfg["table"]}" WHERE {cfg["id_col"]}::text = %s',
                (rid,),
            )
            row = cur.fetchone()
    if not row:
        return jsonify({"error": f"row not found: {rid}"}), 404

    out = {k: row[k] for k in row.keys()}
    # Expose missing-column NULLs explicitly so the frontend renderer can show
    # a placeholder instead of `undefined` lookups.
    for m in missing:
        out.setdefault(m, None)
    return jsonify(out)
