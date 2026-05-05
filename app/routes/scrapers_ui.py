"""Scrapers status page — read-only inventory of scraped sources.

Two routes:
  /scrapers/                         → simple grid of scrapers with totals
  /scrapers/api/<key>/timeline       → daily scrape counts (for the line chart)

The page reads counts and timestamps from the local probate Postgres so the
operator can see exactly what's in the database without leaving the app.
"""

import os
import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, render_template

scrapers_bp = Blueprint("scrapers", __name__)


# Each entry uses a complete, human-readable name. `kind` selects the count
# strategy: "fc" reads from the shared foreclosure_records table partitioned
# by `source`; "tbl" reads from a dedicated per-state table.
SCRAPERS = [
    # Foreclosure feeds aren't tied to a county column in this DB, so the
    # county breakdown for these uses ZIP→state and skips county.
    {"key": "foreclosure_com", "name": "Foreclosure.com",  "category": "Foreclosure",
     "kind": "fc",  "source": "foreclosure_com"},
    {"key": "auction_com",     "name": "Auction.com",      "category": "Foreclosure",
     "kind": "fc",  "source": "auction_com"},

    # Probate feeds — county_col is the column name in the table that holds
    # the county/locality string. Used for the per-county drill-in.
    {"key": "new_york",        "name": "New York",         "category": "Probate",
     "kind": "tbl", "table": "records",          "ts_col": "scrape_timestamp", "county_col": "county"},
    {"key": "north_carolina",  "name": "North Carolina",   "category": "Probate",
     "kind": "tbl", "table": "north_carolina",   "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "maryland",        "name": "Maryland",         "category": "Probate",
     "kind": "tbl", "table": "maryland",         "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "connecticut",     "name": "Connecticut",      "category": "Probate",
     "kind": "tbl", "table": "ct_probate_cases", "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "georgia",         "name": "Georgia",          "category": "Probate",
     "kind": "tbl", "table": "ga_probate_cases", "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "gwinnett",        "name": "Gwinnett County, Georgia", "category": "Probate",
     "kind": "tbl", "table": "gwinnett",         "ts_col": "scraped_at",       "county_col": "location_name"},
    {"key": "oklahoma",        "name": "Oklahoma",         "category": "Probate",
     "kind": "tbl", "table": "ok_probate_cases", "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "wisconsin",       "name": "Wisconsin",        "category": "Probate",
     "kind": "tbl", "table": "wisconsin",        "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "indiana",         "name": "Indiana",          "category": "Probate",
     "kind": "tbl", "table": "indiana",          "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "minnesota",       "name": "Minnesota",        "category": "Probate",
     "kind": "tbl", "table": "minnesota",        "ts_col": "scraped_at",       "county_col": "county"},
    {"key": "michigan",        "name": "Michigan",         "category": "Probate",
     "kind": "tbl", "table": "michigan",         "ts_col": "scraped_at",       "county_col": "county"},
]


def _get_scraper(key: str):
    for s in SCRAPERS:
        if s["key"] == key:
            return s
    return None


def _dsn() -> str:
    # Scrapers page reads the local probate DB by design — that's where the
    # scrape pipeline writes. PROBATE_DATABASE_URL overrides for non-default
    # local setups; Neon is intentionally not consulted here.
    return os.getenv("PROBATE_DATABASE_URL") or "postgresql://localhost:5432/probate"


def _exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _scraper_stats(cur):
    out = []
    for s in SCRAPERS:
        rec = {"key": s["key"], "name": s["name"], "category": s["category"],
               "count": 0, "last_scraped": None, "missing": False}
        try:
            if s["kind"] == "fc":
                if _exists(cur, "foreclosure_records"):
                    cur.execute(
                        "SELECT COUNT(*)::bigint, MAX(scraped_at) "
                        "FROM foreclosure_records WHERE source = %s",
                        (s["source"],),
                    )
                    n, ts = cur.fetchone()
                    rec["count"] = int(n or 0)
                    rec["last_scraped"] = ts.isoformat() if ts else None
                else:
                    rec["missing"] = True
            else:
                if _exists(cur, s["table"]):
                    # Identifier comes from the hardcoded manifest, not user input.
                    cur.execute(
                        f'SELECT COUNT(*)::bigint, MAX("{s["ts_col"]}") '
                        f'FROM "{s["table"]}"'
                    )
                    n, ts = cur.fetchone()
                    rec["count"] = int(n or 0)
                    rec["last_scraped"] = ts.isoformat() if ts else None
                else:
                    rec["missing"] = True
        except Exception as e:  # noqa: BLE001 — show as inactive rather than 500
            rec["error"] = str(e)[:120]
            rec["missing"] = True
        out.append(rec)
    return out


@scrapers_bp.route("/")
def index():
    conn = psycopg2.connect(_dsn(), connect_timeout=5)
    try:
        with conn.cursor() as cur:
            scrapers = _scraper_stats(cur)
    finally:
        conn.close()

    total_records  = sum(s["count"] for s in scrapers)
    active_sources = sum(1 for s in scrapers if s["count"] > 0)

    summary = {
        "total_records":  total_records,
        "active_sources": active_sources,
        "total_sources":  len(scrapers),
    }
    return render_template("scrapers/index.html", scrapers=scrapers, summary=summary)


def _columns(cur, table: str) -> set[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


@scrapers_bp.route("/api/<key>/timeline")
def timeline(key: str):
    """Return per-scrape-day points (line chart) and per-county totals.

    Probate tables group by DATE(ts_col) for points and by county_col for
    counties. Foreclosure feeds skip the county breakdown — they're national
    aggregates indexed by ZIP, not county.
    """
    s = _get_scraper(key)
    if not s:
        return jsonify({"error": f"unknown scraper: {key}"}), 404

    points = []
    counties = []
    conn = psycopg2.connect(_dsn(), connect_timeout=5)
    try:
        with conn.cursor() as cur:
            if s["kind"] == "fc":
                if _exists(cur, "foreclosure_records"):
                    cur.execute(
                        "SELECT DATE(scraped_at) AS day, COUNT(*)::bigint AS count "
                        "FROM foreclosure_records WHERE source = %s "
                        "GROUP BY 1 ORDER BY 1",
                        (s["source"],),
                    )
                    points = [{"day": d.isoformat(), "count": int(n)} for d, n in cur.fetchall()]
            else:
                table   = s["table"]
                ts_col  = s["ts_col"]
                cty_col = s.get("county_col")
                if _exists(cur, table):
                    cur.execute(
                        f'SELECT DATE("{ts_col}") AS day, COUNT(*)::bigint AS count '
                        f'FROM "{table}" WHERE "{ts_col}" IS NOT NULL '
                        f'GROUP BY 1 ORDER BY 1'
                    )
                    points = [{"day": d.isoformat(), "count": int(n)} for d, n in cur.fetchall()]

                    cols = _columns(cur, table) if cty_col else set()
                    if cty_col and cty_col in cols:
                        # INITCAP + strip trailing " County" / " Court" so the
                        # display name is clean ("Wake", "Cobb") regardless of
                        # how the source spells it.
                        cur.execute(f'''
                            SELECT
                                INITCAP(REGEXP_REPLACE(
                                    COALESCE(NULLIF(TRIM("{cty_col}"), ''), '— Unknown'),
                                    '\\s+(County|Court).*$', '', 'i'
                                )) AS county,
                                COUNT(*)::bigint AS count,
                                MAX("{ts_col}") AS last_scraped
                            FROM "{table}"
                            GROUP BY 1
                            ORDER BY count DESC, county ASC
                        ''')
                        counties = [
                            {
                                "county":       r[0],
                                "count":        int(r[1] or 0),
                                "last_scraped": r[2].isoformat() if r[2] else None,
                            }
                            for r in cur.fetchall()
                        ]
    finally:
        conn.close()

    return jsonify({
        "key":      key,
        "name":     s["name"],
        "points":   points,
        "counties": counties,
        "total":    sum(p["count"] for p in points),
    })
