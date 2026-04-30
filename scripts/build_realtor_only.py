"""
Build Realtor-only static JSONs from Neon Postgres.

Replaces the multi-source pipeline (Zillow + Redfin + Census + FHFA) with a
single pass over `realtor_inventory_zip` (current month) and
`realtor_hotness_zip` (36-month history) plus the scraped mail-target counts
from listings.json / listings-zip.json.

Outputs (overwrites existing):
  app/static/data/zip-heatmap.json     — per-ZIP compact metrics
  app/static/data/county-heatmap.json  — per-county aggregates (active-weighted)
  app/static/data/state-heatmap.json   — per-state aggregates
  app/static/data/zip-detail.json      — per-ZIP with 36mo hotness trend
  app/static/data/county-detail.json   — per-county with 36mo aggregated trend
  app/static/data/scatter.json         — Buy/Exit scatter top-N counties

Usage:
  python3 scripts/build_realtor_only.py
"""
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
NEON_DB = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DB:
    sys.exit("NEON_DB not set")

STATIC = ROOT / "app" / "static" / "data"
OUT_ZIP_HEAT    = STATIC / "zip-heatmap.json"
OUT_COUNTY_HEAT = STATIC / "county-heatmap.json"
OUT_STATE_HEAT  = STATIC / "state-heatmap.json"
OUT_ZIP_DETAIL  = STATIC / "zip-detail.json"
OUT_COUNTY_DET  = STATIC / "county-detail.json"
OUT_SCATTER     = STATIC / "scatter.json"

ZIP_BY_COUNTY   = STATIC / "zip-by-county.json"
COUNTY_NAMES    = STATIC / "county-names.json"
LISTINGS_COUNTY = STATIC / "listings.json"
LISTINGS_ZIP    = STATIC / "listings-zip.json"

# Realtor data is point-in-time monthly. Pin trend window to last 36 months.
TREND_MONTHS = 36

# Mail-target bucket weights (from existing chat.py — kept identical).
MAIL_WEIGHTS = {"auc": 1.0, "fc": 1.2, "tl": 0.5, "bk": 0.4, "ss": 0.6}


# ─────────────────────────────────────────────────────────────────────────────
# Scoring (Realtor-only re-derivation)
# ─────────────────────────────────────────────────────────────────────────────

def _piecewise(val, points):
    if val is None:
        return None
    if val <= points[0][0]:
        return points[0][1]
    if val >= points[-1][0]:
        return points[-1][1]
    for i in range(len(points) - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        if x0 <= val <= x1:
            t = (val - x0) / (x1 - x0) if x1 > x0 else 0
            return y0 + t * (y1 - y0)
    return points[-1][1]


def _weighted(sigs, weights, min_coverage=0.4):
    total_w = sum(weights[k] for k, v in sigs.items() if v is not None)
    if total_w < min_coverage:
        return None
    return sum(weights[k] * sigs[k] for k, v in sigs.items() if v is not None) / total_w


def compute_buy_score(d):
    """Realtor-only Buy: price drops + YoY + active/new ratio + DOM."""
    pr = d.get("price_drops")           # 0..1
    yoy = d.get("yoy")                  # ratio, e.g., 0.08 = +8%
    active = d.get("active")
    new_l = d.get("new_listings")
    dom = d.get("dom")
    anr = (active / new_l) if active and new_l and new_l > 0 else None

    sigs = {
        "pr":  _piecewise(pr,  [(0, 0), (0.05, 10), (0.15, 45), (0.25, 70), (0.40, 100)]) if pr is not None else None,
        "yoy": _piecewise(yoy, [(-0.08, 100), (-0.03, 70), (0, 40), (0.05, 15), (0.12, 0)]) if yoy is not None else None,
        "anr": _piecewise(anr, [(1, 0), (2.5, 40), (4, 70), (6, 100)]) if anr else None,
        "dom": _piecewise(dom, [(30, 0), (60, 30), (90, 70), (120, 100)]) if dom else None,
    }
    weights = {"pr": 0.55, "yoy": 0.25, "anr": 0.15, "dom": 0.05}
    base = _weighted(sigs, weights)
    if base is None:
        return None

    # Gates (same as legacy)
    if pr is not None and yoy is not None:
        if pr > 0.25 and yoy < -0.02:
            base *= 1.12
        elif pr < 0.05 and yoy > 0.08:
            base *= 0.70
    return max(0, min(100, int(round(base))))


def compute_exit_score(d):
    """Realtor-only Exit: DOM + pending + hotness_score + YoY."""
    dom = d.get("dom")
    pending = d.get("pending_ratio")     # 0..1
    yoy = d.get("yoy")
    hotness = d.get("hotness_score")     # 0..100 (Realtor's own composite)

    sigs = {
        "dom":     _piecewise(dom,     [(15, 100), (30, 70), (60, 30), (90, 10), (150, 0)]) if dom else None,
        "pending": _piecewise(pending, [(0.03, 0), (0.10, 20), (0.25, 65), (0.40, 100)]) if pending is not None else None,
        "hot":     _piecewise(hotness, [(0, 0), (25, 25), (50, 50), (75, 80), (100, 100)]) if hotness is not None else None,
        "yoy":     _piecewise(yoy,     [(-0.05, 0), (0, 50), (0.05, 85), (0.10, 100)]) if yoy is not None else None,
    }
    weights = {"dom": 0.45, "pending": 0.30, "hot": 0.20, "yoy": 0.05}
    base = _weighted(sigs, weights)
    if base is None:
        return None

    # Gates
    if dom and pending is not None:
        if dom < 30 and pending > 0.40:
            base *= 1.15
        elif dom < 60 and pending > 0.25:
            base *= 1.10
    if dom and dom > 120:
        base *= 0.60
    return max(0, min(100, int(round(base))))


def compute_golden_score(buy, exit_s, active):
    if buy is None or exit_s is None or buy <= 0 or exit_s <= 0:
        return None
    base = math.sqrt(buy * exit_s)
    asymmetry = abs(buy - exit_s) / 100.0
    balance = 1 - (asymmetry * 0.3)
    if not active or active < 15:
        liq = 0.60
    elif active < 50:
        liq = 0.85
    elif active < 150:
        liq = 0.95
    elif active < 500:
        liq = 1.00
    else:
        liq = 1.05
    return max(0, min(100, int(round(base * balance * liq))))


def compute_mail_score(buckets, exit_s):
    """log10(weighted_mailable + 1) × (exit/100)^0.6 × 100. Same formula as legacy."""
    if not buckets:
        return 0
    total_w = sum(MAIL_WEIGHTS[k] * (buckets.get(k) or 0) for k in MAIL_WEIGHTS)
    if total_w <= 0:
        return 0
    e = (exit_s or 50) / 100.0
    return int(round(math.log10(total_w + 1) * (e ** 0.6) * 100))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _round(v, places=None):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    if places is None:
        return f
    return round(f, places)


def _pct(v, places=1):
    """Convert a 0..1 ratio to a 0..100 percentage."""
    if v is None:
        return None
    return _round(float(v) * 100.0, places)


def _wmean(vals, weights):
    """Weighted mean, ignoring None values. Returns None if all-None."""
    pairs = [(v, w) for v, w in zip(vals, weights) if v is not None and w]
    if not pairs:
        return None
    tot_w = sum(w for _, w in pairs)
    if tot_w <= 0:
        return None
    return sum(v * w for v, w in pairs) / tot_w


def _state_from_zip_name(name):
    """ '90210, beverly hills, ca' or 'beverly hills, ca' → 'CA'. """
    if not name:
        return None
    parts = [p.strip() for p in name.split(",") if p.strip()]
    if not parts:
        return None
    last = parts[-1]
    if len(last) == 2 and last.isalpha():
        return last.upper()
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Build
# ─────────────────────────────────────────────────────────────────────────────

def fetch_inventory(conn):
    """Return {zip5: {field...}} for the latest month present."""
    rows = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) AS m FROM realtor_inventory_zip")
        latest = cur.fetchone()["m"]
        cur.execute(
            """
            SELECT postal_code, zip_name, median_listing_price, median_listing_price_yy,
                   active_listing_count, new_listing_count,
                   median_days_on_market, price_reduced_share, price_increased_share,
                   pending_listing_count, pending_ratio, total_listing_count,
                   median_listing_price_per_square_foot, average_listing_price,
                   month_date_yyyymm
              FROM realtor_inventory_zip
             WHERE month_date_yyyymm = %s
            """,
            (latest,),
        )
        for r in cur.fetchall():
            r = dict(r)
            r["month"] = r.pop("month_date_yyyymm")
            rows[r["postal_code"]] = r
    return latest, rows


def fetch_hotness_latest(conn):
    """Return {zip5: hotness_score_latest_month_value}."""
    out = {}
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) FROM realtor_hotness_zip")
        latest = cur.fetchone()[0]
        cur.execute(
            """
            SELECT postal_code, hotness_score, hotness_rank, supply_score, demand_score
              FROM realtor_hotness_zip
             WHERE month_date_yyyymm = %s
            """,
            (latest,),
        )
        for z, hs, hr, ss, ds in cur.fetchall():
            out[z] = {
                "hotness_score": float(hs) if hs is not None else None,
                "hotness_rank":  int(hr) if hr is not None else None,
                "supply_score":  float(ss) if ss is not None else None,
                "demand_score":  float(ds) if ds is not None else None,
            }
    return latest, out


def fetch_hotness_history(conn, months=TREND_MONTHS):
    """Return {zip5: [(yyyymm, list_price, dom, hotness_score), ...]} ordered ascending."""
    out = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) FROM realtor_hotness_zip")
        latest = cur.fetchone()[0]
        # 36 months going back. Compute yyyymm_min loosely.
        yr = latest // 100
        mo = latest % 100
        # Step back `months` months
        total = yr * 12 + (mo - 1) - (months - 1)
        yy = total // 12
        mm = (total % 12) + 1
        min_month = yy * 100 + mm

        cur.execute(
            """
            SELECT postal_code, month_date_yyyymm,
                   median_listing_price, median_days_on_market, hotness_score
              FROM realtor_hotness_zip
             WHERE month_date_yyyymm >= %s
             ORDER BY postal_code, month_date_yyyymm
            """,
            (min_month,),
        )
        for z, m, lp, dom, hs in cur.fetchall():
            out[z].append((
                int(m),
                float(lp) if lp is not None else None,
                float(dom) if dom is not None else None,
                float(hs) if hs is not None else None,
            ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# County / state — read directly from Realtor's authoritative county+state files
# (so dashboard county cards match Realtor's published numbers byte-for-byte)
# ─────────────────────────────────────────────────────────────────────────────

def _strip_state_suffix(name):
    """ 'maricopa, az' -> 'Maricopa'. Realtor uses lowercase 'city, st' format. """
    if not name:
        return None
    parts = [p.strip() for p in name.rsplit(",", 1)]
    base = parts[0] if parts else name
    return base.title() if base else None


def fetch_inventory_county(conn):
    """Return (latest_month, {fips5: {fields...}}). Source of truth for county metrics."""
    rows = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) AS m FROM realtor_inventory_county")
        latest = cur.fetchone()["m"]
        cur.execute(
            """
            SELECT county_fips, county_name, median_listing_price,
                   median_listing_price_yy, active_listing_count, new_listing_count,
                   median_days_on_market, price_reduced_share, price_increased_share,
                   pending_listing_count, pending_ratio, total_listing_count,
                   median_listing_price_per_square_foot, average_listing_price,
                   month_date_yyyymm
              FROM realtor_inventory_county
             WHERE month_date_yyyymm = %s
            """,
            (latest,),
        )
        for r in cur.fetchall():
            r = dict(r)
            r["month"] = r.pop("month_date_yyyymm")
            rows[r["county_fips"]] = r
    return latest, rows


def fetch_hotness_county_latest(conn):
    out = {}
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) FROM realtor_hotness_county")
        latest = cur.fetchone()[0]
        cur.execute(
            """
            SELECT county_fips, hotness_score, hotness_rank, supply_score, demand_score
              FROM realtor_hotness_county
             WHERE month_date_yyyymm = %s
            """,
            (latest,),
        )
        for fips, hs, hr, ss, ds in cur.fetchall():
            out[fips] = {
                "hotness_score": float(hs) if hs is not None else None,
                "hotness_rank":  int(hr) if hr is not None else None,
                "supply_score":  float(ss) if ss is not None else None,
                "demand_score":  float(ds) if ds is not None else None,
            }
    return latest, out


def fetch_hotness_county_history(conn, months=TREND_MONTHS):
    """Return {fips5: [(yyyymm, list_price, dom, hotness_score), ...]} ordered ascending."""
    out = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) FROM realtor_hotness_county")
        latest = cur.fetchone()[0]
        yr = latest // 100
        mo = latest % 100
        total = yr * 12 + (mo - 1) - (months - 1)
        yy = total // 12
        mm = (total % 12) + 1
        min_month = yy * 100 + mm

        cur.execute(
            """
            SELECT county_fips, month_date_yyyymm,
                   median_listing_price, median_days_on_market, hotness_score
              FROM realtor_hotness_county
             WHERE month_date_yyyymm >= %s
             ORDER BY county_fips, month_date_yyyymm
            """,
            (min_month,),
        )
        for fips, m, lp, dom, hs in cur.fetchall():
            out[fips].append((
                int(m),
                float(lp) if lp is not None else None,
                float(dom) if dom is not None else None,
                float(hs) if hs is not None else None,
            ))
    return out


def fetch_inventory_state(conn):
    """Return (latest_month, {state_id: {fields...}})."""
    rows = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) AS m FROM realtor_inventory_state")
        latest = cur.fetchone()["m"]
        cur.execute(
            """
            SELECT state, state_id, median_listing_price, median_listing_price_yy,
                   active_listing_count, new_listing_count,
                   median_days_on_market, price_reduced_share, price_increased_share,
                   pending_listing_count, pending_ratio, total_listing_count
              FROM realtor_inventory_state
             WHERE month_date_yyyymm = %s
            """,
            (latest,),
        )
        for r in cur.fetchall():
            r = dict(r)
            rows[r["state_id"]] = r
    return latest, rows


def main():
    print(f"Connecting to Neon...")
    conn = psycopg2.connect(NEON_DB)
    conn.set_session(readonly=True)
    try:
        latest_inv, inv = fetch_inventory(conn)
        print(f"Inventory (ZIP): {len(inv)} ZIPs for month {latest_inv}")

        latest_hot, hot = fetch_hotness_latest(conn)
        print(f"Hotness latest (ZIP): {len(hot)} ZIPs for month {latest_hot}")

        history = fetch_hotness_history(conn)
        print(f"Hotness history (ZIP): {sum(len(v) for v in history.values())} rows across {len(history)} ZIPs")

        latest_inv_county, inv_county = fetch_inventory_county(conn)
        print(f"Inventory (County): {len(inv_county)} counties for month {latest_inv_county}")

        latest_hot_county, hot_county = fetch_hotness_county_latest(conn)
        print(f"Hotness latest (County): {len(hot_county)} counties")

        history_county = fetch_hotness_county_history(conn)
        print(f"Hotness history (County): {sum(len(v) for v in history_county.values())} rows across {len(history_county)} counties")

        latest_inv_state, inv_state = fetch_inventory_state(conn)
        print(f"Inventory (State): {len(inv_state)} states for month {latest_inv_state}")
    finally:
        conn.close()

    zip_by_county = json.loads(ZIP_BY_COUNTY.read_text()) if ZIP_BY_COUNTY.exists() else {}
    county_names  = json.loads(COUNTY_NAMES.read_text())  if COUNTY_NAMES.exists()  else {}
    listings_county = json.loads(LISTINGS_COUNTY.read_text()) if LISTINGS_COUNTY.exists() else {}
    listings_zip    = json.loads(LISTINGS_ZIP.read_text())    if LISTINGS_ZIP.exists()    else {}

    # ── Per-ZIP heatmap + scoring ────────────────────────────────────────────
    print("Building zip-heatmap.json ...")
    zip_heat = {}
    zip_compute = {}  # full numeric values for aggregation
    all_zips = set(inv.keys()) | set(hot.keys()) | set(listings_zip.keys())

    for z in sorted(all_zips):
        i = inv.get(z) or {}
        h = hot.get(z) or {}
        lz = listings_zip.get(z) or {}

        dom = _round(i.get("median_days_on_market"))
        pending_ratio = _round(i.get("pending_ratio"))
        yoy = _round(i.get("median_listing_price_yy"))
        list_price = _round(i.get("median_listing_price"))
        active = i.get("active_listing_count")
        new_l = i.get("new_listing_count")
        price_drops = _round(i.get("price_reduced_share"))
        hotness_score = h.get("hotness_score")

        feat = {
            "dom": dom, "pending_ratio": pending_ratio, "yoy": yoy,
            "active": int(active) if active is not None else None,
            "new_listings": int(new_l) if new_l is not None else None,
            "price_drops": price_drops, "hotness_score": hotness_score,
            "list_price": list_price,
        }
        bs = compute_buy_score(feat)
        es = compute_exit_score(feat)
        gs = compute_golden_score(bs, es, feat["active"])

        # Mail-target buckets
        buckets = {k: int(lz.get(k) or 0) for k in MAIL_WEIGHTS}
        mail_total = sum(buckets.values())
        ms = compute_mail_score(buckets, es) if mail_total > 0 else 0

        # Compact key set (matches existing UI expectations)
        rec = {
            "g":  gs, "bs": bs, "es": es,
            "lp": int(round(list_price)) if list_price else None,
            "vy": _pct(yoy, 2) if yoy is not None else None,           # YoY %
            "d":  int(round(dom)) if dom else None,
            "pr": _pct(price_drops, 1) if price_drops is not None else None,  # %
            "ppr": _pct(pending_ratio, 1) if pending_ratio is not None else None,
            "a": int(active) if active is not None else None,
            "hs": int(round(hotness_score)) if hotness_score is not None else None,
            "mail_score": ms, "mail_total": mail_total,
            "mail_auc": buckets["auc"], "mail_fc": buckets["fc"],
            "mail_tl": buckets["tl"], "mail_bk": buckets["bk"], "mail_ss": buckets["ss"],
            "fc_ct": buckets["fc"], "au_ct": buckets["auc"],
            "tot_ct": mail_total,
        }
        # Drop None to keep file lean
        zip_heat[z] = {k: v for k, v in rec.items() if v is not None}
        zip_compute[z] = {**feat, "bs": bs, "es": es, "gs": gs, "name": i.get("zip_name"),
                          "state": _state_from_zip_name(i.get("zip_name"))}

    OUT_ZIP_HEAT.write_text(json.dumps(zip_heat, separators=(",", ":")))
    print(f"  → {OUT_ZIP_HEAT.name}: {len(zip_heat)} ZIPs")

    # ── ZIP detail (36mo trend) ──────────────────────────────────────────────
    print("Building zip-detail.json ...")
    zip_detail = {}
    for z, series in history.items():
        if not series:
            continue
        cur = zip_compute.get(z) or {}
        zip_detail[z] = {
            "current": {
                "list_price": cur.get("yoy") and zip_heat.get(z, {}).get("lp"),
                "dom": cur.get("dom"),
                "hotness_score": cur.get("hotness_score"),
                "buy_score": cur.get("bs"),
                "exit_score": cur.get("es"),
                "golden_score": cur.get("gs"),
                "name": cur.get("name"),
                "state": cur.get("state"),
            },
            "trend": [
                {"m": m, "lp": lp, "d": dom, "hs": hs}
                for (m, lp, dom, hs) in series
            ],
        }
    OUT_ZIP_DETAIL.write_text(json.dumps(zip_detail, separators=(",", ":")))
    print(f"  → {OUT_ZIP_DETAIL.name}: {len(zip_detail)} ZIPs")

    # ── County heatmap — read straight from realtor_inventory_county ─────────
    # County numbers now match Realtor's published county file byte-for-byte.
    # ZIP heatmap is unchanged (per-ZIP from realtor_inventory_zip).
    print("Building county-heatmap.json ...")
    county_heat = {}
    county_compute = {}

    # Pull a state code from the county_name suffix, fall back to legacy mapping.
    def _state_from_county_name(nm):
        if not nm or "," not in nm:
            return None
        s = nm.rsplit(",", 1)[1].strip()
        return s.upper() if len(s) == 2 else None

    # Union of every county we have a Realtor record for (covers ~3,108) plus
    # any that appear in the scraped listings.json (mail targets) so we don't
    # drop counties with mail data but no Realtor coverage.
    all_fips = set(inv_county.keys()) | set(listings_county.keys()) | set(zip_by_county.keys())

    for fips in sorted(all_fips):
        c   = inv_county.get(fips) or {}
        h   = hot_county.get(fips) or {}
        lc  = listings_county.get(fips) or {}
        nm_legacy = (county_names.get(fips) or {})

        nm_raw = c.get("county_name")
        cname = _strip_state_suffix(nm_raw) or nm_legacy.get("name")
        state = _state_from_county_name(nm_raw) or nm_legacy.get("sc")

        list_price    = _round(c.get("median_listing_price"))
        yoy           = _round(c.get("median_listing_price_yy"))
        active        = c.get("active_listing_count")
        new_l         = c.get("new_listing_count")
        dom           = _round(c.get("median_days_on_market"))
        price_drops   = _round(c.get("price_reduced_share"))
        pending_ratio = _round(c.get("pending_ratio"))
        hotness_score = h.get("hotness_score")

        feat = {
            "dom": dom, "pending_ratio": pending_ratio, "yoy": yoy,
            "active": int(active) if active is not None else None,
            "new_listings": int(new_l) if new_l is not None else None,
            "price_drops": price_drops, "hotness_score": hotness_score,
            "list_price": list_price,
        }
        bs = compute_buy_score(feat)
        es = compute_exit_score(feat)
        gs = compute_golden_score(bs, es, feat["active"])

        # Mail-target buckets from scraped foreclosure_records via listings.json
        buckets = {k: int(lc.get(k) or 0) for k in MAIL_WEIGHTS}
        mail_total = sum(buckets.values())
        ms = compute_mail_score(buckets, es) if mail_total > 0 else 0

        # ZIP coverage count — handy in tooltips ("23 of 33 ZIPs reporting").
        county_zips = zip_by_county.get(fips) or []
        zc = sum(1 for z in county_zips if z in inv)

        rec = {
            "g":  gs, "bs": bs, "es": es,
            "lp": int(round(list_price)) if list_price else None,
            "vy": _pct(yoy, 2) if yoy is not None else None,
            "d":  int(round(dom)) if dom else None,
            "pr": _pct(price_drops, 1) if price_drops is not None else None,
            "ppr": _pct(pending_ratio, 1) if pending_ratio is not None else None,
            "a":  int(active) if active is not None else None,
            "hs": int(round(hotness_score)) if hotness_score is not None else None,
            "sc": state, "name": cname,
            "mail_score": ms, "mail_total": mail_total,
            "mail_auc": buckets["auc"], "mail_fc": buckets["fc"],
            "mail_tl": buckets["tl"], "mail_bk": buckets["bk"], "mail_ss": buckets["ss"],
            "fc_ct": buckets["fc"], "au_ct": buckets["auc"],
            "tot_ct": mail_total,
            "zc": zc,
        }
        county_heat[fips] = {k: v for k, v in rec.items() if v is not None}
        county_compute[fips] = {**feat, "bs": bs, "es": es, "gs": gs,
                                "state": state, "name": cname}

    OUT_COUNTY_HEAT.write_text(json.dumps(county_heat, separators=(",", ":")))
    print(f"  → {OUT_COUNTY_HEAT.name}: {len(county_heat)} counties")

    # ── State heatmap — read straight from realtor_inventory_state ──────────
    # No state hotness file from Realtor → derive state hotness as a county-
    # weighted mean (active-weighted across that state's counties).
    print("Building state-heatmap.json ...")
    state_heat = {}
    for st, s in inv_state.items():
        list_price    = _round(s.get("median_listing_price"))
        yoy           = _round(s.get("median_listing_price_yy"))
        active        = s.get("active_listing_count")
        new_l         = s.get("new_listing_count")
        dom           = _round(s.get("median_days_on_market"))
        price_drops   = _round(s.get("price_reduced_share"))
        pending_ratio = _round(s.get("pending_ratio"))

        # Hotness rollup from county hotness, weighted by county active listings.
        in_state = [(f, c) for f, c in county_compute.items() if c.get("state") == st]
        if in_state:
            wts = [(c.get("active") or 0) or 1 for _, c in in_state]
            hs_vals = [c.get("hotness_score") for _, c in in_state]
            agg_hs = _wmean(hs_vals, wts)
        else:
            agg_hs = None

        feat = {
            "dom": dom, "pending_ratio": pending_ratio, "yoy": yoy,
            "active": int(active) if active is not None else None,
            "new_listings": int(new_l) if new_l is not None else None,
            "price_drops": price_drops, "hotness_score": agg_hs,
        }
        bs = compute_buy_score(feat)
        es = compute_exit_score(feat)
        gs = compute_golden_score(bs, es, feat["active"])

        # Mail buckets summed across all counties in the state
        buckets = {k: 0 for k in MAIL_WEIGHTS}
        for f, _ in in_state:
            lc = listings_county.get(f) or {}
            for k in MAIL_WEIGHTS:
                buckets[k] += int(lc.get(k) or 0)
        mail_total = sum(buckets.values())
        ms = compute_mail_score(buckets, es) if mail_total > 0 else 0

        rec = {
            "g":  gs, "bs": bs, "es": es,
            "lp": int(round(list_price)) if list_price else None,
            "vy": _pct(yoy, 2) if yoy is not None else None,
            "d":  int(round(dom)) if dom else None,
            "pr": _pct(price_drops, 1) if price_drops is not None else None,
            "ppr": _pct(pending_ratio, 1) if pending_ratio is not None else None,
            "a":  int(active) if active is not None else None,
            "hs": int(round(agg_hs)) if agg_hs is not None else None,
            "name": s.get("state"),
            "mail_score": ms, "mail_total": mail_total,
            "mail_auc": buckets["auc"], "mail_fc": buckets["fc"],
            "mail_tl": buckets["tl"], "mail_bk": buckets["bk"], "mail_ss": buckets["ss"],
            "tot_ct": mail_total,
            "fips_ct": len(in_state),
        }
        state_heat[st] = {k: v for k, v in rec.items() if v is not None}

    OUT_STATE_HEAT.write_text(json.dumps(state_heat, separators=(",", ":")))
    print(f"  → {OUT_STATE_HEAT.name}: {len(state_heat)} states")

    # ── County detail (36-month trend, sourced from county hotness) ──────────
    # Reads realtor_hotness_county directly — no ZIP rollup. The trend matches
    # what Realtor publishes per-county.
    print("Building county-detail.json ...")
    county_detail = {}
    for fips, series in history_county.items():
        if not series:
            continue
        cur = county_compute.get(fips, {})
        county_detail[fips] = {
            "state": cur.get("state"),
            "current": {
                "buy_score":     cur.get("bs"),
                "exit_score":    cur.get("es"),
                "golden_score":  cur.get("gs"),
                "dom":           cur.get("dom"),
                "yoy":           cur.get("yoy"),
                "pending_ratio": cur.get("pending_ratio"),
                "price_drops":   cur.get("price_drops"),
                "hotness_score": cur.get("hotness_score"),
                "active":        cur.get("active"),
                "list_price":    cur.get("list_price"),
            },
            "trend": [
                {"m": m, "lp": lp, "d": dom, "hs": hs}
                for (m, lp, dom, hs) in series
            ],
        }
    OUT_COUNTY_DET.write_text(json.dumps(county_detail, separators=(",", ":")))
    print(f"  → {OUT_COUNTY_DET.name}: {len(county_detail)} counties")

    # ── Scatter (top counties by activity) ────────────────────────────────────
    # JS shape (matches legacy): {fips, name, sc, x=Exit, y=Buy, g, vol=active,
    #   v=list_price (replaces home_value), d=dom, pr=price_drops_%, vy=yoy_%}.
    print("Building scatter.json ...")
    counties = []
    for fips, c in county_compute.items():
        if c.get("bs") is None or c.get("es") is None:
            continue
        active = c.get("active") or 0
        # Use county_heat for already-rounded display fields where helpful.
        ch = county_heat.get(fips, {})
        counties.append({
            "fips": fips,
            "name": c.get("name"),
            "sc":   c.get("state"),
            "x":    c["es"],
            "y":    c["bs"],
            "g":    c.get("gs") or 0,
            "vol":  active,
            "v":    ch.get("lp"),         # list price (was Zillow home value before)
            "d":    ch.get("d"),
            "pr":   ch.get("pr"),
            "vy":   ch.get("vy"),
            "ppr":  ch.get("ppr"),
            "lp":   ch.get("lp"),
            "hs":   ch.get("hs"),
        })
    # Top 600 by active listings (deal flow).
    counties.sort(key=lambda r: r["vol"], reverse=True)
    counties = counties[:600]
    # Median for scatter axes (only consider rows with valid scores).
    xs = sorted([r["x"] for r in counties if r.get("x") is not None])
    ys = sorted([r["y"] for r in counties if r.get("y") is not None])
    med_x = xs[len(xs)//2] if xs else 50
    med_y = ys[len(ys)//2] if ys else 50
    OUT_SCATTER.write_text(json.dumps(
        {"counties": counties, "med_x": med_x, "med_y": med_y},
        separators=(",", ":"),
    ))
    print(f"  → {OUT_SCATTER.name}: {len(counties)} points (med_x={med_x}, med_y={med_y})")

    print(f"\nDone. Latest data month: inventory={latest_inv}, hotness={latest_hot}.")


if __name__ == "__main__":
    main()
