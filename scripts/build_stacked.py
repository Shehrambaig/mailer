"""
Build stacked JSON files at all 5 geo levels by overlapping Realtor + Zillow +
Redfin + scraped foreclosure_records data into a single shape per geo entry.

Inputs:
  Mailer_Data_set/Realtor/Monthly Housing Inventory/{nation,state,metro,county,zip}.csv
  Mailer_Data_set/Realtor/Monthly Market Hotness/{Metro,County,Zip}_History.csv
  Mailer_Data_set/Zillow/Metro&US/{6 wide-format metro files}.csv
  Mailer_Data_set/Zillow/Zip Code Level/Zillow Home Value Index (ZHVI).csv
  Mailer_Data_set/Zillow/Zip Code Level/zil_home_val_month (1).csv         (1y forecast)
  Mailer_Data_set/Redfin/Monthly_marketing_data.csv                        (UTF-16, top 50 metros)
  app/static/data/listings.json        (county scraped counts)
  app/static/data/listings-zip.json    (ZIP scraped counts)
  app/static/data/zip-by-county.json   (ZIP↔FIPS crosswalk)

Outputs:
  app/static/data/stacked/national.json
  app/static/data/stacked/state.json     {ST: {...}}
  app/static/data/stacked/metro.json     {cbsa_code: {...}}
  app/static/data/stacked/county.json    {fips: {...}}
  app/static/data/stacked/zip.json       {zip5: {...}}

Each entry shape (county example):
  {
    "id": "06037",
    "name": "Los Angeles County",
    "state": "CA",
    "metro": {"code": "31080", "title": "Los Angeles-Long Beach-Anaheim, CA"},
    "month": "202603",
    "metrics": {
      "dom_days":          {"v": 45,    "src": "realtor"},
      "list_price":        {"v": 999000,"src": "realtor"},
      "home_value_zhvi":   {"v": 825000,"src": "zillow", "derived":"rollup_zip","n":87},
      ...
    },
    "history": {
      "hotness_score": [["202404",0.30],["202405",0.32],...],   # last 12mo
      "list_price":    [["202404",985000],...]                  # last 12mo
    },
    "scraped": {
      "total": 904, "auc": 43, "fc": 4, "tl": 0, "bk": 855, "ss": 0,
      "mail_score": 67, "next_date": "2026-04-28",
      "avg_val": 200251, "total_val": 47259219
    }
  }

Run:
  python scripts/build_stacked.py
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATASET = os.path.join(ROOT, "Mailer_Data_set")
STATIC = os.path.join(ROOT, "app", "static", "data")
OUT = os.path.join(STATIC, "stacked")
HISTORY_MONTHS = 12        # how much trailing history to keep per metric
ZHVI_HISTORY_MONTHS = 24   # ZHVI is the marquee trend, keep more

os.makedirs(OUT, exist_ok=True)

# ── Helpers ──────────────────────────────────────────────────────────────
def num(s):
    if s is None:
        return None
    s = str(s).strip()
    if not s or s.lower() in ("nan", "null", "none"):
        return None
    try:
        v = float(s.replace(",", "").replace("$", "").replace("%", ""))
    except ValueError:
        return None
    if v != v:  # NaN
        return None
    return v


def yyyymm_from_iso(d):
    """'2026-03-31' → '202603'."""
    return d[:4] + d[5:7] if d else None


def yyyymm_to_int(s):
    return int(s) if s and s.isdigit() else 0


def latest_pair(row, date_cols):
    """Return (yyyymm, value) for the latest non-null cell in `date_cols`."""
    for d in reversed(date_cols):
        v = num(row.get(d))
        if v is not None:
            return yyyymm_from_iso(d), v
    return None, None


def history_pairs(row, date_cols, n):
    """Return [(yyyymm, value), ...] for the last n non-null months."""
    out = []
    for d in reversed(date_cols):
        v = num(row.get(d))
        if v is not None:
            out.append((yyyymm_from_iso(d), v))
            if len(out) >= n:
                break
    return list(reversed(out))


def pad5(s):
    return str(s).strip().zfill(5) if s else ""


def _compact(v):
    """Round numbers to a sensible precision before JSON serialization."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if abs(v) >= 1000:
            return int(round(v))
        return round(v, 4)
    return v


def metric(v, src, **extra):
    """Compact representation:
       - "realtor" is the default source → omit src key when realtor
       - Numbers rounded to 4 decimals (or int when ≥1000)."""
    if v is None:
        return None
    out = {"v": _compact(v)}
    if src and src != "realtor":
        out["src"] = src
    out.update(extra)
    return out


# ── Realtor inventory loaders (single-month, per-level) ──────────────────
def load_realtor_inventory(path, key_field, key_xform=None):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for row in csv.DictReader(f):
            k = (row.get(key_field) or "").strip()
            if key_xform:
                k = key_xform(k)
            if not k:
                continue
            out[k] = row
    return out


# ── Realtor hotness history loader (multi-month, per-level) ──────────────
def load_realtor_hotness_history(path, key_field, key_xform=None):
    """
    Returns:
      latest:  {key: row_dict for the most recent month}
      history: {key: {metric: [(yyyymm, value), ...]}} — last HISTORY_MONTHS
    Also returns county→cbsa map when key_field is county_fips.
    """
    by_key_month = defaultdict(dict)   # key → {month_int: row}
    cbsa_map = {}
    if not os.path.exists(path):
        return {}, {}, cbsa_map
    with open(path) as f:
        for row in csv.DictReader(f):
            k = (row.get(key_field) or "").strip()
            if key_xform:
                k = key_xform(k)
            if not k:
                continue
            m = yyyymm_to_int(row.get("month_date_yyyymm", ""))
            if not m:
                continue
            by_key_month[k][m] = row
            if key_field == "county_fips":
                cbsa = (row.get("cbsa_code") or "").strip()
                if cbsa and k not in cbsa_map:
                    cbsa_map[k] = (cbsa, row.get("cbsa_title", ""))

    latest, history = {}, {}
    HOT_METRICS = ("hotness_score", "hotness_rank", "median_listing_price",
                   "median_days_on_market", "supply_score", "demand_score",
                   "median_listing_price_vs_us")
    for k, months in by_key_month.items():
        sorted_months = sorted(months)
        latest[k] = months[sorted_months[-1]]
        recent = sorted_months[-HISTORY_MONTHS:]
        h = {}
        for col in HOT_METRICS:
            ts = []
            for m in recent:
                v = num(months[m].get(col))
                if v is not None:
                    ts.append([str(m), _compact(v)])
            if ts:
                h[col] = ts
        if h:
            history[k] = h
    return latest, history, cbsa_map


# ── Zillow Metro&US wide-format loader ───────────────────────────────────
def load_zillow_metro_wide(path, history_n=HISTORY_MONTHS):
    """
    Returns:
      by_region: {RegionID(str): {"name", "state", "type", "latest_month",
                                  "latest_value", "history": [(yyyymm,v)]}}
    """
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []
        date_cols = [c for c in cols if len(c) == 10 and c[4] == "-" and c[7] == "-"]
        date_cols.sort()
        out = {}
        for row in rdr:
            rid = row.get("RegionID", "").strip()
            if not rid:
                continue
            mo, val = latest_pair(row, date_cols)
            hist = history_pairs(row, date_cols, history_n)
            out[rid] = {
                "name": row.get("RegionName", "").strip(),
                "state": row.get("StateName", "").strip(),
                "type": row.get("RegionType", "").strip(),
                "latest_month": mo,
                "latest_value": val,
                "history": hist,
            }
    return out


# ── Zillow ZIP wide-format loaders ───────────────────────────────────────
def load_zillow_zip_zhvi(path, history_n=ZHVI_HISTORY_MONTHS):
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []
        date_cols = [c for c in cols if len(c) == 10 and c[4] == "-" and c[7] == "-"]
        date_cols.sort()
        out = {}
        for row in rdr:
            zip5 = pad5(row.get("RegionName"))
            if not zip5:
                continue
            mo, val = latest_pair(row, date_cols)
            if val is None:
                continue
            out[zip5] = {
                "city": row.get("City", "").strip(),
                "state": row.get("State", "").strip(),
                "metro": row.get("Metro", "").strip(),
                "latest_month": mo,
                "latest_value": val,
                "history": [[m, _compact(v)] for m, v in history_pairs(row, date_cols, history_n)],
            }
    return out


def load_zillow_zip_forecast(path):
    """Forecast file: BaseDate is a date string; forward columns are
    cumulative percent changes from base (e.g. '-1.4' = -1.4 %)."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []
        date_cols = sorted(c for c in cols
                           if len(c) == 10 and c[4] == "-" and c[7] == "-"
                           and c != "BaseDate")
        out = {}
        for row in rdr:
            zip5 = pad5(row.get("RegionName"))
            if not zip5:
                continue
            target_mo, target_pct = None, None
            for d in reversed(date_cols):
                v = num(row.get(d))
                if v is not None:
                    target_mo, target_pct = yyyymm_from_iso(d), v
                    break
            if target_pct is not None:
                out[zip5] = {
                    "base_date": (row.get("BaseDate") or "").strip(),
                    "forecast_month": target_mo,
                    "forecast_pct": target_pct,  # already a percent (e.g. -1.4)
                }
    return out


# ── Redfin loader (UTF-16, tab-separated, ~50 metros + National) ─────────
def load_redfin_metro(path):
    if not os.path.exists(path):
        return {}
    with open(path, "rb") as f:
        text = f.read().decode("utf-16").replace("\r\n", "\n")
    lines = [l for l in text.split("\n") if l.strip()]
    if not lines:
        return {}
    header = [h.strip() for h in lines[0].split("\t")]
    out = defaultdict(dict)
    for line in lines[1:]:
        cells = line.split("\t")
        if len(cells) != len(header):
            continue
        rec = dict(zip(header, cells))
        region = rec.get("Region", "").strip()
        month = rec.get("Month of Period End", "").strip()
        if not region or not month:
            continue
        # Pick the latest month per region (sorted lexically by parsed month).
        try:
            mo_dt = datetime.strptime(month, "%B %Y")
        except ValueError:
            continue
        existing = out.get(region, {}).get("_dt")
        if existing and existing >= mo_dt:
            continue
        # Convert money/% strings to floats where possible.
        parsed = {"_dt": mo_dt, "month": mo_dt.strftime("%Y%m")}
        for k, v in rec.items():
            if k in ("Region", "Month of Period End"):
                continue
            v = (v or "").strip()
            if not v:
                continue
            mult = 1.0
            sv = v.replace(",", "").replace("$", "").replace("%", "").strip()
            if sv.endswith("K"):
                mult, sv = 1_000, sv[:-1]
            elif sv.endswith("M"):
                mult, sv = 1_000_000, sv[:-1]
            try:
                parsed[k] = float(sv) * mult
            except ValueError:
                parsed[k] = v
        out[region] = parsed
    # Drop sentinel
    for r in out.values():
        r.pop("_dt", None)
    return dict(out)


def redfin_match_metro(redfin_keys, realtor_metros):
    """Map Redfin region label → cbsa_code by primary-city + first-state match."""
    # Build {("city_lower", "STATE"): cbsa} from Realtor metro file
    idx = {}
    for cbsa, title in realtor_metros.items():
        if "," not in title:
            continue
        cities, states = title.rsplit(", ", 1)
        first_city = cities.split("-")[0].strip().lower()
        first_state = states.split("-")[0].strip().upper()
        idx.setdefault((first_city, first_state), cbsa)
    out = {}
    for r in redfin_keys:
        rl = r.strip()
        if rl == "National":
            out[r] = "_NATION"
            continue
        # "New York, NY metro area" or "Anaheim, CA metro area"
        body = rl.replace(" metro area", "").strip()
        if "," not in body:
            continue
        c, s = body.rsplit(", ", 1)
        c = c.split("-")[0].strip().lower()
        s = s.strip().upper()
        cbsa = idx.get((c, s))
        if cbsa:
            out[r] = cbsa
    return out


def zillow_metro_to_cbsa(zillow_metros, realtor_metros):
    """Map Zillow RegionID → Realtor cbsa_code by primary-city + state match."""
    idx = {}
    for cbsa, title in realtor_metros.items():
        if "," not in title:
            continue
        cities, states = title.rsplit(", ", 1)
        first_city = cities.split("-")[0].strip().lower()
        first_state = states.split("-")[0].strip().upper()
        idx.setdefault((first_city, first_state), cbsa)
    out = {}
    for rid, m in zillow_metros.items():
        name = m["name"]
        if "," not in name:
            continue
        c, s = name.rsplit(", ", 1)
        c = c.split("-")[0].strip().lower()
        s = s.strip().upper()
        cbsa = idx.get((c, s))
        if cbsa:
            out[rid] = cbsa
    return out


# ── Realtor inventory canonicalization ───────────────────────────────────
# Map raw Realtor inventory column → canonical metric key
INV_MAP = {
    "median_listing_price":          "list_price",
    "median_listing_price_yy":       "list_price_yoy",
    "active_listing_count":          "for_sale_count",
    "median_days_on_market":         "dom_days",
    "new_listing_count":             "new_listings",
    "price_increased_count":         "price_increased_count",
    "price_increased_share":         "price_increased_share",
    "price_reduced_count":           "price_drops_count",
    "price_reduced_share":           "price_drops_share",
    "pending_listing_count":         "pending_count",
    "pending_ratio":                 "pending_ratio",
    "median_listing_price_per_square_foot": "ppsf",
    "median_square_feet":            "median_sqft",
    "average_listing_price":         "list_price_avg",
    "total_listing_count":           "total_listings",
}


def canonicalize_realtor_inventory(row):
    """Pull canonical metrics from a Realtor inventory row."""
    out = {}
    for raw, canon in INV_MAP.items():
        v = num(row.get(raw))
        if v is not None:
            out[canon] = metric(v, "realtor")
    return out


def canonicalize_realtor_hotness(row):
    out = {}
    for raw, canon in (
        ("hotness_score", "hotness_score"),
        ("hotness_rank", "hotness_rank"),
        ("supply_score", "supply_score"),
        ("demand_score", "demand_score"),
        ("median_listing_price_vs_us", "list_price_vs_us"),
    ):
        v = num(row.get(raw))
        if v is not None:
            out[canon] = metric(v, "realtor")
    return out


# ── Scraped data loaders ─────────────────────────────────────────────────
def load_scraped():
    """Return (county_dict, zip_dict) keyed by fips and zip5.

    County entries get `mail_score` merged in from county-heatmap.json
    (it's computed by import_listings.py but stored on the heatmap, not the
    listings counts file)."""
    cnty = {}
    p = os.path.join(STATIC, "listings.json")
    if os.path.exists(p):
        cnty = json.load(open(p))
    # Overlay mail_score from county-heatmap.json
    p = os.path.join(STATIC, "county-heatmap.json")
    if os.path.exists(p):
        heat = json.load(open(p))
        for fips, d in cnty.items():
            ms = (heat.get(fips) or {}).get("mail_score")
            if ms is not None:
                d["mail_score"] = ms
    zp = {}
    p = os.path.join(STATIC, "listings-zip.json")
    if os.path.exists(p):
        raw = json.load(open(p))
        for z, d in raw.items():
            zp[z] = {
                "total": d.get("total", 0),
                "auc": d.get("auc", 0), "fc": d.get("fc", 0), "tl": d.get("tl", 0),
                "bk": d.get("bk", 0), "ss": d.get("ss", 0),
                "mail_score": d.get("mail_score", 0),
                "upcoming_30": d.get("upcoming_30", 0),
                "upcoming_60": d.get("upcoming_60", 0),
                "upcoming_90": d.get("upcoming_90", 0),
            }
    return cnty, zp


def aggregate_scraped(zip_scraped, zip_to_metro, zip_to_state):
    """Aggregate ZIP-level scraped counts up to metro and state."""
    metro_agg = defaultdict(lambda: {"total": 0, "auc": 0, "fc": 0, "tl": 0, "bk": 0, "ss": 0,
                                     "upcoming_30": 0, "upcoming_60": 0, "upcoming_90": 0})
    state_agg = defaultdict(lambda: dict(metro_agg.default_factory()))
    nation_agg = dict(metro_agg.default_factory())
    BUCKETS = ("total", "auc", "fc", "tl", "bk", "ss",
               "upcoming_30", "upcoming_60", "upcoming_90")
    for z, d in zip_scraped.items():
        m = zip_to_metro.get(z)
        s = zip_to_state.get(z)
        for k in BUCKETS:
            v = d.get(k, 0)
            if v:
                if m: metro_agg[m][k] += v
                if s: state_agg[s][k] += v
                nation_agg[k] += v
    return dict(metro_agg), dict(state_agg), nation_agg


# ── Builders per geo level ───────────────────────────────────────────────
def build_national(realtor_nation, zillow_metros, redfin, scraped_nation):
    n = realtor_nation.get("USA") or next(iter(realtor_nation.values()), None) or {}
    metrics = canonicalize_realtor_inventory(n) if n else {}
    # Zillow national rows (RegionType == 'country')
    for rid, zm in zillow_metros.items():
        if zm["type"] == "country":
            if zm["latest_value"] is not None:
                # We know which file → which canonical key via the caller.
                # Caller will set ZILLOW_NATIONAL_METRICS via a small dict.
                pass
    rf = redfin.get("National", {})
    if rf.get("Median Sale Price") is not None:
        metrics["sale_price"] = metric(rf["Median Sale Price"], "redfin")
    if rf.get("Homes Sold") is not None:
        metrics["homes_sold"] = metric(rf["Homes Sold"], "redfin")
    if rf.get("Average Sale To List") is not None:
        metrics["sale_to_list"] = metric(rf["Average Sale To List"], "redfin")
    if rf.get("Days on Market") is not None and "dom_days" not in metrics:
        metrics["dom_days"] = metric(rf["Days on Market"], "redfin")
    return {
        "id": "USA",
        "name": "United States",
        "month": yyyymm_from_iso(n.get("month_date_yyyymm", "") + "-01") if n.get("month_date_yyyymm") else None,
        "metrics": metrics,
        "scraped": scraped_nation,
    }


def build_states(realtor_states, scraped_state):
    states = {}
    for sid, row in realtor_states.items():
        st = sid.upper()
        states[st] = {
            "id": st,
            "name": row.get("state", ""),
            "month": row.get("month_date_yyyymm", ""),
            "metrics": canonicalize_realtor_inventory(row),
            "scraped": scraped_state.get(st, {}),
        }
    return states


def build_metros(
    realtor_metros_inv,
    realtor_metros_hot,
    realtor_metros_hot_history,
    zillow_metro_files,        # {canon_metric_key: zillow_dict_keyed_by_RegionID}
    zillow_to_cbsa,
    redfin,
    redfin_to_cbsa,
    scraped_metro,
):
    out = {}
    # Reverse Zillow→CBSA so we can look up Zillow data by CBSA.
    cbsa_to_zillow = defaultdict(dict)
    for rid, cbsa in zillow_to_cbsa.items():
        cbsa_to_zillow[cbsa]["RegionID"] = rid
    for cbsa, row in realtor_metros_inv.items():
        title = row.get("cbsa_title", "")
        metrics = canonicalize_realtor_inventory(row)
        # Realtor hotness latest row (if available)
        hot_row = realtor_metros_hot.get(cbsa)
        if hot_row:
            metrics.update(canonicalize_realtor_hotness(hot_row))
        # Zillow metro overlays
        rid = cbsa_to_zillow.get(cbsa, {}).get("RegionID")
        if rid:
            for canon, zdict in zillow_metro_files.items():
                v = (zdict.get(rid) or {}).get("latest_value")
                if v is not None and canon not in metrics:
                    metrics[canon] = metric(v, "zillow")
                elif v is not None:
                    # Realtor already has it — annotate cross-source presence
                    metrics[canon].setdefault("alt", {})["zillow"] = v
        # Redfin overlay
        for rkey, mapped_cbsa in redfin_to_cbsa.items():
            if mapped_cbsa == cbsa:
                rf = redfin.get(rkey, {})
                if rf.get("Median Sale Price") is not None:
                    metrics["sale_price"] = metric(rf["Median Sale Price"], "redfin")
                if rf.get("Homes Sold") is not None:
                    metrics["homes_sold"] = metric(rf["Homes Sold"], "redfin")
                if rf.get("Average Sale To List") is not None:
                    metrics["sale_to_list"] = metric(rf["Average Sale To List"], "redfin")
                break
        out[cbsa] = {
            "id": cbsa,
            "name": title,
            "month": row.get("month_date_yyyymm", ""),
            "metrics": metrics,
            "history": realtor_metros_hot_history.get(cbsa, {}),
            "scraped": scraped_metro.get(cbsa, {}),
        }
    return out


def build_counties(
    realtor_counties_inv,
    realtor_counties_hot,
    realtor_counties_hot_history,
    cbsa_map,                  # fips → (cbsa, cbsa_title)
    cbsa_titles,
    zip_to_county,
    zhvi_zip,                  # {zip5: {latest_value, ...}}
    zillow_metro_files,        # for roll-down DOM/listings if county lacks them
    realtor_metros_inv,        # to get cbsa_code → row for fall-back metro metrics
    scraped_county,
):
    # Build county→[zips] reverse map using zip_to_county.
    county_to_zips = defaultdict(list)
    for z, fips in zip_to_county.items():
        county_to_zips[fips].append(z)

    out = {}
    for fips, row in realtor_counties_inv.items():
        metrics = canonicalize_realtor_inventory(row)
        hot_row = realtor_counties_hot.get(fips)
        if hot_row:
            metrics.update(canonicalize_realtor_hotness(hot_row))
        # Roll-up ZHVI from constituent ZIPs
        zhvi_vals = [zhvi_zip[z]["latest_value"] for z in county_to_zips.get(fips, [])
                     if z in zhvi_zip and zhvi_zip[z].get("latest_value") is not None]
        if zhvi_vals:
            metrics["home_value_zhvi"] = metric(
                round(statistics.median(zhvi_vals)), "zillow",
                derived="rollup_zip", n=len(zhvi_vals),
            )
        # Bridge to metro: CBSA from hotness map
        meta = cbsa_map.get(fips)
        if meta:
            cbsa = meta[0]
            cbsa_title = meta[1]
        else:
            cbsa, cbsa_title = None, None
        out[fips] = {
            "id": fips,
            "name": row.get("county_name", ""),
            "metro": {"code": cbsa, "title": cbsa_title} if cbsa else None,
            "month": row.get("month_date_yyyymm", ""),
            "metrics": metrics,
            "history": realtor_counties_hot_history.get(fips, {}),
            "scraped": scraped_county.get(fips, {}),
        }
    return out


def build_zips(
    realtor_zips_inv,
    realtor_zips_hot,
    realtor_zips_hot_history,
    zhvi_zip,
    zhvi_forecast,
    zip_to_county,
    cbsa_map,
    scraped_zip,
):
    out = {}
    keys = set(realtor_zips_inv) | set(zhvi_zip)
    for zip5 in keys:
        zip5p = pad5(zip5)
        metrics = {}
        row = realtor_zips_inv.get(zip5p)
        if row:
            metrics.update(canonicalize_realtor_inventory(row))
            hot = realtor_zips_hot.get(zip5p)
            if hot:
                metrics.update(canonicalize_realtor_hotness(hot))
        zh = zhvi_zip.get(zip5p)
        if zh and zh.get("latest_value") is not None:
            metrics["home_value_zhvi"] = metric(round(zh["latest_value"]), "zillow")
        zf = zhvi_forecast.get(zip5p)
        if zf and zf.get("forecast_pct") is not None:
            # Source already a percent (e.g. -1.4 → -1.4 %).
            metrics["zhvi_forecast_1y"] = metric(zf["forecast_pct"], "zillow")
        fips = zip_to_county.get(zip5p)
        meta = cbsa_map.get(fips) if fips else None
        out[zip5p] = {
            "id": zip5p,
            "name": (row.get("zip_name") if row else None) or (zh.get("city") + ", " + zh.get("state") if zh else None),
            "county": fips,
            "metro": {"code": meta[0], "title": meta[1]} if meta else None,
            "state": (zh or {}).get("state") or None,
            "month": (row.get("month_date_yyyymm") if row else None) or (zh.get("latest_month") if zh else None),
            "metrics": metrics,
            "history": realtor_zips_hot_history.get(zip5p, {}),
            "scraped": scraped_zip.get(zip5p, {}),
        }
        if zh and zh.get("history"):
            out[zip5p].setdefault("history", {})["zhvi"] = zh["history"]
    return out


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    t0 = datetime.now()
    print("[1/8] Loading Realtor inventory (5 levels)...")
    R = os.path.join(DATASET, "Realtor", "Monthly Housing Inventory")
    realtor_nation = load_realtor_inventory(
        os.path.join(R, "monthly_inventory_nations.csv"), "country",
        key_xform=lambda s: "USA",
    )
    realtor_states = load_realtor_inventory(
        os.path.join(R, "monthly_Inventory_Metrics_State.csv"), "state_id",
        key_xform=lambda s: s.upper(),
    )
    realtor_metros_inv = load_realtor_inventory(
        os.path.join(R, "monthly_inventory_Metro.csv"), "cbsa_code",
    )
    realtor_counties_inv = load_realtor_inventory(
        os.path.join(R, "Monthly_Inventory__County.csv"), "county_fips",
        key_xform=pad5,
    )
    realtor_zips_inv = load_realtor_inventory(
        os.path.join(R, "Monthly_Inventory_Zip.csv"), "postal_code",
        key_xform=pad5,
    )
    print(f"  nation={len(realtor_nation)} state={len(realtor_states)} "
          f"metro={len(realtor_metros_inv)} county={len(realtor_counties_inv)} "
          f"zip={len(realtor_zips_inv)}")

    print("[2/8] Loading Realtor hotness history (Metro / County / ZIP)...")
    H = os.path.join(DATASET, "Realtor", "Monthly Market Hotness")
    rmh, rmh_hist, _ = load_realtor_hotness_history(
        os.path.join(H, "Inventory_Hotness_Metrics_Metro_History.csv"), "cbsa_code",
    )
    rch, rch_hist, county_to_cbsa = load_realtor_hotness_history(
        os.path.join(H, "Inventory_Hotness_Metrics_County_History.csv"), "county_fips",
        key_xform=pad5,
    )
    rzh, rzh_hist, _ = load_realtor_hotness_history(
        os.path.join(H, "Inventory_Hotness_Metrics_Zip_History.csv"), "postal_code",
        key_xform=pad5,
    )
    print(f"  metro_hot={len(rmh)} county_hot={len(rch)} zip_hot={len(rzh)} "
          f"  county→cbsa={len(county_to_cbsa)}")

    print("[3/8] Loading Zillow Metro&US wide files...")
    Z = os.path.join(DATASET, "Zillow", "Metro&US")
    # Detect identical source files (e.g. pending_list_sale.csv being a stray
    # copy of zil_FOR_SALE_LISTINGS.csv) and skip the dupe so we don't surface
    # the same number under two canonical keys.
    import hashlib
    def _digest(p):
        h = hashlib.sha1()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    spec = [
        ("for_sale_count", "zil_FOR_SALE_LISTINGS.csv"),
        ("pending_count",  "pending_list_sale.csv"),
        ("homes_sold",     "zil_sales_count.csv"),
        ("dom_days",       "zil_days_on_market.csv"),
        ("mkt_temperature","Zil_bal_sale_dem.csv"),
        ("affordability",  "zil_affordability.csv"),
    ]
    zillow_metro_files, seen_digests = {}, {}
    for canon, fname in spec:
        p = os.path.join(Z, fname)
        if not os.path.exists(p):
            continue
        d = _digest(p)
        prior = seen_digests.get(d)
        if prior:
            print(f"  WARN: {fname} is byte-identical to {prior} — "
                  f"skipping '{canon}' to avoid duplicate signal")
            continue
        seen_digests[d] = fname
        zillow_metro_files[canon] = load_zillow_metro_wide(p)
    # Pick any non-empty file as the source of {RegionID → name/state/type}
    zillow_metros_index = {}
    for d in zillow_metro_files.values():
        for rid, m in d.items():
            zillow_metros_index.setdefault(rid, m)
    print(f"  metro+nation regions indexed: {len(zillow_metros_index)} "
          f"(types: {set(m['type'] for m in zillow_metros_index.values())})")

    print("[4/8] Loading Zillow ZIP ZHVI + forecast...")
    ZZ = os.path.join(DATASET, "Zillow", "Zip Code Level")
    zhvi_zip = load_zillow_zip_zhvi(os.path.join(ZZ, "Zillow Home Value Index (ZHVI).csv"))
    zhvi_forecast = load_zillow_zip_forecast(os.path.join(ZZ, "zil_home_val_month (1).csv"))
    print(f"  zhvi_zips={len(zhvi_zip)}  zhvi_forecast_zips={len(zhvi_forecast)}")

    print("[5/8] Loading Redfin metros...")
    redfin = load_redfin_metro(os.path.join(DATASET, "Redfin", "Monthly_marketing_data.csv"))
    print(f"  redfin regions: {len(redfin)}")

    # Crosswalks
    print("[6/8] Building crosswalks...")
    realtor_cbsa_titles = {c: r.get("cbsa_title", "") for c, r in realtor_metros_inv.items()}
    redfin_to_cbsa = redfin_match_metro(list(redfin.keys()), realtor_cbsa_titles)
    zillow_to_cbsa = zillow_metro_to_cbsa(zillow_metros_index, realtor_cbsa_titles)
    print(f"  redfin→cbsa: {len(redfin_to_cbsa)}/{len(redfin)} "
          f" zillow→cbsa: {len(zillow_to_cbsa)}/{sum(1 for m in zillow_metros_index.values() if m['type']=='msa')}")

    zbc = json.load(open(os.path.join(STATIC, "zip-by-county.json")))
    zip_to_county = {z: fips for fips, zips in zbc.items() for z in zips}
    # State-FIPS prefix → USPS abbreviation
    SFIPS_TO_ST = {
        "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC",
        "12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY",
        "22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT",
        "31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH",
        "40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT",
        "50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY","72":"PR",
    }
    zip_to_state = {z: SFIPS_TO_ST.get(fips[:2], "") for z, fips in zip_to_county.items()}
    zip_to_state = {z: s for z, s in zip_to_state.items() if s}
    zip_to_metro = {}
    for z, fips in zip_to_county.items():
        meta = county_to_cbsa.get(fips)
        if meta:
            zip_to_metro[z] = meta[0]

    # Scraped data
    print("[7/8] Loading + aggregating scraped data...")
    scraped_county, scraped_zip = load_scraped()
    scraped_metro, scraped_state, scraped_nation = aggregate_scraped(
        scraped_zip, zip_to_metro, zip_to_state,
    )
    print(f"  scraped county={len(scraped_county)} zip={len(scraped_zip)} "
          f" → metro={len(scraped_metro)} state={len(scraped_state)} nation={scraped_nation.get('total',0):,}")

    # Build outputs
    print("[8/8] Building stacked outputs...")
    nation = build_national(realtor_nation, zillow_metros_index, redfin, scraped_nation)
    state = build_states(realtor_states, scraped_state)
    metros = build_metros(
        realtor_metros_inv, rmh, rmh_hist,
        zillow_metro_files, zillow_to_cbsa,
        redfin, redfin_to_cbsa, scraped_metro,
    )
    counties = build_counties(
        realtor_counties_inv, rch, rch_hist,
        county_to_cbsa, realtor_cbsa_titles,
        zip_to_county, zhvi_zip, zillow_metro_files, realtor_metros_inv,
        scraped_county,
    )
    zips = build_zips(
        realtor_zips_inv, rzh, rzh_hist,
        zhvi_zip, zhvi_forecast,
        zip_to_county, county_to_cbsa,
        scraped_zip,
    )

    def _wrap(level, data):
        return {"level": level, "generated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "geo_count": len(data) if isinstance(data, dict) else 1, "data": data}

    def _write(name, payload):
        p = os.path.join(OUT, name)
        with open(p, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        kb = os.path.getsize(p) / 1024
        n = payload.get("geo_count", 0) if isinstance(payload, dict) else 0
        print(f"  wrote {p}  ({kb:>9,.0f} KB · {n:,} entries)")

    _write("national.json", _wrap("national", nation))
    _write("state.json",    _wrap("state",    state))
    _write("metro.json",    _wrap("metro",    metros))
    _write("county.json",   _wrap("county",   counties))

    # Split ZIPs into per-state files so the dashboard can lazy-load
    # only the state currently in view (~600 ZIPs, ~3 MB each).
    zips_by_state = defaultdict(dict)
    for z, entry in zips.items():
        st = (entry.get("state") or "").strip().upper() or "ZZ"
        zips_by_state[st][z] = entry

    zip_dir = os.path.join(OUT, "zip")
    os.makedirs(zip_dir, exist_ok=True)
    # Clean any stale per-state shards from prior runs.
    for old in os.listdir(zip_dir):
        if old.endswith(".json"):
            os.remove(os.path.join(zip_dir, old))

    state_index = {}
    for st, entries in zips_by_state.items():
        payload = _wrap("zip", entries)
        p = os.path.join(zip_dir, f"{st}.json")
        with open(p, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        state_index[st] = {"count": len(entries), "kb": round(os.path.getsize(p) / 1024)}
    idx_path = os.path.join(zip_dir, "_index.json")
    with open(idx_path, "w") as f:
        json.dump({"states": state_index, "generated": payload["generated"]}, f)
    total_kb = sum(s["kb"] for s in state_index.values())
    print(f"  wrote zip/<ST>.json shards  ({len(state_index)} states · {total_kb:,} KB total · "
          f"avg {total_kb/max(len(state_index),1):.0f} KB/state)")

    dt = (datetime.now() - t0).total_seconds()
    print(f"\nDone in {dt:.1f}s.")


if __name__ == "__main__":
    main()
