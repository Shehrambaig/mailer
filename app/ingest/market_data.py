"""
Unified county-level market intelligence from Redfin, Zillow, and Realtor.com.
Data sources:
  data/realtor_county_current.csv  — Realtor.com monthly (current month, 3100 counties)
  data/realtor_county.csv          — Realtor.com history (for trend charts)
  data/zillow_zhvi_county.csv      — Zillow ZHVI monthly since 2000
  data/redfin_county_full.tsv.gz   — Redfin monthly (DOM, sale price, supply, volume)
  data/redfin_county.tsv.gz        — Redfin partial fallback
"""
import csv
import gzip
import math
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")

FIPS_TO_ABBR = {
    "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE",
    "11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA",
    "20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN",
    "28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM",
    "36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI",
    "45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA",
    "54":"WV","55":"WI","56":"WY",
}

STATE_ABBR = {v: k for k, v in FIPS_TO_ABBR.items()}

_cache = {}


def _f(val):
    """Safe float."""
    try:
        v = float(str(val).strip().strip('"'))
        return None if v != v else v  # NaN check
    except (ValueError, TypeError):
        return None


def _i(val):
    v = _f(val)
    return int(v) if v is not None else None


def load_realtor_county():
    """
    Load Realtor.com current-month county data.
    Returns dict {fips: {...metrics...}}
    Columns: month_date_yyyymm, county_fips, county_name, median_listing_price,
             median_listing_price_yy, active_listing_count, median_days_on_market,
             new_listing_count, price_reduced_share, pending_listing_count,
             pending_ratio, median_listing_price_per_square_foot, quality_flag
    """
    # Prefer current-month file; fall back to history
    for fname in ("realtor_county_current.csv", "realtor_county.csv"):
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            continue

        result = {}
        seen = {}  # for history file: keep latest month per FIPS
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_fips = str(row.get("county_fips", "")).strip()
                if not raw_fips:
                    continue
                fips = raw_fips.zfill(5)
                month = str(row.get("month_date_yyyymm", ""))

                # For history file keep latest month per county
                if fname != "realtor_county_current.csv":
                    if fips in seen and seen[fips] >= month:
                        continue
                    seen[fips] = month

                raw_name = (row.get("county_name") or "").strip().strip('"')
                # Clean name: "big horn, mt" → "Big Horn"
                display = " ".join(p.capitalize() for p in raw_name.split(",")[0].split())

                result[fips] = {
                    "county_name": display,
                    "state_code": FIPS_TO_ABBR.get(fips[:2], ""),
                    "data_month": month,
                    "realtor_list_price": _i(row.get("median_listing_price")),
                    "realtor_list_price_yy": _f(row.get("median_listing_price_yy")),
                    "realtor_active": _i(row.get("active_listing_count")),
                    "realtor_new_listings": _i(row.get("new_listing_count")),
                    "realtor_dom": _i(row.get("median_days_on_market")),
                    "realtor_price_reduced": _f(row.get("price_reduced_share")),
                    "realtor_pending": _i(row.get("pending_listing_count")),
                    "realtor_pending_ratio": _f(row.get("pending_ratio")),
                    "realtor_ppsf": _f(row.get("median_listing_price_per_square_foot")),
                    "realtor_sqft": _i(row.get("median_square_feet")),
                }
        return result
    return {}


def load_realtor_trends():
    """
    Load Realtor.com history for trend charts.
    Returns dict {fips: [{month, dom, list_price, active, price_reduced, pending_ratio}, ...]}
    """
    path = os.path.join(DATA_DIR, "realtor_county.csv")
    if not os.path.exists(path):
        return {}

    raw = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fips = str(row.get("county_fips", "")).strip().zfill(5)
            month = str(row.get("month_date_yyyymm", ""))
            if not fips or not month:
                continue
            raw.setdefault(fips, []).append({
                "month": month,
                "dom": _i(row.get("median_days_on_market")),
                "list_price": _i(row.get("median_listing_price")),
                "active": _i(row.get("active_listing_count")),
                "price_reduced": _f(row.get("price_reduced_share")),
                "pending_ratio": _f(row.get("pending_ratio")),
            })

    result = {}
    for fips, entries in raw.items():
        entries.sort(key=lambda x: x["month"])
        result[fips] = entries[-24:]  # last 24 months
    return result


def load_zillow_county():
    """
    Load Zillow ZHVI county data.
    Returns (current_dict, trends_dict)
    current: {fips: {zhvi, yoy, 5yr, county_name, state_name}}
    trends:  {fips: [{month, zhvi}, ...]} — last 36 months
    """
    path = os.path.join(DATA_DIR, "zillow_zhvi_county.csv")
    if not os.path.exists(path):
        return {}, {}

    current, trends = {}, {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        date_cols = [c for c in headers if c.startswith("20")]

        for row in reader:
            sf = str(row.get("StateCodeFIPS", "")).strip().zfill(2)
            cf = str(row.get("MunicipalCodeFIPS", "")).strip().zfill(3)
            if not sf.strip("0") and not cf.strip("0"):
                continue
            fips = sf + cf

            vals = [(d, _f(row.get(d))) for d in date_cols if _f(row.get(d)) is not None]
            if not vals:
                continue

            latest_d, latest_v = vals[-1]
            yoy = None
            if len(vals) > 12:
                yoy = round((latest_v - vals[-13][1]) / vals[-13][1] * 100, 2)
            five_yr = None
            if len(vals) > 60:
                five_yr = round((latest_v - vals[-61][1]) / vals[-61][1] * 100, 1)

            current[fips] = {
                "zillow_zhvi": int(latest_v),
                "zillow_yoy": yoy,
                "zillow_5yr": five_yr,
                "zillow_date": latest_d,
            }
            trends[fips] = [{"month": d[:7], "zhvi": int(v)} for d, v in vals[-36:]]

    return current, trends


def load_redfin_county():
    """
    Load Redfin county data (monthly: sale price, homes sold, supply, DOM, sale-to-list).
    Tries full file first, falls back to partial.
    Returns (current_dict, trends_dict)
    """
    for fname in ("redfin_county_full.tsv.gz", "redfin_county.tsv.gz"):
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            continue
        try:
            county_rows = {}
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    ptype = (row.get("PROPERTY_TYPE") or "").strip('"')
                    if ptype != "All Residential":
                        continue
                    period = (row.get("PERIOD_BEGIN") or "").strip('"')[:7]
                    region = (row.get("REGION") or "").strip('"')
                    sc = (row.get("STATE_CODE") or "").strip('"')
                    if not period or not region or not sc:
                        continue
                    county_rows.setdefault((sc, region), []).append((period, row))

            current, trends = {}, {}
            # Build name→FIPS lookup from realtor data
            name_to_fips = _build_name_fips_lookup()

            for (sc, region), rows in county_rows.items():
                rows.sort(key=lambda x: x[0], reverse=True)
                fips = name_to_fips.get(f"{sc}|{region.lower().split(',')[0].strip()}")
                if not fips:
                    continue

                _, r = rows[0]
                current[fips] = {
                    "redfin_sale_price": _i(r.get("MEDIAN_SALE_PRICE")),
                    "redfin_homes_sold": _i(r.get("HOMES_SOLD")),
                    "redfin_dom": _i(r.get("MEDIAN_DOM")),
                    "redfin_supply": _f(r.get("MONTHS_OF_SUPPLY")),
                    "redfin_sale_to_list": _f(r.get("AVG_SALE_TO_LIST")),
                    "redfin_sold_above": _f(r.get("SOLD_ABOVE_LIST")),
                    "redfin_price_drops": _f(r.get("PRICE_DROPS")),
                    "redfin_off_market_2wk": _f(r.get("OFF_MARKET_IN_TWO_WEEKS")),
                    "redfin_inventory": _i(r.get("INVENTORY")),
                    "redfin_new_listings": _i(r.get("NEW_LISTINGS")),
                    "redfin_pending": _i(r.get("PENDING_SALES")),
                }

                hist = []
                for period, hr in rows[:24]:
                    hist.append({
                        "month": period,
                        "sale_price": _i(hr.get("MEDIAN_SALE_PRICE")),
                        "dom": _i(hr.get("MEDIAN_DOM")),
                        "homes_sold": _i(hr.get("HOMES_SOLD")),
                        "supply": _f(hr.get("MONTHS_OF_SUPPLY")),
                        "price_drops": _f(hr.get("PRICE_DROPS")),
                    })
                hist.reverse()
                trends[fips] = hist

            return current, trends
        except Exception:
            continue
    return {}, {}


def _build_name_fips_lookup():
    """Build {state_code|county_short_name: fips} from Realtor.com data."""
    if _cache.get("name_fips"):
        return _cache["name_fips"]
    lookup = {}
    for fname in ("realtor_county_current.csv", "realtor_county.csv"):
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            continue
        seen = set()
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fips = str(row.get("county_fips", "")).strip().zfill(5)
                raw = (row.get("county_name") or "").strip('"').lower()
                sc = FIPS_TO_ABBR.get(fips[:2], "")
                short = raw.split(",")[0].strip()
                key = f"{sc}|{short}"
                if key not in seen:
                    lookup[key] = fips
                    seen.add(key)
        break
    _cache["name_fips"] = lookup
    return lookup


def _piecewise(val, points):
    """Linear interpolation between (x, y) breakpoints. Flat extrapolation."""
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
    """Weighted sum with renormalization for missing signals."""
    total_w = sum(weights[k] for k, v in sigs.items() if v is not None)
    if total_w < min_coverage:
        return None
    return sum(weights[k] * sigs[k] for k, v in sigs.items() if v is not None) / total_w


def compute_exit_score(d):
    """
    Exit Speed 0-100: how fast to resell to retail buyers.
    Weighted sum of piecewise-normalized metrics + dual-threshold gate bonuses.
    """
    dom      = d.get("realtor_dom") or d.get("redfin_dom")
    pending  = d.get("realtor_pending_ratio")
    off2wk   = d.get("redfin_off_market_2wk")
    stl      = d.get("redfin_sale_to_list")
    supply   = d.get("redfin_supply")
    yoy      = d.get("realtor_list_price_yy")
    if yoy is None:
        yoy = d.get("zillow_yoy")
        if yoy is not None and abs(yoy) > 1:
            yoy = yoy / 100  # zillow_yoy sometimes stored as percentage

    sigs = {
        "dom":     _piecewise(dom,     [(15, 100), (30, 70), (60, 30), (90, 10), (150, 0)]) if dom else None,
        "pending": _piecewise(pending, [(0.03, 0), (0.10, 20), (0.25, 65), (0.40, 100)]) if pending is not None else None,
        "off2wk":  _piecewise(off2wk,  [(0.05, 0), (0.10, 10), (0.30, 60), (0.50, 100)]) if off2wk is not None else None,
        "stl":     _piecewise(stl,     [(0.93, 0), (0.97, 40), (1.00, 75), (1.02, 100)]) if stl else None,
        "supply":  _piecewise(supply,  [(2, 100), (4, 60), (6, 25), (10, 0)]) if supply else None,
        "yoy":     _piecewise(yoy,     [(-0.05, 0), (0, 50), (0.05, 85), (0.10, 100)]) if yoy is not None else None,
    }
    weights = {"dom": 0.40, "pending": 0.30, "off2wk": 0.12, "stl": 0.10, "supply": 0.05, "yoy": 0.03}

    base = _weighted(sigs, weights)
    if base is None:
        return {"score": 50, "signals": [{"type": "watch", "text": "Insufficient exit-side data"}]}

    # Dual-threshold gate bonuses
    if dom and pending is not None:
        if dom < 30 and pending > 0.40:
            base *= 1.15
        elif dom < 60 and pending > 0.25:
            base *= 1.10
    if dom and dom > 120:
        base *= 0.60  # stagnant override

    score = max(0, min(100, int(round(base))))

    # Human-readable signals
    signals = []
    if dom:
        if dom <= 20:
            signals.append({"type": "hot",    "text": f"{dom} days on market — blazing fast exit"})
        elif dom <= 30:
            signals.append({"type": "strong", "text": f"{dom} days on market — fast market"})
        elif dom <= 60:
            signals.append({"type": "watch",  "text": f"{dom} days on market — moderate pace"})
        elif dom <= 90:
            signals.append({"type": "warn",   "text": f"{dom} days on market — slow exit"})
        else:
            signals.append({"type": "warn",   "text": f"{dom} days on market — stagnant"})
    if pending is not None:
        if pending > 0.40:
            signals.append({"type": "hot",    "text": f"Pending ratio {pending:.0%} — heavy buyer demand"})
        elif pending > 0.25:
            signals.append({"type": "strong", "text": f"Pending ratio {pending:.0%} — solid demand"})
        elif pending < 0.10:
            signals.append({"type": "warn",   "text": f"Pending ratio {pending:.0%} — weak demand"})
    if off2wk and off2wk > 0.30:
        signals.append({"type": "hot", "text": f"{off2wk:.0%} go off-market within 2 weeks"})
    if stl:
        if stl >= 1.01:
            signals.append({"type": "strong", "text": f"{stl:.1%} sale-to-list — bidding above ask"})
        elif stl < 0.97:
            signals.append({"type": "warn",   "text": f"{stl:.1%} sale-to-list — buyer leverage"})
    if supply and supply < 3:
        signals.append({"type": "strong", "text": f"{supply:.1f} months of supply — tight inventory"})
    if dom and pending is not None and dom < 60 and pending > 0.25:
        signals.append({"type": "hot", "text": "Dual threshold met: DOM<60 + Pending>25%"})

    return {"score": score, "signals": signals}


def compute_buy_score(d):
    """
    Buy Opportunity 0-100: ease of acquiring below market from motivated sellers.
    """
    price_reduced = d.get("realtor_price_reduced")
    if price_reduced is None:
        price_reduced = d.get("redfin_price_drops")
    if price_reduced is not None and price_reduced > 1:
        price_reduced = price_reduced / 100  # fix if stored as percent

    yoy     = d.get("realtor_list_price_yy")
    if yoy is None:
        yoy = d.get("zillow_yoy")
        if yoy is not None and abs(yoy) > 1:
            yoy = yoy / 100
    supply  = d.get("redfin_supply")
    active  = d.get("realtor_active")
    new_l   = d.get("realtor_new_listings")
    anr     = (active / new_l) if active and new_l and new_l > 0 else None
    stl     = d.get("redfin_sale_to_list")
    dom     = d.get("realtor_dom") or d.get("redfin_dom")

    sigs = {
        "pr":     _piecewise(price_reduced, [(0, 0), (0.05, 10), (0.15, 45), (0.25, 70), (0.40, 100)]) if price_reduced is not None else None,
        "yoy":    _piecewise(yoy,    [(-0.08, 100), (-0.03, 70), (0, 40), (0.05, 15), (0.12, 0)]) if yoy is not None else None,
        "supply": _piecewise(supply, [(2, 0), (4, 40), (6, 70), (10, 100)]) if supply else None,
        "anr":    _piecewise(anr,    [(1, 0), (2.5, 40), (4, 70), (6, 100)]) if anr else None,
        "stl":    _piecewise(stl,    [(0.93, 100), (0.97, 70), (1.00, 25), (1.02, 0)]) if stl else None,
        "dom":    _piecewise(dom,    [(30, 0), (60, 30), (90, 70), (120, 100)]) if dom else None,
    }
    weights = {"pr": 0.45, "yoy": 0.20, "supply": 0.15, "anr": 0.10, "stl": 0.07, "dom": 0.03}

    base = _weighted(sigs, weights)
    if base is None:
        return {"score": 50, "signals": [{"type": "watch", "text": "Insufficient buy-side data"}]}

    # Gate bonuses / penalties
    if price_reduced is not None and yoy is not None:
        if price_reduced > 0.25 and yoy < -0.02:
            base *= 1.12  # distressed market
        elif price_reduced < 0.05 and yoy > 0.08:
            base *= 0.70  # overheated — hard to find deals

    score = max(0, min(100, int(round(base))))

    signals = []
    if price_reduced is not None:
        pct = price_reduced * 100
        if pct >= 30:
            signals.append({"type": "opportunity", "text": f"{pct:.0f}% of listings have price reductions"})
        elif pct >= 20:
            signals.append({"type": "strong",      "text": f"{pct:.0f}% price reduction rate"})
        elif pct < 8:
            signals.append({"type": "watch",       "text": f"Only {pct:.0f}% reductions — competitive market"})
    if yoy is not None:
        if yoy < -0.05:
            signals.append({"type": "opportunity", "text": f"List prices down {abs(yoy):.0%} YoY"})
        elif yoy < -0.02:
            signals.append({"type": "watch",       "text": f"Prices softening {abs(yoy):.0%} YoY"})
        elif yoy > 0.10:
            signals.append({"type": "hot",         "text": f"Prices +{yoy:.0%} YoY — harder to buy cheap"})
    if supply and supply > 5:
        signals.append({"type": "opportunity", "text": f"{supply:.1f} months of supply — buyer's market"})
    if anr and anr > 4:
        signals.append({"type": "opportunity", "text": f"Stale supply: {anr:.1f}× active vs new listings"})
    if stl and stl < 0.97:
        signals.append({"type": "strong", "text": f"{stl:.1%} sale-to-list — buyer leverage"})
    if dom and dom > 75:
        signals.append({"type": "opportunity", "text": f"{dom} days on market — sellers getting tired"})
    if price_reduced is not None and yoy is not None and price_reduced > 0.25 and yoy < -0.02:
        signals.append({"type": "opportunity", "text": "Distress signal: price cuts + falling prices"})

    return {"score": score, "signals": signals}


def compute_golden_score(buy, exit_s, active):
    """
    Golden Zone 0-100: geometric mean of Buy × Exit with balance penalty + liquidity multiplier.
    Both sides must be strong AND the market must have enough listings to work in.
    """
    if buy <= 0 or exit_s <= 0:
        return 0
    base = math.sqrt(buy * exit_s)
    # Balance penalty — asymmetric markets are red flags
    asymmetry = abs(buy - exit_s) / 100.0
    balance_factor = 1 - (asymmetry * 0.3)
    # Liquidity multiplier — need deal flow to work in the market
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
    return max(0, min(100, int(round(base * balance_factor * liq))))


def get_county_data():
    """Unified county data merged from all sources. Cached."""
    if _cache.get("county"):
        return _cache["county"]

    realtor = load_realtor_county()
    zillow_cur, zillow_trends = load_zillow_county()
    redfin_cur, redfin_trends = load_redfin_county()
    realtor_trends = load_realtor_trends()

    all_fips = set(realtor.keys()) | set(zillow_cur.keys())

    combined = {}
    for fips in all_fips:
        d = {}
        d.update(zillow_cur.get(fips, {}))
        d.update(realtor.get(fips, {}))
        d.update(redfin_cur.get(fips, {}))

        if not d.get("state_code"):
            d["state_code"] = FIPS_TO_ABBR.get(fips[:2], "")

        d["buy_signals"]  = compute_buy_score(d)
        d["exit_signals"] = compute_exit_score(d)
        bs = d["buy_signals"]["score"]
        es = d["exit_signals"]["score"]
        active = d.get("realtor_active") or d.get("redfin_inventory") or 0
        d["golden_score"] = compute_golden_score(bs, es, active)

        combined[fips] = d

    result = {
        "counties": combined,
        "zillow_trends": zillow_trends,
        "redfin_trends": redfin_trends,
        "realtor_trends": realtor_trends,
    }
    _cache["county"] = result
    return result


def clear_county_cache():
    _cache.pop("county", None)
    _cache.pop("name_fips", None)


# ── Legacy state-level functions (kept for reference) ──────────────────────

STATE_ABBR_NAMES = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","District of Columbia":"DC",
    "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL",
    "Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY","Louisiana":"LA",
    "Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
    "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
    "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
    "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
    "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA",
    "Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
}


def load_redfin():
    path = os.path.join(DATA_DIR, "redfin_state.tsv")
    if not os.path.exists(path):
        return {}, {}
    state_rows = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t", quotechar='"')
        for row in reader:
            if (row.get("PROPERTY_TYPE") or "").strip('"') != "All Residential":
                continue
            sc = (row.get("STATE_CODE") or "").strip('"')
            period = (row.get("PERIOD_BEGIN") or "").strip('"')
            if sc and period:
                state_rows.setdefault(sc, []).append((period, row))
    latest, trends = {}, {}
    for sc, rows in state_rows.items():
        rows.sort(key=lambda x: x[0], reverse=True)
        _, r = rows[0]
        latest[sc] = {
            "redfin_median_sale": _i(r.get("MEDIAN_SALE_PRICE")),
            "redfin_dom": _i(r.get("MEDIAN_DOM")),
            "redfin_inventory": _i(r.get("INVENTORY")),
            "redfin_months_supply": _f(r.get("MONTHS_OF_SUPPLY")),
            "redfin_sale_to_list": _f(r.get("AVG_SALE_TO_LIST")),
            "redfin_price_drops": _f(r.get("PRICE_DROPS")),
            "redfin_homes_sold": _i(r.get("HOMES_SOLD")),
        }
        hist = []
        for period, hr in rows[:12]:
            hist.append({"month": period[:7], "median_sale": _i(hr.get("MEDIAN_SALE_PRICE")),
                         "inventory": _i(hr.get("INVENTORY")), "dom": _i(hr.get("MEDIAN_DOM")),
                         "homes_sold": _i(hr.get("HOMES_SOLD"))})
        hist.reverse()
        trends[sc] = hist
    return latest, trends


def load_fhfa():
    path = os.path.join(DATA_DIR, "fhfa_hpi_state.csv")
    if not os.path.exists(path):
        return {}
    state_data = {}
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4:
                continue
            try:
                yr, qtr, hpi = int(row[1]), int(row[2]), float(row[3])
            except (ValueError, TypeError):
                continue
            state_data.setdefault(row[0], []).append((yr, qtr, hpi))
    result = {}
    for st, entries in state_data.items():
        entries.sort()
        if len(entries) < 5:
            continue
        latest = entries[-1]
        prev_yr = [e for e in entries if e[0] == latest[0]-1 and e[1] == latest[1]]
        five_yr = [e for e in entries if e[0] == latest[0]-5 and e[1] == latest[1]]
        result[st] = {
            "fhfa_hpi": round(latest[2], 2),
            "fhfa_yoy": round((latest[2]-prev_yr[0][2])/prev_yr[0][2]*100, 2) if prev_yr else None,
            "fhfa_5yr_appreciation": round((latest[2]-five_yr[0][2])/five_yr[0][2]*100, 1) if five_yr else None,
        }
    return result


def load_zillow():
    path = os.path.join(DATA_DIR, "zillow_zhvi_state.csv")
    if not os.path.exists(path):
        return {}, {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        date_cols = [c for c in headers if c.startswith("20")]
        result, trends = {}, {}
        for row in reader:
            abbr = STATE_ABBR_NAMES.get(row.get("RegionName", ""))
            if not abbr:
                continue
            vals = [(d, _f(row.get(d))) for d in date_cols if _f(row.get(d)) is not None]
            if not vals:
                continue
            latest_d, latest_v = vals[-1]
            yoy = round((latest_v-vals[-13][1])/vals[-13][1]*100, 2) if len(vals)>12 else None
            five_yr = round((latest_v-vals[-61][1])/vals[-61][1]*100, 1) if len(vals)>60 else None
            result[abbr] = {"zillow_zhvi": int(latest_v), "zillow_yoy": yoy,
                            "zillow_5yr_appreciation": five_yr, "zillow_date": latest_d}
            trends[abbr] = [{"month": d[:7], "zhvi": int(v)} for d, v in vals[-12:]]
    return result, trends


def compute_signals(d):
    signals = []
    score = 50
    zyoy = d.get("zillow_yoy")
    if zyoy is not None:
        if zyoy < -3:
            signals.append({"type":"opportunity","text":f"Prices declining {zyoy:.1f}% YoY"}); score+=15
        elif zyoy < 0:
            signals.append({"type":"watch","text":f"Prices softening {zyoy:.1f}% YoY"}); score+=8
        elif zyoy > 8:
            signals.append({"type":"hot","text":f"Rapid appreciation +{zyoy:.1f}% YoY"}); score-=10
    supply = d.get("redfin_months_supply")
    if supply:
        if supply>6: signals.append({"type":"opportunity","text":f"{supply:.1f} months supply"}); score+=12
        elif supply<3: signals.append({"type":"hot","text":f"Only {supply:.1f} months supply"}); score-=8
    dom = d.get("redfin_dom")
    if dom:
        if dom>60: signals.append({"type":"opportunity","text":f"{dom} DOM — leverage"}); score+=10
        elif dom<20: signals.append({"type":"hot","text":f"{dom} DOM — fast market"}); score-=5
    stl = d.get("redfin_sale_to_list")
    if stl:
        if stl<97: score+=8
        elif stl>102: score-=8
    return {"signals": signals, "score": max(0, min(100, score))}


def get_market_data():
    if _cache.get("market"):
        return _cache["market"]
    redfin_latest, redfin_trends = load_redfin()
    fhfa = load_fhfa()
    zillow_latest, zillow_trends = load_zillow()
    all_states = set(redfin_latest)|set(fhfa)|set(zillow_latest)
    combined = {}
    for st in all_states:
        entry = {}
        entry.update(redfin_latest.get(st, {}))
        entry.update(fhfa.get(st, {}))
        entry.update(zillow_latest.get(st, {}))
        entry["signals"] = compute_signals(entry)
        combined[st] = entry
    result = {"states": combined, "redfin_trends": redfin_trends, "zillow_trends": zillow_trends}
    _cache["market"] = result
    return result


def get_national_trends():
    _, redfin_trends = load_redfin()
    months = sorted({e["month"] for st in redfin_trends.values() for e in st})[-12:]
    national = {"months": [], "median_sale": [], "inventory": [], "dom": [], "homes_sold": []}
    for m in months:
        national["months"].append(m)
        sales, invs, doms, solds = [], [], [], []
        for st_data in redfin_trends.values():
            for e in st_data:
                if e["month"] == m:
                    if e.get("median_sale"): sales.append(e["median_sale"])
                    if e.get("inventory"): invs.append(e["inventory"])
                    if e.get("dom"): doms.append(e["dom"])
                    if e.get("homes_sold"): solds.append(e["homes_sold"])
        national["median_sale"].append(int(sum(sales)/len(sales)) if sales else 0)
        national["inventory"].append(sum(invs))
        national["dom"].append(int(sum(doms)/len(doms)) if doms else 0)
        national["homes_sold"].append(sum(solds))
    return national
