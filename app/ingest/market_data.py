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


def compute_exit_score(d):
    """Score 0-100: how fast/easy to sell to retail family buyers."""
    score = 50
    signals = []

    dom = d.get("realtor_dom") or d.get("redfin_dom")
    if dom:
        if dom <= 20:
            score = 90
            signals.append({"type": "hot", "text": f"{dom} days on market — extremely fast exit"})
        elif dom <= 30:
            score = 78
            signals.append({"type": "strong", "text": f"{dom} days on market — fast market"})
        elif dom <= 45:
            score = 62
        elif dom <= 60:
            score = 48
        elif dom <= 90:
            score = 30
            signals.append({"type": "warn", "text": f"{dom} days on market — slow market"})
        else:
            score = 12
            signals.append({"type": "warn", "text": f"{dom} days on market — very slow exit"})

    pr = d.get("realtor_pending_ratio")
    if pr is not None:
        if pr > 0.5:
            score = min(100, score + 15)
            signals.append({"type": "hot", "text": f"Pending ratio {pr:.0%} — strong buyer demand"})
        elif pr > 0.3:
            score = min(100, score + 7)
        elif pr < 0.1:
            score = max(0, score - 8)

    off2wk = d.get("redfin_off_market_2wk")
    if off2wk and off2wk > 0.4:
        score = min(100, score + 10)
        signals.append({"type": "hot", "text": f"{off2wk:.0%} go off-market within 2 weeks"})

    stl = d.get("redfin_sale_to_list")
    if stl and stl > 1.01:
        score = min(100, score + 8)
        signals.append({"type": "strong", "text": f"{stl:.1%} sale-to-list — selling above ask"})
    elif stl and stl < 0.97:
        score = max(0, score - 8)
        signals.append({"type": "warn", "text": f"{stl:.1%} sale-to-list — below ask"})

    price_yy = d.get("realtor_list_price_yy")
    if price_yy and price_yy > 0.08:
        score = min(100, score + 5)
        signals.append({"type": "strong", "text": f"List prices +{price_yy:.0%} YoY — appreciating"})

    return {"score": max(0, min(100, score)), "signals": signals}


def compute_buy_score(d):
    """Score 0-100: how easy to acquire below market from motivated sellers."""
    score = 50
    signals = []

    prs = d.get("realtor_price_reduced") or d.get("redfin_price_drops")
    if prs is not None:
        pct = prs * 100 if prs <= 1 else prs
        if pct >= 40:
            score = 88
            signals.append({"type": "opportunity", "text": f"{pct:.0f}% of listings have price reductions"})
        elif pct >= 25:
            score = 72
            signals.append({"type": "strong", "text": f"{pct:.0f}% price reduction rate"})
        elif pct >= 15:
            score = 55
        elif pct >= 8:
            score = 38
        else:
            score = 22
            signals.append({"type": "watch", "text": f"Only {pct:.0f}% reductions — competitive market"})

    price_yy = d.get("realtor_list_price_yy")
    if price_yy is not None:
        if price_yy < -0.08:
            score = min(100, score + 18)
            signals.append({"type": "opportunity", "text": f"List prices down {abs(price_yy):.0%} YoY"})
        elif price_yy < -0.03:
            score = min(100, score + 9)
            signals.append({"type": "watch", "text": f"Prices softening {abs(price_yy):.0%} YoY"})
        elif price_yy > 0.12:
            score = max(0, score - 12)
            signals.append({"type": "hot", "text": f"Prices +{price_yy:.0%} YoY — harder to buy cheap"})

    active = d.get("realtor_active")
    new_l = d.get("realtor_new_listings")
    if active and new_l and new_l > 0:
        supply_ratio = active / new_l
        if supply_ratio > 4:
            score = min(100, score + 10)
            signals.append({"type": "opportunity", "text": f"Stale supply: {supply_ratio:.1f}× active vs new listings"})
        elif supply_ratio > 2.5:
            score = min(100, score + 5)

    redfin_supply = d.get("redfin_supply")
    if redfin_supply and redfin_supply > 5:
        score = min(100, score + 8)
        signals.append({"type": "opportunity", "text": f"{redfin_supply:.1f} months of supply — buyer's market"})

    return {"score": max(0, min(100, score)), "signals": signals}


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
        import math
        d["golden_score"] = int(math.sqrt(bs * es))

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
