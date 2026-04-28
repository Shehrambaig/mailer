"""
Pull foreclosure_records from the probate Postgres DB, classify each row into
mail-target buckets, aggregate per ZIP and FIPS, and emit:

  app/static/data/listings-zip.json     {zip5: {auc, fc, tl, bk, ss, total, upcoming_30/60/90, mail_score, samples[]}}
  app/static/data/listings.json         {fips: {total, auc, fc, tl, bk, ss, avg_val, avg_price, total_val, next_date, listings[]}}
  Updates app/static/data/county-heatmap.json with per-FIPS counts.

Usage:
  python scripts/import_listings.py
  python scripts/import_listings.py --dsn postgresql://user:pass@localhost:5432/probate

Buckets:
  auc  — Auctions: foreclosure_com `Auction` + auction_com `TRUSTEE`/`REO`/`PRIVATE_SELLER`/`DAY_1_REO`
  fc   — Active Foreclosures: foreclosure_com `Foreclosure`
  tl   — Tax Liens: foreclosure_com `Tax Lien`
  bk   — Bankruptcies: Chapter 7/11/12/13 Filed + `Bankruptcy`
  ss   — Short Sales: foreclosure_com `Short Sale`
  (Drop: Rent to Own, city-owned, HUD, VA, REO listing-tag, fixer-upper, Deal,
   One Hundred Down, Redemption — not mailable as buyer-target distress.)
"""
import argparse
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
import psycopg2.extras

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATIC = os.path.join(ROOT, "app", "static", "data")

# ── Classification ────────────────────────────────────────────────────────
AUCTION = {"auction"}                                  # foreclosure_com
AUCTION_COM_BUYABLE = {"TRUSTEE", "REO", "DAY_1_REO", "PRIVATE_SELLER", "PRIVATE_SELLER_INSPECTION"}
FORECLOSURE = {"foreclosure"}                          # foreclosure_com
TAX_LIEN = {"tax lien"}
BANKRUPTCY = {"chapter 7 filed", "chapter 11 filed", "chapter 12 filed", "chapter 13 filed",
              "chapter 15 filed", "bankruptcy"}
SHORT_SALE = {"short sale"}


def classify(source, classification):
    """Return bucket key (auc/fc/tl/bk/ss) or None to drop."""
    c = (classification or "").strip()
    cl = c.lower()
    if source == "auction_com":
        return "auc" if c in AUCTION_COM_BUYABLE else None
    # foreclosure_com
    if cl in AUCTION:     return "auc"
    if cl in FORECLOSURE: return "fc"
    if cl in TAX_LIEN:    return "tl"
    if cl in BANKRUPTCY:  return "bk"
    if cl in SHORT_SALE:  return "ss"
    return None  # drop everything else


def load_zip_to_fips():
    """Return {zip5: fips} from existing zip-by-county.json (built earlier)."""
    path = os.path.join(STATIC, "zip-by-county.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        zbc = json.load(f)
    out = {}
    for fips, zips in zbc.items():
        for z in zips:
            out[z] = fips
    return out


def load_county_exit_scores():
    """Return {fips: exit_score} from county-heatmap.json so we can compute mail_score."""
    path = os.path.join(STATIC, "county-heatmap.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        h = json.load(f)
    return {fips: d.get("es", 50) for fips, d in h.items()}


def load_zip_exit_scores():
    """Return {zip5: exit_score} from zip-heatmap.json."""
    path = os.path.join(STATIC, "zip-heatmap.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        h = json.load(f)
    return {z: d.get("es", 50) for z, d in h.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=(
        os.getenv("NEON_DB")
        or os.getenv("PROBATE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or "postgresql://localhost:5432/probate"
    ))
    ap.add_argument("--limit-samples", type=int, default=10, help="Sample listings stored per ZIP")
    ap.add_argument("--limit-county-samples", type=int, default=60, help="Sample listings stored per county")
    args = ap.parse_args()

    print(f"Connecting to {args.dsn.split('@')[-1] if '@' in args.dsn else args.dsn}")
    conn = psycopg2.connect(args.dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("Loading zip→fips crosswalk + exit-score lookups...")
    zip_to_fips = load_zip_to_fips()
    county_exit = load_county_exit_scores()
    zip_exit    = load_zip_exit_scores()
    print(f"  {len(zip_to_fips):,} ZIP→FIPS pairs · {len(county_exit):,} county scores · {len(zip_exit):,} ZIP scores")

    print("Querying foreclosure_records (status=active, classified buckets only)...")
    cur.execute("""
        SELECT id, source, listing_id, street, city, state, zip, county,
               latitude, longitude, auction_date, status, estimated_value,
               starting_bid, details_url, classification
        FROM foreclosure_records
        WHERE status = 'active'
    """)

    today = date.today()
    by_zip = {}     # zip5 → bucket counts + samples
    by_fips = {}    # fips → bucket counts
    n_total = n_kept = n_dropped = n_no_zip = 0

    for r in cur:
        n_total += 1
        bucket = classify(r["source"], r["classification"])
        if not bucket:
            n_dropped += 1
            continue
        z = (r["zip"] or "").strip().zfill(5)
        if not z or z == "00000" or len(z) != 5:
            n_no_zip += 1
            continue
        n_kept += 1

        d = by_zip.setdefault(z, {
            "auc": 0, "fc": 0, "tl": 0, "bk": 0, "ss": 0,
            "upcoming_30": 0, "upcoming_60": 0, "upcoming_90": 0,
            "samples": [],
        })
        d[bucket] += 1

        ad = r["auction_date"]
        if ad and isinstance(ad, date):
            days_out = (ad - today).days
            if 0 <= days_out <= 30: d["upcoming_30"] += 1
            if 0 <= days_out <= 60: d["upcoming_60"] += 1
            if 0 <= days_out <= 90: d["upcoming_90"] += 1

        if len(d["samples"]) < args.limit_samples:
            d["samples"].append({
                "id":     r["listing_id"],
                "addr":   r["street"],
                "city":   r["city"],
                "st":     r["state"],
                "zip":    z,
                "type":   bucket,
                "cls":    r["classification"],
                "date":   ad.isoformat() if ad else None,
                "val":    int(r["estimated_value"]) if r["estimated_value"] else None,
                "bid":    int(r["starting_bid"]) if r["starting_bid"] else None,
                "url":    r["details_url"],
            })

        fips = zip_to_fips.get(z) or (r["county"] and None)  # foreclosure_com county is null
        if fips:
            f = by_fips.setdefault(fips, {
                "auc":0,"fc":0,"tl":0,"bk":0,"ss":0,"total":0,
                "_val_sum":0.0,"_val_n":0,
                "_price_sum":0.0,"_price_n":0,
                "_total_val":0.0,
                "next_date":None,
                "listings":[],
            })
            f[bucket] += 1
            f["total"] += 1

            ev = r["estimated_value"]
            if ev:
                f["_val_sum"] += float(ev); f["_val_n"] += 1
                f["_total_val"] += float(ev)
            sb = r["starting_bid"]
            if sb:
                f["_price_sum"] += float(sb); f["_price_n"] += 1

            if ad and isinstance(ad, date) and ad >= today:
                if f["next_date"] is None or ad < f["next_date"]:
                    f["next_date"] = ad

            if len(f["listings"]) < args.limit_county_samples:
                f["listings"].append({
                    "src":   bucket,
                    "addr":  r["street"],
                    "city":  r["city"],
                    "st":    r["state"],
                    "zip":   z,
                    "beds":  None, "baths": None, "sqft": None, "yr": None, "vacant": False,
                    "val":   int(ev) if ev else None,
                    "price": int(sb) if sb else None,
                    "date":  ad.isoformat() if ad else None,
                    "svc":   r["source"],
                    "url":   r["details_url"],
                })

    cur.close()
    conn.close()

    print(f"  scanned {n_total:,} active rows · kept {n_kept:,} · dropped {n_dropped:,} non-mailable · {n_no_zip:,} no/bad ZIP")

    # ── Compute totals + mail_score per ZIP ──────────────────────────────
    print("Computing mail_score per ZIP...")
    for z, d in by_zip.items():
        # weighted mailable count: auctions count more (urgency), tax liens scaled
        # because foreclosure_com Auction class is huge but not all are dated
        mailable = (
            d["auc"] * 1.0 +
            d["fc"]  * 1.2 +   # active foreclosures slightly higher value
            d["tl"]  * 0.5 +   # tax liens — large pool, lower per-piece value
            d["bk"]  * 0.4 +   # bankruptcy — lower mail-response rate
            d["ss"]  * 0.6
        )
        d["total"] = d["auc"] + d["fc"] + d["tl"] + d["bk"] + d["ss"]

        # Cross with Exit Score: a mailable property in a slow-resale ZIP is a trap
        es = zip_exit.get(z, 50)
        # mail_score 0–100: log-scaled mailable count weighted by exit ease
        import math
        if mailable < 1:
            d["mail_score"] = 0
        else:
            # log10(mailable) maps 1→0, 10→1, 100→2, 1000→3.
            # Cap at 3.0 (~1000 listings is more than enough for any campaign).
            density = min(math.log10(mailable + 1) / 3.0, 1.0) * 100
            # Weight by exit score (50 is neutral, 100 is great, 0 is awful).
            weighted = density * (es / 100.0) ** 0.6  # mild weighting
            d["mail_score"] = int(round(min(weighted, 100)))

    # ── Write outputs ────────────────────────────────────────────────────
    out_zip = os.path.join(STATIC, "listings-zip.json")
    with open(out_zip, "w") as f:
        json.dump(by_zip, f, separators=(",", ":"))
    print(f"  wrote {out_zip}  ({os.path.getsize(out_zip)/1024:.0f} KB · {len(by_zip):,} ZIPs)")

    # Merge listing counts into zip-heatmap.json so the ZIP-grid map
    # ("Mail Targets" / "Auctions" / "All Listings" layers) lights up.
    zh_path = os.path.join(STATIC, "zip-heatmap.json")
    if os.path.exists(zh_path):
        with open(zh_path) as f:
            zh = json.load(f)
        for z, d in by_zip.items():
            if z not in zh:
                continue
            zh[z]["mail_score"] = d.get("mail_score", 0)
            zh[z]["mail_total"] = d.get("total", 0)
            zh[z]["mail_auc"]   = d.get("auc", 0)
            zh[z]["mail_fc"]    = d.get("fc", 0)
            zh[z]["mail_tl"]    = d.get("tl", 0)
            zh[z]["mail_bk"]    = d.get("bk", 0)
            zh[z]["mail_ss"]    = d.get("ss", 0)
            # Aliases that match the existing county-layer keys, so the same
            # layer buttons (fc_ct/au_ct/tot_ct) work in ZIP view too.
            zh[z]["fc_ct"]      = d.get("fc", 0) + d.get("tl", 0) + d.get("bk", 0)  # broader distress
            zh[z]["au_ct"]      = d.get("auc", 0)
            zh[z]["tot_ct"]     = d.get("total", 0)
        # Default 0 for ZIPs without listings (so the color scale renders cleanly)
        for z, d in zh.items():
            for k in ("mail_score","mail_total","mail_auc","mail_fc","mail_tl","mail_bk","mail_ss",
                      "fc_ct","au_ct","tot_ct"):
                d.setdefault(k, 0)
        with open(zh_path, "w") as f:
            json.dump(zh, f, separators=(",", ":"))
        n_with = sum(1 for d in zh.values() if d.get("mail_total", 0) > 0)
        print(f"  merged listing counts into zip-heatmap.json  ({n_with:,} of {len(zh):,} ZIPs have listings)")

    # ── Per-county outputs ───────────────────────────────────────────────
    # listings.json: counts + aggregates only (no sample listings) — loaded
    # on page load, must stay small. ~150 KB for ~3k counties.
    # listings-detail.json: per-county sample lists — loaded on demand by
    # /api/listings/<fips> when the user opens the Listings tab.
    counts_out, detail_out = {}, {}
    for fips, c in by_fips.items():
        avg_val   = round(c["_val_sum"]   / c["_val_n"])   if c["_val_n"]   else 0
        avg_price = round(c["_price_sum"] / c["_price_n"]) if c["_price_n"] else 0
        counts_out[fips] = {
            "total":      c["total"],
            "auc":        c["auc"], "fc": c["fc"], "tl": c["tl"],
            "bk":         c["bk"],  "ss": c["ss"],
            "avg_val":    avg_val,
            "avg_price":  avg_price,
            "total_val":  round(c["_total_val"]),
            "vacant_pct": 0,
            "next_date":  c["next_date"].isoformat() if c["next_date"] else None,
        }
        detail_out[fips] = {**counts_out[fips], "listings": c["listings"]}

    out_counts = os.path.join(STATIC, "listings.json")
    with open(out_counts, "w") as f:
        json.dump(counts_out, f, separators=(",", ":"))
    print(f"  wrote {out_counts}  ({os.path.getsize(out_counts)/1024:.0f} KB · {len(counts_out):,} counties, counts only)")

    out_detail = os.path.join(STATIC, "listings-detail.json")
    with open(out_detail, "w") as f:
        json.dump(detail_out, f, separators=(",", ":"))
    print(f"  wrote {out_detail}  ({os.path.getsize(out_detail)/1024:.0f} KB · {len(detail_out):,} counties, with samples)")

    # Merge into county heatmap
    heat_path = os.path.join(STATIC, "county-heatmap.json")
    if os.path.exists(heat_path):
        with open(heat_path) as f:
            heat = json.load(f)
        for fips, c in by_fips.items():
            if fips in heat:
                heat[fips]["mail_auc"] = c["auc"]
                heat[fips]["mail_fc"]  = c["fc"]
                heat[fips]["mail_tl"]  = c["tl"]
                heat[fips]["mail_bk"]  = c["bk"]
                heat[fips]["mail_ss"]  = c["ss"]
                heat[fips]["mail_total"] = c["total"]
                # county-level mail_score: similar formula
                import math
                weighted = (c["auc"]*1.0 + c["fc"]*1.2 + c["tl"]*0.5 + c["bk"]*0.4 + c["ss"]*0.6)
                density = min(math.log10(weighted + 1) / 3.5, 1.0) * 100
                es = county_exit.get(fips, 50)
                heat[fips]["mail_score"] = int(round(min(density * (es/100)**0.6, 100)))
        # Default 0 for counties without listings so the layer renders cleanly
        for fips, d in heat.items():
            for k in ("mail_auc","mail_fc","mail_tl","mail_bk","mail_ss","mail_total","mail_score"):
                d.setdefault(k, 0)
        with open(heat_path, "w") as f:
            json.dump(heat, f, separators=(",", ":"))
        n_with = sum(1 for d in heat.values() if d.get("mail_total", 0) > 0)
        print(f"  merged mail_* into county-heatmap.json  ({n_with:,} of {len(heat):,} counties have listings)")

    # Print summary
    print("\nTop 10 ZIPs by mail_score:")
    top = sorted(by_zip.items(), key=lambda x: x[1]["mail_score"], reverse=True)[:10]
    for z, d in top:
        print(f"  {z}: score={d['mail_score']:>3}  auc={d['auc']:>4} fc={d['fc']:>3} tl={d['tl']:>4} bk={d['bk']:>4} ss={d['ss']}  upcoming30={d['upcoming_30']}")


if __name__ == "__main__":
    main()
