"""
Backfill foreclosure_records.county for foreclosure_com rows.

The auction_com source already populates a bare county name (e.g., "Maricopa",
"Los Angeles"). foreclosure_com rows have ZIP but no county. This script:

  1. Creates a permanent zip_county_map(postal_code, county_fips, county_name,
     state_id) lookup table in Neon (idempotent — runs CREATE TABLE IF NOT
     EXISTS + TRUNCATE + COPY).
  2. Populates it from app/static/data/zip-by-county.json (Census ZIP→FIPS map)
     joined to realtor_inventory_county (latest month) for the human-readable
     county name. Names are normalized to match the auction_com format —
     bare county name, title-cased, no state suffix ("Lowndes" not
     "lowndes, ga").
  3. UPDATEs foreclosure_records SET county = zcm.county_name for rows where
     source='foreclosure_com' AND county IS NULL AND zip joins to the map.

Usage:
  python scripts/backfill_foreclosure_county.py            # apply changes
  python scripts/backfill_foreclosure_county.py --dry-run  # preview only
"""
import argparse
import io
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
NEON_DB = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DB:
    sys.exit("NEON_DB not set")

ZBC_PATH = ROOT / "app" / "static" / "data" / "zip-by-county.json"


# Title-case county names but preserve common small connectors lowercase.
SMALL_WORDS = {"and", "of", "the", "del", "de", "la"}
def smart_title(s):
    if not s:
        return s
    parts = s.split()
    out = []
    for i, w in enumerate(parts):
        wl = w.lower()
        if i > 0 and wl in SMALL_WORDS:
            out.append(wl)
        else:
            # Preserve hyphens: "miami-dade" -> "Miami-Dade"
            out.append("-".join(p.capitalize() for p in w.split("-")))
    return " ".join(out)


def strip_state_suffix(name):
    """ 'lowndes, ga' -> 'Lowndes'  /  'miami-dade, fl' -> 'Miami-Dade' """
    if not name:
        return None
    bare = name.rsplit(",", 1)[0].strip()
    return smart_title(bare) if bare else None


DDL = """
CREATE TABLE IF NOT EXISTS zip_county_map (
  postal_code  VARCHAR(5)  PRIMARY KEY,
  county_fips  VARCHAR(5)  NOT NULL,
  county_name  TEXT,
  state_id     CHAR(2)
);
CREATE INDEX IF NOT EXISTS zip_county_map_fips_idx  ON zip_county_map(county_fips);
CREATE INDEX IF NOT EXISTS zip_county_map_state_idx ON zip_county_map(state_id);
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"Loading {ZBC_PATH.name} ...")
    zbc = json.loads(ZBC_PATH.read_text())
    print(f"  {len(zbc)} counties → {sum(len(z) for z in zbc.values()):,} ZIPs")

    print("Connecting to Neon...")
    conn = psycopg2.connect(NEON_DB)
    try:
        # Pull county_name + state_id from realtor_inventory_county (latest month).
        with conn.cursor() as c:
            c.execute("""
              SELECT county_fips, county_name
              FROM realtor_inventory_county
              WHERE month_date_yyyymm = (SELECT MAX(month_date_yyyymm) FROM realtor_inventory_county)
            """)
            fips_to_name = {fips: strip_state_suffix(n) for fips, n in c.fetchall()}
        print(f"  {len(fips_to_name):,} county names from realtor_inventory_county")

        # Build the (postal_code, county_fips, county_name, state_id) rows.
        rows = []
        no_name = 0
        for fips, zips in zbc.items():
            name = fips_to_name.get(fips)
            state = name.split(",")[-1].strip().upper() if name and "," in name else None
            # state_id from FIPS prefix is more reliable than parsing names.
            # State FIPS → 2-letter mapping is well-known; just take the first 2 chars.
            STATE_FIPS = {
                "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE",
                "11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA",
                "20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN",
                "28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM",
                "36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI",
                "45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA",
                "54":"WV","55":"WI","56":"WY","72":"PR","78":"VI","60":"AS","66":"GU","69":"MP",
            }
            state_id = STATE_FIPS.get(fips[:2])
            if not name:
                no_name += 1
            for z in zips:
                rows.append((z, fips, name, state_id))
        print(f"  {len(rows):,} (zip, fips) pairs to insert")
        if no_name:
            print(f"  {no_name} counties had no Realtor name available — county_name will be NULL for those ZIPs")

        if args.dry_run:
            print("\n[DRY RUN] No changes applied. Sample rows:")
            for r in rows[:5]: print("  ", r)
            return

        with conn.cursor() as c:
            c.execute(DDL)
            c.execute("TRUNCATE zip_county_map")
            buf = io.StringIO()
            for z, fips, name, st in rows:
                # Use COPY-friendly NULL marker
                buf.write(f"{z}\t{fips}\t{name if name is not None else r'\N'}\t{st if st is not None else r'\N'}\n")
            buf.seek(0)
            c.copy_expert(
                "COPY zip_county_map (postal_code, county_fips, county_name, state_id) FROM STDIN",
                buf,
            )
        conn.commit()
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*), COUNT(county_name) FROM zip_county_map")
            n, named = c.fetchone()
        print(f"  zip_county_map populated: {n:,} rows, {named:,} with county_name")

        # Backfill foreclosure_records.county using zip_county_map.
        print("\nBackfilling foreclosure_records.county (source='foreclosure_com', county IS NULL)...")
        with conn.cursor() as c:
            c.execute("""
              UPDATE foreclosure_records fr
              SET county = zcm.county_name
              FROM zip_county_map zcm
              WHERE zcm.postal_code = LPAD(fr.zip, 5, '0')
                AND fr.source = 'foreclosure_com'
                AND (fr.county IS NULL OR fr.county = '')
                AND zcm.county_name IS NOT NULL
            """)
            updated = c.rowcount
        conn.commit()
        print(f"  → updated {updated:,} foreclosure_com rows")

        # Final state
        with conn.cursor() as c:
            c.execute("""
              SELECT source,
                     COUNT(*),
                     COUNT(county) FILTER (WHERE county IS NOT NULL AND county <> '')
              FROM foreclosure_records
              GROUP BY source
            """)
            print(f"\n{'source':<18} {'total':>10} {'with_county':>12}")
            for row in c.fetchall():
                print(f"  {row[0]:<16} {row[1]:>10,} {row[2]:>12,}")

            # Spot-check the user's example
            c.execute("""
              SELECT id, zip, city, state, county, classification
              FROM foreclosure_records
              WHERE id = 14442
            """)
            row = c.fetchone()
            if row:
                print(f"\nUser's example row (id=14442): {row}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
