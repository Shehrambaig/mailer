"""
Backfill foreclosure_records.county / county_fips from the ZIP→FIPS crosswalk.

The auction_com source already populates a bare county name (e.g., "Maricopa",
"Los Angeles"). foreclosure_com rows have ZIP but no county. This script:

  1. Ensures the zip_county_map(postal_code, county_fips, county_name,
     state_id) lookup table exists in Neon, then TRUNCATE + COPY-loads the
     latest crosswalk JSON.
  2. Populates it from app/static/data/zip-by-county-2020.json (Census 2020
     ZCTA → 2020 county PIP) joined to realtor_inventory_county (latest
     month) for the human-readable county name. Names are normalized to
     match the auction_com format — bare county name, title-cased, no state
     suffix ("Lowndes" not "lowndes, ga").
  3. ALTERs foreclosure_records to add county_fips VARCHAR(5) + index
     (idempotent IF NOT EXISTS).
  4. UPDATEs foreclosure_records, filling both county (name) and county_fips
     (id) wherever the ZIP joins to the map and either column is missing or
     stale.

Usage:
  python scripts/backfill_foreclosure_county.py            # apply changes
  python scripts/backfill_foreclosure_county.py --dry-run  # preview only
  python scripts/backfill_foreclosure_county.py --crosswalk app/static/data/zip-by-county-2020.json
                                                           # use a different source file (default already)
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

DEFAULT_CROSSWALK = ROOT / "app" / "static" / "data" / "zip-by-county-2020.json"


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
    ap.add_argument("--crosswalk", default=str(DEFAULT_CROSSWALK),
                    help="Path to zip-by-county JSON {fips: [zip5, ...]}")
    args = ap.parse_args()

    crosswalk_path = Path(args.crosswalk)
    print(f"Loading {crosswalk_path.relative_to(ROOT)} ...")
    zbc = json.loads(crosswalk_path.read_text())
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

        # Ensure foreclosure_records.county_fips exists for direct FIPS joins.
        print("\nEnsuring foreclosure_records.county_fips column + index ...")
        with conn.cursor() as c:
            c.execute("""
              ALTER TABLE foreclosure_records
                ADD COLUMN IF NOT EXISTS county_fips VARCHAR(5);
              CREATE INDEX IF NOT EXISTS foreclosure_records_county_fips_idx
                ON foreclosure_records(county_fips);
            """)
        conn.commit()

        # Backfill foreclosure_records — fills both county (name, when missing)
        # and county_fips (id, whenever stale or missing). county_fips is filled
        # for ALL sources; county (name) is filled only where the scraper left
        # it empty (auction_com already provides its own).
        print("\nBackfilling foreclosure_records.county_fips + county ...")
        with conn.cursor() as c:
            c.execute("""
              UPDATE foreclosure_records fr
              SET county_fips = zcm.county_fips,
                  county      = COALESCE(NULLIF(fr.county, ''), zcm.county_name)
              FROM zip_county_map zcm
              WHERE zcm.postal_code = LPAD(fr.zip, 5, '0')
                AND (
                     fr.county_fips IS DISTINCT FROM zcm.county_fips
                  OR (fr.county IS NULL OR fr.county = '')
                )
            """)
            updated = c.rowcount
        conn.commit()
        print(f"  → updated {updated:,} rows")

        # Final state
        with conn.cursor() as c:
            c.execute("""
              SELECT source,
                     COUNT(*),
                     COUNT(county) FILTER (WHERE county IS NOT NULL AND county <> ''),
                     COUNT(county_fips) FILTER (WHERE county_fips IS NOT NULL)
              FROM foreclosure_records
              GROUP BY source
            """)
            print(f"\n{'source':<18} {'total':>10} {'with_county':>12} {'with_fips':>12}")
            for row in c.fetchall():
                print(f"  {row[0]:<16} {row[1]:>10,} {row[2]:>12,} {row[3]:>12,}")

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
