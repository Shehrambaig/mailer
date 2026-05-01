"""
Refresh the precomputed foreclosure-count lookup tables that the /explore
page joins against. Run this whenever foreclosure_records gains/loses rows
(e.g., after a new scrape) — typically once a week.

Tables refreshed:
  foreclosure_counts_by_zip    (postal_code → fc_count)
  foreclosure_counts_by_county (county_fips → fc_count, derived via zip_county_map)

Tables snapshotted before refresh (used by /explore for week-over-week Δ):
  foreclosure_counts_by_zip_prev
  foreclosure_counts_by_county_prev

The previous snapshot is captured *before* the live tables are dropped. On
the very first run there is nothing to snapshot, so prev tables are seeded
empty and deltas show as ±0 until the second weekly refresh.
"""
import os
import sys
import time

import psycopg2
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DSN = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not DSN:
    sys.exit("NEON_DB not set")


SQL = """
-- 1. Ensure both live and prev tables exist so the snapshot step is safe
--    on first run (TRUNCATE + INSERT ... SELECT requires a target table).
CREATE TABLE IF NOT EXISTS foreclosure_counts_by_zip (
    postal_code text PRIMARY KEY,
    fc_count    int
);
CREATE TABLE IF NOT EXISTS foreclosure_counts_by_county (
    county_fips text PRIMARY KEY,
    fc_count    int
);
CREATE TABLE IF NOT EXISTS foreclosure_counts_by_zip_prev (
    postal_code text PRIMARY KEY,
    fc_count    int
);
CREATE TABLE IF NOT EXISTS foreclosure_counts_by_county_prev (
    county_fips text PRIMARY KEY,
    fc_count    int
);

-- 2. Snapshot the *current* counts into *_prev before rebuilding. After
--    this runs, *_prev holds last week's totals — the basis for the WoW Δ
--    chip rendered in /explore.
TRUNCATE foreclosure_counts_by_zip_prev;
INSERT INTO foreclosure_counts_by_zip_prev (postal_code, fc_count)
SELECT postal_code, fc_count FROM foreclosure_counts_by_zip;

TRUNCATE foreclosure_counts_by_county_prev;
INSERT INTO foreclosure_counts_by_county_prev (county_fips, fc_count)
SELECT county_fips, fc_count FROM foreclosure_counts_by_county;

-- 3. Rebuild the live tables from the latest foreclosure_records.
DROP TABLE IF EXISTS foreclosure_counts_by_zip;
CREATE TABLE foreclosure_counts_by_zip AS
SELECT LPAD(zip, 5, '0') AS postal_code, COUNT(*)::int AS fc_count
FROM foreclosure_records
WHERE status='active' AND zip IS NOT NULL AND zip <> ''
GROUP BY LPAD(zip, 5, '0');
ALTER TABLE foreclosure_counts_by_zip ADD PRIMARY KEY (postal_code);

DROP TABLE IF EXISTS foreclosure_counts_by_county;
CREATE TABLE foreclosure_counts_by_county AS
SELECT zcm.county_fips, SUM(fc.fc_count)::int AS fc_count
FROM foreclosure_counts_by_zip fc
JOIN zip_county_map zcm ON zcm.postal_code = fc.postal_code
GROUP BY zcm.county_fips;
ALTER TABLE foreclosure_counts_by_county ADD PRIMARY KEY (county_fips);
"""


def main():
    print("Connecting to Neon...")
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            t0 = time.monotonic()
            cur.execute(SQL)
            conn.commit()
            print(f"Tables rebuilt in {(time.monotonic()-t0)*1000:.0f}ms")

            cur.execute("SELECT COUNT(*), SUM(fc_count) FROM foreclosure_counts_by_zip")
            n, tot = cur.fetchone()
            print(f"  foreclosure_counts_by_zip        : {n:,} ZIPs    covering {tot or 0:,} records")
            cur.execute("SELECT COUNT(*), SUM(fc_count) FROM foreclosure_counts_by_county")
            n, tot = cur.fetchone()
            print(f"  foreclosure_counts_by_county     : {n:,} counties covering {tot or 0:,} records")
            cur.execute("SELECT COUNT(*), SUM(fc_count) FROM foreclosure_counts_by_zip_prev")
            n, tot = cur.fetchone()
            print(f"  foreclosure_counts_by_zip_prev   : {n:,} ZIPs    covering {tot or 0:,} records (last week)")
            cur.execute("SELECT COUNT(*), SUM(fc_count) FROM foreclosure_counts_by_county_prev")
            n, tot = cur.fetchone()
            print(f"  foreclosure_counts_by_county_prev: {n:,} counties covering {tot or 0:,} records (last week)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
