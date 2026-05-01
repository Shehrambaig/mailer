"""
Refresh the precomputed foreclosure-count lookup tables that the /explore
page joins against. Run this whenever foreclosure_records gains/loses rows
(e.g., after a new scrape).

Tables refreshed:
  foreclosure_counts_by_zip    (postal_code → fc_count)
  foreclosure_counts_by_county (county_fips → fc_count, derived via zip_county_map)

Both tables are dropped and recreated atomically. Takes ~3 seconds.
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
            print(f"  foreclosure_counts_by_zip   : {n:,} ZIPs    covering {tot:,} records")
            cur.execute("SELECT COUNT(*), SUM(fc_count) FROM foreclosure_counts_by_county")
            n, tot = cur.fetchone()
            print(f"  foreclosure_counts_by_county: {n:,} counties covering {tot:,} records")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
