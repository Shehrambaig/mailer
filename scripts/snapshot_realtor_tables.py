"""
Snapshot Realtor tables in Neon before a data refresh.

Creates `<table>_bak_<YYYYMMDD_HHMM>` copies of the inventory + hotness tables
so a bad import can be reverted with a TRUNCATE + INSERT SELECT.

Usage:
  python scripts/snapshot_realtor_tables.py
  python scripts/snapshot_realtor_tables.py --include-scores   # also snapshots realtor_scores_*

Prints the backup table names and a ready-to-paste rollback SQL block.
"""
import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DSN = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not DSN:
    sys.exit("NEON_DB not set")

CORE_TABLES = [
    "realtor_inventory_county",
    "realtor_inventory_zip",
    "realtor_inventory_state",
    "realtor_hotness_county",
    "realtor_hotness_zip",
]
SCORE_TABLES = ["realtor_scores_county", "realtor_scores_zip"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-scores", action="store_true",
                    help="Also snapshot realtor_scores_* (drop+recreate; less critical).")
    args = ap.parse_args()

    tables = CORE_TABLES + (SCORE_TABLES if args.include_scores else [])
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    backups = [(t, f"{t}_bak_{stamp}") for t in tables]

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for src, bak in backups:
                cur.execute(
                    f"SELECT to_regclass(%s) IS NOT NULL", (src,)
                )
                exists = cur.fetchone()[0]
                if not exists:
                    print(f"  [skip] {src} — table does not exist")
                    continue
                cur.execute(f'CREATE TABLE "{bak}" AS SELECT * FROM "{src}"')
                cur.execute(f'SELECT COUNT(*) FROM "{bak}"')
                n = cur.fetchone()[0]
                print(f"  snapshot: {src:32s} → {bak}  ({n:,} rows)")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\nRollback SQL (paste into psql if April import goes wrong):\n")
    print("BEGIN;")
    for src, bak in backups:
        print(f'  TRUNCATE "{src}";')
        print(f'  INSERT INTO "{src}" SELECT * FROM "{bak}";')
    print("COMMIT;")
    print("\n(Or, simpler, to nuke only April 2026:")
    print("  DELETE FROM realtor_inventory_county WHERE month_date_yyyymm = 202604;")
    print("  DELETE FROM realtor_inventory_zip    WHERE month_date_yyyymm = 202604;")
    print("  DELETE FROM realtor_hotness_county   WHERE month_date_yyyymm = 202604;")
    print("  DELETE FROM realtor_hotness_zip      WHERE month_date_yyyymm = 202604;)")


if __name__ == "__main__":
    main()
