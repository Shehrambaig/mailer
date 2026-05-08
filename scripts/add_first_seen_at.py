"""
Add a Neon-only `first_seen_at` column to foreclosure_records.

Set on INSERT (DEFAULT NOW()), never overwritten on conflict — so it always
records the first time we saw a (source, listing_id) pair, regardless of how
many times the row is later re-scraped and re-synced. Lets us answer:

    SELECT source, DATE(first_seen_at) AS day, COUNT(*)
    FROM foreclosure_records
    WHERE first_seen_at >= NOW() - INTERVAL '7 days'
    GROUP BY source, day
    ORDER BY day, source;

The sync script (sync_foreclosure_local_to_neon.py) does NOT include
first_seen_at in its column list, so:
  - new rows hit DEFAULT NOW() on Neon
  - updates of existing rows leave first_seen_at untouched

Backfill policy: every row that exists at the time of this run is stamped
with --baseline-date (default: the most recent Friday). Treats "everything
that was here before this script ran" as a single bulk import, so the
ingestion log has a clean cutoff between historical data and live arrivals.

Idempotent — re-running with the same baseline is a no-op (only rows whose
first_seen_at differs from the baseline AND were stamped by the ALTER's
DEFAULT NOW() — i.e., rows newer than the baseline that have no later
scraped_at — are touched).

Usage:
  python scripts/add_first_seen_at.py                          # apply
  python scripts/add_first_seen_at.py --dry-run                # preview
  python scripts/add_first_seen_at.py --baseline-date 2026-05-01
"""
import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DSN = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not DSN:
    sys.exit("NEON_DB not set")


DDL = """
ALTER TABLE foreclosure_records
  ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS foreclosure_records_first_seen_idx
  ON foreclosure_records(first_seen_at);

CREATE INDEX IF NOT EXISTS foreclosure_records_source_first_seen_idx
  ON foreclosure_records(source, first_seen_at);
"""

BACKFILL = """
UPDATE foreclosure_records
   SET first_seen_at = %s
 WHERE first_seen_at >= %s;
"""
# Bulk-import cutoff: every row already in the table at the time this
# script runs is treated as one historical batch and stamped with the
# supplied baseline. Rows whose first_seen_at is BEFORE the baseline are
# left alone (they were never the ALTER's fault), and on subsequent runs
# rows that have arrived legitimately after the baseline keep their real
# arrival timestamp because the script is meant to be run once.


def _last_friday(today=None):
    today = today or date.today()
    # Mon=0 ... Fri=4 ... Sun=6
    delta = (today.weekday() - 4) % 7
    if delta == 0:
        delta = 7  # if today *is* Friday, take the previous one
    return today - timedelta(days=delta)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--baseline-date", default=None,
                    help="ISO date (YYYY-MM-DD). Defaults to most recent Friday.")
    args = ap.parse_args()

    if args.baseline_date:
        baseline = date.fromisoformat(args.baseline_date)
    else:
        baseline = _last_friday()
    print(f"Baseline date: {baseline.isoformat()} ({baseline.strftime('%A')})")

    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as c:
            if _column_exists(conn, "foreclosure_records", "first_seen_at"):
                c.execute("""
                  SELECT COUNT(*),
                         COUNT(*) FILTER (WHERE first_seen_at IS NULL),
                         COUNT(*) FILTER (WHERE first_seen_at::date >= %s)
                  FROM foreclosure_records
                """, (baseline,))
                total, missing, will_touch = c.fetchone()
                print(f"foreclosure_records: {total:,} rows, "
                      f"{missing:,} NULL, {will_touch:,} ≥ baseline (will be re-stamped)")
            else:
                c.execute("SELECT COUNT(*) FROM foreclosure_records")
                total = c.fetchone()[0]
                print(f"foreclosure_records: {total:,} rows (column does not exist yet)")

        if args.dry_run:
            print("\nDry-run — no changes. Would run:")
            print(DDL)
            print(BACKFILL.strip().replace("%s", f"'{baseline.isoformat()}'", 2))
            return

        print("\nApplying schema change ...")
        with conn.cursor() as c:
            c.execute(DDL)
        conn.commit()
        print("  column + indexes ready")

        print(f"\nStamping rows with first_seen_at >= {baseline} → {baseline} ...")
        with conn.cursor() as c:
            c.execute(BACKFILL, (baseline, baseline))
            n = c.rowcount
        conn.commit()
        print(f"  → {n:,} rows stamped")

        with conn.cursor() as c:
            c.execute("""
              SELECT source,
                     COUNT(*),
                     MIN(first_seen_at),
                     MAX(first_seen_at)
              FROM foreclosure_records
              GROUP BY source
              ORDER BY source
            """)
            print(f"\n{'source':<18} {'rows':>10}  {'earliest':<28} {'latest':<28}")
            for s, n, mn, mx in c.fetchall():
                print(f"  {s:<16} {n:>10,}  {str(mn):<28} {str(mx):<28}")
    finally:
        conn.close()


def _column_exists(conn, table, col):
    with conn.cursor() as c:
        c.execute("""
          SELECT 1 FROM information_schema.columns
          WHERE table_name = %s AND column_name = %s
        """, (table, col))
        return c.fetchone() is not None


if __name__ == "__main__":
    main()
