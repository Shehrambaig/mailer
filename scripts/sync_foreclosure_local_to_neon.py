"""
Upsert foreclosure_records from local Postgres into Neon.

Source : POSTGRES_URI=postgresql://shehrambaig@localhost:5432/probate
Target : NEON_DB

Conflict key: (source, listing_id) — already UNIQUE on both sides.

Behavior on conflict:
  - Most fields overwrite from local (latest scrape wins).
  - `county`        is preserved when local value is NULL/empty:
        COALESCE(NULLIF(EXCLUDED.county, ''), foreclosure_records.county)
    Reason: Neon already has ~877k counties enriched via zip_county_map;
    local has NULL for foreclosure_com rows. Don't wipe enrichment.
  - `county_fips`   is never touched here (Neon-only column).
    The follow-up `backfill_foreclosure_county.py` will fill it for new rows.
  - `first_seen_at` is Neon-only and not in COLS, so:
      * new rows get DEFAULT NOW() on insert (= the moment they first arrived)
      * existing rows keep their original first_seen_at on update
    Run scripts/add_first_seen_at.py once to add the column + index.

Usage:
  python scripts/sync_foreclosure_local_to_neon.py --dry-run
  python scripts/sync_foreclosure_local_to_neon.py
  python scripts/sync_foreclosure_local_to_neon.py --batch 5000
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

LOCAL_DSN = os.getenv("POSTGRES_URI") or "postgresql://shehrambaig@localhost:5432/probate"
NEON_DSN  = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DSN:
    sys.exit("NEON_DB not set in .env")


# Columns transferred (excludes `id` and `county_fips`):
COLS = [
    "source", "listing_id", "street", "city", "state", "zip", "county",
    "full_address", "latitude", "longitude", "bedrooms", "bathrooms",
    "square_footage", "year_built", "property_type", "auction_date",
    "listed_on", "status", "estimated_value", "starting_bid", "details_url",
    "parties", "raw", "scraped_at", "listing_type", "classification",
]
COL_LIST = ", ".join(COLS)

# UPDATE clause on conflict — most fields take EXCLUDED, county is COALESCE'd.
def _update_clause():
    parts = []
    for c in COLS:
        if c in ("source", "listing_id"):
            continue
        if c == "county":
            parts.append(f"county = COALESCE(NULLIF(EXCLUDED.county, ''), foreclosure_records.county)")
        else:
            parts.append(f"{c} = EXCLUDED.{c}")
    return ", ".join(parts)


UPSERT_SQL = f"""
INSERT INTO foreclosure_records ({COL_LIST}) VALUES %s
ON CONFLICT (source, listing_id) DO UPDATE SET {_update_clause()}
"""


def _adapt_row(row):
    """Convert a row tuple from local cursor into the value tuple for execute_values.
    Mostly a no-op, but jsonb fields need str(json) wrapping if not already."""
    out = []
    for v in row:
        if isinstance(v, (dict, list)):
            out.append(psycopg2.extras.Json(v))
        else:
            out.append(v)
    return tuple(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Count what would be inserted/updated, no writes.")
    ap.add_argument("--batch", type=int, default=2000,
                    help="Rows per UPSERT batch (default 2000)")
    args = ap.parse_args()

    print(f"Source: {LOCAL_DSN}")
    print(f"Target: {NEON_DSN[:60]}…")
    print()

    src = psycopg2.connect(LOCAL_DSN, connect_timeout=10)
    src.set_session(readonly=True)
    dst = psycopg2.connect(NEON_DSN, connect_timeout=15)

    try:
        # Pre-flight stats
        with src.cursor() as c:
            c.execute("SELECT COUNT(*) FROM foreclosure_records")
            n_src = c.fetchone()[0]
        with dst.cursor() as c:
            c.execute("SELECT COUNT(*) FROM foreclosure_records")
            n_dst_before = c.fetchone()[0]
        print(f"Local rows : {n_src:>10,}")
        print(f"Neon  rows : {n_dst_before:>10,}")
        print()

        if args.dry_run:
            # How many local (source, listing_id) keys are NEW vs EXISTING in Neon.
            print("Dry-run: estimating split via temp table on Neon …")
            with dst.cursor() as c:
                c.execute("CREATE TEMP TABLE _local_keys (source TEXT, listing_id TEXT)")
            # Stream keys over and COPY into temp table
            with src.cursor(name="ro_keys") as src_c:
                src_c.itersize = 50000
                src_c.execute("SELECT source, listing_id FROM foreclosure_records")
                buf = []
                total = 0
                with dst.cursor() as dst_c:
                    for row in src_c:
                        buf.append(row)
                        if len(buf) >= 50000:
                            psycopg2.extras.execute_values(
                                dst_c, "INSERT INTO _local_keys VALUES %s", buf,
                                page_size=10000,
                            )
                            total += len(buf); buf = []
                            print(f"  …keys streamed: {total:,}", end="\r", flush=True)
                    if buf:
                        psycopg2.extras.execute_values(
                            dst_c, "INSERT INTO _local_keys VALUES %s", buf,
                            page_size=10000,
                        )
                        total += len(buf)
                print(f"  …keys streamed: {total:,}    ")
            with dst.cursor() as c:
                c.execute("""
                  SELECT
                    (SELECT COUNT(*) FROM _local_keys lk
                       LEFT JOIN foreclosure_records fr
                         ON fr.source = lk.source AND fr.listing_id = lk.listing_id
                       WHERE fr.id IS NULL) AS new_rows,
                    (SELECT COUNT(*) FROM _local_keys lk
                       JOIN foreclosure_records fr
                         ON fr.source = lk.source AND fr.listing_id = lk.listing_id) AS existing_rows
                """)
                new_n, exist_n = c.fetchone()
            dst.rollback()  # drop temp table
            print()
            print(f"Would INSERT (new keys)         : {new_n:>10,}")
            print(f"Would UPDATE (already on Neon)  : {exist_n:>10,}")
            print(f"Net Neon row-count delta        : {new_n:>10,}")
            print()
            print("Run again without --dry-run to apply.")
            return

        # Real upsert — stream + batched UPSERT
        t0 = time.time()
        select_sql = f"SELECT {COL_LIST} FROM foreclosure_records"
        with src.cursor(name="ro_stream") as src_c:
            src_c.itersize = args.batch
            src_c.execute(select_sql)
            batch = []
            done = 0
            with dst.cursor() as dst_c:
                while True:
                    row = src_c.fetchone()
                    if row is None:
                        if batch:
                            psycopg2.extras.execute_values(
                                dst_c, UPSERT_SQL, [_adapt_row(r) for r in batch],
                                page_size=args.batch,
                            )
                            done += len(batch)
                            dst.commit()
                        break
                    batch.append(row)
                    if len(batch) >= args.batch:
                        psycopg2.extras.execute_values(
                            dst_c, UPSERT_SQL, [_adapt_row(r) for r in batch],
                            page_size=args.batch,
                        )
                        done += len(batch)
                        batch = []
                        dst.commit()
                        rate = done / max(0.001, time.time() - t0)
                        eta  = (n_src - done) / max(1, rate)
                        print(f"  …upserted {done:>9,}/{n_src:,} "
                              f"({100*done/n_src:5.1f}%)  rate={rate:>5.0f}/s  eta={eta:>5.0f}s", end="\r", flush=True)

        print()
        with dst.cursor() as c:
            c.execute("SELECT COUNT(*) FROM foreclosure_records")
            n_dst_after = c.fetchone()[0]
        elapsed = time.time() - t0
        print()
        print(f"Done in {elapsed:.1f}s")
        print(f"Neon rows: {n_dst_before:,} → {n_dst_after:,} "
              f"(net +{n_dst_after - n_dst_before:,})")
        print()
        print("Next steps:")
        print("  1) python scripts/backfill_foreclosure_county.py   # fill county / county_fips on new rows")
        print("  2) python scripts/refresh_foreclosure_counts.py    # refresh /explore Foreclosures column")
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
