"""
Mirror non-foreclosure scraper tables from local Postgres → Neon so the
deployed /scrapers page sees the same data the local pipeline writes.

What gets synced (the tables the /scrapers manifest reads):
    records, north_carolina, maryland, ct_probate_cases, ga_probate_cases,
    gwinnett, ok_probate_cases, wisconsin, indiana, minnesota, michigan

What does NOT get synced (intentional):
    foreclosure_records / foreclosure_enriched and their precomputed
    foreclosure_counts_* tables — Neon owns those (this script is the
    inverse of sync_foreclosure_local_to_neon.py).

Strategy per table:
  1. pg_dump --schema-only -t <table>   (from local)
  2. On Neon: DROP TABLE IF EXISTS, then re-execute the dumped DDL.
     Rationale: schemas drift between local & Neon (column adds/renames),
     and the simplest way to guarantee the data round-trips cleanly is to
     recreate the empty target.
  3. Stream rows local → Neon in batches via execute_values.

Usage:
  python scripts/sync_probate_local_to_neon.py --dry-run    # counts only
  python scripts/sync_probate_local_to_neon.py              # apply
  python scripts/sync_probate_local_to_neon.py --tables ct_probate_cases,maryland
"""
import argparse
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

LOCAL_DSN = os.getenv("POSTGRES_URI") or "postgresql://localhost:5432/probate"
NEON_DSN  = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DSN:
    sys.exit("NEON_DB not set in .env")

DEFAULT_TABLES = [
    "records",
    "north_carolina",
    "maryland",
    "ct_probate_cases",
    "ga_probate_cases",
    "gwinnett",
    "georgia",            # GPR statewide aggregator
    "ok_probate_cases",
    "wisconsin",
    "indiana",
    "minnesota",
    "michigan",
]

# The script refuses to touch any of these even if explicitly named — they're
# either Neon-owned (foreclosure data) or downstream/derived.
FORBIDDEN = {
    "foreclosure_records",
    "foreclosure_enriched",
    "foreclosure_counts_by_zip",
    "foreclosure_counts_by_county",
    "foreclosure_counts_by_zip_prev",
    "foreclosure_counts_by_county_prev",
}


def _build_ddl(src_conn, table: str) -> str:
    """Build a minimal CREATE TABLE statement from local information_schema.

    We avoid pg_dump because pg_dump 18 emits client-side psql meta-commands
    (`\\restrict`) and PG17-only SET statements (`transaction_timeout`) that
    error on older Neon, AND it overrides the connection's search_path which
    breaks downstream unqualified INSERTs. Indexes/constraints/FKs are skipped
    on purpose — the /scrapers page only needs row counts and timestamps, not
    constraint enforcement on Neon.
    """
    with src_conn.cursor() as c:
        c.execute(
            """
            SELECT column_name, data_type, udt_name,
                   character_maximum_length, numeric_precision, numeric_scale,
                   is_nullable
              FROM information_schema.columns
             WHERE table_schema='public' AND table_name=%s
             ORDER BY ordinal_position
            """,
            (table,),
        )
        cols = c.fetchall()
    if not cols:
        raise RuntimeError(f"no columns found for {table} on source")

    parts = []
    for name, dtype, udt, charlen, precision, scale, nullable in cols:
        if dtype == "character varying":
            t = f"VARCHAR({charlen})" if charlen else "TEXT"
        elif dtype == "character":
            t = f"CHAR({charlen})" if charlen else "CHAR(1)"
        elif dtype == "numeric":
            t = f"NUMERIC({precision},{scale})" if precision else "NUMERIC"
        elif dtype == "ARRAY":
            t = f"{udt[1:].upper()}[]"  # udt_name like "_text" → TEXT[]
        elif dtype == "USER-DEFINED":
            t = udt.upper()
        else:
            t = dtype.upper()
        nn = "" if nullable == "YES" else " NOT NULL"
        parts.append(f'    "{name}" {t}{nn}')

    return f'CREATE TABLE public."{table}" (\n' + ",\n".join(parts) + "\n);"


def _row_count(conn, table: str) -> int | None:
    """Returns row count or None if the table doesn't exist. Rolls back on
    error so the connection stays usable for subsequent statements."""
    try:
        with conn.cursor() as c:
            c.execute(f'SELECT COUNT(*) FROM "{table}"')
            return c.fetchone()[0]
    except Exception:
        conn.rollback()
        return None


def _columns(conn, table: str) -> list[str]:
    with conn.cursor() as c:
        c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s "
            "ORDER BY ordinal_position",
            (table,),
        )
        return [r[0] for r in c.fetchall()]


def _adapt_row(row):
    out = []
    for v in row:
        if isinstance(v, (dict, list)):
            out.append(psycopg2.extras.Json(v))
        else:
            out.append(v)
    return tuple(out)


def _sync_one(table: str, src, dst, batch: int) -> tuple[int, int]:
    """Returns (rows_before_on_neon, rows_after_on_neon)."""
    n_before = _row_count(dst, table) or 0

    ddl = _build_ddl(src, table)

    # Recreate the target — drop first to wipe drifted columns/data cleanly.
    # All identifiers are schema-qualified ("public.<table>") to avoid any
    # search_path surprises on Neon.
    with dst.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
        c.execute(ddl)
    dst.commit()

    cols = _columns(src, table)
    if not cols:
        print(f"  ! {table}: no columns on source — table empty, leaving recreated.")
        return n_before, 0
    col_list = ", ".join(f'"{c}"' for c in cols)
    insert_sql = f'INSERT INTO public."{table}" ({col_list}) VALUES %s'
    select_sql = f'SELECT {col_list} FROM public."{table}"'

    n_done = 0
    with src.cursor(name=f"ro_{table}") as src_c:
        src_c.itersize = batch
        src_c.execute(select_sql)
        buf = []
        with dst.cursor() as dst_c:
            while True:
                row = src_c.fetchone()
                if row is None:
                    break
                buf.append(row)
                if len(buf) >= batch:
                    psycopg2.extras.execute_values(
                        dst_c, insert_sql, [_adapt_row(r) for r in buf],
                        page_size=batch,
                    )
                    n_done += len(buf); buf = []
            if buf:
                psycopg2.extras.execute_values(
                    dst_c, insert_sql, [_adapt_row(r) for r in buf],
                    page_size=batch,
                )
                n_done += len(buf)
    dst.commit()

    n_after = _row_count(dst, table) or 0
    return n_before, n_after


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Show local vs Neon counts without writing.")
    ap.add_argument("--tables", default=",".join(DEFAULT_TABLES),
                    help="Comma-separated table list (default: all probate tables).")
    ap.add_argument("--batch", type=int, default=2000)
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    illegal = [t for t in tables if t in FORBIDDEN]
    if illegal:
        sys.exit(f"Refusing to touch protected tables: {illegal}")

    print(f"Source : {LOCAL_DSN}")
    print(f"Target : {NEON_DSN[:60]}…")
    print(f"Tables : {', '.join(tables)}")
    print()

    src = psycopg2.connect(LOCAL_DSN, connect_timeout=10)
    src.set_session(readonly=True)
    dst = psycopg2.connect(NEON_DSN, connect_timeout=15)
    dst.autocommit = False

    try:
        if args.dry_run:
            print(f"{'table':25s}  {'local':>10}  {'neon':>10}")
            for t in tables:
                ln = _row_count(src, t)
                src.rollback()  # in case the table is missing
                nn = _row_count(dst, t)
                dst.rollback()
                ls = "MISSING" if ln is None else f"{ln:,}"
                ns = "MISSING" if nn is None else f"{nn:,}"
                print(f"{t:25s}  {ls:>10}  {ns:>10}")
            print()
            print("Run again without --dry-run to drop & recreate each table on Neon"
                  " and copy local rows.")
            return

        t0 = time.time()
        for t in tables:
            print(f"→ {t} …")
            tt = time.time()
            try:
                before, after = _sync_one(t, src, dst, args.batch)
                print(f"   neon: {before:>9,} → {after:>9,}   ({time.time()-tt:.1f}s)")
            except Exception as e:
                dst.rollback()
                print(f"   ERROR on {t}: {e}")
        print()
        print(f"All done in {time.time() - t0:.1f}s")
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
