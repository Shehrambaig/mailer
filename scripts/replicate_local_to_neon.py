"""
Replicate non-foreclosure tables from local Postgres into Neon.

For each local table (except `foreclosure_records`):
  - If Neon doesn't have the table  → CREATE (via pg_dump schema-only) + COPY data.
  - If Neon has it with a *matching* column set (or a strict superset of
    local) → TRUNCATE + COPY. Neon-only extra columns stay (the COPY only
    targets local's columns, so extras keep their existing values, which
    after TRUNCATE means NULL — see --keep-extras flag below).
  - If Neon has FEWER columns than local                  → SKIP (schema drift).
  - Tables in NEON_ONLY are never touched.

Usage:
  python scripts/replicate_local_to_neon.py --dry-run
  python scripts/replicate_local_to_neon.py --apply

Sources & policy:
  --skip foreclosure_records   (default — has Neon-only columns county_fips,
                                first_seen_at we want to keep)
  --keep-extras                 (default ON — when truncating Neon and copying
                                local data, target only the columns local has,
                                so Neon's extra columns become NULL on the new
                                rows. WARNING: this is the price of a clean
                                replication.)

Logs per-table action + row counts. No silent failures.
"""
import argparse
import io
import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

LOCAL_DSN = os.getenv("POSTGRES_URI") or "postgresql://shehrambaig@localhost:5432/probate"
NEON_DSN  = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DSN:
    sys.exit("NEON_DB not set")

# Tables we never touch (managed by Neon-side import scripts / app code).
NEON_ONLY_PREFIXES = (
    "realtor_",
    "foreclosure_counts_",
    "chat_",
)
NEON_ONLY = {
    "app_settings",
    "zip_county_map",
}

# Tables we explicitly skip in the local→Neon direction.
DEFAULT_SKIP = {"foreclosure_records"}


def list_columns(conn, table):
    with conn.cursor() as c:
        c.execute("""
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema='public' AND table_name=%s
          ORDER BY ordinal_position
        """, (table,))
        return c.fetchall()


def list_tables(conn):
    with conn.cursor() as c:
        c.execute("""
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema='public' AND table_type='BASE TABLE'
          ORDER BY table_name
        """)
        return [r[0] for r in c.fetchall()]


def row_count(conn, table):
    with conn.cursor() as c:
        c.execute(f'SELECT COUNT(*) FROM "{table}"')
        return c.fetchone()[0]


def pg_dump_schema(table):
    """Return the CREATE TABLE DDL for a single local table, no owners/ACL.
    Uses pg_dump in --schema-only mode."""
    if not shutil.which("pg_dump"):
        sys.exit("pg_dump not found in PATH — install postgresql client tools.")
    res = subprocess.run(
        ["pg_dump", "--no-owner", "--no-acl", "--schema-only",
         "--table", f"public.{table}", LOCAL_DSN],
        capture_output=True, text=True, check=True,
    )
    return res.stdout


def replicate_one(local_conn, neon_conn, table, *, apply: bool, log):
    local_cols = [c[0] for c in list_columns(local_conn, table)]
    if not local_cols:
        log(f"  [SKIP] {table}: not present on local (race?)")
        return "skip"

    neon_cols = [c[0] for c in list_columns(neon_conn, table)]
    n_local = row_count(local_conn, table)

    if not neon_cols:
        # Brand-new on Neon.
        log(f"  [CREATE] {table}: not on Neon — will dump schema + COPY {n_local:,} rows")
        if not apply:
            return "create"
        ddl = pg_dump_schema(table)
        with neon_conn.cursor() as c:
            c.execute(ddl)
        neon_conn.commit()
        _copy_data(local_conn, neon_conn, table, local_cols, n_local, log)
        return "created"

    # Both sides have it — check for schema drift.
    missing_on_neon = [c for c in local_cols if c not in neon_cols]
    extra_on_neon   = [c for c in neon_cols if c not in local_cols]

    if missing_on_neon:
        log(f"  [SKIP-DRIFT] {table}: local has columns Neon doesn't — {missing_on_neon}. "
            f"Manual review needed.")
        return "skip-drift"

    n_neon = row_count(neon_conn, table)
    extras_note = f" (Neon has extras: {extra_on_neon})" if extra_on_neon else ""
    log(f"  [REPLACE] {table}: TRUNCATE Neon ({n_neon:,}) + COPY local ({n_local:,}){extras_note}")
    if not apply:
        return "replace"

    with neon_conn.cursor() as c:
        c.execute(f'TRUNCATE TABLE "{table}"')
    neon_conn.commit()
    _copy_data(local_conn, neon_conn, table, local_cols, n_local, log)
    return "replaced"


def _copy_data(local_conn, neon_conn, table, cols, n, log):
    if n == 0:
        log(f"      (0 rows on local — skipping COPY)")
        return
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    buf = io.BytesIO()
    with local_conn.cursor() as lc:
        lc.copy_expert(f'COPY (SELECT {cols_sql} FROM "{table}") TO STDOUT', buf)
    buf.seek(0)
    with neon_conn.cursor() as nc:
        nc.copy_expert(f'COPY "{table}" ({cols_sql}) FROM STDIN', buf)
    neon_conn.commit()
    log(f"      copied {n:,} rows")


def _is_neon_only(name):
    if name in NEON_ONLY:
        return True
    return any(name.startswith(p) for p in NEON_ONLY_PREFIXES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Plan only, no writes.")
    ap.add_argument("--apply",   action="store_true", help="Actually execute the plan.")
    ap.add_argument("--skip", action="append", default=list(DEFAULT_SKIP),
                    help="Tables to skip (repeatable). Default: foreclosure_records.")
    ap.add_argument("--only", action="append", default=[],
                    help="Restrict to these tables (repeatable).")
    args = ap.parse_args()

    if not args.dry_run and not args.apply:
        sys.exit("Pass --dry-run or --apply.")

    print(f"Source: {LOCAL_DSN}")
    print(f"Target: {NEON_DSN[:60]}…")
    print(f"Mode  : {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Skip  : {sorted(set(args.skip))}")
    if args.only:
        print(f"Only  : {sorted(set(args.only))}")
    print()

    local = psycopg2.connect(LOCAL_DSN)
    local.set_session(readonly=True)
    neon  = psycopg2.connect(NEON_DSN)

    try:
        local_tables = list_tables(local)
        if args.only:
            local_tables = [t for t in local_tables if t in set(args.only)]

        actions = {}
        skipped_neon_only = []

        for t in local_tables:
            if t in set(args.skip):
                actions[t] = "skip-explicit"
                continue
            if _is_neon_only(t):
                # Local somehow has a Neon-only-named table; still skip per policy.
                skipped_neon_only.append(t)
                actions[t] = "skip-neon-only"
                continue
            try:
                actions[t] = replicate_one(local, neon, t,
                                            apply=args.apply, log=print)
            except Exception as e:
                actions[t] = f"error: {e}"
                print(f"  [ERROR] {t}: {e}")
                neon.rollback()

        print()
        print("─" * 60)
        print("Summary:")
        for t, a in actions.items():
            print(f"  {t:<32} {a}")
        if not args.apply:
            print()
            print("Dry-run only. Re-run with --apply to execute.")
    finally:
        local.close()
        neon.close()


if __name__ == "__main__":
    main()
