"""
Auto-refresh the Realtor inventory + hotness data that backs /explore.

Designed to run daily (GitHub Action, days 2–31 of each month). Cheaply
HEAD-checks Realtor's S3 ETags against the last successful import; only
downloads + imports when a CSV has changed. Idempotent — when there is
no new data the script exits 0 with no DB writes.

Logs every run (no-op or import) to `realtor_refresh_log` so the
/explore page can render a "last updated" badge and so we can audit
what the cron has been doing.

Usage:
  python scripts/auto_refresh_realtor.py            # full check + refresh if needed
  python scripts/auto_refresh_realtor.py --dry-run  # HEAD-check only, no writes
  python scripts/auto_refresh_realtor.py --force    # refresh even if ETags match
"""
import argparse
import csv
import os
import shutil
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DSN = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not DSN:
    sys.exit("NEON_DB not set")

REALTOR_DIR = ROOT / "Mailer_Data_set" / "Realtor"
INV_DIR = REALTOR_DIR / "Monthly Housing Inventory"
HOT_DIR = REALTOR_DIR / "Monthly Market Hotness"

# (key, url, target_path)  — same names the importers already expect.
SOURCES = [
    ("county_inv",
     "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv",
     INV_DIR / "RDC_Inventory_Core_Metrics_County_History.csv"),
    ("zip_inv",
     "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv",
     INV_DIR / "RDC_Inventory_Core_Metrics_Zip_History.csv"),
    ("county_hot",
     "https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_County_History.csv",
     HOT_DIR / "Inventory_Hotness_Metrics_County_History.csv"),
    ("zip_hot",
     # NOTE: the /Reports/Core/ copy of this file went stale at 202602 — the
     # canonical, still-updated location is /Reports/Hotness/.
     "https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Zip_History.csv",
     HOT_DIR / "Inventory_Hotness_Metrics_Zip_History.csv"),
]

LOG_DDL = """
CREATE TABLE IF NOT EXISTS realtor_refresh_log (
  id              SERIAL PRIMARY KEY,
  ran_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status          TEXT NOT NULL,
  source_month    INTEGER,
  rows_county_inv INTEGER,
  rows_zip_inv    INTEGER,
  rows_county_hot INTEGER,
  rows_zip_hot    INTEGER,
  etag_county_inv TEXT,
  etag_zip_inv    TEXT,
  etag_county_hot TEXT,
  etag_zip_hot    TEXT,
  notes           TEXT
);
CREATE INDEX IF NOT EXISTS realtor_refresh_log_ran_at_idx
  ON realtor_refresh_log(ran_at DESC);
"""


def head_etag(url):
    """HEAD request → return ETag (without surrounding quotes) or None."""
    req = urllib.request.Request(url, method="HEAD",
                                 headers={"User-Agent": "Mozilla/5.0 mailer-auto-refresh"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            etag = r.headers.get("ETag", "")
            return etag.strip('"') or None
    except urllib.error.HTTPError as e:
        print(f"  HEAD failed for {url}: {e}", file=sys.stderr)
        return None


def last_successful_import(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT etag_county_inv, etag_zip_inv, etag_county_hot, etag_zip_hot, ran_at "
            "FROM realtor_refresh_log "
            "WHERE status = 'imported' "
            "ORDER BY ran_at DESC LIMIT 1"
        )
        return cur.fetchone()


def db_max_month(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(month_date_yyyymm) FROM realtor_inventory_county")
        return cur.fetchone()[0]


def download(url, dest):
    """Stream URL → dest.part → atomic rename."""
    dest.parent.mkdir(parents=True, exist_ok=True)   # CI checkout lacks the gitignored data dirs
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 mailer-auto-refresh"})
    with urllib.request.urlopen(req, timeout=600) as r:
        with open(tmp, "wb") as f:
            shutil.copyfileobj(r, f, length=1024 * 1024)
    os.replace(tmp, dest)
    return dest.stat().st_size


def csv_max_month(path):
    """Stream CSV; return MAX(month_date_yyyymm)."""
    mx = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                m = int(row["month_date_yyyymm"])
            except (KeyError, ValueError, TypeError):
                continue
            if m > mx:
                mx = m
    return mx


def run_importer(script_name, max_month):
    """Run an importer with REALTOR_INV_MAX_YYYYMM set to the CSV's max month."""
    print(f"\n--- running {script_name} (REALTOR_INV_MAX_YYYYMM={max_month}) ---")
    env = {**os.environ, "REALTOR_INV_MAX_YYYYMM": str(max_month)}
    rc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script_name)],
        cwd=str(ROOT), env=env, check=False,
    ).returncode
    if rc != 0:
        raise RuntimeError(f"{script_name} exited with code {rc}")


def table_rowcounts(conn):
    out = {}
    with conn.cursor() as cur:
        for col, table in [
            ("rows_county_inv", "realtor_inventory_county"),
            ("rows_zip_inv",    "realtor_inventory_zip"),
            ("rows_county_hot", "realtor_hotness_county"),
            ("rows_zip_hot",    "realtor_hotness_zip"),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            out[col] = cur.fetchone()[0]
    return out


def log_run(conn, status, etags, rowcounts=None, source_month=None, notes=None):
    cols = ["status", "etag_county_inv", "etag_zip_inv",
            "etag_county_hot", "etag_zip_hot", "source_month", "notes"]
    vals = [status, etags.get("county_inv"), etags.get("zip_inv"),
            etags.get("county_hot"), etags.get("zip_hot"),
            source_month, notes]
    if rowcounts:
        for k in ("rows_county_inv", "rows_zip_inv", "rows_county_hot", "rows_zip_hot"):
            cols.append(k); vals.append(rowcounts.get(k))
    placeholders = ", ".join(["%s"] * len(vals))
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO realtor_refresh_log ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="HEAD-check only; print decision and exit 0.")
    ap.add_argument("--force", action="store_true",
                    help="Refresh even if ETags match the last successful import.")
    args = ap.parse_args()

    print(f"[{datetime.now().isoformat(timespec='seconds')}] auto-refresh-realtor starting "
          f"(dry-run={args.dry_run}, force={args.force})")
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_DDL)
        conn.commit()

        # ── ETag check ────────────────────────────────────────────────
        etags = {key: head_etag(url) for key, url, _ in SOURCES}
        print("  current S3 ETags:")
        for k, v in etags.items():
            print(f"    {k:11s} {v}")

        last = last_successful_import(conn)
        if last:
            last_etags = {
                "county_inv": last[0], "zip_inv": last[1],
                "county_hot": last[2], "zip_hot": last[3],
            }
            unchanged = all(etags[k] and etags[k] == last_etags[k] for k in etags)
        else:
            unchanged = False
            last_etags = {}

        print(f"  db_max_month = {db_max_month(conn)}")
        print(f"  unchanged_since_last_import = {unchanged}")

        if unchanged and not args.force:
            log_run(conn, status="no_change", etags=etags,
                    notes="ETags match the last successful import")
            print("→ NO-OP: ETags unchanged. Nothing to do.")
            return 0

        if args.dry_run:
            print("→ DRY RUN: would download + import (changes detected).")
            return 0

        # ── Download all 4 ────────────────────────────────────────────
        print("\nDownloading 4 CSVs ...")
        for key, url, target in SOURCES:
            print(f"  {key}: {url}")
            size = download(url, target)
            print(f"    → {target}  ({size:,} bytes)")

        source_month = csv_max_month(INV_DIR / "RDC_Inventory_Core_Metrics_County_History.csv")
        print(f"\n  CSV max month = {source_month}")

        # ── Import (env var lifts the cap so the new month is accepted) ──
        run_importer("import_realtor_zip.py", source_month)
        run_importer("import_realtor_county_state.py", source_month)

        rowcounts = table_rowcounts(conn)
        print(f"\n  final row counts: {rowcounts}")

        log_run(conn, status="imported", etags=etags,
                rowcounts=rowcounts, source_month=source_month,
                notes=f"db_max_month after import = {db_max_month(conn)}")
        print("\n→ IMPORTED. /explore now reflects the new month.")
        return 0
    except Exception as e:
        try:
            log_run(conn, status="error", etags={}, notes=str(e)[:500])
        except Exception:
            pass
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
