"""
Import Realtor monthly inventory + hotness ZIP CSVs into Neon Postgres.

Tables:
  - realtor_inventory_zip   (single-snapshot file, current month)
  - realtor_hotness_zip     (history file; we keep only last 36 months)

Idempotent: re-running upserts on (month_date_yyyymm, postal_code).
"""
import csv
import io
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

NEON_DB = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
if not NEON_DB:
    sys.exit("NEON_DB not set")

ROOT = Path(__file__).resolve().parent.parent / "Mailer_Data_set" / "Realtor"
INV_PATH = ROOT / "Monthly Housing Inventory" / "RDC_Inventory_Core_Metrics_Zip_History.csv"
HOT_PATH = ROOT / "Monthly Market Hotness" / "Inventory_Hotness_Metrics_Zip_History.csv"

# 36-month window (3 years) — keep inventory + hotness aligned.
HOTNESS_MIN_YYYYMM = 202304
INV_MIN_YYYYMM = 202304
# Defaults to 202604 (April 2026). The auto-refresh sets
# REALTOR_INV_MAX_YYYYMM dynamically from the CSV's max month so new
# publication months are accepted without needing a code bump.
INV_MAX_YYYYMM = int(os.getenv("REALTOR_INV_MAX_YYYYMM", "202604"))


INV_DDL = """
CREATE TABLE IF NOT EXISTS realtor_inventory_zip (
  month_date_yyyymm INTEGER NOT NULL,
  postal_code       VARCHAR(5) NOT NULL,
  zip_name          TEXT,
  median_listing_price                  NUMERIC,
  median_listing_price_mm               NUMERIC,
  median_listing_price_yy               NUMERIC,
  active_listing_count                  INTEGER,
  active_listing_count_mm               NUMERIC,
  active_listing_count_yy               NUMERIC,
  median_days_on_market                 NUMERIC,
  median_days_on_market_mm              NUMERIC,
  median_days_on_market_yy              NUMERIC,
  new_listing_count                     INTEGER,
  new_listing_count_mm                  NUMERIC,
  new_listing_count_yy                  NUMERIC,
  price_increased_count                 INTEGER,
  price_increased_count_mm              NUMERIC,
  price_increased_count_yy              NUMERIC,
  price_increased_share                 NUMERIC,
  price_increased_share_mm              NUMERIC,
  price_increased_share_yy              NUMERIC,
  price_reduced_count                   INTEGER,
  price_reduced_count_mm                NUMERIC,
  price_reduced_count_yy                NUMERIC,
  price_reduced_share                   NUMERIC,
  price_reduced_share_mm                NUMERIC,
  price_reduced_share_yy                NUMERIC,
  pending_listing_count                 INTEGER,
  pending_listing_count_mm              NUMERIC,
  pending_listing_count_yy              NUMERIC,
  median_listing_price_per_square_foot     NUMERIC,
  median_listing_price_per_square_foot_mm  NUMERIC,
  median_listing_price_per_square_foot_yy  NUMERIC,
  median_square_feet                    NUMERIC,
  median_square_feet_mm                 NUMERIC,
  median_square_feet_yy                 NUMERIC,
  average_listing_price                 NUMERIC,
  average_listing_price_mm              NUMERIC,
  average_listing_price_yy              NUMERIC,
  total_listing_count                   INTEGER,
  total_listing_count_mm                NUMERIC,
  total_listing_count_yy                NUMERIC,
  pending_ratio                         NUMERIC,
  pending_ratio_mm                      NUMERIC,
  pending_ratio_yy                      NUMERIC,
  quality_flag                          SMALLINT,
  PRIMARY KEY (month_date_yyyymm, postal_code)
);
CREATE INDEX IF NOT EXISTS realtor_inventory_zip_postal_idx
  ON realtor_inventory_zip(postal_code);
"""

HOT_DDL = """
CREATE TABLE IF NOT EXISTS realtor_hotness_zip (
  month_date_yyyymm INTEGER NOT NULL,
  postal_code       VARCHAR(5) NOT NULL,
  zip_name          TEXT,
  hh_rank                            INTEGER,
  hotness_rank                       INTEGER,
  hotness_rank_mm                    INTEGER,
  hotness_rank_yy                    INTEGER,
  hotness_score                      NUMERIC,
  supply_score                       NUMERIC,
  demand_score                       NUMERIC,
  median_days_on_market              NUMERIC,
  median_days_on_market_mm           NUMERIC,
  median_dom_mm_day                  NUMERIC,
  median_days_on_market_yy           NUMERIC,
  median_dom_yy_day                  NUMERIC,
  median_dom_vs_us                   NUMERIC,
  page_view_count_per_property_mm    NUMERIC,
  page_view_count_per_property_yy    NUMERIC,
  page_view_count_per_property_vs_us NUMERIC,
  median_listing_price               NUMERIC,
  median_listing_price_mm            NUMERIC,
  median_listing_price_yy            NUMERIC,
  median_listing_price_vs_us         NUMERIC,
  quality_flag                       SMALLINT,
  PRIMARY KEY (month_date_yyyymm, postal_code)
);
CREATE INDEX IF NOT EXISTS realtor_hotness_zip_postal_idx
  ON realtor_hotness_zip(postal_code);
CREATE INDEX IF NOT EXISTS realtor_hotness_zip_month_idx
  ON realtor_hotness_zip(month_date_yyyymm);
"""


def _norm_zip(z):
    z = (z or "").strip()
    if not z:
        return None
    if z.isdigit() and len(z) <= 5:
        return z.zfill(5)
    return z[:5]


def _emit_csv(rows, columns):
    """Yield CSV bytes suitable for COPY ... FROM STDIN with empty=NULL."""
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        w.writerow([("" if r.get(c) is None else r[c]) for c in columns])
    return buf.getvalue().encode("utf-8")


def _coerce_int(v):
    """Coerce values like '72', '72.000', '72.0' to '72'. Return None for empties."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s  # let Postgres error if truly bad


def _read_csv(path, columns, *, integer_columns=(), month_filter=None, month_max=None, batch_size=50_000):
    """Stream CSV, normalize, yield batches of dicts."""
    int_set = set(integer_columns)
    batch = []
    dropped_above_cap = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {}
            try:
                month = int(raw["month_date_yyyymm"])
            except (KeyError, TypeError, ValueError):
                continue
            if month_filter is not None and month < month_filter:
                continue
            if month_max is not None and month > month_max:
                dropped_above_cap[month] = dropped_above_cap.get(month, 0) + 1
                continue
            row["month_date_yyyymm"] = month
            row["postal_code"] = _norm_zip(raw.get("postal_code"))
            if not row["postal_code"]:
                continue
            for col in columns:
                if col in ("month_date_yyyymm", "postal_code"):
                    continue
                v = raw.get(col, "")
                if v is None:
                    row[col] = None
                    continue
                v = v.strip()
                if v == "":
                    row[col] = None
                elif col in int_set:
                    row[col] = _coerce_int(v)
                else:
                    row[col] = v
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch
    if dropped_above_cap:
        total = sum(dropped_above_cap.values())
        months = ", ".join(f"{m}={dropped_above_cap[m]}" for m in sorted(dropped_above_cap))
        print(
            f"\n  !! WARNING: {path.name} contains {total} rows in months ABOVE the cap "
            f"month_max={month_max} — DROPPED. Breakdown: {months}.\n"
            f"  !! Bump INV_MAX_YYYYMM in this script to include them, then re-run.\n",
            file=sys.stderr,
        )


def _upsert(conn, table, columns, rows):
    """Stage with COPY into TEMP table, then INSERT ... ON CONFLICT DO UPDATE."""
    if not rows:
        return 0
    tmp = f"_stg_{table}"
    cols_sql = ", ".join(columns)
    update_cols = [c for c in columns if c not in ("month_date_yyyymm", "postal_code")]
    set_sql = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        copy_sql = (
            f"COPY {tmp} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL '')"
        )
        cur.copy_expert(copy_sql, io.BytesIO(_emit_csv(rows, columns)))
        cur.execute(
            f"""
            INSERT INTO {table} ({cols_sql})
            SELECT DISTINCT ON (month_date_yyyymm, postal_code) {cols_sql}
              FROM {tmp}
              ORDER BY month_date_yyyymm, postal_code
            ON CONFLICT (month_date_yyyymm, postal_code)
            DO UPDATE SET {set_sql}
            """
        )
        n = cur.rowcount
    return n


INV_COLUMNS = [
    "month_date_yyyymm","postal_code","zip_name",
    "median_listing_price","median_listing_price_mm","median_listing_price_yy",
    "active_listing_count","active_listing_count_mm","active_listing_count_yy",
    "median_days_on_market","median_days_on_market_mm","median_days_on_market_yy",
    "new_listing_count","new_listing_count_mm","new_listing_count_yy",
    "price_increased_count","price_increased_count_mm","price_increased_count_yy",
    "price_increased_share","price_increased_share_mm","price_increased_share_yy",
    "price_reduced_count","price_reduced_count_mm","price_reduced_count_yy",
    "price_reduced_share","price_reduced_share_mm","price_reduced_share_yy",
    "pending_listing_count","pending_listing_count_mm","pending_listing_count_yy",
    "median_listing_price_per_square_foot",
    "median_listing_price_per_square_foot_mm",
    "median_listing_price_per_square_foot_yy",
    "median_square_feet","median_square_feet_mm","median_square_feet_yy",
    "average_listing_price","average_listing_price_mm","average_listing_price_yy",
    "total_listing_count","total_listing_count_mm","total_listing_count_yy",
    "pending_ratio","pending_ratio_mm","pending_ratio_yy",
    "quality_flag",
]

HOT_COLUMNS = [
    "month_date_yyyymm","postal_code","zip_name",
    "hh_rank","hotness_rank","hotness_rank_mm","hotness_rank_yy",
    "hotness_score","supply_score","demand_score",
    "median_days_on_market","median_days_on_market_mm","median_dom_mm_day",
    "median_days_on_market_yy","median_dom_yy_day","median_dom_vs_us",
    "page_view_count_per_property_mm","page_view_count_per_property_yy","page_view_count_per_property_vs_us",
    "median_listing_price","median_listing_price_mm","median_listing_price_yy","median_listing_price_vs_us",
    "quality_flag",
]


def main():
    print(f"Connecting to Neon...")
    conn = psycopg2.connect(NEON_DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(INV_DDL)
            cur.execute(HOT_DDL)
        conn.commit()
        print("Tables ready.")

        # Inventory
        inv_int_cols = (
            "active_listing_count", "new_listing_count", "price_increased_count",
            "price_reduced_count", "pending_listing_count", "total_listing_count",
            "quality_flag",
        )
        print(f"Importing {INV_PATH.name} (months {INV_MIN_YYYYMM}–{INV_MAX_YYYYMM}) ...")
        total = 0
        for batch in _read_csv(INV_PATH, INV_COLUMNS,
                               integer_columns=inv_int_cols,
                               month_filter=INV_MIN_YYYYMM,
                               month_max=INV_MAX_YYYYMM):
            _upsert(conn, "realtor_inventory_zip", INV_COLUMNS, batch)
            conn.commit()
            total += len(batch)
            print(f"  inventory: upserted batch ({len(batch)} rows), running total {total}")
        print(f"Inventory done: {total} rows.")

        # Hotness (last 36 months only)
        hot_int_cols = (
            "hh_rank", "hotness_rank", "hotness_rank_mm", "hotness_rank_yy",
            "quality_flag",
        )
        print(f"Importing {HOT_PATH.name} (months >= {HOTNESS_MIN_YYYYMM}) ...")
        total = 0
        for batch in _read_csv(HOT_PATH, HOT_COLUMNS,
                               integer_columns=hot_int_cols,
                               month_filter=HOTNESS_MIN_YYYYMM):
            _upsert(conn, "realtor_hotness_zip", HOT_COLUMNS, batch)
            conn.commit()
            total += len(batch)
            print(f"  hotness: upserted batch ({len(batch)} rows), running total {total}")
        print(f"Hotness done: {total} rows.")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM realtor_inventory_zip")
            inv_n = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM realtor_hotness_zip")
            hot_n = cur.fetchone()[0]
            cur.execute(
                "SELECT MIN(month_date_yyyymm), MAX(month_date_yyyymm) FROM realtor_hotness_zip"
            )
            mn, mx = cur.fetchone()
        print(f"\nFinal: realtor_inventory_zip={inv_n}, realtor_hotness_zip={hot_n} (months {mn}–{mx})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
