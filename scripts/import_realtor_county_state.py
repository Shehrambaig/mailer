"""
Import Realtor county-level + state-level inventory + county-level hotness
into Neon Postgres. Sibling to import_realtor_zip.py.

Tables:
  - realtor_inventory_county    (3,109 rows, current snapshot)
  - realtor_inventory_state     (52 rows, current snapshot)
  - realtor_hotness_county      (~110k rows, last 36 months)

Idempotent — re-running upserts on the natural keys.
"""
import csv
import io
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

R = ROOT / "Mailer_Data_set" / "Realtor"
INV_COUNTY = R / "Monthly Housing Inventory" / "RDC_Inventory_Core_Metrics_County_History.csv"
INV_STATE  = R / "Monthly Housing Inventory" / "monthly_Inventory_Metrics_State.csv"
HOT_COUNTY = R / "Monthly Market Hotness"    / "Inventory_Hotness_Metrics_County_History.csv"

# 36-month window matching realtor_hotness_* tables.
HOT_MIN_YYYYMM = 202304
INV_MIN_YYYYMM = 202304
INV_MAX_YYYYMM = 202603


# ── DDL ──────────────────────────────────────────────────────────────────────
INV_COUNTY_DDL = """
CREATE TABLE IF NOT EXISTS realtor_inventory_county (
  month_date_yyyymm INTEGER NOT NULL,
  county_fips       VARCHAR(5) NOT NULL,
  county_name       TEXT,
  median_listing_price                  NUMERIC,
  median_listing_price_mm               NUMERIC,
  median_listing_price_yy               NUMERIC,
  active_listing_count                  NUMERIC,
  active_listing_count_mm               NUMERIC,
  active_listing_count_yy               NUMERIC,
  median_days_on_market                 NUMERIC,
  median_days_on_market_mm              NUMERIC,
  median_days_on_market_yy              NUMERIC,
  new_listing_count                     NUMERIC,
  new_listing_count_mm                  NUMERIC,
  new_listing_count_yy                  NUMERIC,
  price_increased_count                 NUMERIC,
  price_increased_count_mm              NUMERIC,
  price_increased_count_yy              NUMERIC,
  price_increased_share                 NUMERIC,
  price_increased_share_mm              NUMERIC,
  price_increased_share_yy              NUMERIC,
  price_reduced_count                   NUMERIC,
  price_reduced_count_mm                NUMERIC,
  price_reduced_count_yy                NUMERIC,
  price_reduced_share                   NUMERIC,
  price_reduced_share_mm                NUMERIC,
  price_reduced_share_yy                NUMERIC,
  pending_listing_count                 NUMERIC,
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
  total_listing_count                   NUMERIC,
  total_listing_count_mm                NUMERIC,
  total_listing_count_yy                NUMERIC,
  pending_ratio                         NUMERIC,
  pending_ratio_mm                      NUMERIC,
  pending_ratio_yy                      NUMERIC,
  quality_flag                          SMALLINT,
  PRIMARY KEY (month_date_yyyymm, county_fips)
);
CREATE INDEX IF NOT EXISTS realtor_inventory_county_fips_idx
  ON realtor_inventory_county(county_fips);
"""

INV_STATE_DDL = """
CREATE TABLE IF NOT EXISTS realtor_inventory_state (
  month_date_yyyymm INTEGER NOT NULL,
  state             TEXT,
  state_id          CHAR(2) NOT NULL,
  median_listing_price                  NUMERIC,
  median_listing_price_mm               NUMERIC,
  median_listing_price_yy               NUMERIC,
  active_listing_count                  NUMERIC,
  active_listing_count_mm               NUMERIC,
  active_listing_count_yy               NUMERIC,
  median_days_on_market                 NUMERIC,
  median_days_on_market_mm              NUMERIC,
  median_days_on_market_yy              NUMERIC,
  new_listing_count                     NUMERIC,
  new_listing_count_mm                  NUMERIC,
  new_listing_count_yy                  NUMERIC,
  price_increased_count                 NUMERIC,
  price_increased_count_mm              NUMERIC,
  price_increased_count_yy              NUMERIC,
  price_increased_share                 NUMERIC,
  price_increased_share_mm              NUMERIC,
  price_increased_share_yy              NUMERIC,
  price_reduced_count                   NUMERIC,
  price_reduced_count_mm                NUMERIC,
  price_reduced_count_yy                NUMERIC,
  price_reduced_share                   NUMERIC,
  price_reduced_share_mm                NUMERIC,
  price_reduced_share_yy                NUMERIC,
  pending_listing_count                 NUMERIC,
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
  total_listing_count                   NUMERIC,
  total_listing_count_mm                NUMERIC,
  total_listing_count_yy                NUMERIC,
  pending_ratio                         NUMERIC,
  pending_ratio_mm                      NUMERIC,
  pending_ratio_yy                      NUMERIC,
  quality_flag                          SMALLINT,
  PRIMARY KEY (month_date_yyyymm, state_id)
);
"""

HOT_COUNTY_DDL = """
CREATE TABLE IF NOT EXISTS realtor_hotness_county (
  month_date_yyyymm INTEGER NOT NULL,
  county_fips       VARCHAR(5) NOT NULL,
  county_name       TEXT,
  cbsa_code         VARCHAR(10),
  cbsa_title        TEXT,
  hh_rank                            NUMERIC,
  hotness_rank                       NUMERIC,
  hotness_rank_mm                    NUMERIC,
  hotness_rank_yy                    NUMERIC,
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
  PRIMARY KEY (month_date_yyyymm, county_fips)
);
CREATE INDEX IF NOT EXISTS realtor_hotness_county_fips_idx
  ON realtor_hotness_county(county_fips);
CREATE INDEX IF NOT EXISTS realtor_hotness_county_month_idx
  ON realtor_hotness_county(month_date_yyyymm);
"""


def _coerce_int(v):
    if v is None: return None
    s = str(v).strip()
    if s == "": return None
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _read_csv(path, columns, *, key_xform=None, integer_columns=(), month_filter=None, month_max=None, batch_size=50_000):
    """Stream CSV, normalize values, yield batches of dicts.
    `key_xform`: optional dict {col: callable(val)} for fields like fips-zfill."""
    int_set = set(integer_columns)
    key_xform = key_xform or {}
    batch = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            try:
                month = int(raw["month_date_yyyymm"])
            except (KeyError, TypeError, ValueError):
                continue
            if month_filter is not None and month < month_filter:
                continue
            if month_max is not None and month > month_max:
                continue
            row = {"month_date_yyyymm": month}
            for col in columns:
                if col == "month_date_yyyymm":
                    continue
                v = raw.get(col, "")
                if v is None:
                    row[col] = None
                    continue
                v = v.strip()
                if v == "":
                    row[col] = None
                elif col in key_xform:
                    row[col] = key_xform[col](v)
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


def _emit_csv(rows, columns):
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        w.writerow([("" if r.get(c) is None else r[c]) for c in columns])
    return buf.getvalue().encode("utf-8")


def _upsert(conn, table, columns, key_cols, rows):
    if not rows:
        return 0
    tmp = f"_stg_{table}"
    cols_sql = ", ".join(columns)
    update_cols = [c for c in columns if c not in key_cols]
    set_sql = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
    keys_sql = ", ".join(key_cols)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        copy_sql = f"COPY {tmp} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL '')"
        cur.copy_expert(copy_sql, io.BytesIO(_emit_csv(rows, columns)))
        cur.execute(
            f"""
            INSERT INTO {table} ({cols_sql})
            SELECT DISTINCT ON ({keys_sql}) {cols_sql}
              FROM {tmp}
              ORDER BY {keys_sql}
            ON CONFLICT ({keys_sql}) DO UPDATE SET {set_sql}
            """
        )


# Column lists (ordered the way I want them inserted)
INV_COUNTY_COLS = [
    "month_date_yyyymm","county_fips","county_name",
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

INV_STATE_COLS = [c if c != "county_fips" else "state_id" for c in INV_COUNTY_COLS]
INV_STATE_COLS = [c if c != "county_name" else "state" for c in INV_STATE_COLS]
# state file actually has [state, state_id] but our DDL has both.
INV_STATE_COLS = [
    "month_date_yyyymm","state","state_id",
    *[c for c in INV_COUNTY_COLS if c not in ("month_date_yyyymm","county_fips","county_name")],
]

HOT_COUNTY_COLS = [
    "month_date_yyyymm","county_fips","county_name","cbsa_code","cbsa_title",
    "hh_rank","hotness_rank","hotness_rank_mm","hotness_rank_yy",
    "hotness_score","supply_score","demand_score",
    "median_days_on_market","median_days_on_market_mm","median_dom_mm_day",
    "median_days_on_market_yy","median_dom_yy_day","median_dom_vs_us",
    "page_view_count_per_property_mm","page_view_count_per_property_yy","page_view_count_per_property_vs_us",
    "median_listing_price","median_listing_price_mm","median_listing_price_yy","median_listing_price_vs_us",
    "quality_flag",
]


def main():
    print("Connecting to Neon...")
    conn = psycopg2.connect(NEON_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(INV_COUNTY_DDL)
            cur.execute(INV_STATE_DDL)
            cur.execute(HOT_COUNTY_DDL)
        conn.commit()
        print("Tables ready.")

        # ── County inventory (last 36 months) ───────────────────────────────
        print(f"Importing {INV_COUNTY.name} (months {INV_MIN_YYYYMM}–{INV_MAX_YYYYMM}) ...")
        inv_int_cols = (
            "active_listing_count","new_listing_count","price_increased_count",
            "price_reduced_count","pending_listing_count","total_listing_count",
            "quality_flag",
        )
        total = 0
        for batch in _read_csv(
            INV_COUNTY, INV_COUNTY_COLS,
            key_xform={"county_fips": lambda v: v.zfill(5)},
            integer_columns=inv_int_cols,
            month_filter=INV_MIN_YYYYMM,
            month_max=INV_MAX_YYYYMM,
        ):
            _upsert(conn, "realtor_inventory_county", INV_COUNTY_COLS,
                    ["month_date_yyyymm","county_fips"], batch)
            conn.commit()
            total += len(batch)
            print(f"  county inventory: upserted batch ({len(batch)} rows), running total {total}")
        print(f"  county inventory: {total} rows.")

        # ── State inventory ─────────────────────────────────────────────────
        print(f"Importing {INV_STATE.name} ...")
        total = 0
        for batch in _read_csv(
            INV_STATE, INV_STATE_COLS,
            key_xform={"state_id": lambda v: v.upper()[:2]},
            integer_columns=inv_int_cols,
        ):
            _upsert(conn, "realtor_inventory_state", INV_STATE_COLS,
                    ["month_date_yyyymm","state_id"], batch)
            conn.commit()
            total += len(batch)
        print(f"  state inventory: {total} rows.")

        # ── County hotness (last 36 months) ─────────────────────────────────
        print(f"Importing {HOT_COUNTY.name} (months >= {HOT_MIN_YYYYMM}) ...")
        hot_int_cols = ("hh_rank","hotness_rank","hotness_rank_mm","hotness_rank_yy","quality_flag")
        total = 0
        for batch in _read_csv(
            HOT_COUNTY, HOT_COUNTY_COLS,
            key_xform={"county_fips": lambda v: v.zfill(5)},
            integer_columns=hot_int_cols,
            month_filter=HOT_MIN_YYYYMM,
        ):
            _upsert(conn, "realtor_hotness_county", HOT_COUNTY_COLS,
                    ["month_date_yyyymm","county_fips"], batch)
            conn.commit()
            total += len(batch)
            print(f"  hotness county: running total {total}")
        print(f"  county hotness: {total} rows.")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(month_date_yyyymm), MAX(month_date_yyyymm) FROM realtor_inventory_county")
            print("realtor_inventory_county:", cur.fetchone())
            cur.execute("SELECT COUNT(*), MIN(month_date_yyyymm), MAX(month_date_yyyymm) FROM realtor_inventory_state")
            print("realtor_inventory_state:", cur.fetchone())
            cur.execute("SELECT COUNT(*), MIN(month_date_yyyymm), MAX(month_date_yyyymm), COUNT(DISTINCT month_date_yyyymm) FROM realtor_hotness_county")
            print("realtor_hotness_county:", cur.fetchone())
    finally:
        conn.close()


if __name__ == "__main__":
    main()
