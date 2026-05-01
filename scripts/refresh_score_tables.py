"""
Refresh per-county and per-ZIP score lookup tables in Neon.

The Buy / Exit / Golden scores are computed by build_realtor_only.py and
baked into county-heatmap.json + zip-heatmap.json. This script copies the
g/bs/es values out of those JSONs and into Postgres tables so that
/api/explore can JOIN them as columns.

Tables (drop + recreate idempotently):
  realtor_scores_county (county_fips, buy_score, exit_score, golden_score, hotness_score)
  realtor_scores_zip    (postal_code, buy_score, exit_score, golden_score, hotness_score)

Run after each build_realtor_only.py to keep scores in sync.
"""
import io
import json
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

COUNTY_HEAT = ROOT / "app" / "static" / "data" / "county-heatmap.json"
ZIP_HEAT    = ROOT / "app" / "static" / "data" / "zip-heatmap.json"


DDL = """
DROP TABLE IF EXISTS realtor_scores_county;
CREATE TABLE realtor_scores_county (
  county_fips    VARCHAR(5) PRIMARY KEY,
  buy_score      SMALLINT,
  exit_score     SMALLINT,
  golden_score   SMALLINT,
  hotness_score  SMALLINT
);

DROP TABLE IF EXISTS realtor_scores_zip;
CREATE TABLE realtor_scores_zip (
  postal_code    VARCHAR(5) PRIMARY KEY,
  buy_score      SMALLINT,
  exit_score     SMALLINT,
  golden_score   SMALLINT,
  hotness_score  SMALLINT
);
"""


def _copy_rows(conn, table, key_col, json_path):
    print(f"Loading {json_path.name} ...")
    data = json.loads(json_path.read_text())
    rows = []
    for k, v in data.items():
        bs = v.get("bs"); es = v.get("es"); g = v.get("g"); hs = v.get("hs")
        if bs is None and es is None and g is None and hs is None:
            continue
        rows.append((k, bs, es, g, hs))
    print(f"  {len(rows):,} rows to insert into {table}")

    buf = io.StringIO()
    for k, bs, es, g, hs in rows:
        buf.write("\t".join([
            k,
            "" if bs is None else str(int(bs)),
            "" if es is None else str(int(es)),
            "" if g  is None else str(int(g)),
            "" if hs is None else str(int(hs)),
        ]) + "\n")
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({key_col}, buy_score, exit_score, golden_score, hotness_score) "
            f"FROM STDIN WITH (FORMAT text, NULL '')",
            buf,
        )


def main():
    print("Connecting to Neon...")
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        _copy_rows(conn, "realtor_scores_county", "county_fips",  COUNTY_HEAT)
        _copy_rows(conn, "realtor_scores_zip",    "postal_code",  ZIP_HEAT)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), AVG(exit_score) FROM realtor_scores_county WHERE exit_score IS NOT NULL")
            n_c, avg_es_c = cur.fetchone()
            cur.execute("SELECT COUNT(*), AVG(exit_score) FROM realtor_scores_zip WHERE exit_score IS NOT NULL")
            n_z, avg_es_z = cur.fetchone()
        print(f"\nrealtor_scores_county: {n_c:,} rows, avg exit_score={avg_es_c:.1f}")
        print(f"realtor_scores_zip:    {n_z:,} rows, avg exit_score={avg_es_z:.1f}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
