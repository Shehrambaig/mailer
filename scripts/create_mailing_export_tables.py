"""Create the mailing-export tables on the configured probate DB (Neon by default).

Tables
------
mailing_exports        one row per export click
  id              bigserial PK
  created_at      timestamptz, default now()
  filter_json     jsonb        -- the explore filter snapshot used to build the export
  match_total     int          -- rows that matched the filter before dedupe
  drop_listing_id int          -- rows dropped because (listing_id, source) already exported
  drop_address    int          -- rows dropped because address_norm already exported
  row_count       int          -- final exported row count (= match_total - drops)
  filename        text         -- the filename we returned to the browser
  note            text         -- optional human note
mailing_export_items   one row per exported foreclosure_record
  export_id       bigint  FK → mailing_exports.id  (ON DELETE CASCADE)
  listing_id      varchar(100)
  source          varchar(20)
  address_norm    text
  PRIMARY KEY (export_id, listing_id, source)

Address normalization is whatever the export endpoint stores: roughly
UPPER(REGEXP_REPLACE(street, '[^A-Z0-9 ]', '', 'g')) || '|' || zip5.

Idempotent: run as many times as you like.
"""

import os
import sys
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DSN = (os.getenv("NEON_DB")
       or os.getenv("PROBATE_DATABASE_URL")
       or "postgresql://localhost:5432/probate")

DDL = """
CREATE TABLE IF NOT EXISTS mailing_exports (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    filter_json     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    match_total     INT         NOT NULL DEFAULT 0,
    drop_listing_id INT         NOT NULL DEFAULT 0,
    drop_address    INT         NOT NULL DEFAULT 0,
    row_count       INT         NOT NULL DEFAULT 0,
    filename        TEXT,
    note            TEXT
);

CREATE INDEX IF NOT EXISTS idx_mailing_exports_created_at
    ON mailing_exports (created_at DESC);

CREATE TABLE IF NOT EXISTS mailing_export_items (
    export_id    BIGINT          NOT NULL REFERENCES mailing_exports(id) ON DELETE CASCADE,
    listing_id   VARCHAR(100)    NOT NULL,
    source       VARCHAR(20)     NOT NULL,
    address_norm TEXT,
    PRIMARY KEY (export_id, listing_id, source)
);

-- Look-ups for dedupe on every new export. Two indexes: one keyed by
-- (listing_id, source) for the strict-id match, one keyed by address_norm
-- for the address fallback.
CREATE INDEX IF NOT EXISTS idx_mei_listing
    ON mailing_export_items (listing_id, source);
CREATE INDEX IF NOT EXISTS idx_mei_address
    ON mailing_export_items (address_norm)
    WHERE address_norm IS NOT NULL;
"""


def main():
    print(f"Connecting to {DSN.split('@')[-1].split('/')[0] if '@' in DSN else 'localhost'} ...")
    conn = psycopg2.connect(DSN, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM mailing_exports)        AS exports,
                    (SELECT COUNT(*) FROM mailing_export_items)   AS items
            """)
            exports, items = cur.fetchone()
            print(f"OK. mailing_exports rows: {exports} · mailing_export_items rows: {items}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
