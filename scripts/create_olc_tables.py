"""Create the OpenLetterConnect send-log tables on the configured DB (Neon by default).

Run: .venv/bin/python scripts/create_olc_tables.py

Tables
------
olc_orders             one row per order placed through the OLC API
  id               bigserial PK
  created_at       timestamptz, default now()
  olc_order_id     text         -- the id OLC returned
  name             text         -- order name shown in OLC
  product_id       int
  product_name     text
  template_id      int
  template_name    text
  status           text         -- OLC order status ("On Hold", ...)
  cost             numeric      -- as returned by OLC at creation
  is_live_mode     boolean      -- demo vs production API
  recipient_count  int
  source_note      text         -- e.g. uploaded filename
  response_json    jsonb        -- raw create-order response
  last_polled_at   timestamptz  -- last time we refreshed status from OLC

olc_order_items        one row per recipient in an order
  addr_key uses the Foreclosure-Project canonical form:
  NORM(street)|NORM(city)|NORM(state)|zip5  (see app/routes/olc_send.py)

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
CREATE TABLE IF NOT EXISTS olc_orders (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    olc_order_id    TEXT,
    name            TEXT,
    product_id      INT,
    product_name    TEXT,
    template_id     INT,
    template_name   TEXT,
    status          TEXT,
    cost            NUMERIC(12,2),
    is_live_mode    BOOLEAN,
    recipient_count INT NOT NULL DEFAULT 0,
    source_note     TEXT,
    response_json   JSONB,
    last_polled_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_olc_orders_created_at
    ON olc_orders (created_at DESC);

CREATE TABLE IF NOT EXISTS olc_order_items (
    id             BIGSERIAL PRIMARY KEY,
    order_id       BIGINT NOT NULL REFERENCES olc_orders(id) ON DELETE CASCADE,
    first_name     TEXT,
    last_name      TEXT,
    street         TEXT,
    city           TEXT,
    state          TEXT,
    zip_code       TEXT,
    campaign_phone TEXT,
    auction_date   DATE,
    estimated_value NUMERIC(14,2),
    offer_value    NUMERIC(14,2),
    addr_key       TEXT,
    status         TEXT,
    is_delivered   BOOLEAN NOT NULL DEFAULT FALSE,
    tracking_code  TEXT,
    updated_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_olc_items_order
    ON olc_order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_olc_items_addr
    ON olc_order_items (addr_key)
    WHERE addr_key IS NOT NULL;
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
                    (SELECT COUNT(*) FROM olc_orders)      AS orders,
                    (SELECT COUNT(*) FROM olc_order_items) AS items
            """)
            orders, items = cur.fetchone()
            print(f"OK. olc_orders rows: {orders} · olc_order_items rows: {items}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
