"""Schema for the OLC webhook receiver. Idempotent — safe to re-run.

  olc_webhook_events            every payload OLC sends, archived raw
  olc_order_items.olc_item_id   OLC's own mail-piece id (learned from webhooks)
  olc_order_items.qr_scanned_at / qr_scan_count   QR engagement
"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DDL = [
    """
    CREATE TABLE IF NOT EXISTS olc_webhook_events (
        id           BIGSERIAL PRIMARY KEY,
        received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        event        TEXT,
        payload      JSONB,
        rows_updated INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS olc_webhook_events_event_idx ON olc_webhook_events (event, received_at DESC)",
    # OLC redelivers any non-2xx, so the same body can arrive more than once.
    "ALTER TABLE olc_webhook_events ADD COLUMN IF NOT EXISTS fingerprint TEXT",
    "CREATE UNIQUE INDEX IF NOT EXISTS olc_webhook_events_fingerprint_idx ON olc_webhook_events (fingerprint)",
    # QR scans carry no orderItem id and no address, so the piece can only be
    # matched heuristically. Keep every scan in full here regardless.
    """
    CREATE TABLE IF NOT EXISTS olc_qr_scans (
        id                   BIGSERIAL PRIMARY KEY,
        received_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        order_id             BIGINT,
        item_id              BIGINT,
        olc_order_id         TEXT,
        scanned_at           TEXT,
        number_of_scans      INTEGER,
        contact_company      TEXT,
        contact_name         TEXT,
        contact_email        TEXT,
        utm_property_address TEXT,
        utm                  JSONB
    )
    """,
    # undocumented, but present on every scan: OLC's own piece id
    "ALTER TABLE olc_qr_scans ADD COLUMN IF NOT EXISTS olc_item_id TEXT",
    "CREATE INDEX IF NOT EXISTS olc_qr_scans_order_idx ON olc_qr_scans (order_id, received_at DESC)",
    "CREATE INDEX IF NOT EXISTS olc_qr_scans_item_idx  ON olc_qr_scans (item_id)",
    "ALTER TABLE olc_order_items ADD COLUMN IF NOT EXISTS olc_item_id   TEXT",
    # USPS expands "#3A" to "APT 3A", so the address OLC echoes back does not
    # key the same as the one we sent. This is the designator-free fallback.
    "ALTER TABLE olc_order_items ADD COLUMN IF NOT EXISTS addr_key_loose TEXT",
    "CREATE INDEX IF NOT EXISTS olc_order_items_loose_idx ON olc_order_items (order_id, addr_key_loose)",
    "ALTER TABLE olc_order_items ADD COLUMN IF NOT EXISTS qr_scanned_at TIMESTAMPTZ",
    "ALTER TABLE olc_order_items ADD COLUMN IF NOT EXISTS qr_scan_count INTEGER DEFAULT 0",
    "CREATE INDEX IF NOT EXISTS olc_order_items_olc_item_idx ON olc_order_items (olc_item_id)",
    # the webhook matches on (order_id, addr_key) until it learns olc_item_id
    "CREATE INDEX IF NOT EXISTS olc_order_items_order_addr_idx ON olc_order_items (order_id, addr_key)",
]


def main() -> int:
    dsn = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NEON_DB is not set")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            for statement in DDL:
                cur.execute(statement)
                print("ok:", " ".join(statement.split())[:88])
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("""SELECT column_name FROM information_schema.columns
                           WHERE table_name = 'olc_order_items'
                             AND column_name IN ('olc_item_id','qr_scanned_at','qr_scan_count')
                           ORDER BY column_name""")
            print("\nolc_order_items new columns:", [r[0] for r in cur.fetchall()])
            cur.execute("SELECT COUNT(*) FROM olc_webhook_events")
            print("olc_webhook_events rows:", cur.fetchone()[0])
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
