"""One row per USPS scan, because a piece gets several and they matter.

olc_order_items.tracking_code keeps only the newest scan and each event
overwrites it, so delivery confirmations were being lost behind later
sortation scans. OLC sends three event types — In Transit, Out For Delivery
and Delivered — and 3,370 pieces in the first campaign had more than one.
"""
from __future__ import annotations

from datetime import datetime, timezone

import psycopg2.extras

SCAN_FORMATS = ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y")


def parse_scan_ts(tc: dict):
    """USPS sends the date and time as separate display-formatted strings."""
    d, t = tc.get("scanned_date"), tc.get("scanned_time")
    if not d:
        return None
    for fmt in SCAN_FORMATS:
        try:
            return datetime.strptime(f"{d} {t}".strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def record_scan(cur, local_order_id, olc_order_id, item: dict, tracking) -> bool:
    """Store one USPS scan. Idempotent — a redelivered event is a no-op."""
    if not isinstance(tracking, dict):
        return False
    ts = parse_scan_ts(tracking)
    olc_item_id = item.get("id")
    if not (ts and olc_item_id):
        return False

    cur.execute("SELECT id FROM olc_order_items WHERE olc_item_id = %s LIMIT 1",
                (str(olc_item_id),))
    row = cur.fetchone()

    cur.execute("""
        INSERT INTO olc_piece_scans
          (order_id, item_id, olc_order_id, olc_item_id, event_message, scanned_at,
           barcode, imbdigits, district, state, equipment, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (olc_item_id, scanned_at, event_message) DO NOTHING
    """, (local_order_id, row[0] if row else None, str(olc_order_id), str(olc_item_id),
          tracking.get("event_message"), ts, tracking.get("barcode"),
          tracking.get("imbdigits"), tracking.get("districtnm"),
          tracking.get("state"), tracking.get("equipment"),
          psycopg2.extras.Json(tracking)))
    return cur.rowcount > 0
