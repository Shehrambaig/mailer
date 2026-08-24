"""Extract every USPS scan from archived webhook payloads into olc_piece_scans.

olc_order_items.tracking_code holds one scan per piece and each new event
overwrites it, so the history was being discarded — 3,370 pieces had two scans
and 156 had three. OLC sends three USPS event types (In Transit, Out For
Delivery, Delivered), so the discarded scans included real delivery
confirmations.

One row per (piece, scan). Re-runnable: a unique index makes re-import a no-op.

    python scripts/build_piece_scans.py --dry-run
    python scripts/build_piece_scans.py
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DDL = [
    """
    CREATE TABLE IF NOT EXISTS olc_piece_scans (
        id            BIGSERIAL PRIMARY KEY,
        order_id      BIGINT,
        item_id       BIGINT,
        olc_order_id  TEXT,
        olc_item_id   TEXT,
        event_message TEXT,
        scanned_at    TIMESTAMPTZ,
        barcode       TEXT,
        imbdigits     TEXT,
        district      TEXT,
        state         TEXT,
        equipment     TEXT,
        raw           JSONB
    )
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS olc_piece_scans_uniq
       ON olc_piece_scans (olc_item_id, scanned_at, event_message)""",
    "CREATE INDEX IF NOT EXISTS olc_piece_scans_item_idx ON olc_piece_scans (item_id)",
    "CREATE INDEX IF NOT EXISTS olc_piece_scans_msg_idx ON olc_piece_scans (event_message, scanned_at)",
]


def parse_scan_ts(tc: dict):
    d, t = tc.get("scanned_date"), tc.get("scanned_time")
    if not d:
        return None
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(f"{d} {t}".strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL"))
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()

    cur.execute("SELECT olc_order_id, id FROM olc_orders")
    order_pk = {o: i for o, i in cur.fetchall()}
    cur.execute("SELECT olc_item_id, id FROM olc_order_items WHERE olc_item_id IS NOT NULL")
    item_pk = dict(cur.fetchall())

    cur.execute("""SELECT payload FROM olc_webhook_events
                   WHERE event = 'order_items.updated'
                     AND jsonb_typeof(payload->'data') = 'array'
                   ORDER BY id""")

    rows, seen, skipped = [], set(), 0
    for (payload,) in cur.fetchall():
        for d in (payload.get("data") or []):
            item = d.get("orderItem") or {}
            tc = item.get("trackingCode")
            if not isinstance(tc, dict):
                continue
            ts = parse_scan_ts(tc)
            iid = str(item.get("id") or "")
            msg = tc.get("event_message")
            if not (ts and iid):
                skipped += 1
                continue
            key = (iid, ts, msg)
            if key in seen:
                continue
            seen.add(key)
            oid = str((d.get("order") or {}).get("id") or "")
            rows.append((order_pk.get(oid), item_pk.get(iid), oid, iid, msg, ts,
                         tc.get("barcode"), tc.get("imbdigits"), tc.get("districtnm"),
                         tc.get("state"), tc.get("equipment"),
                         psycopg2.extras.Json(tc)))

    print(f"distinct scans found : {len(rows):,}")
    print(f"unparseable          : {skipped:,}")
    from collections import Counter
    print("by event             :", dict(Counter(r[4] for r in rows)))

    if args.dry_run:
        conn.close()
        return 0

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO olc_piece_scans
          (order_id, item_id, olc_order_id, olc_item_id, event_message, scanned_at,
           barcode, imbdigits, district, state, equipment, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (olc_item_id, scanned_at, event_message) DO NOTHING
    """, rows, page_size=1000)
    conn.commit()

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT olc_item_id) FROM olc_piece_scans")
    n, pieces = cur.fetchone()
    print(f"stored               : {n:,} scans across {pieces:,} pieces")
    cur.execute("""SELECT event_message, COUNT(*), MIN(scanned_at)::date, MAX(scanned_at)::date
                   FROM olc_piece_scans GROUP BY 1 ORDER BY 2 DESC""")
    for msg, c_, lo, hi in cur.fetchall():
        print(f"   {msg:<20} {c_:>7,}   {lo} .. {hi}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
