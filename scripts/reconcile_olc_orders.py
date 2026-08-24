"""Reconcile per-piece status against OLC's order-level record.

Per-piece status is webhook-only, so anything OLC sent while our receiver did
not exist (13-20 Aug) is gone: Delivered is terminal and never re-sent. The
order endpoint still knows the truth, though — status, the lifecycle dates and
deliverableQuantity — so a Completed order tells us every deliverable piece
arrived even when we never heard about them one by one.

This reads GET /orders/{id} only. It places nothing and changes nothing at OLC.

    python scripts/reconcile_olc_orders.py --dry-run
    python scripts/reconcile_olc_orders.py

Promotion rules, chosen so this can never invent a delivery:
  * only orders OLC reports as Completed are reconciled
  * pieces already Failed / Returned to Sender / Canceled / Re-Routed are left
    exactly as they are — a real negative outcome always wins
  * we promote at most (deliverableQuantity - already delivered) pieces, so the
    total lands on OLC's own number instead of over-counting the contacts they
    never ingested
  * pieces that carry an olc_item_id are promoted first: OLC has spoken about
    them at least once, so they were definitely ingested
  * promoted rows are marked status_source='order-completed', keeping them
    distinguishable from webhook-confirmed deliveries forever
"""
from __future__ import annotations

import argparse
import os

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

KEEP = ("Failed", "Returned to Sender", "Canceled", "Re-Routed")
DATE_FIELDS = {
    "mailed_date": "mailedDate",
    "in_transit_date": "inTransitDate",
    "expected_delivery_date": "expectedDeliveryDate",
    "delivered_date": "deliveredDate",
    "completed_date": "completedDate",
}

DDL = [
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS olc_status TEXT",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS quantity INTEGER",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS deliverable_quantity INTEGER",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS payment_status TEXT",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS mailed_date TIMESTAMPTZ",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS in_transit_date TIMESTAMPTZ",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS expected_delivery_date TIMESTAMPTZ",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS delivered_date TIMESTAMPTZ",
    "ALTER TABLE olc_orders ADD COLUMN IF NOT EXISTS completed_date TIMESTAMPTZ",
    # keeps an inferred delivery distinguishable from a webhook-confirmed one
    "ALTER TABLE olc_order_items ADD COLUMN IF NOT EXISTS status_source TEXT",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    key = os.environ["OLC_API_KEY"]
    base = os.environ["OLC_BASE_URL"].rstrip("/")
    headers = {"Authorization": f"Bearer {key}"}

    conn = psycopg2.connect(os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL"))
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()

    cur.execute("SELECT id, olc_order_id, name FROM olc_orders ORDER BY id")
    total_promoted = 0

    for local_id, oid, name in cur.fetchall():
        r = requests.get(f"{base}/orders/{oid}", headers=headers, timeout=60)
        if r.status_code != 200:
            print(f"{oid} {name}: GET failed {r.status_code}")
            continue
        d = (r.json() or {}).get("data") or {}

        olc_status = d.get("status")
        qty = int(d.get("quantity") or 0) or None
        deliverable = int(d.get("deliverableQuantity") or 0) or None

        if not args.dry_run:
            cur.execute(f"""
                UPDATE olc_orders SET olc_status=%s, quantity=%s, deliverable_quantity=%s,
                       payment_status=%s, last_polled_at=NOW(),
                       {", ".join(f"{c}=%s" for c in DATE_FIELDS)}
                WHERE id=%s
            """, (olc_status, qty, deliverable, d.get("paymentStatus"),
                  *[d.get(j) or None for j in DATE_FIELDS.values()], local_id))

        cur.execute("""SELECT COALESCE(status,'Scheduled'), COUNT(*)
                       FROM olc_order_items WHERE order_id=%s GROUP BY 1""", (local_id,))
        counts = dict(cur.fetchall())
        delivered_now = counts.get("Delivered", 0)

        print(f"\n{oid}  {name}")
        print(f"   OLC: {olc_status}  deliverable {deliverable:,}" if deliverable
              else f"   OLC: {olc_status}")
        print(f"   ours: {counts}")

        if olc_status != "Completed" or not deliverable:
            print("   -> not Completed, leaving per-piece status to the webhook")
            continue

        need = deliverable - delivered_now
        if need <= 0:
            print("   -> already reconciled")
            continue

        # olc_item_id first: OLC has acknowledged those pieces at least once.
        cur.execute("""
            SELECT id FROM olc_order_items
            WHERE order_id = %s
              AND COALESCE(status,'Scheduled') NOT IN %s
              AND COALESCE(status,'Scheduled') <> 'Delivered'
            ORDER BY (olc_item_id IS NOT NULL) DESC, id
            LIMIT %s
        """, (local_id, KEEP, need))
        ids = [r[0] for r in cur.fetchall()]
        print(f"   -> promote {len(ids):,} of {need:,} needed to reach OLC's {deliverable:,}")
        total_promoted += len(ids)

        if ids and not args.dry_run:
            psycopg2.extras.execute_batch(cur, """
                UPDATE olc_order_items
                SET status='Delivered', is_delivered=TRUE,
                    status_source='order-completed', updated_at=NOW()
                WHERE id=%s
            """, [(i,) for i in ids], page_size=1000)

    if args.dry_run:
        conn.rollback()
        print(f"\nDRY RUN — would promote {total_promoted:,} pieces")
    else:
        conn.commit()
        print(f"\npromoted {total_promoted:,} pieces")
        cur.execute("""SELECT COALESCE(status,'Scheduled'), COALESCE(status_source,'webhook'),
                              COUNT(*) FROM olc_order_items GROUP BY 1,2 ORDER BY 3 DESC""")
        print("now:")
        for s, src, n in cur.fetchall():
            print(f"   {s:<20} {src:<18} {n:>6,}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
