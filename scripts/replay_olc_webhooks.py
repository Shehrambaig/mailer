"""Re-apply archived OLC webhook payloads to olc_order_items.

Every payload is kept in olc_webhook_events, so a matching rule that was wrong
when an event first arrived can be fixed and the event replayed. This is what
recovers the pieces missed while the address key could not survive USPS unit
standardisation ("#3A" sent, "APT 3A" echoed back).

Only forward transitions are applied and only status/tracking are touched, so a
replay can never move a piece backwards or invent one.

    python scripts/replay_olc_webhooks.py --dry-run
    python scripts/replay_olc_webhooks.py
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.routes.olc_send import addr_key, loose_addr_key  # noqa: E402

load_dotenv()

DELIVERED = {"Delivered"}
# Later states win; a replay must never walk a piece back up the ladder.
RANK = {"Scheduled": 0, "Not Mailed": 1, "Processing": 2, "Mailed": 3,
        "In Transit": 4, "Re-Routed": 5, "Delivered": 6,
        "Returned to Sender": 7, "Canceled": 7, "Failed": 7}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dsn = os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    cur.execute("""SELECT o.olc_order_id, i.id, i.olc_item_id, i.addr_key,
                          i.addr_key_loose, COALESCE(i.status, 'Scheduled')
                   FROM olc_order_items i JOIN olc_orders o ON o.id = i.order_id""")
    by_item, by_key, by_loose, cur_status = {}, {}, {}, {}
    for oid, pk, iid, ak, lk, st in cur.fetchall():
        cur_status[pk] = st
        if iid:
            by_item[(oid, str(iid))] = pk
        if ak:
            by_key[(oid, ak)] = pk
        if lk:
            by_loose[(oid, lk)] = pk

    cur.execute("""SELECT payload FROM olc_webhook_events
                   WHERE event LIKE 'order_items%%' AND jsonb_typeof(payload->'data') = 'array'
                   ORDER BY id""")
    payloads = [r[0] for r in cur.fetchall()]

    pending, still_missing, by_route = {}, 0, {"item_id": 0, "addr_key": 0, "loose": 0}
    for p in payloads:
        for d in (p.get("data") or []):
            order = d.get("order") or {}
            contact = d.get("contact") or {}
            item = d.get("orderItem") or {}
            oid = str(order.get("id") or "")
            iid = str(item.get("id") or "")
            status = item.get("status") or d.get("status")
            if not status:
                continue

            k = addr_key(contact.get("address1"), contact.get("city"),
                         contact.get("state"), contact.get("zip"))
            lk = loose_addr_key(contact.get("address1"), contact.get("city"),
                                contact.get("state"), contact.get("zip"))

            pk = by_item.get((oid, iid))
            route = "item_id"
            if pk is None:
                pk, route = by_key.get((oid, k)), "addr_key"
            if pk is None:
                pk, route = by_loose.get((oid, lk)), "loose"
            if pk is None:
                still_missing += 1
                continue
            by_route[route] += 1

            prev = pending.get(pk)
            if prev is None or RANK.get(status, 0) >= RANK.get(prev[0], 0):
                pending[pk] = (status, iid or None)

    changes = [(pk, st, iid) for pk, (st, iid) in pending.items()
               if RANK.get(st, 0) > RANK.get(cur_status.get(pk, "Scheduled"), 0)]

    print(f"payloads replayed        : {len(payloads):,}")
    print(f"items matched            : {sum(by_route.values()):,}  {by_route}")
    print(f"items still unmatched    : {still_missing:,}")
    print(f"pieces to move forward   : {len(changes):,}")
    if args.dry_run:
        from collections import Counter
        print("would become:", dict(Counter(c[1] for c in changes)))
        conn.close()
        return 0

    import psycopg2.extras as extras
    extras.execute_batch(cur, """
        UPDATE olc_order_items
        SET status = %s,
            is_delivered = CASE WHEN %s THEN TRUE ELSE is_delivered END,
            olc_item_id = COALESCE(olc_item_id, %s),
            updated_at = NOW()
        WHERE id = %s
    """, [(st, st in DELIVERED, iid, pk) for pk, st, iid in changes], page_size=1000)
    conn.commit()
    print(f"applied                  : {len(changes):,}")

    cur.execute("""SELECT COALESCE(status,'Scheduled'), COUNT(*)
                   FROM olc_order_items GROUP BY 1 ORDER BY 2 DESC""")
    print("now:", dict(cur.fetchall()))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
