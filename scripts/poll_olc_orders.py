"""Poll OLC for order + per-piece status and write it back to Neon.

Read-only against OLC (GET /orders/{id}, falling back to a GET /orders search),
which is what the /send page's refresh button does — this is the same logic
runnable without the Flask app, for all orders at once.

    python scripts/poll_olc_orders.py              # every order on record
    python scripts/poll_olc_orders.py --order 3    # one local order id
    python scripts/poll_olc_orders.py --dump out/  # also save the raw JSON
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from app.api.olc import OLCClient, OLCApiError, _unwrap  # noqa: E402
from app.routes.olc_send import addr_key  # noqa: E402

ITEM_KEYS = ("contacts", "orderItems", "items", "records", "rows")


def fetch(client: OLCClient, olc_id: str):
    """GET the order; fall back to searching the list endpoint."""
    try:
        return _unwrap(client.get_order(olc_id))
    except OLCApiError as e:
        print(f"    GET /orders/{olc_id} -> {e.status_code} {e.message}; trying search")
        listing = _unwrap(client.get_orders(search=str(olc_id)))
        rows = listing.get("rows") if isinstance(listing, dict) else listing
        for cand in rows or []:
            if str(cand.get("id")) == str(olc_id):
                return cand
    return None


def item_rows(data: dict) -> list:
    for k in ITEM_KEYS:
        v = data.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--order", type=int, help="local olc_orders.id (default: all)")
    ap.add_argument("--dump", help="directory to save raw JSON responses")
    args = ap.parse_args()

    load_dotenv(os.path.join(ROOT, ".env"))
    conn = psycopg2.connect(os.environ["NEON_DB"])
    with conn.cursor() as cur:
        cur.execute("SELECT id, olc_order_id, name, recipient_count FROM olc_orders "
                    + ("WHERE id = %s " if args.order else "")
                    + "ORDER BY id", (args.order,) if args.order else None)
        orders = cur.fetchall()

    client = OLCClient.from_env()
    print(f"polling {len(orders)} order(s) against {client.base_url}\n")
    now = datetime.now(timezone.utc)

    for local_id, olc_id, name, count in orders:
        print(f"#{local_id}  OLC {olc_id}  {name}  ({count:,} pieces)")
        data = fetch(client, olc_id)
        if not isinstance(data, dict):
            print("    not found on the OLC side\n")
            continue

        if args.dump:
            os.makedirs(args.dump, exist_ok=True)
            with open(os.path.join(args.dump, f"order-{olc_id}.json"), "w") as fh:
                json.dump(data, fh, indent=2, default=str)

        status = data.get("status") or data.get("orderStatus")
        print(f"    status={status!r}  cost={data.get('cost')}  "
              f"payment={data.get('paymentStatus')}  mailDate={data.get('mailDate') or data.get('projectedMailDate')}")

        items = item_rows(data)
        print(f"    per-piece records returned: {len(items):,}")
        if items:
            print(f"    item fields: {sorted(items[0].keys())}")

        updated = 0
        with conn.cursor() as cur:
            cur.execute("UPDATE olc_orders SET status = COALESCE(%s, status), "
                        "cost = COALESCE(%s, cost), last_polled_at = %s WHERE id = %s",
                        (status, data.get("cost"), now, local_id))
            for it in items:
                k = addr_key(it.get("address1") or it.get("address"), it.get("city"),
                             it.get("state"), it.get("zip") or it.get("zipCode"))
                cur.execute("""
                    UPDATE olc_order_items
                    SET status = COALESCE(%s, status),
                        is_delivered = COALESCE(%s, is_delivered),
                        tracking_code = COALESCE(%s, tracking_code),
                        updated_at = %s
                    WHERE order_id = %s AND addr_key = %s
                """, (it.get("status"), it.get("isDelivered"),
                      it.get("trackingCode") or it.get("tracking_code"),
                      now, local_id, k))
                updated += cur.rowcount
        conn.commit()
        print(f"    Neon rows updated: {updated:,}")
        if items:
            print("    status mix:",
                  dict(collections.Counter(i.get("status") for i in items)))
        print()

    with conn.cursor() as cur:
        cur.execute("""SELECT o.id, o.olc_order_id, o.name, o.status, o.cost,
                              i.status, COUNT(*)
                       FROM olc_orders o JOIN olc_order_items i ON i.order_id = o.id
                       GROUP BY 1,2,3,4,5,6 ORDER BY 1, 7 DESC""")
        print("=" * 72)
        print("NEON ROLLUP")
        for r in cur.fetchall():
            print(f"  #{r[0]} {r[2][:26]:26} order={r[3]!r:12} cost={r[4]} "
                  f"| item status={r[5]!r:14} {r[6]:>7,}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
