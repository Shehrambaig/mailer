"""Place an OLC order from an upload CSV, and log it to Neon.

Safety model (OLC support, 2026-08-18):
  * no cap on contacts per order — 13,000 in one request is fine
  * the response returns once the order record exists; per-contact processing
    continues in the background
  * every request carries an idempotency-key header AND an order_uuid in the
    body, so a retry after a timeout can never place the order twice
  * ingestion is not all-or-nothing — undeliverable contacts are dropped from
    the mailing and you are billed only for the deliverable quantity

The key is written to <out>.orderkey.json BEFORE the request goes out, so if
this dies mid-flight you re-run with --resume-key and the retry is safe.

Nothing is sent without --confirm; the default is a dry run.

    # dry run, history-excluded
    python scripts/place_olc_order.py --csv ~/Downloads/olc-upload-2026-08-17.csv \
        --name "Aug 2026 PFC W2" --history exclude

    # test order of 20, then the real thing
    python scripts/place_olc_order.py --csv ... --name "test" --history exclude \
        --sample 20 --confirm
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.api.olc import OLCClient, OLCApiError, _unwrap  # noqa: E402

csv.field_size_limit(10 ** 9)

TEMPLATE_ID, TEMPLATE_NAME = 11736, "Faiz text/Gabe Open Letter Team Fixed"
PRODUCT_ID, PRODUCT_NAME = 18, "8.5x11 Snap Pack Mailer, First Class"


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())).strip()


def addr_key(r: dict) -> str:
    return "|".join((norm(r["Address 1"]), norm(r["City"]), norm(r["State"]),
                     r["Zip Code"][:5]))


def contact(r: dict) -> dict:
    """CSV row -> OLC contact. Same shape the AUG22 orders used."""
    custom = {"Offer Amount": r["Offer Amount"],
              "Offer Amount Words": r["Offer Amount Words"]}
    c = {
        "firstName": r["First Name"], "lastName": r["Last Name"],
        "companyName": r["Company Name"],
        "address1": r["Address 1"], "city": r["City"],
        "state": r["State"], "zip": r["Zip Code"],
        "propertyAddress": r["Property Address"], "propertyCity": r["Property City"],
        "propertyState": r["Property State"], "propertyZip": r["Property Zip"],
    }
    c.update(custom)
    c["customFields"] = custom
    return c


def return_address(r: dict) -> dict:
    first, _, last = r["Return Name"].partition(" ")
    return {"firstName": first, "lastName": last, "address1": r["Return Address"],
            "city": r["Return City"], "state": r["Return State"],
            "zip": r["Return Zip"], "phoneNo": r["Return Phone"]}


def history_keys(conn, keys: list[str]) -> set[str]:
    """Addresses already mailed — olc_order_items plus mailed mailer rows."""
    from app.routes.olc_send import MAILER_KEY_SQL
    hits: set[str] = set()
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT addr_key FROM olc_order_items WHERE addr_key = ANY(%s)",
                    (keys,))
        hits |= {r[0] for r in cur.fetchall()}
        cur.execute(f"SELECT DISTINCT k FROM (SELECT {MAILER_KEY_SQL} AS k FROM mailer "
                    "WHERE mailed_at IS NOT NULL) m WHERE m.k = ANY(%s)", (keys,))
        hits |= {r[0] for r in cur.fetchall()}
    return hits


def log_to_neon(conn, resp, name: str, rows: list[dict], live: bool, note: str) -> int:
    data = _unwrap(resp) or {}
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO olc_orders
                (olc_order_id, name, product_id, product_name, template_id,
                 template_name, status, cost, is_live_mode, recipient_count,
                 source_note, response_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) RETURNING id
        """, (str(data.get("id")) if data.get("id") is not None else None,
              name, PRODUCT_ID, PRODUCT_NAME, TEMPLATE_ID, TEMPLATE_NAME,
              data.get("status"), data.get("cost"), data.get("isLiveMode", live),
              len(rows), note, json.dumps(resp, default=str)))
        local_id = cur.fetchone()[0]
        psycopg2.extras.execute_values(cur, """
            INSERT INTO olc_order_items
                (order_id, first_name, last_name, street, city, state, zip_code,
                 offer_value, addr_key, status, updated_at)
            VALUES %s
        """, [(local_id, None, r["Company Name"], r["Address 1"], r["City"],
               r["State"], r["Zip Code"],
               None if r["Offer Amount"] == "Call for Price"
               else int(r["Offer Amount"].lstrip("$").replace(",", "")),
               addr_key(r), data.get("status"), now) for r in rows])
    conn.commit()
    return local_id


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--name", required=True, help="order name shown in the OLC dashboard")
    ap.add_argument("--history", required=True, choices=["exclude", "include"],
                    help="exclude = drop addresses already mailed (weeks 1-3 of the "
                         "cadence); include = send-all week")
    ap.add_argument("--sample", type=int, help="place only N random rows (test order)")
    ap.add_argument("--seed", type=int, default=20)
    ap.add_argument("--confirm", action="store_true", help="actually place the order")
    ap.add_argument("--resume-key", help="reuse a saved idempotency key after a timeout")
    args = ap.parse_args()

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    rows = list(csv.DictReader(open(os.path.expanduser(args.csv), encoding="utf-8-sig")))
    print(f"file: {args.csv}\nrows: {len(rows):,}")

    conn = psycopg2.connect(os.environ["NEON_DB"])
    keys = [addr_key(r) for r in rows]
    hits = history_keys(conn, keys)
    print(f"already-mailed addresses in this file: {len(hits):,}")

    if args.history == "exclude":
        rows = [r for r, k in zip(rows, keys) if k not in hits]
        print(f"history excluded -> {len(rows):,} rows")
    else:
        print("history INCLUDED (send-all week) -> re-mailing "
              f"{len(hits):,} previously mailed addresses")

    if args.sample:
        random.seed(args.seed)
        rows = random.sample(rows, min(args.sample, len(rows)))
        print(f"sampled -> {len(rows):,} rows")

    if not rows:
        print("nothing to send")
        return 1

    amounts = [int(r["Offer Amount"].lstrip("$").replace(",", ""))
               for r in rows if r["Offer Amount"] != "Call for Price"]
    print(f"\n  template {TEMPLATE_ID} ({TEMPLATE_NAME})\n  product  {PRODUCT_ID} ({PRODUCT_NAME})")
    print(f"  return   {rows[0]['Return Name']}, {rows[0]['Return Address']}, "
          f"{rows[0]['Return City']} {rows[0]['Return State']} {rows[0]['Return Zip']}")
    print(f"  offers   ${min(amounts):,} - ${max(amounts):,}  "
          f"({len(rows) - len(amounts)} Call-for-Price)")
    print(f"  est cost ${len(rows) * 1.35:,.0f} at the AUG22 rate of $1.35/piece")

    key = args.resume_key or str(uuid.uuid4())
    payload = {"name": args.name, "productId": PRODUCT_ID, "templateId": TEMPLATE_ID,
               "order_uuid": key, "contacts": [contact(r) for r in rows],
               "returnAddress": return_address(rows[0])}
    body_mb = len(json.dumps(payload)) / 1e6
    print(f"  payload  {body_mb:.2f} MB, {len(rows):,} contacts, idempotency key {key}")

    if not args.confirm:
        print("\nDRY RUN — nothing sent. Re-run with --confirm to place this order.")
        return 0

    keyfile = os.path.splitext(os.path.expanduser(args.csv))[0] + ".orderkey.json"
    with open(keyfile, "w") as fh:
        json.dump({"key": key, "name": args.name, "rows": len(rows),
                   "csv": args.csv, "history": args.history}, fh, indent=2)
    print(f"\nkey saved to {keyfile} — re-run with --resume-key {key} if this times out")

    client = OLCClient.from_env()
    print(f"posting to {client.base_url} (live={client.is_live}) …")
    try:
        resp = client.create_order(payload, idempotency_key=key)
    except OLCApiError as e:
        print(f"OLC rejected the order ({e.status_code}): {e.message}")
        print("DO NOT blindly retry — check GET /orders first, or re-run with "
              f"--resume-key {key} which is safe by design.")
        return 1

    data = _unwrap(resp) or {}
    print(f"\nOLC order #{data.get('id')}  status={data.get('status')}  "
          f"cost={data.get('cost')}  live={data.get('isLiveMode')}")
    local_id = log_to_neon(conn, resp, args.name, rows, client.is_live,
                           f"{os.path.basename(args.csv)} history={args.history}")
    print(f"logged as local olc_orders #{local_id} with {len(rows):,} items "
          "(now active suppression history)")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
