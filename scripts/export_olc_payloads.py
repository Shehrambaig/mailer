"""Export archived OLC webhook payloads to JSON for review.

The Secret Key is masked in every export. Payloads carry real names and
addresses, so write them somewhere outside this repo — it is public.

    python scripts/export_olc_payloads.py                  # one full sample per event type
    python scripts/export_olc_payloads.py --all            # every envelope, as JSONL
    python scripts/export_olc_payloads.py --out ~/Desktop
"""
from __future__ import annotations

import argparse
import json
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def mask(payload):
    p = json.loads(json.dumps(payload, default=str))
    if isinstance(p, dict) and p.get("token"):
        p["token"] = "<SECRET KEY — masked>"
    return p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.expanduser("~/Downloads"))
    ap.add_argument("--all", action="store_true",
                    help="every envelope as JSONL, not just one sample per event type")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    conn = psycopg2.connect(os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL"))
    cur = conn.cursor()

    # ── one complete example of each event type ──────────────────────────
    cur.execute("SELECT DISTINCT event FROM olc_webhook_events ORDER BY 1")
    samples = {}
    for (event,) in cur.fetchall():
        cur.execute("""SELECT id, received_at, rows_updated, payload
                       FROM olc_webhook_events WHERE event = %s
                       ORDER BY id DESC LIMIT 1""", (event,))
        eid, ts, upd, payload = cur.fetchone()
        data = payload.get("data")
        samples[event] = {
            "_event_id": eid,
            "_received_at": ts.isoformat(),
            "_items_in_payload": len(data) if isinstance(data, list) else 1,
            "_rows_we_updated": upd,
            "payload": mask(payload),
        }

    path = os.path.join(args.out, "olc-payload-samples.json")
    with open(path, "w") as fh:
        json.dump(samples, fh, indent=2, ensure_ascii=False)
    print(f"wrote {path}")
    for ev, s in samples.items():
        print(f"   {ev:<30} {s['_items_in_payload']:>4} items  received {s['_received_at'][:19]}")

    # ── a per-piece item on its own, easiest thing to read ───────────────
    cur.execute("""SELECT d FROM olc_webhook_events e,
                          LATERAL jsonb_array_elements(e.payload->'data') d
                   WHERE e.event = 'order_items.updated'
                     AND d->'orderItem'->>'status' = 'Delivered'
                     AND jsonb_typeof(d->'orderItem'->'trackingCode') = 'object'
                   LIMIT 3""")
    items = [r[0] for r in cur.fetchall()]
    if items:
        path = os.path.join(args.out, "olc-per-piece-items.json")
        with open(path, "w") as fh:
            json.dump(items, fh, indent=2, ensure_ascii=False)
        print(f"wrote {path}  ({len(items)} single pieces, unwrapped)")

    # ── everything, one envelope per line ────────────────────────────────
    if args.all:
        path = os.path.join(args.out, "olc-payloads-all.jsonl")
        cur.execute("""SELECT id, event, received_at, rows_updated, payload
                       FROM olc_webhook_events ORDER BY id""")
        n = 0
        with open(path, "w") as fh:
            for eid, event, ts, upd, payload in cur:
                fh.write(json.dumps({
                    "event_id": eid, "event": event,
                    "received_at": ts.isoformat(), "rows_we_updated": upd,
                    "payload": mask(payload),
                }, ensure_ascii=False) + "\n")
                n += 1
        mb = os.path.getsize(path) / 1_048_576
        print(f"wrote {path}  ({n} envelopes, {mb:.1f} MB)")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
