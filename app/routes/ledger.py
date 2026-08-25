"""/ledger — what happened to every piece of mail we paid to send.

Reads Neon on every request, so it reflects whatever the webhook receiver has
written as of page load. Nothing here calls the OpenLetterConnect API.
"""
from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, jsonify, render_template, request

from app.routes.olc_send import _conn

ledger_bp = Blueprint("ledger", __name__)

PER_PIECE = 1.15          # OLC's flat First Class Snap Pack rate

# OpenLetterConnect's per-piece lifecycle, in the order a piece moves through
# it. Anything OLC sends that is not on this list still lands in the DB and is
# shown under "Other" rather than being silently dropped.
LADDER = [
    ("Scheduled",          "Not yet reported"),
    ("Not Mailed",         "Not mailed"),
    ("Processing",         "Processing"),
    ("Mailed",             "Mailed"),
    ("In Transit",         "In transit"),
    ("Re-Routed",          "Re-routed"),
    ("Delivered",          "Delivered"),
    ("Returned to Sender", "Returned"),
    ("Canceled",           "Canceled"),
    ("Failed",             "Failed"),
]
BAD_STATUSES = ("Returned to Sender", "Failed", "Canceled")

# one colour per lifecycle state, kept away from the interface accent so
# semantic colour never competes with navigation
STATUS_COLOR = {
    "Scheduled":          "var(--st-scheduled)",
    "Not Mailed":         "var(--st-scheduled)",
    "Processing":         "var(--st-processing)",
    "Mailed":             "var(--st-mailed)",
    "In Transit":         "var(--st-transit)",
    "Re-Routed":          "var(--st-rerouted)",
    "Delivered":          "var(--st-delivered)",
    "Returned to Sender": "var(--st-returned)",
    "Canceled":           "var(--st-canceled)",
    "Failed":             "var(--st-failed)",
}


def _blank_counts() -> dict:
    return {k: 0 for k, _ in LADDER}


def _collect():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.id, o.olc_order_id, o.name, o.status, o.cost,
                       o.template_id, o.recipient_count, o.created_at,
                       o.is_live_mode, o.last_polled_at,
                       o.olc_status, o.deliverable_quantity, o.delivered_date,
                       o.mailed_date, o.in_transit_date, o.expected_delivery_date
                FROM olc_orders o ORDER BY o.id DESC
            """)
            orders = [{
                "id": r[0], "olc_order_id": r[1], "name": r[2], "status": r[3],
                "cost": float(r[4]) if r[4] else None,
                "template_id": r[5], "recipient_count": r[6], "created_at": r[7],
                "is_live": r[8], "last_polled_at": r[9],
                "olc_status": r[10], "deliverable": r[11], "delivered_date": r[12],
                "mailed_date": r[13], "in_transit_date": r[14], "expected_date": r[15],
                "first_scan": None, "last_scan": None, "scans": {},
                "usps_delivered": 0, "usps_delivered_on": None,
                "counts": _blank_counts(), "other": {}, "pieces": 0,
                "inferred": 0,
            } for r in cur.fetchall()]
            by_id = {o["id"]: o for o in orders}

            cur.execute("""
                SELECT order_id, COALESCE(status, 'Scheduled'), COUNT(*)
                FROM olc_order_items GROUP BY 1, 2
            """)
            for order_id, status, n in cur.fetchall():
                o = by_id.get(order_id)
                if not o:
                    continue
                o["pieces"] += n
                if status in o["counts"]:
                    o["counts"][status] += n
                else:
                    o["other"][status] = o["other"].get(status, 0) + n

            # Every piece that did not make it — the list you act on.
            cur.execute("""
                SELECT o.olc_order_id, o.name, i.status,
                       i.last_name, i.first_name, i.street, i.city, i.state, i.zip_code,
                       i.olc_item_id, i.updated_at
                FROM olc_order_items i JOIN olc_orders o ON o.id = i.order_id
                WHERE i.status = ANY(%s)
                ORDER BY i.updated_at DESC NULLS LAST
                LIMIT 500
            """, (list(BAD_STATUSES),))
            failures = [{
                "olc_order_id": r[0], "order": r[1], "status": r[2],
                "name": (r[3] or r[4] or "").strip() or "(no name)",
                "addr": ", ".join(p for p in (r[5], r[6],
                                              f"{r[7] or ''} {r[8] or ''}".strip()) if p),
                "item_id": r[9], "at": r[10],
            } for r in cur.fetchall()]

            cur.execute("""
                SELECT order_id, MIN(scanned_at), MAX(scanned_at)
                FROM olc_piece_scans WHERE order_id IS NOT NULL GROUP BY 1
            """)
            for order_id, lo, hi in cur.fetchall():
                if order_id in by_id:
                    by_id[order_id]["first_scan"] = lo
                    by_id[order_id]["last_scan"] = hi

            # real USPS delivery scans, as opposed to OLC's batch stamp
            cur.execute("""
                SELECT order_id, COUNT(*), MIN(scanned_at), MAX(scanned_at)
                FROM olc_piece_scans
                WHERE event_message = 'Delivered' AND order_id IS NOT NULL
                GROUP BY 1
            """)
            for order_id, n, lo, hi in cur.fetchall():
                if order_id in by_id:
                    by_id[order_id]["usps_delivered"] = n
                    by_id[order_id]["usps_delivered_on"] = lo

            cur.execute("""
                SELECT order_id, event_message, COUNT(*)
                FROM olc_piece_scans WHERE order_id IS NOT NULL GROUP BY 1, 2
            """)
            for order_id, msg, n in cur.fetchall():
                if order_id in by_id:
                    by_id[order_id]["scans"][msg or "?"] = n

            cur.execute("""
                SELECT order_id, COUNT(*) FROM olc_order_items
                WHERE status_source = 'order-completed' AND status = 'Delivered'
                GROUP BY 1
            """)
            for order_id, n in cur.fetchall():
                if order_id in by_id:
                    by_id[order_id]["inferred"] = n

            cur.execute("""
                SELECT COUNT(*), MAX(received_at), COALESCE(SUM(rows_updated), 0),
                       MIN(received_at)
                FROM olc_webhook_events
            """)
            ev_count, ev_last, ev_rows, listening_since = cur.fetchone()

            # A scan older than the day we started listening cannot be proven
            # to be the first one: anything USPS did before that was carried in
            # events we never received.
            for o in orders:
                fs = o["first_scan"]
                o["scan_is_first"] = bool(fs and listening_since and fs > listening_since)

            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(number_of_scans), 0),
                       COUNT(*) FILTER (WHERE item_id IS NOT NULL)
                FROM olc_qr_scans
            """)
            qr_rows, qr_scans, qr_matched = cur.fetchone()

            # Whoever scanned the QR responded to the mail — the warmest
            # signal the campaign produces, so it gets names and addresses.
            cur.execute("""
                SELECT s.contact_company, s.contact_name, s.contact_email,
                       s.scanned_at, s.number_of_scans, s.olc_item_id,
                       s.olc_order_id, o.name,
                       i.street, i.city, i.state, i.zip_code
                FROM olc_qr_scans s
                LEFT JOIN olc_orders o      ON o.id = s.order_id
                LEFT JOIN olc_order_items i ON i.id = s.item_id
                ORDER BY s.scanned_at DESC NULLS LAST, s.id DESC
                LIMIT 300
            """)
            responders = [{
                "who": (r[0] or r[1] or "").strip() or "(unnamed)",
                "email": r[2],
                "at": r[3],
                "scans": r[4] or 0,
                "piece": r[5],
                "olc_order_id": r[6],
                "order": r[7],
                "mailed_to": ", ".join(p for p in (r[8], r[9],
                                       f"{r[10] or ''} {r[11] or ''}".strip()) if p),
            } for r in cur.fetchall()]
    finally:
        conn.close()

    grand = _blank_counts()
    other_grand: dict = {}
    for o in orders:
        for k in grand:
            grand[k] += o["counts"][k]
        for k, v in o["other"].items():
            other_grand[k] = other_grand.get(k, 0) + v

    total_pieces = sum(o["pieces"] for o in orders)
    return {
        "orders": orders,
        "ladder": LADDER,
        "grand": grand,
        "other_grand": other_grand,
        "failures": failures,
        "total_pieces": total_pieces,
        "total_cost": sum(o["cost"] or 0 for o in orders),
        "per_piece": PER_PIECE,
        "reported": total_pieces - grand["Scheduled"],
        "inferred_total": sum(o["inferred"] for o in orders),
        "events": {"count": ev_count, "last": ev_last, "rows": ev_rows,
                   "listening_since": listening_since},
        "qr": {"rows": qr_rows, "scans": qr_scans, "matched": qr_matched},
        "responders": responders,
        # Response rate is only meaningful against pieces actually delivered.
        "delivered": grand["Delivered"],
    }


@ledger_bp.route("/ledger")
def ledger():
    data = _collect()
    data["now"] = datetime.now(timezone.utc)
    data["statuscolor"] = STATUS_COLOR
    # htmx polls this and swaps just the body, so a refresh never reloads the
    # page, loses scroll position, or refetches the stylesheet.
    template = "_ledger_body.html" if request.args.get("fragment") else "ledger.html"
    return render_template(template, **data)


@ledger_bp.route("/api/ledger")
def ledger_json():
    """Same numbers as the page, for polling or a quick curl."""
    d = _collect()
    for o in d["orders"]:
        for k in ("created_at", "last_polled_at", "delivered_date", "mailed_date",
                  "in_transit_date", "expected_date", "first_scan", "last_scan",
                  "usps_delivered_on"):
            o[k] = o[k].isoformat() if o[k] else None
    for f in d["failures"]:
        f["at"] = f["at"].isoformat() if f["at"] else None
    for r in d["responders"]:
        if hasattr(r["at"], "isoformat"):
            r["at"] = r["at"].isoformat()
    for k in ("last", "listening_since"):
        if d["events"].get(k):
            d["events"][k] = d["events"][k].isoformat()
    return jsonify(d)
