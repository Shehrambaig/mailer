"""/ledger — what happened to every piece of mail we paid to send.

Reads Neon on every request, so it reflects whatever the webhook receiver has
written as of page load. Nothing here calls the OpenLetterConnect API.
"""
from __future__ import annotations

from flask import Blueprint, jsonify, render_template

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


def _blank_counts() -> dict:
    return {k: 0 for k, _ in LADDER}


def _collect():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.id, o.olc_order_id, o.name, o.status, o.cost,
                       o.template_id, o.recipient_count, o.created_at,
                       o.is_live_mode, o.last_polled_at
                FROM olc_orders o ORDER BY o.id DESC
            """)
            orders = [{
                "id": r[0], "olc_order_id": r[1], "name": r[2], "status": r[3],
                "cost": float(r[4]) if r[4] else None,
                "template_id": r[5], "recipient_count": r[6], "created_at": r[7],
                "is_live": r[8], "last_polled_at": r[9],
                "counts": _blank_counts(), "other": {}, "pieces": 0,
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
                SELECT COUNT(*), MAX(received_at), COALESCE(SUM(rows_updated), 0)
                FROM olc_webhook_events
            """)
            ev_count, ev_last, ev_rows = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(number_of_scans), 0),
                       COUNT(*) FILTER (WHERE item_id IS NOT NULL)
                FROM olc_qr_scans
            """)
            qr_rows, qr_scans, qr_matched = cur.fetchone()
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
        "events": {"count": ev_count, "last": ev_last, "rows": ev_rows},
        "qr": {"rows": qr_rows, "scans": qr_scans, "matched": qr_matched},
    }


@ledger_bp.route("/ledger")
def ledger():
    return render_template("ledger.html", **_collect())


@ledger_bp.route("/api/ledger")
def ledger_json():
    """Same numbers as the page, for polling or a quick curl."""
    d = _collect()
    for o in d["orders"]:
        for k in ("created_at", "last_polled_at"):
            o[k] = o[k].isoformat() if o[k] else None
    for f in d["failures"]:
        f["at"] = f["at"].isoformat() if f["at"] else None
    if d["events"]["last"]:
        d["events"]["last"] = d["events"]["last"].isoformat()
    return jsonify(d)
