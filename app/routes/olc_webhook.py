"""Receiver for OpenLetterConnect webhooks — per-mail-piece status.

Polling cannot give us piece-level status: `GET /orders/{id}` carries no
contacts array, `/orders/detail/{id}` is only a cost/quantity breakdown, and
`/orders/{id}/items` is 403. The `order_items.updated` webhook is the only
route to Mailed / In Transit / Delivered / **Returned to Sender** per piece.

OLC registers webhooks in the dashboard only (profile icon -> Settings ->
Webhooks -> Create) and cannot send HTTP Basic Auth, so this blueprint is
exempted from the app-wide auth gate in app/__init__.py. It authenticates
instead on the `token` field OLC includes in every payload — the Secret Key
shown once when the webhook is created. Put it in OLC_WEBHOOK_SECRET.

Events handled:
    orders.created / orders.updated        -> olc_orders.status
    order_items.updated                    -> per-piece status + tracking
    order_items.qr_code_scanned            -> qr_scanned_at on the piece

Every payload is archived to olc_webhook_events regardless, so nothing is
lost if a matching rule needs fixing later.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections import Counter
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, request

from app.api import slack_log
from app.routes.olc_send import _conn, addr_key, loose_addr_key

olc_webhook_bp = Blueprint("olc_webhook", __name__)

# Path prefix the Basic Auth gate skips — keep in sync with app/__init__.py.
WEBHOOK_PATH = "/webhooks/olc"

DELIVERED_STATUSES = {"Delivered"}
# Worth naming individually in Slack rather than folding into a count.
ALERT_STATUSES = {"Returned to Sender", "Failed", "Canceled"}
MAX_ALERTS_SHOWN = 12
# The docs index lists this as order_items.qr_code_scanned; the event page
# gives order_item.qr_code_scanned. Accept both rather than guess.
QR_EVENTS = {"order_item.qr_code_scanned", "order_items.qr_code_scanned"}


def _secret() -> str | None:
    """The Secret Key OLC shows once at webhook creation.

    Accepts OLC_WEBHOOK too — a name slip there would otherwise fail closed
    with a 503 that looks identical to 'not configured yet'.
    """
    return os.getenv("OLC_WEBHOOK_SECRET") or os.getenv("OLC_WEBHOOK")


def _handle_qr(cur, items, now, order_labels, statuses):
    """QR scans arrive on their own payload shape — nothing like the others.

    Documented body (webhook-docs, order-item-scanned):
        data.scanned_by     {contact_first, contact_last, contact_company, contact_email}
        data.scan_timestamp
        data.number_of_scans          cumulative, not a per-ping increment
        data.order_details  {order_id, order_name}
        data.utm_details    {utm_source, utm_medium, utm_campaign,
                             utm_email, utm_phone, utm_property_address}

    There is no orderItem id and no mailing address, so the piece can only be
    matched heuristically. Every scan is therefore recorded in full in
    olc_qr_scans regardless; olc_order_items is updated only when a single
    unambiguous piece matches, so a wrong guess never overwrites good data.
    """
    updated = matched = 0

    for entry in items:
        od = entry.get("order_details") or {}
        olc_order_id = od.get("order_id")
        if olc_order_id is None:
            continue

        cur.execute("SELECT id, name FROM olc_orders WHERE olc_order_id = %s",
                    (str(olc_order_id),))
        row = cur.fetchone()
        if not row:
            continue
        local_order_id, order_name = row[0], row[1]
        order_labels[str(olc_order_id)] = order_name
        matched += 1

        who = entry.get("scanned_by") or {}
        utm = entry.get("utm_details") or {}
        company = (who.get("contact_company") or "").strip()
        person = " ".join(p for p in ((who.get("contact_first") or "").strip(),
                                      (who.get("contact_last") or "").strip()) if p)

        try:
            scans = int(str(entry.get("number_of_scans") or "").strip() or 0)
        except ValueError:
            scans = 0

        # Best-effort: the payee name is what we put in companyName at send time.
        item_id = None
        for candidate in (company, person):
            if not candidate:
                continue
            cur.execute("""
                SELECT id FROM olc_order_items
                WHERE order_id = %s
                  AND (UPPER(COALESCE(last_name, '')) = UPPER(%s)
                       OR UPPER(TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,''))) = UPPER(%s))
                LIMIT 2
            """, (local_order_id, candidate, candidate))
            hits = cur.fetchall()
            if len(hits) == 1:            # ambiguous match is no match
                item_id = hits[0][0]
                break

        cur.execute("""
            INSERT INTO olc_qr_scans
              (order_id, item_id, olc_order_id, scanned_at, number_of_scans,
               contact_company, contact_name, contact_email,
               utm_property_address, utm, received_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
        """, (local_order_id, item_id, str(olc_order_id),
              entry.get("scan_timestamp"), scans, company or None, person or None,
              (who.get("contact_email") or None),
              (utm.get("utm_property_address") or None),
              json.dumps(utm, default=str), now))

        if item_id:
            # number_of_scans is cumulative, so SET it — never increment, or a
            # redelivery would inflate the count.
            cur.execute("""
                UPDATE olc_order_items
                SET qr_scanned_at = COALESCE(qr_scanned_at, %s),
                    qr_scan_count = GREATEST(COALESCE(qr_scan_count, 0), %s),
                    updated_at    = %s
                WHERE id = %s
            """, (now, scans, now, item_id))
            updated += cur.rowcount

        statuses["QR scanned" if item_id else "QR scanned (unmatched piece)"] += 1

    return updated, matched, order_labels, statuses


def _addr(contact: dict) -> str:
    parts = [contact.get("address1"), contact.get("city"),
             f"{contact.get('state') or ''} {contact.get('zip') or ''}".strip()]
    return ", ".join(p for p in parts if p) or "(no address)"


def _slack_summary(event, items, order_labels, statuses, alerts, updated) -> str:
    icon = "🔴" if alerts else "📬"
    if order_labels:
        who = ", ".join(f"{name} (#{oid})" for oid, name in order_labels.items())
    else:
        who = "no matching order in Neon"

    lines = [f"{icon} *{event}* — {who}",
             f"    {len(items)} item(s) in payload · {updated} row(s) updated"]
    if statuses:
        lines.append("    " + " · ".join(f"{s} {n}" for s, n in statuses.most_common()))
    for a in alerts[:MAX_ALERTS_SHOWN]:
        lines.append(f"    ⚠️ {a}")
    if len(alerts) > MAX_ALERTS_SHOWN:
        lines.append(f"    …and {len(alerts) - MAX_ALERTS_SHOWN} more "
                     f"(query olc_order_items for the full list)")
    return "\n".join(lines)


def _payload_items(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, dict):
        return [data]
    return [d for d in (data or []) if isinstance(d, dict)]


@olc_webhook_bp.route(WEBHOOK_PATH, methods=["POST"])
def olc_webhook():
    raw = request.get_data() or b""
    payload = request.get_json(silent=True) or {}
    event = payload.get("event") or ""

    secret = _secret()
    if not secret:
        # Fail closed: an unauthenticated public endpoint must not write.
        return jsonify({"error": "OLC_WEBHOOK_SECRET is not configured"}), 503
    if not hmac.compare_digest(str(payload.get("token") or ""), secret):
        return jsonify({"error": "bad token"}), 401

    now = datetime.now(timezone.utc)
    items = _payload_items(payload)
    updated = matched_orders = 0
    order_labels: dict = {}
    statuses: Counter = Counter()
    alerts: list = []

    # OLC redelivers anything that did not return 2xx, so the same body can
    # arrive twice. Without this, a retry would double-count qr_scan_count.
    fingerprint = hashlib.sha256(raw).hexdigest()

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO olc_webhook_events (event, payload, received_at, fingerprint)
                VALUES (%s, %s::jsonb, %s, %s)
                ON CONFLICT (fingerprint) DO NOTHING
                RETURNING id
            """, (event, json.dumps(payload, default=str), now, fingerprint))
            row = cur.fetchone()
            if row is None:
                conn.commit()
                return jsonify({"ok": True, "event": event, "duplicate": True})
            event_row_id = row[0]

            if event in QR_EVENTS:
                updated, matched_orders, order_labels, statuses = _handle_qr(
                    cur, items, now, order_labels, statuses)
                cur.execute("UPDATE olc_webhook_events SET rows_updated = %s WHERE id = %s",
                            (updated, event_row_id))
                conn.commit()
                slack_ok = slack_log.post(
                    _slack_summary(event, items, order_labels, statuses, alerts, updated))
                return jsonify({"ok": True, "event": event, "items": len(items),
                                "orders_matched": matched_orders,
                                "rows_updated": updated, "slack": slack_ok})

            for entry in items:
                order = entry.get("order") or {}
                contact = entry.get("contact") or {}
                item = entry.get("orderItem") or {}
                olc_order_id = order.get("id")
                if olc_order_id is None:
                    continue

                cur.execute("SELECT id, name FROM olc_orders WHERE olc_order_id = %s",
                            (str(olc_order_id),))
                row = cur.fetchone()
                if not row:
                    continue          # an order placed outside this app
                local_order_id, order_name = row[0], row[1]
                order_labels[str(olc_order_id)] = order_name
                matched_orders += 1

                if event.startswith("orders."):
                    cur.execute("""
                        UPDATE olc_orders SET status = COALESCE(%s, status),
                               last_polled_at = %s
                        WHERE id = %s
                    """, (order.get("status"), now, local_order_id))
                    updated += cur.rowcount
                    if order.get("status"):
                        statuses[f"order → {order['status']}"] += 1
                    continue

                status = item.get("status") or entry.get("status")
                tracking = item.get("trackingCode")
                # trackingCode is an object or a list of USPS scan records
                tracking_json = (json.dumps(tracking, default=str)
                                 if tracking not in (None, "", [], {}) else None)

                # Prefer OLC's own item id once we have seen it; fall back to
                # the canonical address key, which is how the rows were written.
                item_id = item.get("id")
                key = addr_key(contact.get("address1"), contact.get("city"),
                               contact.get("state"), contact.get("zip"))
                loose = loose_addr_key(contact.get("address1"), contact.get("city"),
                                       contact.get("state"), contact.get("zip"))

                if status:
                    statuses[status] += 1
                if status in ALERT_STATUSES:
                    alerts.append(f"{status} — {_addr(contact)}")

                cur.execute("""
                        UPDATE olc_order_items
                        SET status        = COALESCE(%s, status),
                            is_delivered  = CASE WHEN %s THEN TRUE ELSE is_delivered END,
                            tracking_code = COALESCE(%s, tracking_code),
                            olc_item_id   = COALESCE(olc_item_id, %s),
                            updated_at    = %s
                        WHERE order_id = %s
                          AND (olc_item_id = %s OR addr_key = %s
                               OR addr_key_loose = %s)
                    """, (status, status in DELIVERED_STATUSES, tracking_json,
                          str(item_id) if item_id else None, now,
                          local_order_id, str(item_id) if item_id else None, key, loose))
                updated += cur.rowcount

            cur.execute("UPDATE olc_webhook_events SET rows_updated = %s WHERE id = %s",
                        (updated, event_row_id))
        conn.commit()
    finally:
        conn.close()

    # After the commit, so Slack only ever reports what is durably stored.
    # One message per payload, not per piece: OLC batches items into `data`,
    # and 3,128 pieces moving through five statuses would otherwise blow past
    # Slack's ~1 message/second limit and bury the channel.
    slack_ok = slack_log.post(
        _slack_summary(event, items, order_labels, statuses, alerts, updated))

    # 200 with a body OLC can log; never leak internals on the public path.
    return jsonify({"ok": True, "event": event, "items": len(items),
                    "orders_matched": matched_orders, "rows_updated": updated,
                    "slack": slack_ok})


@olc_webhook_bp.route(WEBHOOK_PATH, methods=["GET"])
def olc_webhook_probe():
    """Lets you (and OLC's 'test' button) confirm the URL is reachable."""
    return jsonify({"ok": True, "endpoint": "olc-webhook",
                    "configured": bool(_secret()),
                    "slack": slack_log.configured()})
