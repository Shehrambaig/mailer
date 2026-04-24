from datetime import datetime, timezone
from app.extensions import db
from app.models import WebhookEvent, MailPiece, Campaign


# Map PCM webhook status strings to our internal statuses
STATUS_MAP = {
    "Processing": "processing",
    "Printed": "printed",
    "InTransit": "in_transit",
    "Delivered": "delivered",
    "Returned": "returned",
    "Cancelled": "cancelled",
    "Error": "error",
}


def process_webhook(payload):
    """Process an incoming PostcardMania webhook event.

    1. Log raw event
    2. Update mail_piece status
    3. Update campaign aggregate stats
    """
    event = WebhookEvent(
        event_type=payload.get("eventType") or payload.get("status"),
        payload=payload,
    )
    db.session.add(event)

    order_id = str(payload.get("orderID") or payload.get("orderId") or "")
    raw_status = payload.get("status") or payload.get("eventType") or ""
    new_status = STATUS_MAP.get(raw_status, raw_status.lower() if raw_status else None)

    if order_id and new_status:
        pieces = MailPiece.query.filter_by(pcm_order_id=order_id).all()
        for piece in pieces:
            old_status = piece.status
            piece.status = new_status
            piece.status_updated_at = datetime.now(timezone.utc)

            # Update campaign stats
            campaign = piece.campaign
            if old_status != new_status:
                if new_status == "delivered":
                    campaign.pieces_delivered = (campaign.pieces_delivered or 0) + 1
                elif new_status == "returned":
                    campaign.pieces_returned = (campaign.pieces_returned or 0) + 1
                elif new_status in ("processing", "printed", "in_transit") and old_status == "pending":
                    campaign.pieces_sent = (campaign.pieces_sent or 0) + 1

        event.processed = True

    db.session.commit()
    return event
