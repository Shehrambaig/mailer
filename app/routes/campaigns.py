from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.extensions import db
from app.models import Campaign, Lead, MailPiece
from app.api.client import PCMClient, PCMApiError
from app.api.designs import list_designs, get_design
from app.api.orders import create_postcard_order, create_letter_order, build_recipient

campaigns_bp = Blueprint("campaigns", __name__)


@campaigns_bp.route("/")
def index():
    campaigns = Campaign.query.order_by(Campaign.created_at.desc()).all()
    return render_template("campaigns/index.html", campaigns=campaigns)


@campaigns_bp.route("/new", methods=["GET", "POST"])
def create():
    if request.method == "POST":
        name = request.form.get("name")
        mail_type = request.form.get("mail_type", "postcard")
        design_id = request.form.get("design_id")
        mail_class = request.form.get("mail_class", "FirstClass")

        # Target criteria
        criteria = {}
        if request.form.get("states"):
            criteria["states"] = [s.strip() for s in request.form["states"].split(",") if s.strip()]
        if request.form.get("min_value"):
            criteria["min_value"] = int(request.form["min_value"])
        if request.form.get("max_value"):
            criteria["max_value"] = int(request.form["max_value"])
        if request.form.get("status_filter"):
            criteria["status"] = request.form["status_filter"]
        if request.form.get("source"):
            criteria["source"] = request.form["source"]

        campaign = Campaign(
            name=name,
            mail_type=mail_type,
            design_id=design_id or None,
            target_criteria=criteria,
            merge_template={"mail_class": mail_class},
            status="draft",
        )
        db.session.add(campaign)
        db.session.commit()

        # Create mail pieces for matching leads
        leads = _query_leads(criteria)
        count = 0
        for lead in leads:
            piece = MailPiece(campaign_id=campaign.id, lead_id=lead.id)
            db.session.add(piece)
            count += 1
        campaign.total_pieces = count
        db.session.commit()

        flash(f"Campaign '{name}' created with {count} recipients", "success")
        return redirect(url_for("campaigns.detail", campaign_id=campaign.id))

    # GET — show creation form
    try:
        designs = list_designs()
    except PCMApiError:
        designs = []
        flash("Could not load designs from PostcardMania", "error")

    states = db.session.query(Lead.state).distinct().order_by(Lead.state).all()
    states = [s[0] for s in states]
    sources = db.session.query(Lead.source).distinct().order_by(Lead.source).all()
    sources = [s[0] for s in sources]

    return render_template("campaigns/create.html", designs=designs, states=states, sources=sources)


@campaigns_bp.route("/<int:campaign_id>")
def detail(campaign_id):
    campaign = db.get_or_404(Campaign, campaign_id)
    pieces = campaign.mail_pieces.order_by(MailPiece.created_at.desc()).limit(100).all()

    # Status breakdown
    status_counts = {}
    for row in db.session.query(MailPiece.status, db.func.count(MailPiece.id)).filter_by(
        campaign_id=campaign_id
    ).group_by(MailPiece.status).all():
        status_counts[row[0]] = row[1]

    return render_template(
        "campaigns/detail.html",
        campaign=campaign,
        pieces=pieces,
        status_counts=status_counts,
    )


@campaigns_bp.route("/<int:campaign_id>/send", methods=["POST"])
def send(campaign_id):
    campaign = db.get_or_404(Campaign, campaign_id)

    if campaign.status not in ("draft", "scheduled"):
        flash("Campaign already sent", "error")
        return redirect(url_for("campaigns.detail", campaign_id=campaign_id))

    pieces = campaign.mail_pieces.filter_by(status="pending").all()
    if not pieces:
        flash("No pending pieces to send", "error")
        return redirect(url_for("campaigns.detail", campaign_id=campaign_id))

    # Build recipients in batches of 5000 (PCM max is 50000)
    client = PCMClient.from_config()
    mail_class = (campaign.merge_template or {}).get("mail_class", "FirstClass")
    batch_size = 5000
    total_sent = 0

    try:
        for i in range(0, len(pieces), batch_size):
            batch_pieces = pieces[i:i + batch_size]
            recipients = []
            for piece in batch_pieces:
                lead = piece.lead
                recipients.append(build_recipient(lead))

            if campaign.mail_type == "postcard":
                result = create_postcard_order(
                    recipients,
                    client=client,
                    design_id=int(campaign.design_id) if campaign.design_id else None,
                    mail_class=mail_class,
                )
            else:
                result = create_letter_order(
                    recipients,
                    client=client,
                    design_id=int(campaign.design_id) if campaign.design_id else None,
                    mail_class=mail_class,
                )

            # Store order ID on each piece
            order_id = str(result.get("orderID") or result.get("orderId", ""))
            for piece in batch_pieces:
                piece.pcm_order_id = order_id
                piece.status = "submitted"
            total_sent += len(batch_pieces)

        campaign.status = "sent"
        campaign.pieces_sent = total_sent
        db.session.commit()
        flash(f"Sent {total_sent} pieces via PostcardMania", "success")

    except PCMApiError as e:
        db.session.rollback()
        flash(f"PostcardMania API error: {e.message} — {e.data}", "error")

    return redirect(url_for("campaigns.detail", campaign_id=campaign_id))


@campaigns_bp.route("/<int:campaign_id>/cancel", methods=["POST"])
def cancel(campaign_id):
    campaign = db.get_or_404(Campaign, campaign_id)

    if campaign.status == "draft":
        # Just delete the campaign and pieces
        MailPiece.query.filter_by(campaign_id=campaign_id).delete()
        db.session.delete(campaign)
        db.session.commit()
        flash("Campaign deleted", "success")
        return redirect(url_for("campaigns.index"))

    # Try to cancel via PCM API
    from app.api.orders import cancel_order
    client = PCMClient.from_config()
    order_ids = set()
    for piece in campaign.mail_pieces.all():
        if piece.pcm_order_id:
            order_ids.add(piece.pcm_order_id)

    errors = []
    for oid in order_ids:
        try:
            cancel_order(oid, client=client)
        except PCMApiError as e:
            errors.append(f"Order {oid}: {e.message}")

    if errors:
        flash(f"Some orders could not be cancelled: {'; '.join(errors)}", "error")
    else:
        campaign.status = "cancelled"
        for piece in campaign.mail_pieces.all():
            piece.status = "cancelled"
        db.session.commit()
        flash("Campaign cancelled", "success")

    return redirect(url_for("campaigns.detail", campaign_id=campaign_id))


def _query_leads(criteria):
    """Query leads matching campaign target criteria."""
    query = Lead.query

    states = criteria.get("states", [])
    if states:
        query = query.filter(Lead.state.in_(states))

    min_val = criteria.get("min_value")
    if min_val:
        query = query.filter(Lead.estimated_value >= min_val)

    max_val = criteria.get("max_value")
    if max_val:
        query = query.filter(Lead.estimated_value <= max_val)

    status = criteria.get("status")
    if status:
        query = query.filter_by(status=status)
    else:
        # Default: only new leads
        query = query.filter_by(status="new")

    source = criteria.get("source")
    if source:
        query = query.filter_by(source=source)

    return query.all()
