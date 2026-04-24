from datetime import datetime, timezone
from app.extensions import db


class Lead(db.Model):
    __tablename__ = "leads"

    id = db.Column(db.Integer, primary_key=True)

    # Source tracking
    source = db.Column(db.String(50), nullable=False)  # excel_mailing, auction_com, ny_surrogate, etc.
    source_id = db.Column(db.String(100))  # listing_id, case number, etc.
    source_url = db.Column(db.Text)

    # Address
    street = db.Column(db.String(255), nullable=False)
    city = db.Column(db.String(100), nullable=False)
    state = db.Column(db.String(2), nullable=False)
    zip_code = db.Column(db.String(10), nullable=False)
    county = db.Column(db.String(100))

    # Address validation
    address_verified = db.Column(db.Boolean, default=False)
    dpv_match_code = db.Column(db.String(1))

    # Property details
    property_type = db.Column(db.String(50))
    bedrooms = db.Column(db.SmallInteger)
    bathrooms = db.Column(db.Numeric(3, 1))
    square_footage = db.Column(db.Integer)
    lot_size_acres = db.Column(db.Numeric(8, 2))
    year_built = db.Column(db.SmallInteger)
    latitude = db.Column(db.Numeric(10, 7))
    longitude = db.Column(db.Numeric(10, 7))

    # Financial
    estimated_value = db.Column(db.Integer)
    offer_value = db.Column(db.Integer)
    starting_bid = db.Column(db.Integer)
    reserve_price = db.Column(db.Integer)

    # Auction details
    auction_date = db.Column(db.DateTime)
    auction_end_date = db.Column(db.DateTime)
    auction_type = db.Column(db.String(20))  # ONLINE, LIVE
    asset_type = db.Column(db.String(30))  # FORECLOSURE, BANK_OWNED
    product_type = db.Column(db.String(30))  # TRUSTEE, REO
    occupancy_status = db.Column(db.String(20))
    listing_status = db.Column(db.String(30))

    # Parties
    servicer_name = db.Column(db.String(200))
    investor_name = db.Column(db.String(200))
    attorney_name = db.Column(db.String(200))

    # Probate-specific
    case_number = db.Column(db.String(50))
    filing_date = db.Column(db.Date)
    decedent_name = db.Column(db.String(200))
    executor_name = db.Column(db.String(200))

    # Campaign
    campaign_phone = db.Column(db.String(20))

    # Lifecycle
    status = db.Column(db.String(20), default="new")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Dedup
    __table_args__ = (
        db.UniqueConstraint("street", "city", "state", "zip_code", name="uq_lead_address"),
        db.Index("idx_leads_state", "state"),
        db.Index("idx_leads_status", "status"),
        db.Index("idx_leads_auction_date", "auction_date"),
        db.Index("idx_leads_source", "source"),
    )

    mail_pieces = db.relationship("MailPiece", backref="lead", lazy="dynamic")


class Campaign(db.Model):
    __tablename__ = "campaigns"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    mail_type = db.Column(db.String(20), nullable=False)  # postcard, letter
    design_id = db.Column(db.String(100))

    target_criteria = db.Column(db.JSON)
    merge_template = db.Column(db.JSON)

    scheduled_date = db.Column(db.Date)
    send_before_auction_days = db.Column(db.Integer, default=14)

    # Stats
    total_pieces = db.Column(db.Integer, default=0)
    pieces_sent = db.Column(db.Integer, default=0)
    pieces_delivered = db.Column(db.Integer, default=0)
    pieces_returned = db.Column(db.Integer, default=0)

    status = db.Column(db.String(20), default="draft")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    mail_pieces = db.relationship("MailPiece", backref="campaign", lazy="dynamic")


class MailPiece(db.Model):
    __tablename__ = "mail_pieces"

    id = db.Column(db.Integer, primary_key=True)
    campaign_id = db.Column(db.Integer, db.ForeignKey("campaigns.id"), nullable=False)
    lead_id = db.Column(db.Integer, db.ForeignKey("leads.id"), nullable=False)

    pcm_order_id = db.Column(db.String(100))
    pcm_piece_id = db.Column(db.String(100))

    merge_data = db.Column(db.JSON)
    proof_url = db.Column(db.Text)

    status = db.Column(db.String(30), default="pending")
    status_updated_at = db.Column(db.DateTime)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("campaign_id", "lead_id", name="uq_campaign_lead"),
        db.Index("idx_mail_pieces_campaign", "campaign_id"),
        db.Index("idx_mail_pieces_lead", "lead_id"),
        db.Index("idx_mail_pieces_status", "status"),
        db.Index("idx_mail_pieces_pcm_order", "pcm_order_id"),
    )


class ScraperRun(db.Model):
    __tablename__ = "scraper_runs"

    id = db.Column(db.Integer, primary_key=True)
    scraper_name = db.Column(db.String(50), nullable=False)
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = db.Column(db.DateTime)
    status = db.Column(db.String(20), default="running")
    records_found = db.Column(db.Integer, default=0)
    records_new = db.Column(db.Integer, default=0)
    records_updated = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text)
    run_config = db.Column(db.JSON)


class WebhookEvent(db.Model):
    __tablename__ = "webhook_events"

    id = db.Column(db.Integer, primary_key=True)
    received_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    event_type = db.Column(db.String(50))
    payload = db.Column(db.JSON, nullable=False)
    processed = db.Column(db.Boolean, default=False)
