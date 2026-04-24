from abc import ABC, abstractmethod
from datetime import datetime, timezone
from app.extensions import db
from app.models import Lead, ScraperRun
from app.ingest.normalizer import normalize_street, normalize_state, normalize_zip, safe_int, safe_float


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""

    name: str = ""
    description: str = ""

    @abstractmethod
    def scrape(self, **kwargs) -> list[dict]:
        """Run the scraper and return a list of lead dicts.

        Each dict should have at minimum: street, city, state, zip_code
        Optional: source_id, source_url, county, property_type, bedrooms,
                  bathrooms, square_footage, lot_size_acres, year_built,
                  latitude, longitude, estimated_value, auction_date,
                  asset_type, product_type, occupancy_status,
                  case_number, filing_date, decedent_name, executor_name
        """
        pass

    def run(self, **kwargs):
        """Execute the scraper with tracking."""
        run = ScraperRun(scraper_name=self.name, run_config=kwargs or {})
        db.session.add(run)
        db.session.commit()

        try:
            results = self.scrape(**kwargs)
            run.records_found = len(results)

            new_count = 0
            updated_count = 0
            for record in results:
                was_new = self._save_lead(record)
                if was_new:
                    new_count += 1
                else:
                    updated_count += 1

            db.session.commit()
            run.records_new = new_count
            run.records_updated = updated_count
            run.status = "completed"
            run.finished_at = datetime.now(timezone.utc)
            db.session.commit()

            return run

        except Exception as e:
            run.status = "failed"
            run.error_message = str(e)
            run.finished_at = datetime.now(timezone.utc)
            db.session.commit()
            raise

    def _save_lead(self, record):
        """Save a lead record, returning True if new, False if updated."""
        street = normalize_street(record.get("street")) or ""
        city = (record.get("city") or "").strip().upper()
        state = normalize_state(record.get("state"))
        zip_code = normalize_zip(record.get("zip_code"))

        if not street or not city or not state or not zip_code:
            return False

        existing = Lead.query.filter_by(
            street=street, city=city, state=state, zip_code=zip_code
        ).first()

        if existing:
            # Enrich existing lead with new data
            for field in ["source_url", "county", "property_type", "bedrooms",
                          "bathrooms", "square_footage", "lot_size_acres", "year_built",
                          "latitude", "longitude", "estimated_value", "offer_value",
                          "starting_bid", "auction_date", "asset_type", "product_type",
                          "occupancy_status", "case_number", "filing_date",
                          "decedent_name", "executor_name"]:
                new_val = record.get(field)
                if new_val is not None:
                    setattr(existing, field, new_val)
            return False
        else:
            lead = Lead(
                source=self.name,
                source_id=record.get("source_id"),
                source_url=record.get("source_url"),
                street=street,
                city=city,
                state=state,
                zip_code=zip_code,
                county=record.get("county"),
                property_type=record.get("property_type"),
                bedrooms=safe_int(record.get("bedrooms")),
                bathrooms=safe_float(record.get("bathrooms")),
                square_footage=safe_int(record.get("square_footage")),
                lot_size_acres=safe_float(record.get("lot_size_acres")),
                year_built=safe_int(record.get("year_built")),
                latitude=safe_float(record.get("latitude")),
                longitude=safe_float(record.get("longitude")),
                estimated_value=safe_int(record.get("estimated_value")),
                offer_value=safe_int(record.get("offer_value")),
                starting_bid=safe_int(record.get("starting_bid")),
                auction_date=record.get("auction_date"),
                asset_type=record.get("asset_type"),
                product_type=record.get("product_type"),
                occupancy_status=record.get("occupancy_status"),
                case_number=record.get("case_number"),
                filing_date=record.get("filing_date"),
                decedent_name=record.get("decedent_name"),
                executor_name=record.get("executor_name"),
            )
            db.session.add(lead)
            return True
