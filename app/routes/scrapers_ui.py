import threading
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from app.models import ScraperRun
from app.scrapers.registry import get_scraper, SCRAPER_REGISTRY

scrapers_bp = Blueprint("scrapers", __name__)


@scrapers_bp.route("/")
def index():
    runs = ScraperRun.query.order_by(ScraperRun.started_at.desc()).limit(20).all()

    # Build scraper info with last run data
    scrapers = []
    for name, cls in SCRAPER_REGISTRY.items():
        last_run = ScraperRun.query.filter_by(scraper_name=name).order_by(
            ScraperRun.started_at.desc()
        ).first()
        scrapers.append({
            "name": name,
            "description": cls.description,
            "last_run": last_run,
        })

    return render_template("scrapers/index.html", scrapers=scrapers, runs=runs)


@scrapers_bp.route("/<name>/run", methods=["POST"])
def run_scraper(name):
    if name not in SCRAPER_REGISTRY:
        flash(f"Unknown scraper: {name}", "error")
        return redirect(url_for("scrapers.index"))

    # Run in background thread so the UI doesn't block
    app = current_app._get_current_object()

    def _run():
        with app.app_context():
            try:
                scraper = get_scraper(name)
                scraper.run()
            except Exception as e:
                print(f"Scraper {name} failed: {e}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    flash(f"Scraper '{name}' started in background", "success")
    return redirect(url_for("scrapers.index"))
