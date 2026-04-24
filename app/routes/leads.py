from flask import Blueprint, render_template, request, redirect, url_for, flash
from app.extensions import db
from app.models import Lead

leads_bp = Blueprint("leads", __name__)


@leads_bp.route("/")
def index():
    page = request.args.get("page", 1, type=int)
    state_filter = request.args.get("state", "")
    status_filter = request.args.get("status", "")

    query = Lead.query
    if state_filter:
        query = query.filter_by(state=state_filter)
    if status_filter:
        query = query.filter_by(status=status_filter)

    leads = query.order_by(Lead.created_at.desc()).paginate(page=page, per_page=50)

    states = db.session.query(Lead.state).distinct().order_by(Lead.state).all()
    states = [s[0] for s in states]

    return render_template(
        "leads/index.html",
        leads=leads,
        states=states,
        state_filter=state_filter,
        status_filter=status_filter,
    )


@leads_bp.route("/<int:lead_id>")
def detail(lead_id):
    lead = db.get_or_404(Lead, lead_id)
    return render_template("leads/detail.html", lead=lead)


@leads_bp.route("/import", methods=["GET", "POST"])
def import_leads():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename:
            flash("No file selected", "error")
            return redirect(url_for("leads.import_leads"))

        import os, tempfile
        from app.ingest.excel_loader import ExcelLoader

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
        file.save(tmp.name)
        tmp.close()

        try:
            loader = ExcelLoader()
            count = loader.load_file(tmp.name)
            flash(f"Imported {count} leads", "success")
        except Exception as e:
            flash(f"Import failed: {e}", "error")
        finally:
            os.unlink(tmp.name)

        return redirect(url_for("leads.index"))

    return render_template("leads/import.html")
