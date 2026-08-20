import os
from flask import Flask, request, Response
from config import config


def create_app(config_name="default"):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    from app.routes.dashboard import dashboard_bp
    from app.routes.api_routes import api_bp
    from app.routes.chat import chat_bp
    from app.routes.explore import explore_bp
    from app.routes.scrapers_ui import scrapers_bp
    from app.routes.mailing_export import mailing_bp
    from app.routes.dossier import dossier_bp
    from app.routes.olc_send import olc_bp
    from app.routes.olc_webhook import olc_webhook_bp, WEBHOOK_PATH
    from app.routes.ledger import ledger_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(chat_bp)
    app.register_blueprint(explore_bp)
    app.register_blueprint(scrapers_bp, url_prefix="/scrapers")
    app.register_blueprint(mailing_bp)
    app.register_blueprint(dossier_bp)
    app.register_blueprint(olc_bp)
    app.register_blueprint(olc_webhook_bp)
    app.register_blueprint(ledger_bp)

    # ── HTTP Basic Auth ────────────────────────────────────────────
    # No password default: this repo is public, so a literal here is a
    # published credential. Unset AUTH_PASS denies everything rather than
    # falling back to something an attacker can read on GitHub.
    auth_user = os.getenv("AUTH_USER", "admin")
    auth_pass = os.getenv("AUTH_PASS")

    @app.before_request
    def _require_auth():
        # OLC posts webhooks from its own servers and cannot send Basic Auth;
        # that path authenticates on the payload's Secret Key instead.
        if request.path.startswith(WEBHOOK_PATH):
            return None
        if not auth_pass:
            return Response("Server auth is not configured.", 503)
        auth = request.authorization
        if not auth or auth.username != auth_user or auth.password != auth_pass:
            return Response(
                "Authentication required.", 401,
                {"WWW-Authenticate": 'Basic realm="Market Intel"'},
            )

    return app
