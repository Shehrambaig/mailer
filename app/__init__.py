import os
from flask import Flask, request, Response
from config import config


def create_app(config_name="default"):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    from app.routes.dashboard import dashboard_bp
    from app.routes.api_routes import api_bp
    from app.routes.chat import chat_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(chat_bp)

    # ── HTTP Basic Auth ────────────────────────────────────────────
    auth_user = os.getenv("AUTH_USER", "admin")
    auth_pass = os.getenv("AUTH_PASS", "360homes")

    @app.before_request
    def _require_auth():
        auth = request.authorization
        if not auth or auth.username != auth_user or auth.password != auth_pass:
            return Response(
                "Authentication required.", 401,
                {"WWW-Authenticate": 'Basic realm="Market Intel"'},
            )

    return app
