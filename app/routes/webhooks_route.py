from flask import Blueprint, request, jsonify
from app.api.webhooks import process_webhook

webhooks_bp = Blueprint("webhooks", __name__)


@webhooks_bp.route("/pcm", methods=["POST"])
def pcm_webhook():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "no payload"}), 400

    process_webhook(payload)
    return jsonify({"ok": True}), 200
