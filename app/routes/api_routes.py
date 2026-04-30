import json
import os
from urllib.request import urlopen
from flask import Blueprint, jsonify

api_bp = Blueprint("api", __name__)

_DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "app", "static", "data")

_cache = {}

# Files too large to bundle into the Vercel function (>10 MB). Hosted on Vercel
# Blob; fetched once per cold start and cached in memory.
_BLOB_URLS_PATH = os.path.join(_DATA, "blob-urls.json")
try:
    with open(_BLOB_URLS_PATH) as _f:
        _BLOB_URLS = json.load(_f)
except FileNotFoundError:
    _BLOB_URLS = {}


def _load(name):
    if name in _cache:
        return _cache[name]
    # Prefer the local file when it exists (covers local dev and any file we
    # rebuilt). Fall back to the Vercel Blob URL only when the file isn't
    # bundled — that's the production path for files >10MB.
    local_path = os.path.join(_DATA, name)
    if os.path.exists(local_path):
        with open(local_path) as f:
            _cache[name] = json.load(f)
        return _cache[name]
    blob_url = _BLOB_URLS.get(name)
    if blob_url:
        with urlopen(blob_url, timeout=30) as r:
            _cache[name] = json.loads(r.read().decode("utf-8"))
        return _cache[name]
    raise FileNotFoundError(name)


@api_bp.route("/county-heatmap")
def county_heatmap():
    return jsonify(_load("county-heatmap.json"))


@api_bp.route("/state-heatmap")
def state_heatmap():
    try:
        return jsonify(_load("state-heatmap.json"))
    except FileNotFoundError:
        return jsonify({}), 404


@api_bp.route("/scatter")
def scatter_data():
    return jsonify(_load("scatter.json"))


@api_bp.route("/county/<fips>")
def county_detail(fips):
    detail = _load("county-detail.json")
    entry = detail.get(fips)
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/listings")
def listings():
    return jsonify(_load("listings.json"))


@api_bp.route("/listings/<fips>")
def listings_detail(fips):
    try:
        detail = _load("listings-detail.json")
    except FileNotFoundError:
        return jsonify({}), 404
    entry = detail.get(fips)
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/sources")
def sources():
    return jsonify(_load("sources.json"))


@api_bp.route("/zip-heatmap")
def zip_heatmap():
    try:
        return jsonify(_load("zip-heatmap.json"))
    except FileNotFoundError:
        return jsonify({}), 404


@api_bp.route("/zip/<zip5>")
def zip_detail(zip5):
    try:
        detail = _load("zip-detail.json")
    except FileNotFoundError:
        return jsonify({}), 404
    entry = detail.get(zip5)
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/cache/clear", methods=["POST"])
def clear_cache():
    _cache.clear()
    return jsonify({"ok": True})
