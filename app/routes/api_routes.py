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
    blob_url = _BLOB_URLS.get(name)
    if blob_url:
        with urlopen(blob_url, timeout=30) as r:
            _cache[name] = json.loads(r.read().decode("utf-8"))
    else:
        with open(os.path.join(_DATA, name)) as f:
            _cache[name] = json.load(f)
    return _cache[name]


@api_bp.route("/county-heatmap")
def county_heatmap():
    return jsonify(_load("county-heatmap.json"))


@api_bp.route("/scatter")
def scatter_data():
    return jsonify(_load("scatter.json"))


@api_bp.route("/census-income")
def census_income_data():
    return jsonify(_load("census-income.json"))


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


@api_bp.route("/stacked/national")
def stacked_national():
    return jsonify(_load("stacked/national.json"))


@api_bp.route("/stacked/state")
def stacked_state():
    return jsonify(_load("stacked/state.json"))


@api_bp.route("/stacked/state/<st>")
def stacked_state_one(st):
    payload = _load("stacked/state.json")
    entry = (payload.get("data") or {}).get(st.upper())
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/stacked/metro")
def stacked_metro():
    return jsonify(_load("stacked/metro.json"))


@api_bp.route("/stacked/metro/<cbsa>")
def stacked_metro_one(cbsa):
    payload = _load("stacked/metro.json")
    entry = (payload.get("data") or {}).get(cbsa)
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/stacked/county")
def stacked_county():
    return jsonify(_load("stacked/county.json"))


@api_bp.route("/stacked/county/<fips>")
def stacked_county_one(fips):
    payload = _load("stacked/county.json")
    entry = (payload.get("data") or {}).get(fips)
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/stacked/zip/<st>")
def stacked_zip_state(st):
    """Per-state ZIP shard — loaded on demand by the ZIP drill-down view."""
    try:
        return jsonify(_load(f"stacked/zip/{st.upper()}.json"))
    except FileNotFoundError:
        return jsonify({}), 404


@api_bp.route("/stacked/zip/<st>/<zip5>")
def stacked_zip_one(st, zip5):
    try:
        payload = _load(f"stacked/zip/{st.upper()}.json")
    except FileNotFoundError:
        return jsonify({}), 404
    entry = (payload.get("data") or {}).get(zip5.zfill(5))
    if not entry:
        return jsonify({}), 404
    return jsonify(entry)


@api_bp.route("/cache/clear", methods=["POST"])
def clear_cache():
    _cache.clear()
    return jsonify({"ok": True})
