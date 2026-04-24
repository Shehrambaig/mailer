import json
import os
from flask import Blueprint, jsonify

api_bp = Blueprint("api", __name__)

_DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "app", "static", "data")

_cache = {}


def _load(name):
    if name not in _cache:
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


@api_bp.route("/cache/clear", methods=["POST"])
def clear_cache():
    _cache.clear()
    return jsonify({"ok": True})
