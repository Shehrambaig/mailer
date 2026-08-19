"""Probate dossier dashboard — April 2026 cases with eliteress matches."""
from __future__ import annotations
import os
import re
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Blueprint, render_template, jsonify

dossier_bp = Blueprint("dossier", __name__)


def _dsn() -> str:
    # The user works from the *local* probate DB for this dashboard. We
    # deliberately skip NEON_DB (used elsewhere in the app) because the
    # eliteress_data column lives on local. Override with DOSSIER_DSN if needed.
    return (
        os.getenv("DOSSIER_DSN")
        or os.getenv("PROBATE_DATABASE_URL")
        or "postgresql://localhost:5432/probate"
    )


_STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}


def _norm_state(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if len(s) == 2:
        return s.upper()
    return _STATE_NAME_TO_CODE.get(s.lower(), s.upper()[:2] if s else None)


def _money(v):
    try:
        n = float(v)
        if n >= 1_000_000:
            return f"${n / 1_000_000:.2f}M"
        if n >= 1_000:
            return f"${n / 1_000:.0f}k"
        return f"${n:,.0f}"
    except (TypeError, ValueError):
        return None


def _shape_person(p: dict) -> dict:
    addr = p.get("address") or {}
    state = _norm_state(addr.get("state"))
    return {
        "role": p.get("role"),
        "full_name": p.get("full_name"),
        "relationship": p.get("relationship_to_decedent"),
        "is_petitioner": bool(p.get("is_petitioner")),
        "is_minor": bool(p.get("is_minor")),
        "phone": p.get("phone"),
        "email": p.get("email"),
        "phone_belongs_to": p.get("phone_belongs_to"),
        "signature_present": p.get("signature_present"),
        "address": {
            "usps": addr.get("usps_normalized"),
            "raw": addr.get("raw"),
            "street": addr.get("street"),
            "city": addr.get("city"),
            "state": state,
            "zip": addr.get("zip"),
        } if addr else None,
        "oos": bool(state and state != "NY"),
    }


def _shape_case(row: dict) -> dict:
    te = row.get("tiered_extraction") or {}
    persons_raw = te.get("persons") or []
    persons = [_shape_person(p) for p in persons_raw]

    # Court-recorded petitioner (the "PR" the user is mailing)
    pet = row.get("petitioner") or {}
    pet_addr = row.get("petitioner_address") or {}
    pet_state = _norm_state(pet_addr.get("state"))

    pr = {
        "full_name": pet.get("full_name") or " ".join(filter(None, [pet.get("first"), pet.get("middle"), pet.get("last")])).strip(),
        "phone": (row.get("petitioner_phone") or "").strip() or None,
        "email": (row.get("petitioner_email") or "").strip() or None,
        "address": {
            "street": pet_addr.get("street"),
            "city": pet_addr.get("city"),
            "state": pet_state,
            "zip": pet_addr.get("zip"),
        } if pet_addr else None,
        "oos": bool(pet_state and pet_state != "NY"),
        "role": row.get("petitioner_role"),
        "active": row.get("petitioner_active"),
    }

    # Out-of-state any-person flag (petitioner OR heirs in tiered)
    heirs_oos = [p for p in persons if p["oos"] and p["role"] in ("heir", "petitioner")]

    el = row.get("eliteress_data") or {}
    parcel = el.get("primary_parcel") or {}
    owner_match = el.get("owner_match") or {}

    property_info = None
    if parcel:
        market_value = parcel.get("marketValue") or parcel.get("estimatedValue")
        equity = parcel.get("estimatedEquity")
        equity_pct = parcel.get("equityPercentage")
        property_info = {
            "address": parcel.get("address_label") or " ".join(filter(None, [
                parcel.get("street"), parcel.get("city"), parcel.get("state"), parcel.get("zip"),
            ])),
            "street": parcel.get("street"),
            "city": parcel.get("city"),
            "state": parcel.get("state"),
            "zip": parcel.get("zip"),
            "county": parcel.get("countyName") or parcel.get("county"),
            "owner1": parcel.get("owner1"),
            "owner2": parcel.get("owner2"),
            "land_use": parcel.get("landUse"),
            "year_built": parcel.get("yearBuilt"),
            "beds": parcel.get("bedrooms"),
            "baths": parcel.get("bathrooms"),
            "sqft": parcel.get("squareFeet"),
            "lot_sqft": parcel.get("lotSquareFeet"),
            "market_value": market_value,
            "market_value_fmt": _money(market_value),
            "estimated_value": parcel.get("estimatedValue"),
            "estimated_value_fmt": _money(parcel.get("estimatedValue")),
            "estimated_equity": equity,
            "estimated_equity_fmt": _money(equity),
            "equity_pct": equity_pct,
            "mortgage_balance": parcel.get("mortgageBalance"),
            "mortgage_balance_fmt": _money(parcel.get("mortgageBalance")),
            "apn": parcel.get("apn"),
            "lat": parcel.get("latitude"),
            "lng": parcel.get("longitude"),
        }

    decedent_name = (te.get("decedent") or {}).get("full_name") or el.get("decedent_name")
    decedent_dod = (te.get("decedent") or {}).get("date_of_death")

    # Geographic distribution: states of all people on this case
    state_counts: dict[str, int] = {}
    for p in persons:
        s = (p.get("address") or {}).get("state")
        if s:
            state_counts[s] = state_counts.get(s, 0) + 1

    return {
        "case_number": row.get("case_number"),
        "filed_date": row.get("filed_date"),
        "proceeding": row.get("proceeding"),
        "court": te.get("court"),
        "county": (parcel.get("countyName") or parcel.get("county")) if parcel else None,
        "decedent_name": decedent_name,
        "decedent_dod": decedent_dod,
        "pr": pr,
        "persons": persons,
        "n_persons": len(persons),
        "n_heirs": sum(1 for p in persons if p["role"] == "heir"),
        "n_petitioners": sum(1 for p in persons if p["role"] == "petitioner"),
        "heirs_oos_count": len(heirs_oos),
        "any_oos": pr["oos"] or any(p["oos"] for p in persons),
        "has_phone": bool(pr["phone"]) or any(p.get("phone") for p in persons),
        "has_email": bool(pr["email"]) or any(p.get("email") for p in persons),
        "property": property_info,
        "owner_match": {
            "level": owner_match.get("level"),
            "owner": owner_match.get("owner"),
            "score": owner_match.get("score"),
            "reason": owner_match.get("reason"),
        } if owner_match else None,
        "state_counts": state_counts,
        "additional_properties_count": len(el.get("additional_properties") or []),
        "eliteress_query": el.get("eliteress_query"),
        "eliteress_source": el.get("decedent_address_source"),
        "eliteress_raw": el,  # full payload for the drawer
    }


def _load_cases() -> list[dict]:
    sql = """
    SELECT case_number, filed_date, proceeding,
           petitioner, petitioner_address,
           petitioner_phone, petitioner_email, petitioner_role, petitioner_active,
           tiered_extraction, eliteress_data
    FROM records
    WHERE eliteress_data IS NOT NULL
      AND proceeding NOT IN (
        'VOLUNTARY ADMIN AFFIDAVIT (ARTICLE 13) WITH WILL',
        'VOLUNTARY ADMIN AFFIDAVIT (ARTICLE 13) WITHOUT WILL'
      )
      AND NULLIF(filed_date, '')::date BETWEEN %s AND %s
    ORDER BY NULLIF(filed_date, '')::date DESC, case_number
    """
    with psycopg2.connect(_dsn(), connect_timeout=10) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (date(2026, 4, 1), date(2026, 4, 30)))
            rows = cur.fetchall()
    return [_shape_case(dict(r)) for r in rows]


@dossier_bp.route("/dossier")
def dossier_page():
    return render_template("dossier.html")


@dossier_bp.route("/dossier/api/cases")
def dossier_cases():
    cases = _load_cases()
    # Aggregate stats for the header strip
    total = len(cases)
    oos = sum(1 for c in cases if c["any_oos"])
    with_phone = sum(1 for c in cases if c["has_phone"])
    with_email = sum(1 for c in cases if c["has_email"])
    with_match = sum(1 for c in cases if c.get("owner_match") and c["owner_match"].get("level") == "full")
    # Sum only positive equity — some parcels have mortgage > value (negative
    # equity) and that's a mailing signal, not something to add into a "total
    # equity captured" headline.
    total_equity = sum(
        max(0, c["property"]["estimated_equity"] or 0)
        for c in cases
        if c.get("property") and isinstance(c["property"].get("estimated_equity"), (int, float))
    )
    counties = sorted({c["county"] for c in cases if c.get("county")})
    proceedings = sorted({c["proceeding"] for c in cases if c.get("proceeding")})
    return jsonify({
        "cases": cases,
        "stats": {
            "total": total,
            "oos": oos,
            "with_phone": with_phone,
            "with_email": with_email,
            "with_match": with_match,
            "total_equity": total_equity,
            "total_equity_fmt": _money(total_equity),
        },
        "counties": counties,
        "proceedings": proceedings,
    })
