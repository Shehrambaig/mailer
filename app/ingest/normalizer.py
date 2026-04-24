import re


def normalize_state(state):
    if not state:
        return None
    return state.strip().upper()[:2]


def normalize_zip(zip_code):
    if not zip_code:
        return None
    z = str(zip_code).strip()
    # Handle float zips like 63050.0
    if "." in z:
        z = z.split(".")[0]
    # Keep 5-digit or 5+4 format
    z = re.sub(r"[^\d-]", "", z)
    return z[:10] if z else None


def normalize_street(street):
    """Clean street address. Handle embedded full addresses like '400 Forest Drive, North Syracuse, NY 13212'."""
    if not street:
        return None
    s = street.strip()
    # Check for embedded city, state, zip pattern: "street, city, ST ZIP"
    # If there's a state abbreviation followed by a zip, it's probably a full address
    match = re.match(r"^(.+?),\s*[^,]+,\s*[A-Za-z]{2}\s+\d{5}", s)
    if match:
        s = match.group(1).strip()
    return s.upper()


def pick_best_address(row_dict):
    """Pick the best address from Postgrid-validated, USPS-standardized, or raw fields.

    Returns dict with street, city, state, zip_code, address_verified, dpv_match_code.
    """
    dpv = row_dict.get("postgrid_dpv_match_code")
    pg_street = row_dict.get("postgrid_street")
    usps_street = row_dict.get("usps_street")
    raw_street = row_dict.get("raw_street")

    # Priority 1: Postgrid-validated with DPV match
    if dpv == "Y" and pg_street:
        return {
            "street": pg_street.strip().upper(),
            "city": (row_dict.get("postgrid_city") or row_dict.get("raw_city", "")).strip().upper(),
            "state": normalize_state(row_dict.get("postgrid_state") or row_dict.get("raw_state")),
            "zip_code": normalize_zip(row_dict.get("postgrid_zip") or row_dict.get("raw_zip")),
            "address_verified": True,
            "dpv_match_code": "Y",
        }

    # Priority 2: USPS-standardized address (if it doesn't contain embedded city/state)
    if usps_street and "," not in usps_street:
        return {
            "street": usps_street.strip().upper(),
            "city": (row_dict.get("raw_city") or "").strip().upper(),
            "state": normalize_state(row_dict.get("raw_state")),
            "zip_code": normalize_zip(row_dict.get("raw_zip")),
            "address_verified": False,
            "dpv_match_code": dpv,
        }

    # Priority 3: Raw address (cleaned)
    street = normalize_street(raw_street or row_dict.get("street_address", ""))
    return {
        "street": street or "",
        "city": (row_dict.get("raw_city") or row_dict.get("municipality") or "").strip().upper(),
        "state": normalize_state(row_dict.get("raw_state") or row_dict.get("state")),
        "zip_code": normalize_zip(row_dict.get("raw_zip") or row_dict.get("postal_code")),
        "address_verified": False,
        "dpv_match_code": dpv,
    }


def calculate_offer(estimated_value, pct=0.85):
    if not estimated_value:
        return None
    try:
        return int(float(estimated_value) * pct)
    except (ValueError, TypeError):
        return None


def extract_party(row, role_target):
    """Extract party name by role from party_1 through party_5 fields."""
    for i in range(1, 6):
        role = row.get(f"party_{i}_role")
        name = row.get(f"party_{i}_name")
        if role and role_target.lower() in role.lower():
            return name
    return None


def safe_int(val):
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
