"""
Build simplified ZCTA (ZIP) polygons + a ZIP→county membership map.

Outputs:
  app/static/data/zcta/<state>.json   — simplified GeoJSON per state
  app/static/data/zip-by-county.json  — {fips: [zip5, ...]} (county → ZIPs)

Source: OpenDataDE/State-zip-code-GeoJSON (Census ZCTA boundaries, public domain).
County polygons reused from data/counties-fips.geojson for point-in-polygon.

Usage:
  python scripts/build_zcta.py                    # all 50 states + DC
  python scripts/build_zcta.py --states ID CA     # subset
  python scripts/build_zcta.py --tolerance 0.005  # simplification (default 0.005)
"""
import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shapely.geometry import shape, mapping, Point  # noqa: E402
from shapely.strtree import STRtree  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CACHE_DIR = os.path.join(ROOT, "data", "_zcta_cache")
OUT_DIR   = os.path.join(ROOT, "app", "static", "data", "zcta")
COUNTY_GEO = os.path.join(ROOT, "data", "counties-fips.geojson")

STATES = {
    "AL":"alabama","AK":"alaska","AZ":"arizona","AR":"arkansas","CA":"california",
    "CO":"colorado","CT":"connecticut","DE":"delaware","DC":"district_of_columbia",
    "FL":"florida","GA":"georgia","HI":"hawaii","ID":"idaho","IL":"illinois",
    "IN":"indiana","IA":"iowa","KS":"kansas","KY":"kentucky","LA":"louisiana",
    "ME":"maine","MD":"maryland","MA":"massachusetts","MI":"michigan","MN":"minnesota",
    "MS":"mississippi","MO":"missouri","MT":"montana","NE":"nebraska","NV":"nevada",
    "NH":"new_hampshire","NJ":"new_jersey","NM":"new_mexico","NY":"new_york",
    "NC":"north_carolina","ND":"north_dakota","OH":"ohio","OK":"oklahoma","OR":"oregon",
    "PA":"pennsylvania","RI":"rhode_island","SC":"south_carolina","SD":"south_dakota",
    "TN":"tennessee","TX":"texas","UT":"utah","VT":"vermont","VA":"virginia",
    "WA":"washington","WV":"west_virginia","WI":"wisconsin","WY":"wyoming",
}

URL_TMPL = "https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/{abbr_lc}_{name}_zip_codes_geo.min.json"


def _download(url, dest):
    if os.path.exists(dest) and os.path.getsize(dest) > 1000:
        return dest
    print(f"  [get] {os.path.basename(dest)}  ←  {url}")
    tmp = dest + ".part"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=180) as resp, open(tmp, "wb") as f:
        while True:
            chunk = resp.read(64 * 1024)
            if not chunk:
                break
            f.write(chunk)
    os.replace(tmp, dest)
    return dest


def _load_county_index():
    """Build STRtree over county polygons. Returns (tree, fips_list, shapes_list)."""
    print(f"Loading county polygons from {os.path.relpath(COUNTY_GEO, ROOT)}")
    with open(COUNTY_GEO) as f:
        geo = json.load(f)
    shapes, fips_list = [], []
    for feat in geo["features"]:
        fips = str(feat.get("id") or feat.get("properties", {}).get("GEOID") or "").zfill(5)
        if len(fips) != 5:
            continue
        try:
            shapes.append(shape(feat["geometry"]))
            fips_list.append(fips)
        except Exception:
            pass
    print(f"  indexed {len(shapes)} county polygons")
    return STRtree(shapes), fips_list, shapes


def _zip_field(props):
    """Find the ZIP code field. OpenDataDE uses 'ZCTA5CE10' or 'ZCTA5CE20'."""
    for k in ("ZCTA5CE20", "ZCTA5CE10", "ZCTA5", "GEOID", "GEOID20", "ZIP", "ZIPCODE"):
        v = props.get(k)
        if v:
            return str(v).zfill(5)
    return None


def process_state(abbr, tree, county_fips, county_shapes, tolerance):
    name = STATES[abbr]
    abbr_lc = abbr.lower()
    url = URL_TMPL.format(abbr_lc=abbr_lc, name=name)
    cache = os.path.join(CACHE_DIR, f"{abbr_lc}_{name}.geojson")
    _download(url, cache)

    with open(cache) as f:
        gj = json.load(f)

    out_features = []
    zip_to_fips = {}
    skipped = 0
    for feat in gj.get("features", []):
        props = feat.get("properties") or {}
        z = _zip_field(props)
        if not z or len(z) != 5:
            skipped += 1
            continue
        try:
            poly = shape(feat["geometry"])
        except Exception:
            skipped += 1
            continue

        # County membership via centroid PIP (cheap; sufficient for assignment).
        try:
            cen = poly.representative_point()
        except Exception:
            cen = poly.centroid
        for i in tree.query(cen):
            if county_shapes[i].contains(cen):
                zip_to_fips[z] = county_fips[i]
                break

        # Simplify geometry for transport.
        try:
            simp = poly.simplify(tolerance, preserve_topology=True)
            if simp.is_empty:
                simp = poly
        except Exception:
            simp = poly

        out_features.append({
            "type": "Feature",
            "id": z,
            "properties": {"zip": z, "fips": zip_to_fips.get(z, "")},
            "geometry": mapping(simp),
        })

    out_path = os.path.join(OUT_DIR, f"{abbr}.json")
    with open(out_path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": out_features}, f, separators=(",", ":"))
    raw_size = os.path.getsize(cache) / 1024 / 1024
    out_size = os.path.getsize(out_path) / 1024 / 1024
    print(f"  {abbr}: {len(out_features):>5} ZIPs · raw {raw_size:>5.1f} MB → simplified {out_size:>5.1f} MB"
          + (f" · {skipped} skipped" if skipped else ""))
    return zip_to_fips


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", nargs="+", help="state abbreviations (default: all)")
    ap.add_argument("--tolerance", type=float, default=0.005,
                    help="shapely simplification tolerance in degrees (default: 0.005)")
    args = ap.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    tree, county_fips, county_shapes = _load_county_index()

    targets = [s.upper() for s in (args.states or list(STATES.keys()))]
    bad = [s for s in targets if s not in STATES]
    if bad:
        print(f"Unknown states: {bad}")
        sys.exit(1)

    # Merge with existing zip-by-county.json if present (so partial runs accumulate).
    membership_path = os.path.join(ROOT, "app", "static", "data", "zip-by-county.json")
    fips_to_zips = {}
    if os.path.exists(membership_path):
        try:
            with open(membership_path) as f:
                fips_to_zips = json.load(f)
        except Exception:
            fips_to_zips = {}

    # Drop existing entries for states we're about to rebuild so we don't keep stale ZIPs.
    for fips in list(fips_to_zips.keys()):
        if any(fips.startswith(_state_fips_prefix(s)) for s in targets):
            del fips_to_zips[fips]

    print(f"\nProcessing {len(targets)} state(s) (tolerance={args.tolerance})")
    for abbr in targets:
        try:
            zip_to_fips = process_state(abbr, tree, county_fips, county_shapes, args.tolerance)
        except Exception as e:
            print(f"  {abbr}: FAILED — {e}")
            continue
        # Merge this state's assignments into fips_to_zips and write incrementally.
        for z, fips in zip_to_fips.items():
            fips_to_zips.setdefault(fips, []).append(z)
        for fips in list(fips_to_zips.keys()):
            fips_to_zips[fips] = sorted(set(fips_to_zips[fips]))
        with open(membership_path, "w") as f:
            json.dump(fips_to_zips, f, separators=(",", ":"))

    print(f"\nWrote {membership_path}")
    print(f"  {len(fips_to_zips)} counties contain ZIPs · "
          f"{sum(len(v) for v in fips_to_zips.values())} ZIP↔county assignments")


_STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19",
    "KS":"20","KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27",
    "MS":"28","MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35",
    "NY":"36","NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44",
    "SC":"45","SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53",
    "WV":"54","WI":"55","WY":"56",
}

def _state_fips_prefix(abbr):
    return _STATE_FIPS.get(abbr, "")


if __name__ == "__main__":
    main()
