"""
Build a 2020-vintage ZIP→county crosswalk from Census TIGER files.

Inputs (both EPSG:4326, produced via ogr2ogr from official Census TIGER 2020):
  data/zcta_2020_us.geojson      — 33,791 ZCTA polygons
  data/counties_2020_us.geojson  — 3,234 county polygons

Output:
  app/static/data/zip-by-county-2020.json  — {fips: [zip5, ...]}

Also prints a diff against the existing app/static/data/zip-by-county.json.
"""
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

from shapely.geometry import shape
from shapely.strtree import STRtree

ROOT = Path(__file__).resolve().parent.parent
ZCTA   = ROOT / "data" / "zcta_2020_us.geojson"
COUNTY = ROOT / "data" / "counties_2020_us.geojson"
OUT    = ROOT / "app" / "static" / "data" / "zip-by-county-2020.json"
OLD    = ROOT / "app" / "static" / "data" / "zip-by-county.json"


def t(msg, t0):
    print(f"[{time.time()-t0:5.1f}s] {msg}", flush=True)


def main():
    t0 = time.time()

    t(f"Loading counties: {COUNTY.name} ({COUNTY.stat().st_size/1024/1024:.0f} MB)", t0)
    with open(COUNTY) as f:
        counties = json.load(f)

    county_shapes, county_fips, county_name = [], [], []
    for feat in counties["features"]:
        p = feat.get("properties") or {}
        fips = (p.get("GEOID") or "").zfill(5)
        if len(fips) != 5:
            continue
        try:
            county_shapes.append(shape(feat["geometry"]))
            county_fips.append(fips)
            county_name.append(p.get("NAMELSAD") or p.get("NAME") or "")
        except Exception:
            pass
    t(f"  {len(county_shapes):,} county polygons indexed", t0)

    tree = STRtree(county_shapes)
    t("STRtree built", t0)

    t(f"Loading ZCTAs: {ZCTA.name} ({ZCTA.stat().st_size/1024/1024:.0f} MB)", t0)
    with open(ZCTA) as f:
        zctas = json.load(f)
    t(f"  {len(zctas['features']):,} ZIP polygons", t0)

    zip_to_fips = {}
    misses = []
    skipped_geom = 0
    for i, feat in enumerate(zctas["features"]):
        if i and i % 5000 == 0:
            t(f"  ...PIP {i:>6,}/{len(zctas['features']):,} matched={len(zip_to_fips):,} miss={len(misses):,}", t0)
        p = feat.get("properties") or {}
        zip5 = (p.get("ZCTA5CE20") or p.get("GEOID20") or "").zfill(5)
        if len(zip5) != 5:
            continue
        try:
            poly = shape(feat["geometry"])
        except Exception:
            skipped_geom += 1
            continue
        try:
            point = poly.representative_point()
        except Exception:
            point = poly.centroid
        matched = False
        for j in tree.query(point):
            if county_shapes[j].contains(point):
                zip_to_fips[zip5] = county_fips[j]
                matched = True
                break
        if not matched:
            # Fallback: nearest county centroid (handles ZCTAs that fall in
            # water-only polygons or just outside any county polygon).
            best, best_d = None, float("inf")
            for j in tree.query(point.buffer(0.5)):
                d = county_shapes[j].distance(point)
                if d < best_d:
                    best_d, best = d, j
            if best is not None and best_d < 0.05:  # ~5 km tolerance
                zip_to_fips[zip5] = county_fips[best]
            else:
                misses.append((zip5, point.x, point.y))

    t(f"PIP done. matched={len(zip_to_fips):,} miss={len(misses):,} skipped_geom={skipped_geom}", t0)
    if misses[:5]:
        print("  sample misses:", misses[:5])

    fips_to_zips = defaultdict(set)
    for zip5, fips in zip_to_fips.items():
        fips_to_zips[fips].add(zip5)
    out = {fips: sorted(zips) for fips, zips in fips_to_zips.items()}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    t(f"Wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size/1024:.0f} KB · {len(out):,} counties · {sum(len(z) for z in out.values()):,} ZIPs)", t0)

    # ── Diff against existing crosswalk ──────────────────────────────────
    if OLD.exists():
        t(f"Diffing vs. existing {OLD.relative_to(ROOT)}", t0)
        with open(OLD) as f:
            old = json.load(f)
        old_zip_to_fips = {z: f for f, zs in old.items() for z in zs}
        new_zip_to_fips = zip_to_fips

        added   = set(new_zip_to_fips) - set(old_zip_to_fips)
        dropped = set(old_zip_to_fips) - set(new_zip_to_fips)
        common  = set(old_zip_to_fips) & set(new_zip_to_fips)
        moved   = {z: (old_zip_to_fips[z], new_zip_to_fips[z])
                   for z in common if old_zip_to_fips[z] != new_zip_to_fips[z]}

        print()
        print("─" * 60)
        print(f"  ZIPs in OLD only (dropped):    {len(dropped):>6,}")
        print(f"  ZIPs in NEW only (added):      {len(added):>6,}")
        print(f"  Common ZIPs:                   {len(common):>6,}")
        print(f"  Common but reassigned to new FIPS: {len(moved):>5,}")
        print("─" * 60)

        if moved:
            print("\nSample reassignments (zip: old_fips → new_fips):")
            for z, (a, b) in list(moved.items())[:15]:
                print(f"  {z}: {a} → {b}")
        if added:
            print(f"\nSample added ZIPs (in 2020, not in old): {sorted(added)[:15]}")
        if dropped:
            print(f"\nSample dropped ZIPs (in old, not in 2020): {sorted(dropped)[:15]}")
    else:
        t("No existing crosswalk found — skipping diff", t0)


if __name__ == "__main__":
    main()
