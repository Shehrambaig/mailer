"""Render view-proof PDFs for hand-picked rows of an OLC upload CSV.

POST /orders/view-proof is free and mails nothing — it renders the template
with one recipient's real data and returns the PDF. The return contact is
passed explicitly (the .env OLC_RETURN_* block still holds the retired
Carlson persona and no postal address), so proofs match what the AUG22
orders actually printed.

    python scripts/olc_proofs.py --csv ~/Downloads/olc-upload-2026-08-17.csv \
        --out proofs/2026-08-17 --picks picks.json [--dry-run]
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import sys

import requests
from dotenv import load_dotenv

csv.field_size_limit(10 ** 9)

TEMPLATE_ID = 11736  # "Faiz text/Gabe Open Letter Team Fixed" — active template
PRODUCT_ID = 18      # 8.5x11 Snap Pack Mailer, First Class


def contact_from_row(row: dict) -> dict:
    """CSV row -> OLC contact, identical to what an order would carry."""
    custom = {
        "Offer Amount": row["Offer Amount"],
        "Offer Amount Words": row["Offer Amount Words"],
    }
    contact = {
        "firstName": row["First Name"],
        "lastName": row["Last Name"],
        "companyName": row["Company Name"],
        "address1": row["Address 1"],
        "city": row["City"],
        "state": row["State"],
        "zip": row["Zip Code"],
        "propertyAddress": row["Property Address"],
        "propertyCity": row["Property City"],
        "propertyState": row["Property State"],
        "propertyZip": row["Property Zip"],
    }
    # Flat keys are what view-proof reads; the nested block is the documented
    # contact shape. Send both so either merge path resolves.
    contact.update(custom)
    contact["customFields"] = custom
    return contact


def return_contact(row: dict) -> dict:
    first, _, last = row["Return Name"].partition(" ")
    return {
        "firstName": first,
        "lastName": last,
        "address1": row["Return Address"],
        "city": row["Return City"],
        "state": row["Return State"],
        "zip": row["Return Zip"],
        "phoneNo": row["Return Phone"],
    }


def slug(text: str, limit: int = 44) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9]+", "-", text)).strip("-")[:limit]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--picks", required=True,
                    help='JSON list of {"row": <0-based index>, "label": "..."}')
    ap.add_argument("--dry-run", action="store_true",
                    help="print the payloads, call nothing")
    args = ap.parse_args()

    load_dotenv()
    rows = list(csv.DictReader(open(os.path.expanduser(args.csv), encoding="utf-8-sig")))
    picks = json.load(open(os.path.expanduser(args.picks)))
    out_dir = os.path.expanduser(args.out)
    os.makedirs(out_dir, exist_ok=True)

    base = os.getenv("OLC_BASE_URL", "").rstrip("/")
    key = os.getenv("OLC_API_KEY")
    if not args.dry_run and not (base and key):
        print("OLC_BASE_URL / OLC_API_KEY missing from .env", file=sys.stderr)
        return 1

    manifest = []
    for i, pick in enumerate(picks, start=1):
        row = rows[pick["row"]]
        payload = {
            "templateId": TEMPLATE_ID,
            "contact": contact_from_row(row),
            "returnContact": return_contact(row),
        }
        name = f"{i:02d}-{pick['label']}-{slug(row['Company Name'])}.pdf"
        print(f"\n[{i}/{len(picks)}] {pick['label']}: {row['Company Name']} — "
              f"{row['Address 1']}, {row['City']} {row['State']} {row['Zip Code']} — "
              f"{row['Offer Amount']}")
        if args.dry_run:
            print(json.dumps(payload, indent=2))
            continue

        resp = requests.post(
            f"{base}/orders/view-proof", json=payload,
            headers={"Authorization": f"Bearer {key}"}, timeout=300,
        )
        if resp.status_code >= 400:
            print(f"   FAILED {resp.status_code}: {resp.text[:300]}")
            manifest.append({**pick, "company": row["Company Name"],
                             "error": f"{resp.status_code} {resp.text[:200]}"})
            continue
        b64 = ((resp.json() or {}).get("data") or {}).get("base64")
        if not b64:
            print(f"   no PDF in response: {resp.text[:200]}")
            continue
        path = os.path.join(out_dir, name)
        with open(path, "wb") as fh:
            fh.write(base64.b64decode(b64))
        print(f"   saved {name} ({os.path.getsize(path) / 1e6:.1f} MB)")
        manifest.append({
            **pick, "file": name, "company": row["Company Name"],
            "mailing": f"{row['Address 1']}, {row['City']}, {row['State']} {row['Zip Code']}",
            "property": f"{row['Property Address']}, {row['Property City']}, "
                        f"{row['Property State']} {row['Property Zip']}",
            "offer": row["Offer Amount"], "words": row["Offer Amount Words"],
        })

    if not args.dry_run:
        with open(os.path.join(out_dir, "manifest.json"), "w") as fh:
            json.dump({"templateId": TEMPLATE_ID, "productId": PRODUCT_ID,
                       "source": args.csv, "proofs": manifest}, fh, indent=2)
        print(f"\nwrote {len(manifest)} proofs -> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
