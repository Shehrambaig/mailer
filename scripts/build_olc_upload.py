"""Build the OLC upload CSV from the weekly Person + LLC/Trust consolidated files.

Reproduces the AUG22 pipeline (2026-08-11 → 2026-08-13) exactly:

  1. mailability filter  DO_NOT_MAIL != 'Y' and Mailing Deliverable == 'Y'
     (last week's "Combined MAILABLE …" sources arrived pre-filtered; the
     "Consolidated Unique …" files do not, so the filter lives here now)
  2. map to the 19-column OLC upload format (check-window field hijack)
  3. dedupe on the canonical mailing-address key, highest offer wins
  4. drop offers outside --min-offer … $2,500,000 ("Call for Price" rows exempt;
     the floor was $20k for AUG22, raised to $50k on 2026-08-18)

Every dropped row is written to a review CSV so nothing disappears silently.

    python scripts/build_olc_upload.py \
        --person  "~/Downloads/Consolidated Unique Person 2026-08-17.csv" \
        --entity  "~/Downloads/Consolidated Unique LLC+Trust 2026-08-17.csv" \
        --out     "~/Downloads/olc-upload-2026-08-17.csv"

    # replay last week and diff against the shipped files
    python scripts/build_olc_upload.py --person ... --entity ... --out /tmp/x.csv \
        --no-mailability-filter --verify-combined <combined.csv> --verify-final <final.csv>
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter

csv.field_size_limit(10 ** 9)

OUT_COLUMNS = [
    "First Name", "Last Name", "Company Name",
    "Address 1", "City", "State", "Zip Code",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Offer Amount", "Offer Amount Words",
    "Return Name", "Return Address", "Return City", "Return State", "Return Zip",
    "Return Phone",
]

# Sender block printed on the letter (SPF.* merge fields). Reference columns
# only — the order payload carries them as returnAddress.
RETURN = {
    "Return Name": "Gabriel Guerra",
    "Return Address": "4212 San Felipe St #400",
    "Return City": "Houston",
    "Return State": "TX",
    "Return Zip": "77027",
    "Return Phone": "(470) 600-2477",
}

CALL_FOR_PRICE = "Call for Price"
MIN_OFFER = 50_000      # user raised the floor from 20k on 2026-08-18
MAX_OFFER = 2_500_000

_ONES = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight",
         "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen",
         "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
_TENS = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy",
         "Eighty", "Ninety"]


def _words_under_1000(n: int) -> str:
    parts = []
    if n >= 100:
        parts.append(f"{_ONES[n // 100]} Hundred")
        n %= 100
    if n >= 20:
        word = _TENS[n // 10]
        if n % 10:
            word += f"-{_ONES[n % 10]}"
        parts.append(word)
    elif n:
        parts.append(_ONES[n])
    return " ".join(parts)


def dollars_in_words(amount) -> str:
    """175000 -> 'One Hundred Seventy-Five Thousand' (no trailing ' Dollars':
    the 11736 check art prints its own label). Mirrors olc_send.py."""
    n = int(round(float(amount)))
    if n <= 0 or n >= 1_000_000_000:
        return ""
    parts = []
    for scale, label in ((1_000_000, "Million"), (1_000, "Thousand"), (1, "")):
        if n >= scale:
            parts.append(f"{_words_under_1000(n // scale)} {label}".strip())
            n %= scale
    return " ".join(parts)


def norm(s: str) -> str:
    """Canonical address token — matches dedup_mailer.py / MAILER_KEY_SQL."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())).strip()


def addr_key(row: dict) -> str:
    return "|".join((norm(row["Address 1"]), norm(row["City"]),
                     norm(row["State"]), row["Zip Code"][:5]))


def zip5(value: str) -> str:
    return (value or "").strip().split("-")[0].zfill(5)


def load(path: str) -> list[dict]:
    with open(os.path.expanduser(path), encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def to_olc_row(src: dict, kind: str) -> dict:
    """One consolidated row -> one OLC upload row.

    The check window prints "FIRST LAST" then companyName, so the name fields
    are hijacked to read "PAY TO THE ORDER OF:" above the real payee.
    People are title-cased; entity names pass through verbatim (they are
    already upper in the source and the exact legal string matters).
    """
    name = f"{src['First Name']} {src['Last Name']}".strip()
    row = {
        "First Name": "PAY TO THE",
        "Last Name": "ORDER OF:",
        "Company Name": name.title() if kind == "person" else name,
        "Address 1": src["Mailing Address"].title(),
        "City": src["Mailing City"].title(),
        "State": src["Mailing State"].upper(),
        "Zip Code": zip5(src["Mailing Zip"]),
        "Property Address": src["Property Address"],
        "Property City": src["Property City"],
        "Property State": src["Property State"],
        "Property Zip": zip5(src["Property Zip"]),
    }
    offer = (src.get("Offer Value") or "").strip()
    if offer == CALL_FOR_PRICE or not offer:
        row["Offer Amount"] = CALL_FOR_PRICE
        row["Offer Amount Words"] = CALL_FOR_PRICE
    else:
        row["Offer Amount"] = f"${float(offer):,.0f}"
        row["Offer Amount Words"] = dollars_in_words(offer)
    row.update(RETURN)
    return row


def offer_number(row: dict):
    """Numeric offer, or None for Call-for-Price rows."""
    if row["Offer Amount"] == CALL_FOR_PRICE:
        return None
    return int(row["Offer Amount"].lstrip("$").replace(",", ""))


def write_csv(path: str, rows: list[dict], columns: list[str]) -> None:
    path = os.path.expanduser(path)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--person", required=True)
    ap.add_argument("--entity", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-offer", type=int, default=MIN_OFFER,
                    help=f"offer floor (default ${MIN_OFFER:,}; AUG22 used $20,000)")
    ap.add_argument("--no-mailability-filter", action="store_true",
                    help="sources are already filtered (last week's MAILABLE files)")
    ap.add_argument("--verify-combined", help="expected post-map, pre-dedupe CSV")
    ap.add_argument("--verify-final", help="expected post-dedupe/bounds CSV")
    args = ap.parse_args()
    min_offer = args.min_offer

    stats: dict = {}
    combined: list[dict] = []
    dropped: list[dict] = []

    for kind, path in (("person", args.person), ("entity", args.entity)):
        src_rows = load(path)
        stats[f"{kind}_in"] = len(src_rows)
        kept = 0
        for src in src_rows:
            reason = None
            if not args.no_mailability_filter:
                if (src.get("DO_NOT_MAIL") or "").strip().upper() == "Y":
                    reason = f"DO_NOT_MAIL ({src.get('DNM Reason') or 'no reason given'})"
                elif (src.get("Mailing Deliverable") or "").strip().upper() != "Y":
                    reason = (f"not deliverable (Mailing Deliverable="
                              f"{src.get('Mailing Deliverable') or 'blank'!r}, "
                              f"PG status={src.get('Postgrid Mailing Verification Status') or 'blank'})")
            if not reason and not (src.get("Mailing Address") or "").strip():
                reason = "no mailing address"
            if reason:
                dropped.append({"stage": "mailability", "kind": kind, "reason": reason,
                                "name": f"{src['First Name']} {src['Last Name']}".strip(),
                                "mailing": " ".join(filter(None, [
                                    src.get("Mailing Address"), src.get("Mailing City"),
                                    src.get("Mailing State"), src.get("Mailing Zip")])),
                                "property": " ".join(filter(None, [
                                    src.get("Property Address"), src.get("Property City"),
                                    src.get("Property State"), src.get("Property Zip")])),
                                "offer": src.get("Offer Value", "")})
                continue
            combined.append(to_olc_row(src, kind))
            kept += 1
        stats[f"{kind}_mailable"] = kept

    stats["combined"] = len(combined)

    if args.verify_combined:
        expected = load(args.verify_combined)
        diffs = [i for i, (a, b) in enumerate(zip(combined, expected)) if a != b]
        print(f"verify combined: built {len(combined)} vs expected {len(expected)}, "
              f"{len(diffs)} differing rows")
        for i in diffs[:3]:
            print("  built   ", combined[i])
            print("  expected", expected[i])

    # ── dedupe: one piece per mailing address, highest offer wins ──────────
    best: dict[str, dict] = {}
    for row in combined:
        key = addr_key(row)
        prev = best.get(key)
        if prev is None:
            best[key] = row
            continue
        a, b = offer_number(row) or -1, offer_number(prev) or -1
        if a > b:
            dropped.append({"stage": "dup-address", "kind": "", "reason":
                            f"duplicate mailing address, kept ${a:,} over ${b:,}",
                            "name": prev["Company Name"],
                            "mailing": f"{prev['Address 1']} {prev['City']} {prev['State']} {prev['Zip Code']}",
                            "property": f"{prev['Property Address']} {prev['Property City']}",
                            "offer": prev["Offer Amount"]})
            best[key] = row
        else:
            dropped.append({"stage": "dup-address", "kind": "", "reason":
                            f"duplicate mailing address, kept ${b:,} over ${a:,}",
                            "name": row["Company Name"],
                            "mailing": f"{row['Address 1']} {row['City']} {row['State']} {row['Zip Code']}",
                            "property": f"{row['Property Address']} {row['Property City']}",
                            "offer": row["Offer Amount"]})
    deduped = list(best.values())
    stats["deduped"] = len(deduped)
    stats["dropped_duplicate"] = len(combined) - len(deduped)

    # ── offer bounds ───────────────────────────────────────────────────────
    final = []
    for row in deduped:
        amount = offer_number(row)
        if amount is not None and amount > MAX_OFFER:
            dropped.append({"stage": "offer-high", "kind": "", "reason":
                            f"offer ${amount:,} over ${MAX_OFFER:,}",
                            "name": row["Company Name"],
                            "mailing": f"{row['Address 1']} {row['City']} {row['State']} {row['Zip Code']}",
                            "property": f"{row['Property Address']} {row['Property City']}",
                            "offer": row["Offer Amount"]})
        elif amount is not None and amount < min_offer:
            dropped.append({"stage": "offer-low", "kind": "", "reason":
                            f"offer ${amount:,} under ${min_offer:,}",
                            "name": row["Company Name"],
                            "mailing": f"{row['Address 1']} {row['City']} {row['State']} {row['Zip Code']}",
                            "property": f"{row['Property Address']} {row['Property City']}",
                            "offer": row["Offer Amount"]})
        else:
            final.append(row)
    stats["dropped_offer_high"] = sum(1 for d in dropped if d["stage"] == "offer-high")
    stats["dropped_offer_low"] = sum(1 for d in dropped if d["stage"] == "offer-low")
    stats["final"] = len(final)
    stats["call_for_price"] = sum(1 for r in final if r["Offer Amount"] == CALL_FOR_PRICE)

    if args.verify_final:
        expected = load(args.verify_final)
        exp_keys = {addr_key(r) for r in expected}
        got_keys = {addr_key(r) for r in final}
        print(f"verify final: built {len(final)} vs expected {len(expected)}; "
              f"only-in-built {len(got_keys - exp_keys)}, only-in-expected {len(exp_keys - got_keys)}")

    write_csv(args.out, final, OUT_COLUMNS)
    review = os.path.splitext(os.path.expanduser(args.out))[0] + "-dropped.csv"
    write_csv(review, dropped,
              ["stage", "kind", "reason", "name", "mailing", "property", "offer"])

    print(json.dumps(stats, indent=2))
    print(f"\nwrote {len(final):,} rows -> {os.path.expanduser(args.out)}")
    print(f"wrote {len(dropped):,} dropped rows -> {review}")
    print("drop breakdown:", dict(Counter(d["stage"] for d in dropped)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
