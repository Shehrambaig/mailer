"""Build the /scrapers report snapshot.

One row per source written to `scraper_report_snapshot.data` (jsonb).
The Flask UI reads only from this snapshot, so the page stays fast.

Run:
    python scripts/build_scraper_report.py            # uses NEON_DB env
    python scripts/build_scraper_report.py --dsn ...  # explicit

Also (re)materializes a `derived_status` text column on each source table
that has a doc-override rule (NY, NC, GA, Gwinnett, OK).
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime, date
from typing import Any

import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Per-source declarative config

# Active "doc markers" (authority-grant) — informational only; not used to flip
# bucket. Used to render the reference panel on the UI.
# Close "doc markers" — case-insensitive substrings searched in the per-source
# doc-name JSON path. If a row's source status is "open" but any close marker
# matches, the row flips to `closed_by_doc`.

NY = {
    "key": "new_york",
    "name": "New York Surrogate's Court",
    "level": "State",
    "state": "New York",
    "table": "records",
    "date_col": "filed_date",
    "ts_col": "scrape_timestamp",
    "county_col": "county",
    "active_markers": [
        "DECREE GRANTING PROBATE",
        "DECREE GRANTING ADMINISTRATION",
        "LETTERS TESTAMENTARY",
        "LETTERS OF ADMINISTRATION",
    ],
    "close_markers": [
        "REPORT AND ACCOUNT IN SETTLEMENT OF ESTATE",
        "ORDER REVOKING LETTERS",
        # "RECEIPT" is too generic to match safely on document_names alone.
    ],
    "active_field": "petitioner_active (extracted from PDFs)",
    "active_values": "'Y' = active_qualified (Letters detected); '' = active_pending (filed, awaiting Letters / extraction inconclusive)",
    "closed_values": "'N' = closed (Letters revoked / not granted); close-marker doc in document_names = closed_by_doc",
    "non_owner": "Skipped — to be defined globally later.",
    "address_match_rule": (
        "If the filing exposes a decedent address, match it to BatchLeads as "
        "decedent_address = property (push to Tool 3 with match_tier='decedent_address'). "
        "When decedent address is missing (~33% of NY rows), fall back to strict "
        "Full+Middle+Last name match against propstream/elitress within the same "
        "county (match_tier='full_name_match')."
    ),
    "scraping_notes": (
        "Single statewide portal (NYSCEF) covers all 62 surrogate's courts. "
        "After scrape, an Anthropic-powered tier-extraction step parses each "
        "case's PDF bundle into tiered_extraction jsonb (decedent, persons[], "
        "real_property[]). petitioner_active is set during this step: 'Y' if "
        "Letters were granted, 'N' if revoked/denied, '' if undetectable."
    ),
}

NC = {
    "key": "north_carolina",
    "name": "North Carolina e-Courts",
    "level": "State",
    "state": "North Carolina",
    "table": "north_carolina",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
    "active_markers": [
        "OAIL",
        "Order Authorizing Issuance Of Letters",
        "LEFC",
        "Letters",
        "AFCP",
        "AFCT",
    ],
    "close_markers": [
        "AFDD",
        "Affidavit of Collection, Disbursement",
        "Order of Discharge",
        "Final Account",
        "Order Closing",
    ],
    "active_field": "documents[].event_type + documents[].document_name (NC has no top-level case_status)",
    "active_values": "'OAIL' / 'LEFC' / 'Letters' / 'AFCP' / 'AFCT' present = active_qualified (Letters/PR issued); no such doc = active_pending (filed, awaiting Letters)",
    "closed_values": "'AFDD' / 'Affidavit of Collection, Disbursement' / 'Order of Discharge' / 'Final Account' / 'Order Closing' = closed_by_doc",
    "non_owner": "Skipped — to be defined globally later.",
    "address_match_rule": (
        "NC parties[] carries decedent + PR addresses directly when the case "
        "has been scraped to detail. Match decedent_address (parties[].roles "
        "= Decedent → addresses[0]) to BatchLeads first; if missing, fall "
        "back to Full+Middle+Last name match within the same county. "
        "Beneficiary addresses are also captured per party and become a "
        "post-distribution mail bucket once a closure doc fires."
    ),
    "scraping_notes": (
        "Single statewide Tyler Technologies portal "
        "(portal-nc.tylertech.cloud) covers 100 counties. Guardianship "
        "cases (Ward / Guardian roles) are pulled by the same scraper but "
        "are filtered out everywhere on this page — they are not estate "
        "matters. NC has no top-level case_status; status is derived "
        "from documents[].event_type and document_name."
    ),
    # Filter expression appended as ` WHERE <exclude_sql>` everywhere we
    # aggregate. Guardianship rows are tagged 'guardianship' in
    # derived_status by src_nc(); we drop them here.
    "exclude_sql": "derived_status IS DISTINCT FROM 'guardianship'",
}

GA_COUNTY = {
    "key": "ga_probate",
    "name": "Georgia (Cobb + Rockdale)",
    "level": "County",
    "table": "ga_probate_cases",
    "date_col": "filed_date",
    "ts_col": "scraped_at",
    "county_col": "county",
    "active_markers": [
        "ORDER ADMITTING WILL",
        "ORDER APPOINTING ADMINISTRATOR",
        "ORDER, OATH, & LETTERS",
        "Letters Testamentary",
        "Letters of Administration",
    ],
    "close_markers": [
        "Discharge Order",
        "Petition for Discharge",
        "Final Order for Year's Support",
        "PYS Final Order",
        "NAN Final Order",
        "No Administration Necessary",
    ],
}

GWINNETT = {
    "key": "gwinnett",
    "name": "Gwinnett County, Georgia",
    "level": "County",
    "table": "gwinnett",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "location_name",
    "active_markers": [
        "Order Admitting Will",
        "Order Appointing",
        "Order, Oath, & Letters",
        "Letters Testamentary",
        "Letters of Administration",
    ],
    "close_markers": [
        "Discharge",
        "Final Order for Year's Support",
        "No Administration Necessary",
        "Petition for Discharge",
    ],
}

OK = {
    "key": "oklahoma",
    "name": "Oklahoma OSCN",
    "level": "State",
    "state": "Oklahoma",
    "table": "ok_probate_cases",
    "date_col": "filed_date",
    "ts_col": "scraped_at",
    "county_col": "county",
    "active_markers": [
        "ORDER ADMITTING WILL",
        "ORDER APPOINTING",
        "LETTERS TESTAMENTARY",
        "LETTERS OF ADMINISTRATION",
    ],
    "close_markers": [
        "FINAL DECREE OF DISTRIBUTION",
        "DECREE OF DISTRIBUTION",
        "ORDER OF DISCHARGE",
        "ORDER SETTLING FINAL ACCOUNT",
    ],
    "active_field": "closed_date + pdf_urls[].name (proxy)",
    "active_values": "closed_date IS NULL = active (default for filings without closure)",
    "closed_values": "closed_date IS NOT NULL = closed; 'FINAL DECREE OF DISTRIBUTION' / 'DECREE OF DISTRIBUTION' / 'ORDER OF DISCHARGE' / 'ORDER SETTLING FINAL ACCOUNT' in pdf_urls[].name = closed_by_doc",
    "non_owner": "Skipped — to be defined globally later.",
    "address_match_rule": (
        "Oklahoma source captures NO addresses in structured columns — "
        "all decedent and PR addresses live exclusively inside the PDFs. "
        "pdf_urls[] is populated for 93% of cases (454 / 486) but PDF tier "
        "extraction has not been wired in yet (pdf_extracted_at is NULL "
        "for every row). Until PDF extraction runs, the only match path "
        "is strict Full+Middle+Last name match against propstream / "
        "elitress within the same county."
    ),
    "scraping_notes": (
        "Single statewide OSCN portal (oscn.net) covers all 77 Oklahoma "
        "counties. Currently scraped: 12 counties (Oklahoma, Tulsa, "
        "Cleveland, Canadian dominate). The Final Decree of Distribution "
        "is the closure document — but it is also a high-value document "
        "for a different campaign: it lists distributees and the specific "
        "real properties they received, becoming a post-distribution heir "
        "outreach bucket once extraction is wired in. parties[] is sparse "
        "(15% of rows) and only carries first/middle/last/desc — no roles "
        "or addresses; it is mostly redundant with the petitioner_* columns."
    ),
}

CT = {
    "key": "connecticut",
    "name": "Connecticut Probate Court",
    "level": "State",
    "state": "Connecticut",
    "table": "ct_probate_cases",
    "date_col": "filed_date",
    "ts_col": "scraped_at",
    "county_col": "county",
    "active_field": "case_type_code (no actual status field exposed by the portal)",
    "active_values": "All filed cases default to active_pending — CT does not expose Letters / closure events",
    "closed_values": "Not detectable from the source today — closure would require docket / doc inspection (not yet scraped)",
    "non_owner": "Skipped — to be defined globally later.",
    "address_match_rule": (
        "CT does not capture decedent residence anywhere — only petitioner "
        "address lives in parties[].street (~11% of rows have an address; "
        "the rest only carry name parts). Match strategy: (1) parties[].street "
        "→ BatchLeads when present (PR is often a relative living at the "
        "decedent's property); (2) Full+Middle+Last name match against "
        "propstream/elitress within the same district / county. Decedent "
        "DOD is not captured by CT at all."
    ),
    "scraping_notes": (
        "Single statewide portal (ctprobate.gov) covers 60 probate districts "
        "(rolled up to 8 counties on this page). case_type_code is the only "
        "categorical signal: DR = Decedent's Estate Registration (main "
        "probate path, 885 rows); DAT = Tax-only filing (370); DT = "
        "transitional / trust (367); DS = Small Estate (91); DW = Will "
        "Only (61); TT/TO/TL = trust filings (39). DAT/DT/DW are not "
        "necessarily mailable estates — operator should filter by case_type_code."
    ),
}

IN_SRC = {
    "key": "indiana",
    "name": "Indiana mycase.in.gov",
    "level": "State",
    "table": "indiana",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
}

MD = {
    "key": "maryland",
    "name": "Maryland Register of Wills",
    "level": "State",
    "state": "Maryland",
    "table": "maryland",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
    "active_field": "case_status (set directly by the Register of Wills portal)",
    "active_values": "'OPEN' = active",
    "closed_values": "'CLOSED' = closed; NULL = unknown (failed scrape)",
    "non_owner": "Skipped — to be defined globally later.",
    "address_match_rule": (
        "Maryland source captures rep (PR) address but NOT decedent "
        "residence in structured columns — decedent residence is buried in "
        "raw_detail_html (3,149 of 3,160 rows have unparsed HTML). Until "
        "an HTML extractor backfills decedent residence, match strategy "
        "is: (1) rep_address → BatchLeads — many PRs are surviving "
        "spouses living at the decedent's property; (2) fall back to "
        "Full+Middle+Last name match against propstream/elitress within "
        "the same county."
    ),
    "scraping_notes": (
        "Single statewide portal (registers.maryland.gov / Estates) covers "
        "24 jurisdictions including Baltimore City as a separate locality. "
        "case_type codes — SE (small estate), RE (regular estate), UN, LO, "
        "MA, RJ, SJ, FP, NP — describe the filing path. Reliable "
        "case_status field (OPEN / CLOSED) means no doc-override is needed."
    ),
}

MI = {
    "key": "michigan",
    "name": "Michigan MiCOURT",
    "level": "State",
    "table": "michigan",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
}

MN = {
    "key": "minnesota",
    "name": "Minnesota MCRO",
    "level": "State",
    "table": "minnesota",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
}

WI = {
    "key": "wisconsin",
    "name": "Wisconsin Circuit Courts",
    "level": "State",
    "table": "wisconsin",
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
}

GPR = {
    "key": "georgia_probate_records",
    "name": "georgiaprobaterecords.com (statewide)",
    "level": "State aggregator",
    "table": "georgia",  # may not exist on Neon
    "date_col": "file_date",
    "ts_col": "scraped_at",
    "county_col": "county",
}

ALL_SOURCES = [NY, NC, GA_COUNTY, GWINNETT, OK, CT, IN_SRC, MD, MI, MN, WI, GPR]


# ─────────────────────────────────────────────────────────────────────────────
# Doc-type playbook (static reference, used by the UI)
DOC_PLAYBOOK = [
    {"doc": "Petition for Letters / Application for Probate",
     "yields": "PR name + address, decedent name + address + DOD, heirs/distributees + addresses, will Y/N"},
    {"doc": "Letters Testamentary / Letters of Administration / Order Authorizing Issuance",
     "yields": "Confirms PR appointed, PR name + address, appointment date"},
    {"doc": "Affidavit of Heirship / Family Tree",
     "yields": "Heir names, relationships, addresses, ages"},
    {"doc": "Will (Recorded / Probated)",
     "yields": "Beneficiaries, executor nominee, real-property bequests"},
    {"doc": "Inventory",
     "yields": "Real property addresses, parcel IDs, valuations — best source for property targeting"},
    {"doc": "Decree Granting Probate / Decree Granting Administration",
     "yields": "Final order, appointment confirmed"},
    {"doc": "Order of Discharge / Final Account / Order Closing",
     "yields": "Closure event — flips status to closed"},
    {"doc": "Affidavit of Collection (small estate)",
     "yields": "Substitute for Letters in small-estate cases — same PR + heir data"},
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers

def _dsn() -> str:
    return os.getenv("NEON_DB") or os.getenv("PROBATE_DATABASE_URL") or "postgresql://localhost:5432/probate"


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s",
        (table,),
    )
    return cur.fetchone() is not None


def _has_column(cur, table: str, col: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s AND column_name=%s",
        (table, col),
    )
    return cur.fetchone() is not None


def _ensure_derived_status(cur, table: str) -> None:
    if _table_exists(cur, table) and not _has_column(cur, table, "derived_status"):
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN derived_status TEXT')


def _array_lit(strings: list[str]) -> str:
    """Build a SQL ARRAY[...] literal of single-quoted strings (escaped)."""
    esc = lambda s: s.replace("'", "''")
    return "ARRAY[" + ",".join(f"'{esc(s)}'" for s in strings) + "]"


# ─────────────────────────────────────────────────────────────────────────────
# Per-source SQL generators
#
# Each source returns:
#  - `bucket_expr`: SQL CASE expression returning bucket key per row
#  - `tier1_expr` : boolean expression (decedent street + rep street present)
#  - `heir_expr`  : boolean expression (any heir-like party with street present)
#                   (NULL if heir info not detectable from this source)

def src_ny():
    """NY — records.

    Bucket mapping (mirrors NC's active_pending / active_qualified split):

      Article 13 voluntary admin            → article13
      petitioner_active = 'N'               → closed
      close-marker doc in document_names    → closed_by_doc
      petitioner_active = 'Y'               → active_qualified  (Letters detected)
      everything else                       → active_pending    (filed, awaiting Letters)

    Article 13 takes priority — those filings have no decree and end silently
    regardless of what the extractor saw. petitioner_active='N' is the source's
    own assertion that Letters were not granted / were revoked, so it beats
    doc heuristics. Otherwise, a close-marker doc is authoritative. Cases
    where the AI extractor returned petitioner_active='' (failed PDF, no
    Letters language detected, or pre-qualification) fold into active_pending
    — same treatment NC gives its filed-but-no-Letters cases.

    Tier-1: decedent_address.street AND petitioner_address.street present.
    Heir: tiered_extraction.persons has any role='heir' with address.street.
    """
    close_doc_check = (
        "EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements_text(COALESCE(document_names,'[]'::jsonb)) d"
        "  WHERE EXISTS ("
        f"    SELECT 1 FROM unnest({_array_lit(NY['close_markers'])}) m"
        "      WHERE UPPER(d) LIKE '%' || UPPER(m) || '%'"
        "  )"
        ")"
    )
    bucket = (
        "CASE "
        "  WHEN UPPER(COALESCE(proceeding,'')) LIKE '%ARTICLE 13%' THEN 'article13' "
        "  WHEN petitioner_active = 'N' THEN 'closed' "
        f" WHEN {close_doc_check} THEN 'closed_by_doc' "
        "  WHEN petitioner_active = 'Y' THEN 'active_qualified' "
        "  ELSE 'active_pending' "
        "END"
    )
    tier1 = (
        "(COALESCE(decedent_address->>'street','') <> '' "
        "  AND COALESCE(petitioner_address->>'street','') <> '')"
    )
    heir = (
        "EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements(COALESCE(tiered_extraction->'persons','[]'::jsonb)) p"
        "  WHERE p->>'role' = 'heir' "
        "    AND COALESCE(p->'address'->>'street','') <> ''"
        ")"
    )
    return bucket, tier1, heir


def src_nc():
    """NC — north_carolina.

    Guardianship cases (parties[].roles contains Ward or any Guardian* role)
    are tagged 'guardianship' and excluded from active/closed aggregations.

    For estate cases:
      - close_marker doc present (AFDD etc.)        → closed_by_doc
      - active_marker doc present (OAIL/Letters/…)  → active_qualified
      - else                                          → active_pending

    parties[] is a richer object than I originally assumed:
      {first, middle, last, suffix, roles[], addresses[{line1,city,state,zip}],
       formatted_name, party_id, date_of_death, …}.
    Tier-1: decedent has an address.line1 AND any PR-role party has line1.
    PR roles considered: Applicant, Executor, Co-Executor, Administrator,
    Co-Administrator, Petitioner, Affiant, Administrator CTA, Ancillary
    Executor/Administrator, Public Administrator, Collector.
    """
    guardianship_check = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
        "        WHERE jsonb_typeof(p->'roles')='array' "
        "          AND (p->'roles' ?| array['Ward','Guardian of the Person',"
        "                                   'General Guardian','Guardian of the Estate',"
        "                                   'Limited Guardian of the Person','Guardian ad Litem']))"
    )
    close_doc_check = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(documents,'[]'::jsonb)) d "
        f"        WHERE EXISTS (SELECT 1 FROM unnest({_array_lit(NC['close_markers'])}) m "
        "               WHERE UPPER(COALESCE(d->>'document_name','')) LIKE '%' || UPPER(m) || '%' "
        "                  OR UPPER(COALESCE(d->>'event_type','')) LIKE '%' || UPPER(m) || '%'))"
    )
    active_marker_check = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(documents,'[]'::jsonb)) d "
        f"        WHERE EXISTS (SELECT 1 FROM unnest({_array_lit(NC['active_markers'])}) m "
        "               WHERE UPPER(COALESCE(d->>'document_name','')) LIKE '%' || UPPER(m) || '%' "
        "                  OR UPPER(COALESCE(d->>'event_type','')) LIKE '%' || UPPER(m) || '%'))"
    )
    bucket = (
        "CASE "
        f"  WHEN {guardianship_check} THEN 'guardianship' "
        f"  WHEN {close_doc_check} THEN 'closed_by_doc' "
        f"  WHEN {active_marker_check} THEN 'active_qualified' "
        "   ELSE 'active_pending' "
        "END"
    )
    tier1 = (
        "("
        "  EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
        "                       jsonb_array_elements(p->'addresses') a "
        "          WHERE jsonb_typeof(p->'roles')='array' "
        "            AND p->'roles' ? 'Decedent' "
        "            AND COALESCE(a->>'line1','') <> '') "
        "  AND EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
        "                          jsonb_array_elements(p->'addresses') a "
        "          WHERE jsonb_typeof(p->'roles')='array' "
        "            AND (p->'roles' ?| array['Applicant','Executor','Co-Executor',"
        "                                     'Administrator','Co-Administrator','Petitioner',"
        "                                     'Affiant','Administrator CTA','Ancillary Executor',"
        "                                     'Ancillary Administrator','Public Administrator',"
        "                                     'Collector']) "
        "            AND COALESCE(a->>'line1','') <> '')"
        ")"
    )
    heir = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
        "                    jsonb_array_elements(p->'addresses') a "
        "        WHERE jsonb_typeof(p->'roles')='array' "
        "          AND (p->'roles' ?| array['Beneficiary','Next of Kin','Interested Person','Trust','Minor']) "
        "          AND COALESCE(a->>'line1','') <> '')"
    )
    return bucket, tier1, heir


def src_ga_county():
    """GA Cobb+Rockdale — ga_probate_cases.

    Cobb has no source status; Rockdale has 'active' or NULL.
    pdf_refs.name is sometimes present (Rockdale), sometimes only {cid,digest} (Cobb).
    """
    close_doc_check = (
        "EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements(COALESCE(pdf_refs,'[]'::jsonb)) d"
        "  WHERE EXISTS ("
        f"    SELECT 1 FROM unnest({_array_lit(GA_COUNTY['close_markers'])}) m"
        "      WHERE UPPER(COALESCE(d->>'name','')) LIKE '%' || UPPER(m) || '%'"
        "  )"
        ")"
    )
    bucket = (
        "CASE "
        f"  WHEN {close_doc_check} THEN 'closed_by_doc' "
        "   WHEN raw->>'status' IN ('active') THEN 'active' "
        "   ELSE 'unknown' "
        "END"
    )
    tier1 = (
        "(COALESCE(decedent_street,'') <> '' AND COALESCE(petitioner_street,'') <> '')"
    )
    heir = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(parties,'[]'::jsonb)) p "
        "        WHERE LOWER(COALESCE(p->>'role','')) IN ('heir','beneficiary','distributee') "
        "          AND COALESCE(p->>'street','') <> '')"
    )
    return bucket, tier1, heir


def src_gwinnett():
    """Gwinnett — events[].name is the doc list.

    case_status: Filed, Granted, Incomplete, Appealed (active); Discharged (closed).
    """
    close_doc_check = (
        "EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements(COALESCE(events,'[]'::jsonb)) e"
        "  WHERE EXISTS ("
        f"    SELECT 1 FROM unnest({_array_lit(GWINNETT['close_markers'])}) m"
        "      WHERE UPPER(COALESCE(e->>'name','')) LIKE '%' || UPPER(m) || '%'"
        "  )"
        ")"
    )
    bucket = (
        "CASE "
        "   WHEN case_status = 'Discharged' THEN 'closed' "
        f"  WHEN {close_doc_check} THEN 'closed_by_doc' "
        "   WHEN case_status IN ('Filed','Granted','Incomplete','Appealed') THEN 'active' "
        "   ELSE 'unknown' "
        "END"
    )
    tier1 = (
        "(COALESCE(decedent_street,'') <> '' "
        " AND EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(parties,'[]'::jsonb)) p "
        "             WHERE LOWER(COALESCE(p->>'role','')) IN ('petitioner','applicant','executor','administrator') "
        "               AND COALESCE(p->>'street','') <> ''))"
    )
    heir = (
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(parties,'[]'::jsonb)) p "
        "        WHERE LOWER(COALESCE(p->>'role','')) IN ('heir','beneficiary','distributee') "
        "          AND COALESCE(p->>'street','') <> '')"
    )
    return bucket, tier1, heir


def src_ok():
    """OK — closed_date IS NOT NULL = closed; pdf_urls.name has doc titles."""
    close_doc_check = (
        "EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements(COALESCE(pdf_urls,'[]'::jsonb)) d"
        "  WHERE EXISTS ("
        f"    SELECT 1 FROM unnest({_array_lit(OK['close_markers'])}) m"
        "      WHERE UPPER(COALESCE(d->>'name','')) LIKE '%' || UPPER(m) || '%'"
        "  )"
        ")"
    )
    bucket = (
        "CASE "
        "   WHEN closed_date IS NOT NULL THEN 'closed' "
        f"  WHEN {close_doc_check} THEN 'closed_by_doc' "
        "   ELSE 'active' "
        "END"
    )
    # OK addresses live only in PDFs — tier-1 cols are 0% currently.
    tier1 = (
        "(COALESCE(decedent_street,'') <> '' AND COALESCE(petitioner_street,'') <> '')"
    )
    heir = "FALSE"
    return bucket, tier1, heir


def src_simple_status(table: str, status_col: str, mapping: dict[str, str], tier1_sql: str, heir_sql: str | None):
    """Generic status mapping for sources without doc-override (CT, IN, MD, MI, MN, WI, GPR)."""
    if not mapping:
        # No status mapping → mark all rows unknown.
        bucket = "'unknown'::text"
    else:
        cases = []
        for status_value, b in mapping.items():
            if status_value is None:
                cases.append(f"WHEN {status_col} IS NULL THEN '{b}'")
            else:
                esc = status_value.replace("'", "''")
                cases.append(f"WHEN UPPER({status_col}) = UPPER('{esc}') THEN '{b}'")
        bucket = "CASE " + " ".join(cases) + " ELSE 'unknown' END"
    return bucket, tier1_sql, heir_sql or "FALSE"


SRC_LOGIC = {
    "new_york": src_ny,
    "north_carolina": src_nc,
    "ga_probate": src_ga_county,
    "gwinnett": src_gwinnett,
    "oklahoma": src_ok,
    "connecticut": lambda: (
        # CT has no status field and no doc list to override — every row
        # defaults to active_pending. Tier-1 = any party in parties[] has a
        # street; heir = ≥2 parties with a street (multi-party filing).
        "'active_pending'::text",
        "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
        "        WHERE jsonb_typeof(parties)='array' "
        "          AND COALESCE(p->>'street','') <> '')",
        "(SELECT COUNT(*) FROM jsonb_array_elements(parties) p "
        "        WHERE jsonb_typeof(parties)='array' "
        "          AND COALESCE(p->>'street','') <> '') > 1",
    ),
    "indiana": lambda: src_simple_status(
        "indiana", "case_status",
        {"Pending": "active", "Decided": "closed"},
        "(COALESCE(decedent_street,'') <> '' AND COALESCE(rep_street,'') <> '')",
        None,
    ),
    "maryland": lambda: src_simple_status(
        "maryland", "case_status",
        {"OPEN": "active", "CLOSED": "closed", None: "unknown"},
        "(COALESCE(rep_street,'') <> '')",  # MD never captures decedent street
        None,
    ),
    "michigan": lambda: src_simple_status(
        "michigan", "case_status",
        {"Open": "active", "Closed": "closed"},
        "(COALESCE(decedent_street,'') <> '' "
        " AND EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(parties,'[]'::jsonb)) p "
        "             WHERE LOWER(COALESCE(p->>'role','')) IN ('petitioner','executor','administrator') "
        "               AND COALESCE(p->>'street','') <> ''))",
        None,
    ),
    "minnesota": lambda: src_simple_status(
        "minnesota", "case_status",
        {"Open": "active", "Under Court Jurisdiction": "active", "Closed": "closed", None: "unknown"},
        "(COALESCE(decedent_street,'') <> '' "
        " AND EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(parties,'[]'::jsonb)) p "
        "             WHERE LOWER(COALESCE(p->>'role','')) IN ('petitioner','executor','administrator') "
        "               AND COALESCE(p->>'street','') <> ''))",
        None,
    ),
    "wisconsin": lambda: src_simple_status(
        "wisconsin", "case_status",
        {"Open": "active", "Reopened": "active", "Closed": "closed"},
        "(COALESCE(decedent_street,'') <> '')",  # WI rep address not captured
        None,
    ),
    "georgia_probate_records": lambda: src_simple_status(
        "georgia", "status",
        {"OPEN": "active", "PENDING": "active", "CLOSED": "closed"},
        "TRUE",  # GPR addresses are structurally in parties — placeholder
        None,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot computation per source

def compute_source(cur, src: dict) -> dict:
    table = src["table"]
    if not _table_exists(cur, table):
        return {**src, "missing": True, "total": 0}

    bucket_sql, tier1_sql, heir_sql = SRC_LOGIC[src["key"]]()
    date_col = src["date_col"]
    ts_col = src["ts_col"]
    county_col = src["county_col"]

    # Materialize derived_status for sources that have a doc-override rule.
    if src["key"] in ("new_york", "north_carolina", "ga_probate", "gwinnett", "oklahoma"):
        _ensure_derived_status(cur, table)
        cur.execute(f'UPDATE "{table}" SET derived_status = ({bucket_sql})')
        print(f"[snapshot]   {src['key']} UPDATE derived_status -> {cur.rowcount} rows")
        cur.connection.commit()  # commit DDL+UPDATE before later SELECTs in same txn

    # Optional WHERE filter — used to exclude row classes that should not be
    # counted in this source's report (NC guardianship, etc).
    exclude_sql = src.get("exclude_sql", "")
    where_clause = f" WHERE {exclude_sql} " if exclude_sql else " "

    # Bucket aggregates: count, tier-1 complete, tier-1+heir complete.
    cur.execute(f"""
        SELECT
            ({bucket_sql})                                AS bucket,
            COUNT(*)::bigint                              AS rows,
            COUNT(*) FILTER (WHERE {tier1_sql})::bigint   AS tier1,
            COUNT(*) FILTER (WHERE {tier1_sql} AND {heir_sql})::bigint AS tier1_heir
        FROM "{table}"
        {where_clause}
        GROUP BY 1
        ORDER BY 1
    """)
    buckets = {}
    for b, n, t1, t1h in cur.fetchall():
        buckets[b] = {
            "count": int(n),
            "tier1_complete": int(t1),
            "tier1_complete_pct": round(100.0 * t1 / n, 1) if n else 0.0,
            "tier1_heir_complete": int(t1h),
            "tier1_heir_complete_pct": round(100.0 * t1h / n, 1) if n else 0.0,
        }

    # Total + last_scraped + scrape window.
    cur.execute(f'SELECT COUNT(*), MAX("{ts_col}") FROM "{table}" {where_clause}')
    total, last_scraped = cur.fetchone()
    total = int(total or 0)

    # Scrape window — handle text vs date columns.
    try:
        cur.execute(
            f'SELECT MIN("{date_col}"::date)::text, MAX("{date_col}"::date)::text '
            f'FROM "{table}" {where_clause if exclude_sql else "WHERE"} '
            f'{"AND" if exclude_sql else ""} "{date_col}" IS NOT NULL'
        )
        scrape_window = cur.fetchone()
    except psycopg2.Error:
        cur.connection.rollback()
        try:
            cur.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}" {where_clause}')
            r = cur.fetchone()
            scrape_window = (str(r[0]) if r[0] else None, str(r[1]) if r[1] else None)
        except psycopg2.Error:
            cur.connection.rollback()
            scrape_window = (None, None)

    # Daily timeline — count of rows per scrape day (uses ts_col).
    timeline_where = f" {where_clause} AND " if exclude_sql else " WHERE "
    cur.execute(f"""
        SELECT DATE("{ts_col}") AS d, COUNT(*)::bigint
        FROM "{table}"
        {timeline_where} "{ts_col}" IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)
    timeline = [{"day": d.isoformat(), "count": int(n)} for d, n in cur.fetchall()]

    # County breakdown — count + active/closed/closed_by_doc + completeness + last scraped.
    cur.execute(f"""
        SELECT
            COALESCE(NULLIF(TRIM("{county_col}"::text),''),'— Unknown') AS county,
            COUNT(*)::bigint                                            AS rows,
            COUNT(*) FILTER (WHERE ({bucket_sql}) IN ('active','active_pending','active_qualified'))::bigint AS active,
            COUNT(*) FILTER (WHERE ({bucket_sql}) = 'closed')::bigint                          AS closed,
            COUNT(*) FILTER (WHERE ({bucket_sql}) = 'closed_by_doc')::bigint                   AS closed_by_doc,
            COUNT(*) FILTER (WHERE ({bucket_sql}) = 'article13')::bigint                       AS article13,
            COUNT(*) FILTER (WHERE {tier1_sql})::bigint                                        AS tier1,
            MAX("{ts_col}")                                                                    AS last_scraped
        FROM "{table}"
        {where_clause}
        GROUP BY 1
        ORDER BY rows DESC, county ASC
    """)
    counties = []
    for row in cur.fetchall():
        county, rows, active, closed, cbd, art13, tier1, ls = row
        counties.append({
            "county": county,
            "count": int(rows),
            "active": int(active),
            "closed": int(closed),
            "closed_by_doc": int(cbd),
            "article13": int(art13),
            "tier1_complete_pct": round(100.0 * tier1 / rows, 1) if rows else 0.0,
            "last_scraped": ls.isoformat() if ls else None,
        })

    # Doc playbook — per-source counts of cases that have ≥1 doc matching each
    # playbook category (best-effort; the source may not surface doc names).
    doc_counts = _doc_playbook_counts(cur, src)

    # Field coverage — per-source list of {label, present, present_pct} rows.
    # `total` here is the filtered-set total so percentages are honest.
    field_coverage = _field_coverage(cur, src, total)

    # Track guardianship rows separately (NC) so the user can see how many
    # were excluded.
    guardianship_excluded = 0
    if src["key"] == "north_carolina":
        cur.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE derived_status = %s',
            ("guardianship",),
        )
        guardianship_excluded = int(cur.fetchone()[0] or 0)

    profile = {
        "state":              src.get("state"),
        "active_field":       src.get("active_field"),
        "active_values":      src.get("active_values"),
        "closed_values":      src.get("closed_values"),
        "non_owner":          src.get("non_owner"),
        "address_match_rule": src.get("address_match_rule"),
        "scraping_notes":     src.get("scraping_notes"),
    }

    return {
        "key": src["key"],
        "name": src["name"],
        "level": src["level"],
        "table": table,
        "date_field": date_col,
        "scrape_window": list(scrape_window) if scrape_window else [None, None],
        "total": total,
        "last_scraped": last_scraped.isoformat() if last_scraped else None,
        "buckets": buckets,
        "active_markers": src.get("active_markers", []),
        "close_markers": src.get("close_markers", []),
        "timeline": timeline,
        "counties": counties,
        "doc_playbook_counts": doc_counts,
        "field_coverage": field_coverage,
        "profile": profile,
        "guardianship_excluded": guardianship_excluded,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Doc playbook — per-source counts

PLAYBOOK_PATTERNS = [
    ("Petition / Application",        ["PETITION", "APPLICATION FOR PROBATE"]),
    ("Letters / Order Authorizing",   ["LETTERS", "ORDER AUTHORIZING ISSUANCE"]),
    ("Affidavit of Heirship / Family", ["AFFIDAVIT OF HEIRSHIP", "FAMILY TREE", "FAMILY HISTORY"]),
    ("Will",                          ["WILL"]),
    ("Inventory",                     ["INVENTORY"]),
    ("Decree Granting",               ["DECREE GRANTING"]),
    ("Discharge / Final Account",     ["DISCHARGE", "FINAL ACCOUNT", "ORDER CLOSING"]),
    ("Affidavit of Collection (small)", ["AFFIDAVIT OF COLLECTION", "AFCT", "AFCP"]),
]


def _doc_playbook_counts(cur, src: dict) -> list[dict]:
    """For each playbook row, count cases with at least one doc matching the patterns.

    Doc-name JSON path varies per source. If no doc-name list is available
    (e.g., CT, IN, MD without document tracking), return empty.
    """
    table = src["table"]
    key = src["key"]

    # Source → SQL fragment that returns text doc names per row.
    if key == "new_york":
        names_expr = "jsonb_array_elements_text(COALESCE(document_names,'[]'::jsonb))"
    elif key == "north_carolina":
        names_expr = "(jsonb_array_elements(COALESCE(documents,'[]'::jsonb))->>'document_name')"
    elif key == "ga_probate":
        names_expr = "(jsonb_array_elements(COALESCE(pdf_refs,'[]'::jsonb))->>'name')"
    elif key == "gwinnett":
        names_expr = "(jsonb_array_elements(COALESCE(events,'[]'::jsonb))->>'name')"
    elif key == "oklahoma":
        names_expr = "(jsonb_array_elements(COALESCE(pdf_urls,'[]'::jsonb))->>'name')"
    else:
        return []

    exclude_sql = src.get("exclude_sql", "")
    extra_filter = f" AND ({exclude_sql})" if exclude_sql else ""

    out = []
    for label, patterns in PLAYBOOK_PATTERNS:
        # Build OR of UPPER(name) LIKE '%PAT%'
        cond = " OR ".join("UPPER(n) LIKE '%%' || %s || '%%'" for _ in patterns)
        params = [p.upper() for p in patterns]
        cur.execute(
            f'SELECT COUNT(DISTINCT t.id) FROM "{table}" t, '
            f'LATERAL (SELECT {names_expr} AS n) x '
            f'WHERE x.n IS NOT NULL AND ({cond}){extra_filter}',
            params,
        )
        n = cur.fetchone()[0] or 0
        out.append({"doc_type": label, "rows_with_doc": int(n), "yields": _playbook_yields(label)})
    return out


def _playbook_yields(label: str) -> str:
    for entry in DOC_PLAYBOOK:
        if any(part.strip().lower() in entry["doc"].lower() for part in label.split("/")):
            return entry["yields"]
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Field coverage — per-source presence stats for the fields the campaign cares
# about (decedent + PR + heir name/address/phone/email).

def _field_coverage(cur, src: dict, total: int) -> list[dict]:
    """Return [{label, present, present_pct, source}] for each tracked field.

    The exact SQL boolean varies per source. Keep the label list stable so the
    UI table is consistent across sources; sources that don't capture a field
    return present=0.
    """
    if not total:
        return []
    key = src["key"]
    table = src["table"]

    # Each (label, sql_bool, source_path) — sql_bool counted via FILTER.
    rows: list[tuple[str, str, str]] = []

    if key == "new_york":
        rows = [
            ("Decedent name",        "decedent->>'full_name' IS NOT NULL AND decedent->>'full_name' <> ''",
                                     "decedent.full_name"),
            ("Decedent street",      "COALESCE(decedent_address->>'street','') <> ''",
                                     "decedent_address.street"),
            ("Decedent city/state/zip","COALESCE(decedent_address->>'city','') <> '' "
                                       "AND COALESCE(decedent_address->>'state','') <> '' "
                                       "AND COALESCE(decedent_address->>'zip','') <> ''",
                                     "decedent_address.{city,state,zip}"),
            ("Decedent date of death","death_date IS NOT NULL AND death_date <> ''",
                                     "death_date"),
            ("PR name",              "COALESCE(petitioner->>'full_name','') <> ''",
                                     "petitioner.full_name"),
            ("PR street",            "COALESCE(petitioner_address->>'street','') <> ''",
                                     "petitioner_address.street"),
            ("PR city/state/zip",    "COALESCE(petitioner_address->>'city','') <> '' "
                                     "AND COALESCE(petitioner_address->>'state','') <> '' "
                                     "AND COALESCE(petitioner_address->>'zip','') <> ''",
                                     "petitioner_address.{city,state,zip}"),
            ("PR phone",             "COALESCE(petitioner_phone,'') <> ''",
                                     "petitioner_phone"),
            ("PR email",             "COALESCE(petitioner_email,'') <> ''",
                                     "petitioner_email"),
            ("≥1 heir with name",    "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(tiered_extraction->'persons','[]'::jsonb)) p "
                                     "        WHERE p->>'role'='heir' AND COALESCE(p->>'full_name','') <> '')",
                                     "tiered_extraction.persons[role=heir].full_name"),
            ("≥1 heir with street",  "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(tiered_extraction->'persons','[]'::jsonb)) p "
                                     "        WHERE p->>'role'='heir' AND COALESCE(p->'address'->>'street','') <> '')",
                                     "tiered_extraction.persons[role=heir].address.street"),
            ("≥1 heir with phone",   "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(tiered_extraction->'persons','[]'::jsonb)) p "
                                     "        WHERE p->>'role'='heir' AND COALESCE(p->>'phone','') <> '')",
                                     "tiered_extraction.persons[role=heir].phone"),
            ("≥1 heir with email",   "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(tiered_extraction->'persons','[]'::jsonb)) p "
                                     "        WHERE p->>'role'='heir' AND COALESCE(p->>'email','') <> '')",
                                     "tiered_extraction.persons[role=heir].email"),
            ("≥1 real property",     "jsonb_array_length(COALESCE(tiered_extraction->'real_property','[]'::jsonb)) > 0",
                                     "tiered_extraction.real_property[]"),
            ("Tier-extracted (any)", "tiered_extraction IS NOT NULL",
                                     "tiered_extraction"),
        ]
    elif key == "connecticut":
        any_party_with_street = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            "        WHERE jsonb_typeof(parties)='array' "
            "          AND COALESCE(p->>'street','') <> '')"
        )
        any_party_with_phone = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            "        WHERE jsonb_typeof(parties)='array' "
            "          AND COALESCE(p->>'phone','') <> '')"
        )
        multi_party_with_street = (
            "(SELECT COUNT(*) FROM jsonb_array_elements(parties) p "
            "        WHERE jsonb_typeof(parties)='array' "
            "          AND COALESCE(p->>'street','') <> '') > 1"
        )
        rows = [
            ("Decedent name",         "COALESCE(decedent_full,'') <> ''",  "decedent_full"),
            ("Decedent date of death","FALSE",                              "(CT source does not capture DOD)"),
            ("Decedent street",       "FALSE",                              "(CT source does not capture decedent residence)"),
            ("PR name",               "COALESCE(petitioner_full,'') <> ''", "petitioner_full"),
            ("PR street",             "COALESCE(petitioner_street,'') <> ''", "petitioner_street"),
            ("PR city/state/zip",     "COALESCE(petitioner_city,'') <> '' AND COALESCE(petitioner_state,'') <> '' AND COALESCE(petitioner_zip,'') <> ''", "petitioner_{city,state,zip}"),
            ("PR phone",              "COALESCE(petitioner_phone,'') <> ''",  "petitioner_phone"),
            ("PR email",              "FALSE",                              "(CT source does not capture email)"),
            ("≥1 party in parties[]", "jsonb_typeof(parties)='array' AND jsonb_array_length(parties) > 0", "parties[]"),
            ("≥1 party with street",  any_party_with_street,                "parties[].street"),
            ("≥1 party with phone",   any_party_with_phone,                 "parties[].phone"),
            ("≥2 parties with street (multi-party filing)", multi_party_with_street, "parties[].street count > 1"),
            ("File date",             "filed_date IS NOT NULL",             "filed_date"),
        ]
    elif key == "oklahoma":
        rows = [
            ("Decedent name",          "COALESCE(decedent_first,'') <> '' OR COALESCE(decedent_last,'') <> ''",
                                       "decedent_{first,middle,last}"),
            ("Decedent street",        "FALSE",
                                       "(OK source does not expose decedent address — addresses live only inside PDFs)"),
            ("Decedent city/state/zip","FALSE",
                                       "(OK source does not expose decedent address — addresses live only inside PDFs)"),
            ("PR name",                "COALESCE(petitioner_first,'') <> '' OR COALESCE(petitioner_last,'') <> ''",
                                       "petitioner_{first,middle,last}"),
            ("PR street",              "FALSE",
                                       "(OK source does not expose PR address — addresses live only inside PDFs)"),
            ("PR city/state/zip",      "FALSE",
                                       "(OK source does not expose PR address — addresses live only inside PDFs)"),
            ("PR phone",               "COALESCE(petitioner_phone,'') <> ''",
                                       "petitioner_phone (column exists but never populated)"),
            ("PR email",               "COALESCE(petitioner_email,'') <> ''",
                                       "petitioner_email (column exists but never populated)"),
            ("Filed date",             "filed_date IS NOT NULL",   "filed_date"),
            ("Closed date",            "closed_date IS NOT NULL",  "closed_date"),
            ("Parties JSON populated", "jsonb_typeof(parties)='array' AND jsonb_array_length(parties) > 0",
                                       "parties[] (sparse — only ~15% of rows; first/middle/last/desc only, no addresses)"),
            ("≥1 PDF URL captured",    "jsonb_typeof(pdf_urls)='array' AND jsonb_array_length(pdf_urls) > 0",
                                       "pdf_urls[]"),
            ("≥1 active-marker doc",
             "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(pdf_urls,'[]'::jsonb)) d "
             "        WHERE UPPER(COALESCE(d->>'name','')) LIKE '%LETTERS%' "
             "           OR UPPER(COALESCE(d->>'name','')) LIKE '%ORDER ADMITTING WILL%' "
             "           OR UPPER(COALESCE(d->>'name','')) LIKE '%ORDER APPOINTING%')",
             "pdf_urls[].name LIKE %LETTERS%/%ORDER ADMITTING WILL%/%ORDER APPOINTING%"),
            ("PDF tier-extracted",     "pdf_extracted_at IS NOT NULL",
                                       "pdf_extracted_at  (extractor not yet wired in)"),
        ]
    elif key == "maryland":
        rows = [
            ("Decedent name",          "COALESCE(decedent_full,'') <> ''",            "decedent_full"),
            ("Decedent date of death", "COALESCE(decedent_dod,'') <> ''",             "decedent_dod"),
            ("Decedent aliases",       "COALESCE(aliases,'') NOT IN ('','None')",     "aliases"),
            ("Decedent street",        "FALSE",  "(MD source does not expose decedent residence — buried in raw_detail_html)"),
            ("PR name",                "COALESCE(rep_full,'') <> ''",                 "rep_full"),
            ("PR street",              "COALESCE(rep_street,'') <> ''",               "rep_street"),
            ("PR city/state/zip",      "COALESCE(rep_city,'') <> '' AND COALESCE(rep_state,'') <> '' AND COALESCE(rep_zip,'') <> ''",  "rep_{city,state,zip}"),
            ("PR phone",               "FALSE",  "(MD source does not capture phone)"),
            ("PR email",               "FALSE",  "(MD source does not capture email)"),
            ("File date",              "COALESCE(file_date,'') <> ''",                "file_date"),
            ("Date opened",            "COALESCE(date_opened,'') <> ''",              "date_opened"),
            ("Date closed",            "COALESCE(date_closed,'') <> ''",              "date_closed"),
            ("Will status set",        "COALESCE(will,'') NOT IN ('','None','none')", "will (NO WILL / UNPROBATED / PROBATED / FOREIGN WILL)"),
            ("Probate date",           "COALESCE(probate_date,'') <> ''",             "probate_date"),
            ("Raw detail HTML cached", "raw_detail_html IS NOT NULL AND raw_detail_html <> ''",
                                       "raw_detail_html  (decedent residence parser pending)"),
        ]
    elif key == "north_carolina":
        # PR-equivalent role list reused below.
        pr_roles = (
            "'Applicant','Executor','Co-Executor','Administrator',"
            "'Co-Administrator','Petitioner','Affiant','Administrator CTA',"
            "'Ancillary Executor','Ancillary Administrator',"
            "'Public Administrator','Collector'"
        )
        heir_roles = "'Beneficiary','Next of Kin','Interested Person','Trust','Minor'"

        decedent_p = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            "        WHERE jsonb_typeof(p->'roles')='array' "
            "          AND p->'roles' ? 'Decedent' "
            "          AND COALESCE(p->>'formatted_name','') <> '')"
        )
        decedent_addr = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
            "                    jsonb_array_elements(p->'addresses') a "
            "        WHERE jsonb_typeof(p->'roles')='array' "
            "          AND p->'roles' ? 'Decedent' "
            "          AND COALESCE(a->>'line1','') <> '')"
        )
        decedent_csz = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
            "                    jsonb_array_elements(p->'addresses') a "
            "        WHERE jsonb_typeof(p->'roles')='array' "
            "          AND p->'roles' ? 'Decedent' "
            "          AND COALESCE(a->>'city','') <> '' "
            "          AND COALESCE(a->>'state','') <> '' "
            "          AND COALESCE(a->>'zip','') <> '')"
        )
        decedent_dod = (
            "EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            "        WHERE jsonb_typeof(p->'roles')='array' "
            "          AND p->'roles' ? 'Decedent' "
            "          AND COALESCE(p->>'date_of_death','') <> '')"
        )
        pr_name = (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            f"        WHERE jsonb_typeof(p->'roles')='array' "
            f"          AND (p->'roles' ?| array[{pr_roles}]) "
            f"          AND COALESCE(p->>'formatted_name','') <> '')"
        )
        pr_addr = (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
            f"                    jsonb_array_elements(p->'addresses') a "
            f"        WHERE jsonb_typeof(p->'roles')='array' "
            f"          AND (p->'roles' ?| array[{pr_roles}]) "
            f"          AND COALESCE(a->>'line1','') <> '')"
        )
        pr_csz = (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
            f"                    jsonb_array_elements(p->'addresses') a "
            f"        WHERE jsonb_typeof(p->'roles')='array' "
            f"          AND (p->'roles' ?| array[{pr_roles}]) "
            f"          AND COALESCE(a->>'city','') <> '' "
            f"          AND COALESCE(a->>'state','') <> '' "
            f"          AND COALESCE(a->>'zip','') <> '')"
        )
        heir_name = (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p "
            f"        WHERE jsonb_typeof(p->'roles')='array' "
            f"          AND (p->'roles' ?| array[{heir_roles}]) "
            f"          AND COALESCE(p->>'formatted_name','') <> '')"
        )
        heir_addr = (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(parties) p, "
            f"                    jsonb_array_elements(p->'addresses') a "
            f"        WHERE jsonb_typeof(p->'roles')='array' "
            f"          AND (p->'roles' ?| array[{heir_roles}]) "
            f"          AND COALESCE(a->>'line1','') <> '')"
        )
        rows = [
            ("Decedent name",          decedent_p,     "parties[roles=Decedent].formatted_name"),
            ("Decedent street",        decedent_addr,  "parties[roles=Decedent].addresses[].line1"),
            ("Decedent city/state/zip",decedent_csz,   "parties[roles=Decedent].addresses[].{city,state,zip}"),
            ("Decedent date of death", decedent_dod,   "parties[roles=Decedent].date_of_death"),
            ("PR name",                pr_name,        "parties[roles in PR set].formatted_name"),
            ("PR street",              pr_addr,        "parties[roles in PR set].addresses[].line1"),
            ("PR city/state/zip",      pr_csz,         "parties[roles in PR set].addresses[].{city,state,zip}"),
            ("PR phone",               "FALSE",        "(NC parties payload does not carry phone — would require PDF extraction)"),
            ("PR email",               "FALSE",        "(NC parties payload does not carry email — would require PDF extraction)"),
            ("≥1 heir/beneficiary with name",   heir_name,  "parties[roles in heir set].formatted_name"),
            ("≥1 heir/beneficiary with street", heir_addr,  "parties[roles in heir set].addresses[].line1"),
            ("≥1 document on file",
             "jsonb_typeof(documents)='array' AND jsonb_array_length(documents) > 0",
             "documents[]"),
            ("≥1 active-marker doc (Letters/OAIL)",
             "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(documents,'[]'::jsonb)) d "
             "        WHERE UPPER(COALESCE(d->>'event_type','')) LIKE '%LETTERS%' "
             "           OR UPPER(COALESCE(d->>'document_name','')) LIKE '%LETTERS%' "
             "           OR UPPER(COALESCE(d->>'event_type','')) LIKE '%OAIL%' "
             "           OR UPPER(COALESCE(d->>'document_name','')) LIKE '%OAIL%')",
             "documents[event_type/document_name LIKE %LETTERS%/%OAIL%]"),
            ("PDF tier-extracted",
             "jsonb_typeof(pdf_extractions)='array' AND jsonb_array_length(pdf_extractions) > 0",
             "pdf_extractions[]"),
        ]
    else:
        # Other sources will be filled in source-by-source as we move through
        # them. Returning [] is fine — the UI just hides the section.
        return []

    if not rows:
        return []

    exclude_sql = src.get("exclude_sql", "")
    where_clause = f" WHERE {exclude_sql}" if exclude_sql else ""

    # One round-trip: SELECT COUNT(*) FILTER (WHERE …) per row, aliased.
    parts = []
    for i, (_, sql, _) in enumerate(rows):
        parts.append(f"COUNT(*) FILTER (WHERE {sql})::bigint AS f{i}")
    cur.execute(f'SELECT {", ".join(parts)} FROM "{table}"{where_clause}')
    counts = cur.fetchone()

    out = []
    for (label, _, source_path), n in zip(rows, counts):
        n = int(n or 0)
        out.append({
            "label":   label,
            "present": n,
            "present_pct": round(100.0 * n / total, 1),
            "source":  source_path,
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot table

def ensure_snapshot_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scraper_report_snapshot (
            code         TEXT PRIMARY KEY,
            data         JSONB NOT NULL,
            computed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def write_snapshot(cur, key: str, payload: dict) -> None:
    cur.execute(
        "INSERT INTO scraper_report_snapshot (code, data, computed_at) "
        "VALUES (%s, %s::jsonb, NOW()) "
        "ON CONFLICT (code) DO UPDATE SET data = EXCLUDED.data, computed_at = NOW()",
        (key, json.dumps(payload, default=str)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=None, help="Postgres DSN (defaults to NEON_DB / PROBATE_DATABASE_URL)")
    ap.add_argument("--only", default=None, help="Comma-separated source keys to process")
    args = ap.parse_args(argv)

    dsn = args.dsn or _dsn()
    only = set(s.strip() for s in args.only.split(",")) if args.only else None

    print(f"[snapshot] connecting to {dsn.split('@')[-1].split('?')[0]}")
    conn = psycopg2.connect(dsn, connect_timeout=15)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            ensure_snapshot_table(cur)
        conn.commit()

        with conn.cursor() as cur:
            for src in ALL_SOURCES:
                if only and src["key"] not in only:
                    continue
                t0 = datetime.now()
                try:
                    payload = compute_source(cur, src)
                    write_snapshot(cur, src["key"], payload)
                    conn.commit()
                    elapsed = (datetime.now() - t0).total_seconds()
                    if payload.get("missing"):
                        print(f"[snapshot] {src['key']:24} table {src['table']!r} not found — skipped")
                    else:
                        b = payload.get("buckets", {})
                        summary = " ".join(f"{k}={v['count']}" for k, v in b.items())
                        print(f"[snapshot] {src['key']:24} total={payload['total']:>5}  {summary}  ({elapsed:.1f}s)")
                except Exception as e:
                    conn.rollback()
                    import traceback
                    print(f"[snapshot] {src['key']:24} ERROR: {e}", file=sys.stderr)
                    traceback.print_exc()

        print("[snapshot] done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
