# Probate Scrapers — Source-of-Truth Field Map

Generated 2026-05-06 from live Postgres (`probate` DB). Excludes foreclosure and auction.com.
Counts reflect rows currently in the DB, not steady-state production rates.

"Complete" definition (used for completeness % below):

- **Tier-1 complete**: row has a non-empty decedent street address AND a non-empty representative
  (PR / Petitioner / Executor / Administrator) street address. These are the two anchors enrichment
  needs. Stricter definitions are noted per scraper.
- Where a scraper does not capture decedent or rep address as columns (CT, OK, WI), we measure
  presence inside `parties` JSON or note that the address only lives inside PDFs.

---

## Quick matrix

| Code | Source | Level | Table(s) | Date field | Status field | Status values | Counties seen | Rows |
|---|---|---|---|---|---|---|---|---|
| NY | NY Surrogate's Court (e-filing portal) | State (62 surrogate's courts) | `records`, `enriched_records` | `filed_date` (death_date sparse) | `petitioner_active` (Y/N) — derived | Y / N / "" | 23 | 1 063 |
| CT | CT Probate Court | State (60 districts → 8 counties) | `ct_probate_cases`, `ct_probate_enriched` | `filed_date` | none — `case_type_code` only | DR / DAT / DT / DS / DW / TT / TO / TL | 8 | 1 813 |
| GA (Cobb+Rockdale) | Cobb (Benchmark) + Rockdale (Tyler) | County | `ga_probate_cases` | `filed_date` (death_date present) | `status` (Rockdale only) | active / null | 2 | 631 |
| GPR | georgiaprobaterecords.com (statewide) | State aggregator (94 of 159 GA counties) | `georgia` | `file_date` (death_date sparse) | `status` | OPEN / PENDING / CLOSED | 90 | 1 343 |
| Gwinnett | Gwinnett Tyler eCourts + Odyssey IDP SSO | County | `gwinnett` | `file_date` | `case_status` | Filed / Granted / Incomplete / Appealed / Discharged | 1 | 1 144 |
| IN | mycase.in.gov | State (92 counties) | `indiana` | `file_date` (text) | `case_status` | Pending / Decided | 82 | 390 |
| MD | registers.maryland.gov | State (24 counties) | `maryland` | `file_date` / `date_opened` / `date_closed` | `case_status` | OPEN / CLOSED / null | 24 | 3 160 |
| MI | MiCOURT | State (42 probate courts) | `michigan` | `file_date` | `case_status` | (no rows yet) | 0 | 0 |
| MN | MCRO | State (87 counties) | `minnesota` | `file_date` | `case_status` | Open / Closed / Under Court Jurisdiction / null | 2 | 17 |
| NC | NC Tyler Portal (e-Courts) | State (87 of 100 counties) | `north_carolina` | `file_date` | none — derived from documents | active_pending / active_qualified / closed (proxy) | 120 | 5 910 |
| OK | OSCN | State (all counties) | `ok_probate_cases`, `ok_probate_enriched` | `filed_date`, `closed_date` | `closed_date IS NULL` proxy | active / closed | 12 | 546 |
| WI | WCCA (Wisconsin Circuit Courts) | State (72 counties) | `wisconsin` | `file_date` | `case_status` | Open / Closed / Reopened | 8 | 564 |

Notes:

- "Counties seen" = distinct values currently in the table; not the universe the scraper covers.
- MI, MN have very few rows because scrape runs are still being scheduled.
- "Range" support — every scraper accepts a from/to filed-date range. Server caps differ:
  WI = 30-day windows server-side; NC = no enforced cap; GPR = death-date filter only on detail; etc.

---

## Active / Closed × Completeness (current snapshot)

Run on 2026-05-06.

### NY (`records`) — 1 063 rows

`estate_closed` is empty for every row, so it is not a useable status field. Use
`petitioner_active` (set during PDF tier-1 extraction).

| Active flag | Rows | Tier-1 complete | % complete |
|---|---:|---:|---:|
| Y (active) | 530 | 431 | 81.3 % |
| "" (unknown — extraction missed it) | 424 | 79 | 18.6 % |
| N (closed/inactive) | 109 | 86 | 78.9 % |
| **Total** | **1 063** | **596** | **56.1 %** |

Date field: `filed_date` (text, MM/DD/YYYY). Range observed: 2025-04-01 → 2025-09-30.
Death date populated for 941 / 1 063 rows (88 %).

### CT (`ct_probate_cases`) — 1 813 rows

No status field in the JSON; `case_type_code` is the closest classifier. The user-supplied note
"status=1 means open" does not appear in `raw` — only `code` (which is the type). **Open question:
does CT JSON expose a status code we are not yet capturing?**

| case_type_code | Rows | Petitioner addr present | % | Any party addr present | % |
|---|---:|---:|---:|---:|---:|
| DR (Decedent's Estate Reg.) | 885 | 124 | 14.0 % | 124 | 14.0 % |
| DAT (Tax-only Filing) | 370 | 6 | 1.6 % | 6 | 1.6 % |
| DT | 367 | 2 | 0.5 % | 2 | 0.5 % |
| DS (Small Estate) | 91 | 60 | 65.9 % | 60 | 65.9 % |
| DW (Will Only) | 61 | 0 | 0 % | 0 | 0 % |
| TT / TO / TL (trust types) | 39 | 5 | 12.8 % | 5 | 12.8 % |

Filed-date range observed: 2026-04-01 → 2026-04-30.

### GA Cobb + Rockdale (`ga_probate_cases`) — 631 rows

| County | Status | Rows | dec.street + pet.street | % |
|---|---|---:|---:|---:|
| Cobb | (null) — no status field at source | 464 | 1 | 0.2 % |
| Rockdale | active | 139 | 0 | 0 % |
| Rockdale | (null) | 28 | 0 | 0 % |

Cobb is doc-name driven (Benchmark portal); Rockdale exposes a status field (Tyler us-gov-west-1 WAF).
Decedent + petitioner streets are almost never populated — addresses live inside PDFs that have not
yet been extracted into structured columns.

### GPR (`georgia`, statewide aggregator) — 1 343 rows, 90 counties

| Status | Rows | dec.street + party-with-street | % |
|---|---:|---:|---:|
| OPEN | 1 004 | 870 | 86.7 % |
| PENDING | 227 | 206 | 90.7 % |
| CLOSED | 112 | 100 | 89.3 % |

This is the highest-completeness source we have because the GPR portal exposes structured party
data including addresses on the search detail page itself.

### Gwinnett (`gwinnett`) — 1 144 rows, 1 county (Gwinnett)

| case_status | Rows | dec.street + petitioner-party with street | % |
|---|---:|---:|---:|
| Filed | 640 | 504 | 78.8 % |
| Granted (letters issued — case is "active w/ PR") | 450 | 418 | 92.9 % |
| Incomplete | 52 | 52 | 100 % |
| Appealed | 1 | 1 | 100 % |
| Discharged (closed) | 1 | 1 | 100 % |

Active-vs-closed mapping for Gwinnett:

- **Active** = Filed, Granted, Incomplete, Appealed (95 %+ of file)
- **Closed** = Discharged (and arguably "Granted" if the goal is "letters already issued")

### IN (`indiana`) — 390 rows, 82 counties

| case_status | Rows | dec.street + rep.street | % |
|---|---:|---:|---:|
| Pending (active) | 318 | 69 | 21.7 % |
| Decided (closed) | 72 | 19 | 26.4 % |

Decedent street is rare (91/390 = 23 %) — IN portal often only gives city/state, requires PDF
extraction for full address.

### MD (`maryland`) — 3 160 rows, 24 counties

MD does not surface a decedent residence column — only the rep (PR) address.

| case_status | Rows | rep.street | % |
|---|---:|---:|---:|
| OPEN (active) | 2 243 | 1 697 | 75.7 % |
| CLOSED | 906 | 568 | 62.7 % |
| (null) | 11 | 0 | 0 % |

To upgrade MD completeness we'd need to parse `raw_detail_html` for the decedent residence — the
HTML is captured for 3 149 rows.

### MN (`minnesota`) — 17 rows (still ramping)

| case_status | Rows | dec.street | % |
|---|---:|---:|---:|
| Closed | 9 | 9 | 100 % |
| (null) | 5 | 0 | 0 % |
| Open | 2 | 1 | 50 % |
| Under Court Jurisdiction | 1 | 0 | 0 % |

### NC (`north_carolina`) — 5 910 rows, 120 counties

NC Tyler does not expose a top-level case status. We derive it from the `documents` array
(see `runs/probate_search_methods.xlsx` and the per-doc `event_type` codes).

Status proxy used:

- **closed** if any document has event_type matching `%Discharge%`, `%Final Account%`, or
  `Order Closing%` → 0 rows currently match (closure docs may use different language; needs tuning).
- **active_qualified** if any document is `Order Authorizing Issuance Of Letters` / `Letters%` /
  `Certificate of Probate%` → letters have been issued.
- **active_pending** otherwise → file opened, no letters yet.

| Proxy status | Rows | tier-1 complete (dec.line1 + applicant/exec/admin/petitioner.line1 in `parties`) | % |
|---|---:|---:|---:|
| active_pending | 3 679 | 983 | 26.7 % |
| active_qualified | 2 231 | 947 | 42.4 % |

PDF download status: 982 / 5 610 cases have at least one downloaded PDF; 4 126 / 30 101 doc refs
fetched. Tier extraction has only been run on 1 case so far (the rest are pending).

**This is the largest gap to close**: the NC pipeline needs a doc-driven status mapping committed
to a column (`case_status` text) plus full PDF extraction to lift completeness above 50 %.

### OK (`ok_probate_cases`) — 546 rows, 12 counties

| Status (closed_date IS NULL?) | Rows | dec.street | petitioner.street |
|---|---:|---:|---:|
| active | 335 | 0 | 0 |
| closed | 211 | 0 | 0 |

OK is doc-driven: addresses live exclusively inside PDFs. `pdf_urls` is populated for 523/546 cases
but `pdf_extracted_at` is never set — the OK PDF→fields extractor has not run yet.

### WI (`wisconsin`) — 564 rows, 8 counties

| case_status | Rows | dec.street | %  |
|---|---:|---:|---:|
| Open | 456 | 0 | 0 % |
| Closed | 99 | 0 | 0 % |
| Reopened | 9 | 0 | 0 % |

WI WCCA returns a per-case caption + status + filing date but no address. `parties` is `[]` for
every row (the WCCA party endpoint is gated by hCaptcha and isn't being called in current code).
**To get WI complete we need a second-stage party fetch + PDF extraction.**

### MI (`michigan`) — 0 rows

Schema is in place; no production rows yet.

---

## Doc / event → field availability (PDF playbook)

This is the per-doc-type "if this PDF is captured, you can expect to get these fields" mapping the
UI should surface.

### Common doc types (every state)

| Doc type | Yields |
|---|---|
| Petition for Letters / Petition for Probate / Application for Probate | PR name, PR address, PR phone, decedent name, decedent address, decedent DOD, heirs/distributees + addresses, will Y/N |
| Letters Testamentary / Letters of Administration / Order Authorizing Issuance of Letters | Confirms PR appointed (status: active_qualified), PR name, PR address, appointment date |
| Affidavit of Heirship / Family Tree / Family History Documentation | Heir names, relationships, addresses, ages |
| Will (Recorded / Probated) | Beneficiaries, executor nominee, real-property bequests |
| Inventory | Real property addresses, parcel IDs, valuations — **best source for property targeting** |
| Decree Granting Probate / Decree Granting Administration | Final order, appointment confirmed |
| Order of Discharge / Final Account / Order Closing | Closure event — flips status to closed |
| Funeral Bill / Obituary | Funeral home address, decedent residence cross-check |
| Notice to Beneficiary / Citation | Heir/beneficiary names + service addresses |
| Affidavit for Collection (Small Estate) | Substitute for Letters in small-estate cases — same PR + heir data |

### Per-source specifics

- **NY** — `tiered_extraction` JSONB is the structured output. Schema:
  `{decedent, persons[{role, full_name, address, relationship_to_decedent, is_petitioner, is_minor, source_docs, source_field}], real_property[]}`. 944 / 1 063 rows have it populated; the
  field shape is exactly what the UI should render.
- **NC** — `pdf_extractions` JSONB: `[{tier, event_type, document_name, data: {decedent, persons,
  real_property, doc_type_detected}}, ...]`. Only 1 row populated currently.
- **OK** — addresses come **only** from PDFs; doc names already encode the type
  (`PETITION TO ADMINISTER...`, `ORDER FOR HEARING`, etc.). Wire OK PDF extraction next.
- **GPR** — addresses come from the search detail page directly (no PDF needed); `documents[].name`
  + `is_petition` lets the UI flag which docs are the high-value ones.
- **Gwinnett** — `events` array is the doc/event timeline (`name`, `description`, `date`).
- **Cobb / Rockdale** — `pdf_refs` is just `{cid, digest}`; doc-name → field mapping needs the PDF
  fetch & extract step (not yet implemented in production).

---

## Patterns & caveats per source

- **Document ID patterns** (useful for the UI to guess where a doc lives):
  - NC Tyler: `pdf_url` matches `https://portal-nc.tylertech.cloud/Portal//DocumentViewer/DisplayDoc?documentID=<id>&caseNum=<case>&...`
  - GPR: `https://georgiaprobaterecords.com/Imaging/ViewScannedImage.aspx?DocType=<n>&CaseID=<base64>&CID=<base64>`
  - OK OSCN: `https://www.oscn.net/dockets/GetDocument.aspx?ct=<county>&bc=<docid>&cn=<casenum>&fmt=pdf`
  - Gwinnett: `case_type` is always `Estate*` (the asterisk is literal, server-side wildcard)
- **Within-state heterogeneity** — Cobb (Benchmark) and Rockdale (Tyler us-gov-west-1) are the same
  state but completely different portals. Same for GPR vs Gwinnett (statewide aggregator vs
  county-specific Tyler).
- **Within-county doc heterogeneity** — Affidavit for Heirship vs Petition for Probate yield
  different fields (heirs vs PR + heirs). The UI should surface this rather than treating
  "any probate doc" uniformly.
- **"Non-Owner"** — not currently captured as a column anywhere. The closest thing is
  `enriched_records.batchlead_owner_occupied` (BatchLeads enrichment, post-scrape). Decision
  needed: is "non-owner" a derived flag (`decedent_address ≠ batchlead_property_address`) or
  something we want from the source filings?

---

## Open questions for the UI agent

1. **NY status** — `estate_closed` is empty for every row. Should we (a) populate it from the
   tiered extraction's `case_status` field, (b) drop it, or (c) compute "active" purely from
   `petitioner_active = Y`?
2. **CT status** — user note says "status=1 means open" but `raw` in DB doesn't have that field.
   Do we need to re-scrape CT to capture it, or is the user referring to a different attribute?
3. **NC status** — currently derived from `documents`. Do we want to materialize a `case_status`
   text column on the table for fast filtering?
4. **OK / WI / MI / MN** — addresses are missing; PDF extraction (OK) or party endpoints
   (WI hCaptcha) need to be wired before completeness % is meaningful.
5. **MD** — `raw_detail_html` is captured but unparsed for decedent address. Is it worth a
   one-pass HTML-extractor backfill?
6. **"Non-Owner"** — definition needed (see above).

---

## Suggested data shape for the UI

For each scraper produce one row per state-level overview plus N rows per county (with rollups):

```json
{
  "code": "NC",
  "source_name": "NC Tyler Portal",
  "level": "State",
  "counties_in_scope": 100,
  "counties_scraped": 120,    // includes inactive variants
  "table": "north_carolina",
  "date_field_used": "file_date",
  "death_date_supported": false,
  "scrape_window_observed": ["2026-04-01", "2026-04-30"],
  "active_field": "documents[].event_type (proxy)",
  "active_values": ["active_pending", "active_qualified"],
  "closed_values": ["closed (Order of Discharge / Final Account)"],
  "records_active": 5910,
  "records_closed": 0,
  "complete_active_pct": 32.7,
  "complete_closed_pct": null,
  "pdf_capture_pct": 17.5,    // 982 / 5610 cases
  "doc_id_pattern": "https://portal-nc.tylertech.cloud/.../DisplayDoc?documentID=<id>",
  "non_owner_flag": "from enriched_records.batchlead_owner_occupied",
  "open_questions": ["materialize case_status column", "increase PDF download success rate"]
}
```
