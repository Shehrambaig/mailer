# OpenLetterConnect (OLC) setup

The `/send` page places direct-mail orders through the
[OpenLetterConnect API](https://api-docs.openletterconnect.com/) and logs every
send to Neon (`olc_orders` / `olc_order_items`). Until the account below is
created, the page works in "review-only" mode: you can upload and dedupe lists,
but the send button stays disabled.

## One-time onboarding

1. **Create the account** at <https://app.openletterconnect.com/>.
2. **Create an API key**: profile icon → Settings → API Keys → Create.
   Give it a name, pick a source, leave expiry blank for a non-expiring key.
3. **Design a letter template** in the OLC dashboard (their template builder
   supports merge fields like first name and property address). Note which
   product you'll send — e.g. Professional Letter 8.5x11, First Class.
4. **Set env vars** — locally in `.env` and on Vercel:

   ```
   OLC_API_KEY=...            # required
   OLC_BASE_URL=...           # optional; defaults to the DEMO environment
   ```

   | Environment | Base URL |
   |---|---|
   | Demo (default) | `https://demoapi.openletterconnect.com/api/v1` |
   | Production | `https://api.openletterconnect.com/api/v1` |

   Leave `OLC_BASE_URL` unset while testing; set it to the production URL to
   send real mail. The page shows a DEMO/LIVE chip so you always know which
   environment the key is pointed at.

5. **Create the send-log tables** (already done once, idempotent):

   ```
   .venv/bin/python scripts/create_olc_tables.py
   ```

## How the /send flow works

1. **Compose** — pick a product and template (loaded live from OLC) and name
   the order.
2. **Recipients** — two sources:
   - **Upload** the sendable `.xlsx`/`.csv` (the output of the
     Foreclosure-Project dedupe pipeline: First Name, Last Name, Street, City,
     State, Zip Code, Auction Date, Estimated Value, Offer Value, Campaign
     Phone — header spellings are matched loosely, extra columns ignored).
   - **Load queued from mailer table** — rows the pipeline queued directly in
     Neon (`mailer.mailed_at IS NULL`). See "Queue flow" below.
3. **Review** — every row is validated and checked against three send-history
   sources; flagged rows are deselected by default:
   - `sent · olc` — address already sent through this page (`olc_order_items`)
   - `sent · mailer` — address in the hand-loaded `mailer` history table
   - `exported` — address in a previous mail-house export
     (`mailing_export_items`)
   - `dup` — same address appears earlier in the uploaded file
   - rows without a name send as "Current Resident"
4. **Send** — one OLC order is placed for all selected recipients
   (`POST /orders` with the full contacts array), then logged to Neon.
5. **Track** — the Sent Orders table polls OLC on demand (refresh button) and
   records per-recipient status, delivery flag, and tracking code. No inbound
   webhook is needed, so everything stays behind the app's Basic Auth.
   Undeliverable pieces show up as OLC item status `Returned to Sender`
   (other statuses: Not Mailed, Processing, Mailed, In Transit, Re-Routed,
   Delivered, Canceled, Failed).

## Queue flow (DB-direct, added 2026-08-06)

The `mailer` table is now **queue + send history** in one:

- `mailed_at IS NULL` → the row is **queued**, waiting to be sent
- `mailed_at` set → the row is **send history** and feeds suppression
  (`olc_order_id` points at the local `olc_orders.id` that sent it)

Migration: `scripts/add_mailer_queue_columns.py` (idempotent; the first run
backfilled all 110,948 pre-queue rows as sent using their `loaded_at`).

**Pipeline insert contract** — the Foreclosure-Project pipeline queues a row
by inserting it with these columns populated (and `mailed_at` left NULL):

```
first_name, last_name          -- else the letter goes to "Current Resident"
street, city, state, zip_code  -- required; state 2-letter, zip 5-digit
                               -- (keep leading zeros — NJ/MA/CT zips!)
auction_date                   -- optional
estimated_value, offer_value   -- offer_value fills the check amount
campaign_phone                 -- tracking number printed on the letter
source_file                    -- batch tag, e.g. 'pipeline-2026-08-10'
```

The weekly re-mail policy (**3 weeks deduped + last week send-all**) stays in
the pipeline: it decides *what gets queued*. Because of that, on the /send
page queue rows treat the send-history flags (`sent · olc`, `sent · mailer`,
`exported`) as **warnings only** — they stay selected. In-file duplicate and
invalid-address rows are still deselected. File uploads keep the old strict
behavior (history flags deselect).

Placing an order from the queue stamps the sent rows' `mailed_at` +
`olc_order_id` in the same transaction that writes `olc_orders` /
`olc_order_items`, so they leave the queue and immediately become
suppression history.

**Anyone querying `mailer` as send history must now filter
`WHERE mailed_at IS NOT NULL`** — including ad-hoc DataGrip queries and any
future SMS/skiptrace tooling; queued rows haven't been mailed yet.

## Merge variables (template "Check mailer pfc", id 11597)

Verified 2026-08-04 via OLC's view-proof endpoint (free, renders a PDF
without mailing anything — the **Preview proof** button on /send does this
for the first selected recipient):

| Tag in template | Filled from |
|---|---|
| `{{C.FIRST_NAME}}`, `{{C.PROPERTY_ADDRESS}}` | per-recipient contact fields |
| `{{SPF.FIRST_NAME/LAST_NAME/COMPANY_NAME/PHONE_NUMBER}}` | the order's `returnAddress` — set the `OLC_RETURN_*` vars in `.env`, else OLC account settings |
| `{{CF.OFFER_AMOUNT}}` | the "Offer Amount" **custom field** (account-level, id 1632). Per-recipient value is passed on the contact under the key `"Offer Amount"` (the field's display name — snake_case/camelCase are ignored). The send page maps the upload's Offer Value column to it automatically, formatted `$175,000`. |
| `{{CF.OFFER_AMOUNT_WORDS}}` | the "Offer Amount Words" custom field (id 1633) — the same offer spelled out ("One Hundred Seventy-Five Thousand Dollars"), auto-generated from the Offer Value column. Place it on the check's written-amount line in the builder. |

Gotchas learned the hard way:
- Custom-field names may only contain alphanumerics and spaces (no
  underscores), and hand-typing `{{sometag}}` as text in the template builder
  renders literally as `{{sometag:invalid}}` — insert fields from the
  builder's merge-field picker instead.
- The check-style template is tied to product 18 ("8.5x11 Snap Pack Mailer,
  First Class").

## Address fingerprint

Suppression uses the Foreclosure-Project canonical key so it agrees with the
xlsx dedupe pipeline (`dedup_mailer.py`):

```
NORM(street)|NORM(city)|NORM(state)|zip5
NORM: uppercase, non-[A-Z0-9 ] → space, collapse whitespace
zip5: digits only, first five
```

The `mailing_export_items` comparison additionally uses that table's own
legacy fingerprint (alnum-only street + zero-padded zip).
