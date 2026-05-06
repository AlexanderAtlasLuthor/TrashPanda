# TrashPanda — example data

Synthetic sample files used in tests, demos, and operator docs. None
of the addresses or domains in these files are real — do not pilot
or send to anything you find here.

## Files

### `sample_contacts.csv`

Tiny 4-row input that demonstrates the **input schema** TrashPanda
accepts (id, email, domain, fname, lname, state, address, county,
city, zip, website, ip). Useful as a smoke fixture: feed it to the
cleaner end-to-end and you should get one row tagged as a typo
(`bob@gmial.com` → `gmail.com`), one duplicate, and one suspicious
test-domain row.

### `WY_small.csv`

Real-data subset (~600 rows) sampled from the Wyoming voter list
(public record). Same input schema as `sample_contacts.csv`. Used
by the cleaner test fixtures and by the small-batch calibration
script `scripts/calibrate_wy_small_v2.py`.

### `pilot_send_sample.csv`

Synthetic snapshot of a **pilot_send_tracker** SQLite table after a
50-row pilot batch has been launched and the bounce poller has run
twice. Mirrors the real schema exactly:

| column | meaning |
|--------|---------|
| `id` | tracker row id (autoincrement) |
| `job_id`, `batch_id` | scope per launch |
| `source_row` | row index in the original input file |
| `email`, `domain`, `provider_family` | recipient identity |
| `verp_token` | per-row VERP token used in the Return-Path |
| `message_id` | locally-generated `Message-ID` header |
| `sent_at` | when the SMTP transaction succeeded |
| `state` | `pending_send` → `sent` → `verdict_ready` → `expired` |
| `dsn_status` | `delivered` / `hard_bounce` / `soft_bounce` / `blocked` / `deferred` / `unknown` |
| `dsn_received_at`, `dsn_diagnostic`, `dsn_smtp_code` | what the IMAP poller parsed from the bounce |
| `last_polled_at` | last IMAP sweep that touched this row |
| `created_at`, `updated_at` | tracker bookkeeping |

The 50 rows are split to look like a realistic mid-flight pilot:

| state | rows | meaning |
|-------|------|---------|
| `sent` (no DSN) | 20 | still inside the 24h wait window — neither bounced nor confirmed delivered yet |
| `verdict_ready` / `delivered` | 8 | positive MDN received from the destination |
| `verdict_ready` / `hard_bounce` | 8 | 5xx, address does not exist |
| `verdict_ready` / `soft_bounce` | 5 | 4xx, mailbox full / temporary failure |
| `verdict_ready` / `blocked` | 4 | 5xx, spam policy / IP reputation |
| `verdict_ready` / `deferred` | 3 | 4xx greylisting |
| `verdict_ready` / `unknown` | 2 | network error or all-MX-failed before any reply |

Provider family mix matches what you'd see on a B2C list: 12 gmail,
8 yahoo, 6 outlook/hotmail, 2 aol, 10 corporate (known infra), 12
corporate (unknown infra).

Use this file as a regression fixture when working on
`finalize.py`, the operator UI, or any verdict-aggregation logic —
it covers every state/verdict combination the rest of the system has
to handle, so a query that sums `hard_bounce` over the file should
return exactly 8.
