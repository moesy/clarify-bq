# clarify-bq

Back up a [Clarify](https://clarify.ai) CRM workspace to BigQuery.

Each run appends a full snapshot — records with relationships, schemas, lists,
users, workflows, settings, per-record activities and attachment metadata —
and maintains flat views of the latest snapshot for easy querying. Append-only:
history is never overwritten, so any past state is one query away.

> Unofficial; not affiliated with Clarify.

## Install

```sh
cargo install clarify-bq
```

## Setup

1. Store your Clarify API key in Google Secret Manager (or set `CLARIFY_API_KEY`).
2. Have [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
   with `secretmanager.secretAccessor`, `bigquery.dataEditor`, and `bigquery.jobUser`.
3. Verify:

```sh
clarify-bq check \
  --workspace my-workspace \
  --project my-gcp-project \
  --secret projects/my-gcp-project/secrets/clarify-api-key
```

## Back up

```sh
clarify-bq backup --workspace my-workspace --project my-gcp-project \
  --secret projects/my-gcp-project/secrets/clarify-api-key
```

Flags come from env vars too (`CLARIFY_WORKSPACE`, `BQ_PROJECT`, `CLARIFY_SECRET`,
`BQ_DATASET`, `BQ_LOCATION`). Useful options:

| Flag | Effect |
|------|--------|
| `--dry-run` | print the plan, write nothing |
| `--objects person,deal` | only these objects' records |
| `--skip activities,attachments` | skip the per-record fetches (much faster) |
| `--partition-expiration-days 400` | retention; `0` keeps forever |
| `--output json` | machine-readable run summary |

## Query

The latest snapshot is always live in flat views — no refresh needed:

```sql
SELECT record_id, name_full_name, company_id
FROM clarify_crm_latest.person;
```

One view per object with typed columns generated from its schema, plus
pass-through views for lists, users, activities, and the rest. Full history
lives in `clarify_crm`: one day-partitioned table per resource, the complete
payload in a JSON `data` column, every run identified by `run_id` in the
`runs` ledger.

## Schedule

Run it from cron. A lockfile prevents overlap; exit codes tell cron what
happened: `0` complete · `1` failed · `2` partial · `3` config/auth ·
`4` snapshot shrank suspiciously · `5` another run holds the lock.

## License

MIT or Apache-2.0, at your option.
