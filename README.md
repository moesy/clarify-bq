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

1. Store your Clarify API key in Google Secret Manager — or skip Secret
   Manager entirely and set `CLARIFY_API_KEY`.
2. Have [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
   with `secretmanager.secretAccessor`, `bigquery.dataEditor`, and `bigquery.jobUser`.
3. Point the CLI at your workspace — every flag is also an env var, so set the
   connection up once:

```sh
export CLARIFY_WORKSPACE=my-workspace
export BQ_PROJECT=my-gcp-project
export CLARIFY_SECRET=projects/my-gcp-project/secrets/clarify-api-key
# ...or, without Secret Manager:
# export CLARIFY_API_KEY=sk-your-key
```

4. Verify:

```sh
clarify-bq check
```

## Back up

```sh
clarify-bq backup
```

Prefer flags over env? `--workspace`, `--project`, `--secret`, `--dataset`,
and `--location` work everywhere. Useful options:

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
pass-through views for lists, users, activities, and the rest. Each backup
re-generates the view definitions so new CRM fields appear as columns
automatically; `clarify-bq views` rebuilds them on demand (after `--no-views`,
or to target another dataset with `--views-dataset`). Full history lives in
`clarify_crm`: one day-partitioned table per resource, the complete payload in
a JSON `data` column, every run identified by `run_id` in the `runs` ledger.

## Schedule

Run it from cron. A lockfile prevents overlap; exit codes tell cron what
happened: `0` complete · `1` failed · `2` partial · `3` config/auth ·
`4` snapshot shrank suspiciously · `5` another run holds the lock.

## License

MIT or Apache-2.0, at your option.
