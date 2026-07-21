# clarify-bq

Append-only backup of a [Clarify](https://clarify.ai) CRM workspace into Google BigQuery.

Every run fetches a **full snapshot** of the workspace — records (with
relationships), object schemas, lists and list rows, users, workflows,
settings, per-record activities (change history + comments), and attachment
metadata — and appends it to BigQuery, stamped with a `run_id` and
`snapshot_at`. Nothing is ever updated or deleted: point-in-time recovery is a
`WHERE` clause, and daily snapshots double as a longitudinal analytics dataset.

> Unofficial tool; not affiliated with Clarify.

## Install

```sh
cargo install clarify-bq
```

## Setup

You need:

1. A Clarify API key, stored in Google Secret Manager (or in the
   `CLARIFY_API_KEY` env var for local use).
2. Google Application Default Credentials
   (`gcloud auth application-default login`, a service-account key via
   `GOOGLE_APPLICATION_CREDENTIALS`, or a GCP runtime identity) with:
   - `roles/secretmanager.secretAccessor` on the secret
   - `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` on the project

Verify everything before the first run:

```sh
clarify-bq check \
  --workspace your-workspace \
  --project your-gcp-project \
  --secret projects/your-gcp-project/secrets/clarify-api-key
```

## Usage

```sh
clarify-bq backup --workspace your-workspace --project your-gcp-project \
  --secret projects/your-gcp-project/secrets/clarify-api-key

clarify-bq backup ... --objects person,deal      # only these objects' records
clarify-bq backup ... --skip activities,attachments  # trade history for speed
clarify-bq backup ... --dry-run                  # print the plan, write nothing
clarify-bq objects ...                           # list discoverable object types
clarify-bq mark-complete <run_id> ...            # repair an unmarked run
```

All connection flags are also environment variables: `CLARIFY_WORKSPACE`,
`BQ_PROJECT`, `CLARIFY_SECRET`, `BQ_DATASET` (default `clarify_crm`),
`BQ_LOCATION` (default `US`; immutable once the dataset is created).

`--skip` accepts: `records`, `schemas`, `lists`, `list_rows`, `users`,
`workflows`, `settings`, `activities`, `attachments`, and `records:<object>`.
Unknown tokens are rejected before anything is fetched.

## Scheduling

The CLI is one-shot; schedule it with cron or CI. A lockfile prevents
overlapping runs (exit 5 = benign skip). Exit codes:

| Code | Meaning |
|------|---------|
| 0 | complete |
| 1 | failed (records failed, or systemic error) |
| 2 | partial (an auxiliary resource failed; records are fine) |
| 3 | configuration or auth error |
| 4 | shrink check tripped — data loaded, but a resource shrank >5% vs the previous run |
| 5 | another run holds the lock |

`--output json` prints a machine-readable end-of-run summary. Recommended
warehouse-side dead-man's alert: no `runs` row with `status='complete'` in the
last 25 hours.

## Reading the data

Each resource lands in its own day-partitioned, `run_id`-clustered table with
the payload in a native `JSON` column. The `runs` table is the ledger: a run
only counts once its row exists with `status='complete'`.

```sql
DECLARE latest STRUCT<run_id STRING, snapshot_at TIMESTAMP> DEFAULT (
  SELECT AS STRUCT run_id, snapshot_at FROM clarify_crm.runs
  WHERE status = 'complete' ORDER BY snapshot_at DESC LIMIT 1);

SELECT record_id, JSON_VALUE(data.attributes.name) AS name
FROM clarify_crm.records_person
WHERE run_id = latest.run_id
  AND snapshot_at BETWEEN latest.snapshot_at
      AND TIMESTAMP_ADD(latest.snapshot_at, INTERVAL 1 DAY);
```

Always filter on `snapshot_at` as well as `run_id` so partition pruning keeps
query cost at one snapshot, not the table's full history.

## Retention

Partitions expire after **400 days** by default (13 months: enough for
year-over-year comparisons). Configure with `--partition-expiration-days`;
`0` keeps everything forever. The `runs` ledger never expires. Raise the value
*before* history you care about ages out — expired partitions are gone.

## Notes and limits

- Snapshots are per-object read-committed, not point-in-time consistent; each
  resource's fetch window is recorded in the `runs` row.
- Activities/attachments cost one paged request per record. Large workspaces
  spend most of the run there (~35 min per 100k records at Clarify's rate
  ceiling); `--skip activities,attachments` when speed matters more.
- Attachment *content* is not downloaded (Clarify serves short-lived URLs);
  metadata only. Meetings and transcripts have no read API and cannot be
  backed up.
- Record payloads never appear in logs at any verbosity.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
