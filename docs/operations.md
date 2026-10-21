# Operations

## Daily runs

Each pipeline runs once a day, for the previous day: the run starting at midnight UTC on 21 October loads that pipeline's file for 20 October. Days run one after another, in date order.

When a pipeline is first switched on, a run starts for yesterday. Locally, `landing-zone-upload` simulates the delivery of the sample file as yesterday's on start. If the DAG is first switched on a day or more later, deliver it again under the new date:

```bash
docker compose run --rm landing-zone-upload
```

To deliver it for another day, e.g. before triggering or backfilling that day, give the day as `DELIVERY_DATE`:

```bash
docker compose run --rm -e DELIVERY_DATE=2026-10-15 landing-zone-upload
```

Each CSV in `data/` is delivered as `<file name>/<day>.csv`, the folder its pipeline's source URI reads from.

## Running a day again, or a day without a run

- **A day that already ran,** e.g. after a corrected file was delivered: **clear** its run in the UI. Its rows replace those of the earlier load of that file. If the file has not changed, the load is skipped (`skipped` in `audit.load_log`) and nothing is rewritten.
- **A day without a run:** **trigger** the DAG with that day as its logical date, in the trigger form or with the command below. Locally, deliver that day's file first:
  ```bash
  docker compose run --rm -e DELIVERY_DATE=2026-10-19 landing-zone-upload
  docker compose exec airflow-scheduler airflow dags trigger customer_transactions_pipeline --logical-date 2026-10-19
  ```
  A trigger without a logical date, Airflow 3's default for manual runs, has no day to load: its load fails at once, without retrying, saying what to do.

## Failed and missing days

A day that fails, for example because its file was not delivered, alerts its pipeline's owner and **blocks the following days**: their tasks wait, rather than load data out of order. Either:
- fix the cause (deliver the file, fix the code) and clear the failed run, or
- mark the failed run's tasks as success to skip the day; it can still be run later, by clearing it.

## Backfills

```bash
docker compose exec airflow-scheduler airflow backfill create --dag-id customer_transactions_pipeline \
  --from-date 2026-10-01 --to-date 2026-10-10 --reprocess-behavior failed
```

- `--reprocess-behavior failed` is required: it runs the days without a successful run, and leaves the others.
- The DAG must be switched on: backfill runs wait while it is paused.
- Days run one after another in date order, whatever the backfill's concurrency, and `--run-backwards` is rejected: incremental models rely on loads arriving in order.

## Housekeeping

The `platform_maintenance` DAG expires audit entries and task logs older than their retention (see [Retention](warehouse.md#retention)). Switch it on like any other DAG.

Airflow's own metadata is cleaned by a separate job, which `docker compose up` does not start, because Airflow 3 tasks cannot access the metadata database:

```bash
docker compose run --rm airflow-db-clean
```

It deletes metadata older than 90 days and exits. To run it daily, schedule it on the host, for example with cron (`crontab -e`), here at 03:00:

```
0 3 * * * cd /path/to/data-platform-case-study && docker compose run --rm airflow-db-clean >> /tmp/airflow-db-clean.log 2>&1
```

It runs outside Airflow, so its failures are not alerted: check its output, here `/tmp/airflow-db-clean.log`. In production this could be a scheduled job next to Airflow, such as a Kubernetes CronJob.
