# Runbook

This file shows how the pipeline behaves when things go wrong, with the evidence from the tests we ran, and how to check and recover the system.

## Where to look

| Question | Where to look |
|---|---|
| Did the last run work? | Airflow, DAG `disasterlens_pipeline`, Graph view (http://localhost:8080) |
| Why did a run fail? | Click the red task in Airflow, then Log. The same error is saved in `ops.pipeline_runs.error_message` |
| Is the data fresh and correct? | Dashboard, tabs Data quality and Pipeline health |
| Are the raw files there? | MinIO console (http://localhost:9001), bucket `disasterlens-lake`, folder `raw/` |

## Failure tests

Each test was run on the real stack. The screenshots are in `docs/screenshots/`.

### A. Rerun (idempotency)

Running the same pipeline twice must not create duplicate events.

- A scheduled run fetched 215 records and inserted 212 new events.
- A manual run straight after fetched 214 records and inserted 0 and updated 0.
- Why: the merge uses `event_key` as the key (`INSERT ... ON CONFLICT DO UPDATE`), and it only changes a row when the source reports a newer update time.

### B. API failure

The USGS address in `.env` was changed to a wrong one (`all_dayXX.geojson`) and the pipeline was triggered.

- `extract_usgs_to_lake` failed, went to `up_for_retry`, retried twice (2 minutes apart), then ended as `failed`. Screenshots: `p1-07_api_failure_retry.png`, `p1-07_api_failure_failed.png`.
- Every task after it was marked `upstream_failed`, so no half-finished data reached the warehouse. `extract_eonet_to_lake` still succeeded and `record_run` still ran. Screenshot: `p1-07_api_failure_graph.png`.
- `ops.pipeline_runs` got a row with status `failed` and the error `extract_usgs_to_lake failed`.
- After the URL was fixed, the next run finished green.

### C. Empty response

The USGS address was changed to a query that returns no earthquakes (magnitude 9.9 or higher since 2026-11-18).

- The run finished `success`: 2 records fetched (only EONET had data), 0 inserted, 0 updated, no error.
- Nothing was deleted from the warehouse.

### D. Restart

All containers were stopped with `docker compose down` (without `-v`) and started again with `docker compose up -d`.

- Before the restart: 39,584 events and 12 logged runs.
- After the restart: 39,584 events and 12 logged runs. Nothing was lost, because PostgreSQL and MinIO keep their data in Docker volumes.

## How to restart the system

```
docker compose up -d
```

To stop it without losing data, use `docker compose down`. Never add `-v`, because it deletes the data volumes (warehouse and data lake).

## If a run fails

1. Open the failed task in Airflow and read the Log.
2. Fix the cause (for example a wrong URL in `.env`, then `docker compose up -d --force-recreate airflow-scheduler airflow-webserver`).
3. Trigger the DAG again. The merge is safe to repeat, so no cleanup is needed.
4. Check the Pipeline health tab on the dashboard. The newest run should be `success`.

## If data looks wrong

1. Open the Data quality tab. Rules marked FAIL (errors) mean the data is wrong. Warnings are worth a look.
2. Check the raw files of that run in MinIO. They are kept as they arrived, so a run can be traced back to its source response.
3. Fix the cause and rerun. Rows that are already correct are left unchanged.
