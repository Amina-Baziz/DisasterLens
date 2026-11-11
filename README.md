# DisasterLens

DisasterLens collects earthquake data from the USGS API and natural event data from NASA EONET, stores the raw responses in MinIO, cleans and deduplicates them with PySpark, loads them into a PostgreSQL warehouse and shows them in a Streamlit dashboard. Airflow runs the pipeline every hour.

## Repository layout

| Path | Purpose |
| --- | --- |
| `dags/` | Airflow DAGs for the hourly pipeline and historical backfill |
| `src/` | API clients, MinIO storage helpers, PySpark transform and warehouse load |
| `sql/` | Warehouse schema, analytics queries and data quality rules |
| `dashboard/` | Streamlit dashboard |
| `tests/` | Tests for the transform job |
| `docs/` | Architecture description |
| `Dockerfile` | Airflow image with Spark |
| `docker-compose.yml` | Local stack: Airflow, MinIO, PostgreSQL and the dashboard |
| `.env.example` | Environment variable names for the stack |
| `requirements.txt` | Python packages used by the project |
| `.gitignore` | Files and folders kept out of version control |


## Environment variables

Create your own `.env` file by copying `.env.example` to `.env`, then fill in the values. No account or API key is needed, these are local settings.

- Passwords and usernames: choose your own.
- Hosts and endpoints: use the service names `postgres` and `minio:9000`.
- `AIRFLOW_UID`: use `50000`.
- `AIRFLOW__CORE__FERNET_KEY`: leave empty for now.
- `USGS_API_URL` and `EONET_API_URL`: use the public endpoints of the two sources.

The `.env` file stays on your own machine and is never committed.