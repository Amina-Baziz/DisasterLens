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
| `Dockerfile` | Airflow image with Java, PySpark and the PostgreSQL JDBC driver |
| `docker-compose.yml` | Local stack: Airflow, MinIO (community image `pgsty/minio`, console branded SILO) and PostgreSQL |
| `.env.example` | Environment variable names for the stack |
| `requirements.txt` | Python packages used by the project |
| `.gitignore` | Files and folders kept out of version control |

## Prerequisites

- Git
- Docker Desktop, running, with at least 6 GB of memory

## Quick start

1. Clone the repository and open the folder.
2. Create your own `.env` file by copying `.env.example` to `.env` (PowerShell: `Copy-Item .env.example .env`).
3. Fill in the values in `.env` (see below).
4. Start the stack: `docker compose up -d`. The first start builds the Airflow image and takes a few minutes. If the build fails with a download error, run the command again.
5. Check that everything is up: `docker compose ps -a`. `airflow-init` should show `Exited (0)`, which is normal. The other services should be running.
6. Open the MinIO console and create a bucket with the same name as `MINIO_BUCKET`.

## Environment variables

No account or API key is needed, these are local settings. Your `.env` file stays on your own machine and is never committed.

- Passwords and usernames: choose your own, using letters and numbers only. The MinIO password needs at least 8 characters.
- `POSTGRES_HOST` and `MINIO_ENDPOINT`: use the service names `postgres` and `minio:9000`.
- `POSTGRES_PORT`: use `5432`.
- `POSTGRES_HOST_PORT`: use `5432`, or another number if that port is already in use on your machine.
- `POSTGRES_DB` and `MINIO_BUCKET`: for example `disasterlens` and `disasterlens-lake`.
- `AIRFLOW_UID`: use `50000`.
- `AIRFLOW__CORE__FERNET_KEY`: generate one with `python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"` and paste the result.
- `USGS_API_URL` and `EONET_API_URL`: use the public endpoints of the two sources.

## Services

| Service | Address | Login |
| --- | --- | --- |
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` and `AIRFLOW_ADMIN_PASSWORD` |
| MinIO console | http://localhost:9001 | `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` |
| PostgreSQL | `localhost:<POSTGRES_HOST_PORT>` | `POSTGRES_USER` and `POSTGRES_PASSWORD` |

Airflow stores its own tables in the same PostgreSQL database as the warehouse. The warehouse tables use separate schemas.

## Stopping and resetting

- Stop the stack and keep the data: `docker compose down`
