# Digital Library Data Publication Pipeline

You can access the library at [MAGIC Project](https://www.magic.unina.it).

This pipeline is developed primarily for the MAGIC research project.

Each run detects new Word documents (metadata) and JPEG image scans, parses the data, validates and enriches the book descriptions and summaries with LLM, writes them to the PostgreSQL database, builds IIIF manifests, transfers all files to the image server, and notifies the team via email, Slack, and PagerDuty.

---

## Pipeline Architecture

![Pipeline Architecture](./etl.jpg)

### DAG

| Step | Task ID | What it does |
|------|---------|-------------|
| 1 | `wait_for_new_books` | Polls S3 (or local) for unprocessed `.docx` + matching image folder pairs |
| 2 | `extract_data` | Parses Word documents into 18+ structured fields |
| 3 | `llm_validate` | Auto-corrects author format, dimensions, and weight via HuggingFace LLM |
| 4 | `write_to_postgres` | Upserts `books`, `book_descriptions`, and `viewers` via SSH-tunnelled psycopg2 |
| 5 | `llm_summarize` | Generates an Italian prose summary per book; scores it 0–100 and writes to DB |
| 6 | `generate_manifests` | Builds a IIIF Presentation API v2 `manifest.json` per book |
| 7 | `prepare_uploads` → `upload_to_s3` | Builds S3 upload list (images + manifest), then delivers to the public delivery bucket |
| 8 | `mark_books_processed` | Persists per-book step flags and `fully_processed` to Airflow Variable |
| 9 | `send_email_notification` + `send_slack_notification` | Sends HTML run report and Slack message; triggers PagerDuty on failures |

### LLM Quality Score

Step 5 scores each generated Italian-language summary (0–100) and stores it in `book_descriptions.summary_score`. Summaries below **60** are flagged in the email report.

| Dimension | Weight |
|---|---|
| Length adequacy (3–6 sentences) | 25 pts |
| Prose format (no lists/bullets) | 20 pts |
| Entity coverage (author, title, physical attributes) | 35 pts |
| Language detection (Italian lexical markers) | 20 pts |

### Secrets & Configuration

All connections and variables are resolved in this priority order:

1. **AWS SSM Parameter Store** — configured via `AIRFLOW__SECRETS__BACKEND` in `docker-compose.yaml` (prefix: `/airflow/connections/` and `/airflow/variables/`)
2. **Airflow Connections/Variables** — set through the Airflow UI or CLI
3. **Environment variables** — fallback for CLI / non-Airflow contexts only

### Shared Storage

The `data/` directory is mounted from a named Docker volume (`airflow-shared-data`), not a local host path, so all Celery workers share the same filesystem view. For production deployments with multiple hosts, replace the volume with an NFS/EFS mount or an S3-backed FUSE filesystem.

---

## Prerequisites

- Docker ≥ 24 and Docker Compose V2
- A `.env` file with all required credentials (see below)
- SSH access to the remote image server
- _(Optional)_ A HuggingFace Inference API token for LLM validation and summarisation

---

## Local Setup

```bash
# 1. Clone
git clone <repo-url>
cd data-publication-pipeline

# 2. Create your .env from the template
cp .env.example .env
# Fill in all blank values in .env

# 3. Generate a Fernet key and add it to .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# → paste as AIRFLOW__CORE__FERNET_KEY=...

# 4. Build the custom image and start all services
docker compose up --build -d

# 5. Open the Airflow UI
open http://localhost:8080
# Default login: AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD (from .env)
```

The first startup runs `airflow db migrate` and creates the admin user automatically via the `airflow-init` service.

---

## Environment Variables

All secrets live in `.env` (git-ignored). Copy `.env.example` and fill in every blank value.

| Variable | Required | Description |
|---|---|---|
| `AIRFLOW_UID` | Yes | Host user UID (`id -u`) — prevents volume permission issues |
| `AIRFLOW__CORE__FERNET_KEY` | Yes | Encryption key for Airflow connections (generate once) |
| `AIRFLOW_ADMIN_USER` | Yes | Airflow UI admin username |
| `AIRFLOW_ADMIN_PASSWORD` | Yes | Airflow UI admin password |
| `AIRFLOW__API__SECRET_KEY` | Yes | API token signing key |
| `POSTGRES_PASSWORD` | Yes | Internal Airflow metadata DB password |
| `AIRFLOW_CONN_SSH_DEFAULT` | Yes | SSH connection URI for `FileTransferOperator` |
| `SSH_HOST` / `SSH_USER` / `SSH_PASSWORD` | Yes | Credentials for the `DatabaseUpdateOperator` SSH tunnel |
| `PIPELINE_DB_HOST` / `PIPELINE_DB_PORT` / `PIPELINE_DB_NAME` / `PIPELINE_DB_USER` / `PIPELINE_DB_PASSWORD` | Yes | Remote PostgreSQL (reached through the SSH tunnel) |
| `AIRFLOW__SMTP__SMTP_USER` / `AIRFLOW__SMTP__SMTP_PASSWORD` | Yes | SMTP credentials for email notifications |
| `HUGGINGFACEHUB_API_TOKEN` | Optional | HuggingFace Inference API token (required for steps 3 & 5: LLM validation and summarisation) |
| `AWS_ACCESS_KEY_ID` | Optional* | AWS credentials for SSM Parameter Store secrets backend |
| `AWS_SECRET_ACCESS_KEY` | Optional* | AWS credentials for SSM Parameter Store secrets backend |
| `AWS_DEFAULT_REGION` | Optional* | AWS region where SSM parameters are stored (e.g. `eu-west-1`) |

_\* Required when using AWS SSM as the secrets backend (recommended for production)._

---

## AWS Integration

### SSM Parameter Store (Secrets Backend)

The pipeline uses **AWS SSM Parameter Store** as its primary secrets backend, configured via `AIRFLOW__SECRETS__BACKEND` in `docker-compose.yaml`. This keeps all sensitive credentials out of Airflow's metadata database.

**Parameter naming conventions:**

| Type | SSM path | Example |
|---|---|---|
| Connections | `/airflow/connections/<conn_id>` | `/airflow/connections/ssh_default` |
| Variables | `/airflow/variables/<var_name>` | `/airflow/variables/pipeline_book_status` |

**Setup:**

```bash
# Store a connection (URI format)
aws ssm put-parameter \
  --name "/airflow/connections/ssh_default" \
  --value "ssh://user:password@host:22" \
  --type SecureString

# Store a variable
aws ssm put-parameter \
  --name "/airflow/variables/pipeline_book_status" \
  --value '{}' \
  --type SecureString
```

The IAM role or user must have `ssm:GetParameter` and `ssm:GetParametersByPath` on the `/airflow/*` path prefix.

**Resolution order** (highest → lowest priority):

1. AWS SSM Parameter Store
2. Airflow Connections/Variables (UI or CLI)
3. Environment variables (local/dev fallback)

### S3 for Shared Storage (production)

For multi-host deployments, replace the `airflow-shared-data` Docker volume with an **Amazon S3** bucket mounted via [Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3):

```yaml
# docker-compose.yaml (production override)
volumes:
  airflow-shared-data:
    driver: local
    driver_opts:
      type: fuse.mountpoint-s3
      device: s3://<your-bucket>
      o: "allow_other,uid=50000"
```

This gives all Celery workers a shared filesystem view across hosts without NFS.

## Project Structure

```
.
├── dags/
│   └── magic_book_ingest.py             # Single production DAG (9 steps)
├── operators/
│   ├── sensors/
│   │   └── file_sensor.py               # Custom sensor: watches descriptions/ for book pairs; state from Airflow Variable
│   ├── config/
│   │   └── pipeline_config.yaml         # Pipeline configuration
│   ├── data_extractor.py                # Word doc parser → 18+ structured fields (standard & simple formats)
│   ├── database_operator.py             # PostgreSQL upsert via SSH tunnel; reads creds from Airflow Connections
│   ├── manifest_generator_operator.py   # IIIF Presentation API 2 manifest builder
│   ├── file_transfer_operator.py        # SFTP transfer to image server (single SSH session, batch mode)
│   ├── llm_summarizer_operator.py       # HuggingFace summarisation + quality scoring (0–100)
│   ├── ai_validator.py                  # HuggingFace field validation (author, dimensions, weight)
│   ├── email_notification_operator.py   # HTML email + Slack webhook + PagerDuty alerting
│   └── processed_books.py              # Per-book status tracking via Airflow Variable (pipeline_book_status)
├── utils/
│   ├── fast_process.py                  # Fast processing utilities
│   ├── generator.py                     # Generation utilities
│   ├── manifest_generator.py            # Manifest generation helpers
│   └── js_file_mirador_generator.py     # Mirador viewer JavaScript generator
├── tests/
│   ├── test_data_extractor.py           # Unit tests — extraction utilities
│   ├── test_summary_scoring.py          # Unit tests — LLM quality scoring
│   ├── test_file_transfer.py            # Unit tests — FileTransferOperator
│   ├── test_file_sensor.py              # Unit tests — NewBookSensor
│   ├── test_llm_pipeline.py             # Integration tests — LLM pipeline
│   └── test_dag_integrity.py            # DAG import validation (requires Airflow)
├── scripts/
│   ├── healthcheck.sh                   # Service health check
│   └── custom_run.sh                    # Custom execution script
├── data/
│   ├── descriptions/                    # INPUT: Word documents (*.docx)
│   ├── books/                           # INPUT: JPEG image folders per book
│   ├── manifests/                       # OUTPUT: Generated IIIF manifest.json files
│   └── viewers/                         # OUTPUT: Viewer configuration files
├── .github/workflows/ci.yml             # GitHub Actions CI/CD
├── Dockerfile                           # Custom Airflow 3.1.3 image
├── docker-compose.yaml                  # Full stack: Postgres, Redis, Airflow, Flower (with AWS SSM & Shared Data volumes)
├── requirements.txt                     # Python dependencies
├── pyproject.toml                       # Ruff + pytest config
└── .env.example                         # Template for .env
```

---

## Development

### Running tests locally

```bash
pip install pytest python-docx paramiko pyyaml
pytest tests/test_data_extractor.py tests/test_summary_scoring.py -v
```

### Linting

```bash
pip install ruff
ruff check dags/ operators/ tests/
```

### DAG integrity check (requires Airflow installed)

```bash
export AIRFLOW_HOME=/tmp/airflow_dev
airflow db migrate
PYTHONPATH=. airflow dags list
pytest tests/test_dag_integrity.py -v
```

---

## CI/CD

GitHub Actions runs on every push and pull request to `main`:

| Job | What it checks |
|---|---|
| `lint` | `ruff check` across `dags/`, `operators/`, `tests/` |
| `unit-tests` | Pure-Python tests (no Airflow needed) — extractor utilities + LLM scoring |
| `dag-integrity` | Full Airflow 3.1.3 install → `airflow dags list` + DAG import tests |
| `deploy` | SSH deploy to production server (on push to `main` only) |

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Required GitHub Secrets for the deploy job: `SERVER_HOST`, `SERVER_USER`, `SERVER_PASSWORD`, `SERVER_DEPLOY_PATH`.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.1.3 (Celery executor) |
| Containerization | Docker + Docker Compose |
| Metadata DB | PostgreSQL 15 |
| Task queue | Redis 7 |
| Document parsing | python-docx |
| Remote DB access | psycopg2 + paramiko (SSH tunnel) |
| Digital library standard | IIIF Presentation API 2 |
| LLM integration | HuggingFace Inference API (validation + summarisation) |
| Secrets management | AWS SSM Parameter Store (SecureString) → Airflow Connections/Variables → env vars |
| Cloud (optional) | AWS S3 (shared volume via Mountpoint) |
| Logging | Python `logging` module → Airflow task logs (all `self.log` / `log.*`) |
| Alerting | Email (SMTP), Slack Webhook, PagerDuty Events API v2 |
| State tracking | Airflow Variable `pipeline_book_status` (JSON, per-book flags) |
| Linting | Ruff |
| Testing | pytest |
| CI/CD | GitHub Actions |
