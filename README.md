# temporal-workflows-lab

A Go + Temporal learning lab demonstrating durable workflow orchestration with a PostgreSQL-backed job store, connection pooling, and transactional job submission.

---

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Quick start (Docker Compose)](#quick-start-docker-compose)
4. [Running locally (without Docker)](#running-locally-without-docker)
5. [Environment variables](#environment-variables)
6. [API reference & curl examples](#api-reference--curl-examples)
7. [Observing workflows in Temporal UI](#observing-workflows-in-temporal-ui)
8. [Failure simulation](#failure-simulation)
9. [Project layout](#project-layout)

---

## Architecture overview

```
┌─────────────┐   POST /jobs    ┌──────────────┐   gRPC    ┌────────────────┐
│   Client    │ ──────────────► │   Go API     │ ─────────► │ Temporal Server│
└─────────────┘                 │  (gin HTTP)  │            └────────┬───────┘
                                └──────┬───────┘                     │
                                       │ txn: CreateJob + StartWf    │ task queue
                                       ▼                             ▼
                                ┌─────────────┐            ┌────────────────┐
                                │ app-postgres │◄───────────│   Go Worker    │
                                │  (pgxpool)  │ upsert     │  (activities)  │
                                └─────────────┘            └────────────────┘
```

**Key design decisions:**

- **Transactional submission** — `SubmitJob` opens a DB transaction, inserts the job record, then starts the Temporal workflow. If Temporal rejects the workflow the transaction is rolled back, keeping the two systems consistent.
- **pgx / pgxpool** — uses `github.com/jackc/pgx/v5/pgxpool` for native PostgreSQL support, richer error types, and first-class connection pooling.
- **Connection pool** — `MaxConns` (default 10) prevents overloading PostgreSQL. Override with `DB_MAX_CONNS`.
- **Flyway migrations** — schema changes live in `db/migrations/` and are applied automatically before the API or worker start.

---

## Prerequisites

| Tool | Minimum version | Notes |
|------|-----------------|-------|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | 24+ | Required for Compose |
| [Docker Compose](https://docs.docker.com/compose/) | v2 | Bundled with Docker Desktop |
| [Go](https://go.dev/dl/) | 1.27 | Only needed for local development |

---

## Quick start (Docker Compose)

```bash
# 1. Clone the repository
git clone <repo-url>
cd temporal-workflows-lab

# 2. Build and start all services
docker compose up --build

# Services that come up:
#   temporal-postgres  — Temporal's internal PostgreSQL
#   app-postgres       — application PostgreSQL (port 5433)
#   flyway-migrate     — runs DB migrations, then exits
#   temporal           — Temporal Server (port 7233)
#   temporal-ui        — Temporal Web UI (port 8080)
#   temporal-lab-api   — Go HTTP API (port 8081)
#   temporal-lab-worker— Go worker
```

Wait for the log line `API listening on :8081` before sending requests.

To stop and remove all containers and volumes:

```bash
docker compose down -v
```

---

## Running locally (without Docker)

Use this when you want fast iteration on the Go code without rebuilding images.

### 1. Start infrastructure only

```bash
docker compose up postgresql app-postgres flyway temporal temporal-ui
```

### 2. Run the worker

```bash
cd go
APP_DB_DSN="postgres://lab:lab@localhost:5433/lab?sslmode=disable" \
TEMPORAL_HOST="localhost:7233" \
go run ./cmd/worker
```

### 3. Run the API server

```bash
cd go
APP_DB_DSN="postgres://lab:lab@localhost:5433/lab?sslmode=disable" \
TEMPORAL_HOST="localhost:7233" \
API_PORT="8081" \
go run ./cmd/api
```

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `API_PORT` | `8081` | Port the HTTP API listens on |
| `APP_DB_DSN` | `postgres://lab:lab@localhost:5433/lab?sslmode=disable` | PostgreSQL DSN for the application DB |
| `DB_MAX_CONNS` | `10` | Upper limit on open connections in the pgxpool. Raise it if you observe pool exhaustion under load. |

---

## API reference & curl examples

### Health check

```bash
curl -s http://localhost:8081/health | jq
```

Expected response (200):

```json
{
  "status": "ok",
  "temporal": "localhost:7233"
}
```

---

### Submit a job — `POST /jobs`

Starts a `DataProcessingWorkflow` and atomically records the job in PostgreSQL.

**Minimal request** (all optional fields use defaults):

```bash
curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{"tenantId": "acme", "itemCount": 5}' | jq
```

**Full request with all fields:**

```bash
curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobId":        "job-001",
    "tenantId":     "acme",
    "priority":     "HIGH",
    "fairnessKey":  "team-alpha",
    "itemCount":    10
  }' | jq
```

Expected response (202):

```json
{
  "jobId":      "job-001",
  "workflowId": "job-001",
  "runId":      "a1b2c3d4-...",
  "status":     "RUNNING"
}
```

**Priority values:** `HIGH` | `MEDIUM` (default) | `LOW`

---

### Get job status — `GET /jobs/:id`

Queries Temporal for the live workflow state.

```bash
curl -s http://localhost:8081/jobs/job-001 | jq
```

While running:

```json
{
  "jobId":  "job-001",
  "runId":  "a1b2c3d4-...",
  "status": "RUNNING"
}
```

After completion:

```json
{
  "jobId":          "job-001",
  "status":         "COMPLETED",
  "itemsProcessed": 10,
  "itemsFailed":    0,
  "duration":       2500000000
}
```

---

### Cancel a job — `POST /jobs/:id/cancel`

Sends a graceful cancellation request to the running workflow.

```bash
curl -s -X POST http://localhost:8081/jobs/job-001/cancel | jq
```

Expected response (200):

```json
{
  "jobId":  "job-001",
  "status": "CANCELLED"
}
```

---

### End-to-end test sequence

```bash
BASE=http://localhost:8081

# 1. Health check
curl -s $BASE/health | jq

# 2. Submit a job
JOB=$(curl -s -X POST $BASE/jobs \
  -H "Content-Type: application/json" \
  -d '{"tenantId":"test","itemCount":3}')
echo $JOB | jq
JOB_ID=$(echo $JOB | jq -r .jobId)

# 3. Poll until completed
for i in $(seq 1 10); do
  STATUS=$(curl -s $BASE/jobs/$JOB_ID | jq -r .status)
  echo "status: $STATUS"
  [ "$STATUS" != "RUNNING" ] && break
  sleep 2
done

# 4. Final result
curl -s $BASE/jobs/$JOB_ID | jq
```

---

## Observing workflows in Temporal UI

Open [http://localhost:8080](http://localhost:8080) in your browser.

- **Workflows** tab shows all submitted jobs, their status, and history.
- Clicking a workflow shows the full event history: activity schedule/start/complete, retries, and inputs/outputs.
- Use the **Query** tab to run Temporal queries against running workflows.

---

## Failure simulation

The workflow supports two injection flags to observe Temporal's retry behaviour:

### Simulate processing failure (`simulateFailure`)

`ProcessItemsActivity` fails on attempts 1–2 with a retryable error, then succeeds on attempt 3.

```bash
curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId":        "acme",
    "itemCount":       5,
    "simulateFailure": true
  }' | jq
```

### Simulate store failure (`simulateStoreFailure`)

`StoreResultsActivity` fails on attempts 1–2, then succeeds on attempt 3.

```bash
curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId":            "acme",
    "itemCount":           5,
    "simulateStoreFailure": true
  }' | jq
```

Watch the retries progress in real time at [http://localhost:8080](http://localhost:8080).

---

## Project layout

```
temporal-workflows-lab/
├── db/
│   └── migrations/
│       └── V1__create_jobs_table.sql   # Flyway-managed schema
├── docs/
│   └── temporal-spec.md                # Phase-by-phase design spec
├── go/
│   ├── cmd/
│   │   ├── api/main.go                 # HTTP API entry point
│   │   └── worker/main.go              # Temporal worker entry point
│   ├── internal/
│   │   ├── activity/                   # Temporal activity implementations
│   │   │   ├── activities.go           # Activities struct (holds dependencies)
│   │   │   ├── fetch.go                # FetchItemsActivity
│   │   │   ├── processing.go           # ProcessItemsActivity
│   │   │   ├── storage.go              # StoreResultsActivity
│   │   │   └── validate.go             # ValidateJobActivity
│   │   ├── api/
│   │   │   └── handlers.go             # HTTP handlers (SubmitJob, GetJob, CancelJob)
│   │   ├── model/
│   │   │   └── job.go                  # Domain types (JobRequest, JobResult, Priority …)
│   │   ├── store/
│   │   │   ├── postgres.go             # pgxpool implementation + DBConfig + RunInTx
│   │   │   └── store.go                # Store interface
│   │   ├── temporalclient/
│   │   │   └── dial.go                 # Retry-aware Temporal client dialer
│   │   └── workflow/
│   │       └── data_processing.go      # DataProcessingWorkflow definition
│   ├── Dockerfile                       # Multi-stage build (api + worker targets)
│   ├── go.mod
│   └── go.sum
└── docker-compose.yml                   # Full stack (Temporal + app services)
```
