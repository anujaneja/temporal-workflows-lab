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
9. [Running tests](#running-tests)
10. [Project layout](#project-layout)

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
- **Child workflows for batching** — `BatchProcessingWorkflow` splits work across `ProcessBatchWorkflow` child workflow executions run concurrently, each with its own retryable activities and `ParentClosePolicy: REQUEST_CANCEL` so parent cancellation propagates gracefully to every running child.

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

**Routing to a different workflow** (all default to the sequential `DataProcessingWorkflow`):

| Field | Type | Effect |
|-------|------|--------|
| `useParallelWorkflow` | bool | Routes to `ParallelProcessingWorkflow` (splits items into two halves, processes them concurrently, then aggregates). |
| `useBatchWorkflow` | bool | Routes to `BatchProcessingWorkflow` (splits items into `batchCount` batches, each processed by its own `ProcessBatchWorkflow` **child workflow**, run concurrently, then aggregated). |
| `batchCount` | int | Number of child workflows `BatchProcessingWorkflow` fans out to. Only applies with `useBatchWorkflow`. Defaults to 3; capped at `itemCount` so no batch is empty. |

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

## Running tests

The project has two test layers. Both use **Ginkgo v2 + Gomega** so the output format is consistent.

### Unit tests (fast, no infrastructure)

Uses the Temporal SDK test harness and in-memory fakes — no live server or database required.

```bash
# Run all unit tests
make test-unit

# Equivalent long form
cd go && go test ./internal/... -v -count=1

# Single package with verbose Ginkgo output
cd go && go test ./internal/activity/... -v

# With race detector
cd go && go test -race ./internal/...
```

**Test coverage per package:**

| Package | Specs | What is covered |
|---------|-------|----------------|
| `internal/activity` | 39 | ValidateJobActivity, FetchItemsActivity, ProcessItems/PartA/PartB, StoreResultsActivity, AggregateResultsActivity, ProcessBatchActivity, StoreBatchActivity — all success + failure paths |
| `internal/workflow` | 19 | DataProcessingWorkflow (4 scenarios), ParallelProcessingWorkflow (6 scenarios), ProcessBatchWorkflow (3 scenarios), BatchProcessingWorkflow (5 scenarios, including parent cancellation) with mocked activities |
| `internal/api` | 14 | SubmitJob, GetJob, CancelJob — valid, invalid, and error cases |
| `internal/store` | 6 | DBConfig defaults and field values |

### API integration tests (full stack, containers required)

Uses [testcontainers-go](https://golang.testcontainers.org/) to spin up the complete stack automatically — **no manual `docker compose up` needed**. The test binary manages every container's lifecycle: two PostgreSQL instances, the Temporal server, Flyway migrations, the Worker, and the API.

**Prerequisite:** Docker must be running.

```bash
# Run all API integration tests (recommended)
make test-api

# Equivalent long form
cd go && go test ./tests/integration/... -tags integration -v -count=1 -timeout 10m
```

**What happens when you run `make test-api`:**

1. A private Docker bridge network is created.
2. `postgres:16-alpine` starts for Temporal's internal state.
3. `postgres:16-alpine` starts for the application database.
4. `temporalio/auto-setup:1.25.2` starts and connects to its postgres (up to 120 s to become healthy).
5. `flyway/flyway:10-alpine` runs the migrations from `db/migrations/` and exits.
6. The **Worker** image is built from `go/Dockerfile` (target `worker`) and started.
7. The **API** image is built from `go/Dockerfile` (target `api`) and started; the suite waits for `GET /health → 200` before running any specs.
8. All specs run against the live API over a random host port.
9. All containers are terminated and the network is removed in `AfterSuite`.

> **First run** pulls images and builds the Go binaries — expect 2–4 minutes. Subsequent runs reuse Docker's layer cache and finish much faster.

**Integration test coverage:**

| Describe block | Specs | What is covered |
|----------------|-------|----------------|
| `GET /health` | 1 | 200 with `status: ok` and correct Temporal address |
| `POST /jobs` — immediate response | 3 | 202 shape, explicit jobId, 400 on malformed JSON |
| `POST /jobs` — sequential workflow | 2 | Polls to `COMPLETED`, verifies `itemsProcessed`; fairness fields |
| `POST /jobs` — parallel workflow | 1 | `useParallelWorkflow: true` reaches `COMPLETED` |
| `POST /jobs` — batch workflow | 1 | `useBatchWorkflow: true` fans out to child workflows and reaches `COMPLETED` |
| `POST /jobs` — failure simulation | 3 | `simulateFailure`, `simulateStoreFailure`, and `simulateChildFailure` all retry and complete |
| `GET /jobs/:id` | 3 | 404 unknown ID, `RUNNING` immediately after submit, full result on `COMPLETED` |
| `POST /jobs/:id/cancel` | 3 | Running job cancelled and confirmed by polling; batch job's children cancelled via `ParentClosePolicy`; 500 on non-existent job |

### Run everything

```bash
make test-all
```

This runs unit tests first (fast), then the integration suite.

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

### Simulate a failing child batch (`simulateChildFailure`)

Only applies when `useBatchWorkflow` is `true`. `ProcessBatchActivity` fails on attempts 1–2 for the targeted batch(es), then succeeds on attempt 3 — observable as independent per-child retries in the Temporal UI.

```bash
curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId":             "acme",
    "itemCount":            9,
    "useBatchWorkflow":     true,
    "batchCount":           3,
    "simulateChildFailure": "FIRST"
  }' | jq
```

**Values:** `FIRST` (only batch 0 fails) | `ALL` (every batch fails) | omit for no failure.

Watch the retries progress in real time at [http://localhost:8080](http://localhost:8080).

### Cancelling a batch job (parent → child cancellation)

Cancelling a job started with `useBatchWorkflow: true` cancels `BatchProcessingWorkflow`, which starts each `ProcessBatchWorkflow` child with `ParentClosePolicy: REQUEST_CANCEL`. Every still-running child receives a cancellation request instead of being abruptly terminated — visible in the Temporal UI as `WorkflowExecutionCancelRequested` events on each child execution.

```bash
JOB=$(curl -s -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{"tenantId":"acme","itemCount":60,"useBatchWorkflow":true,"batchCount":3}')
JOB_ID=$(echo $JOB | jq -r .jobId)
curl -s -X POST http://localhost:8081/jobs/$JOB_ID/cancel | jq
```

---

## Project layout

```
temporal-workflows-lab/
├── Makefile                             # test-unit / test-api / test-all targets
├── db/
│   └── migrations/
│       ├── V1__create_jobs_table.sql        # Flyway-managed schema
│       └── V2__create_job_batches_table.sql # Per-batch results for BatchProcessingWorkflow
├── docs/
│   └── temporal-spec.md                # Phase-by-phase design spec
├── go/
│   ├── cmd/
│   │   ├── api/main.go                 # HTTP API entry point
│   │   └── worker/main.go              # Temporal worker entry point
│   ├── internal/
│   │   ├── activity/                   # Temporal activity implementations
│   │   │   ├── activities.go           # Activities struct (holds dependencies)
│   │   │   ├── aggregate.go            # AggregateResultsActivity
│   │   │   ├── batch.go                # ProcessBatchActivity, StoreBatchActivity
│   │   │   ├── fetch.go                # FetchItemsActivity
│   │   │   ├── processing.go           # ProcessItemsActivity, ProcessPartA/BActivity
│   │   │   ├── storage.go              # StoreResultsActivity
│   │   │   └── validate.go             # ValidateJobActivity
│   │   ├── api/
│   │   │   └── handlers.go             # HTTP handlers (SubmitJob, GetJob, CancelJob)
│   │   ├── model/
│   │   │   └── job.go                  # Domain types (JobRequest, JobResult, Priority …)
│   │   ├── store/
│   │   │   ├── postgres.go             # pgxpool implementation + DBConfig + RunInTx
│   │   │   └── store.go                # Store interface + JobRecord/BatchRecord
│   │   ├── temporalclient/
│   │   │   └── dial.go                 # Retry-aware Temporal client dialer
│   │   └── workflow/
│   │       ├── data_processing.go      # DataProcessingWorkflow (sequential)
│   │       ├── parallel_processing.go  # ParallelProcessingWorkflow (parallel activities)
│   │       ├── batch_processing.go     # BatchProcessingWorkflow (parallel child workflows)
│   │       └── process_batch.go        # ProcessBatchWorkflow (child workflow)
│   ├── tests/
│   │   └── integration/                # API integration tests (build tag: integration)
│   │       ├── suite_test.go           # Container lifecycle (BeforeSuite / AfterSuite)
│   │       ├── helpers_test.go         # HTTP client helpers + pollJobUntilDone
│   │       ├── health_test.go          # GET /health specs
│   │       └── jobs_test.go            # POST /jobs, GET /jobs/:id, POST /jobs/:id/cancel specs
│   ├── Dockerfile                       # Multi-stage build (api + worker targets)
│   ├── go.mod
│   └── go.sum
└── docker-compose.yml                   # Full stack (Temporal + app services)
```
