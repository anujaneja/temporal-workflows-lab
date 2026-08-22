# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go + Temporal learning lab (see `docs/temporal-spec.md` for the full spec). It's a REST API that submits jobs, which run as Temporal workflows, backed by a PostgreSQL job store. The project is built in phases (see `docs/temporal-spec.md` section 21, "Development Phases"); Phases 1–5 (infrastructure, basic workflow, reliability/retries, parallel dependencies, child workflows) are done. Phase 6 (scheduling) is next. A parallel Java + Spring Boot implementation of the same experiments is planned but not yet started — all current code is under `go/`.

## Commands

```bash
# Unit tests (fast, no infra) — Ginkgo v2 + Gomega
make test-unit
cd go && go test ./internal/... -v -count=1          # long form
cd go && go test ./internal/activity/... -v          # single package
cd go && go test -race ./internal/...                # with race detector

# API integration tests — spins up the full stack via testcontainers-go automatically
# (Docker must be running; no manual `docker compose up` needed)
make test-api
cd go && go test ./tests/integration/... -tags integration -v -count=1 -timeout 10m

# Both, in order
make test-all

# Full local stack via Docker Compose
docker compose up --build
docker compose down -v

# Local dev without Docker (after `docker compose up postgresql app-postgres flyway temporal temporal-ui`)
cd go && APP_DB_DSN="postgres://lab:lab@localhost:5433/lab?sslmode=disable" TEMPORAL_HOST="localhost:7233" go run ./cmd/worker
cd go && APP_DB_DSN="postgres://lab:lab@localhost:5433/lab?sslmode=disable" TEMPORAL_HOST="localhost:7233" API_PORT="8081" go run ./cmd/api
```

Temporal UI: http://localhost:8080. API: http://localhost:8081. App Postgres is exposed on host port 5433 (container port 5432) to avoid colliding with Temporal's internal Postgres.

## Architecture

```
Client --POST /jobs--> Go API (gin) --gRPC--> Temporal Server --task queue--> Go Worker (activities)
                            |                                                       |
                            +-------------- txn: CreateJob + StartWorkflow ---------+--> app-postgres (pgxpool)
```

- **Transactional submission**: `Handler.SubmitJob` (`go/internal/api/handlers.go`) opens a DB transaction via `Store.RunInTx`, inserts the job row, then starts the Temporal workflow inside that same transaction closure. If `ExecuteWorkflow` fails, the transaction rolls back so the DB and Temporal never disagree about whether a job exists. Note: if the *commit* fails after Temporal already accepted the workflow, the DB row is briefly absent until `StoreResultsActivity` upserts it on completion — a documented, inherent limitation of dual-write systems, not a bug to "fix" reflexively.
- **Temporal is the source of truth for live state**; Postgres is the record of completed jobs. `GetJob` always queries Temporal (`DescribeWorkflowExecution`) rather than the DB.
- **Store abstraction** (`go/internal/store/store.go`): `Store` interface with a Postgres implementation (`postgres.go`, pgx/pgxpool) and hand-rolled fakes per test package (`fakeStore` in each `_test` package — not a shared mock). `RunInTx` returns a `txStore` that reuses the same `pgx.Tx`; nested `RunInTx` calls do not create savepoints, they just reuse the outer transaction. `BatchRecord`/`SaveBatchResult` persist per-child-workflow batch results into `job_batches`, separate from the final `jobs` row.
- **Three workflows share one task queue** (`workflow.TaskQueue = "workflow-lab"`, `go/internal/workflow/`):
  - `DataProcessingWorkflow` — sequential: Validate → Fetch → Process → Store.
  - `ParallelProcessingWorkflow` — Validate → Fetch → fan-out (`ProcessPartA` + `ProcessPartB` executed concurrently, both `ExecuteActivity` calls issued before either `.Get()`) → `AggregateResults` → Store. Item list is split in half; `SimulateDependencyFailure` (`"PART_A"`/`"PART_B"`/`"BOTH"`) injects a retryable failure into one or both branches for observing Temporal's per-branch retry behavior.
  - `BatchProcessingWorkflow` — Validate → Fetch → fan-out into `JobRequest.BatchCount` (default 3) **child workflow** executions of `ProcessBatchWorkflow`, run concurrently and awaited in order → Store (aggregated). Each child is started with `ChildWorkflowOptions.ParentClosePolicy = PARENT_CLOSE_POLICY_REQUEST_CANCEL` so cancelling the parent asks every running child to cancel gracefully rather than terminating it. `SimulateChildFailure` (`"FIRST"`/`"ALL"`) injects a retryable failure into one or all batches.
  - `ProcessBatchWorkflow` is a genuine child workflow (not just a local activity fan-out): `ProcessBatchActivity` → `StoreBatchActivity`, registered on the worker like any other workflow.
  - `SubmitJob` routes between the three based on `JobRequest.UseParallelWorkflow` / `UseBatchWorkflow`.
  - Activity options (timeouts + `RetryPolicy`) are defined once per activity *kind* in `data_processing.go` (`validateActivityOptions`, `fetchActivityOptions`, `processActivityOptions`, `storeActivityOptions`) and reused by all three workflows — don't duplicate these when adding a workflow.
- **Activities** (`go/internal/activity/`): all activity methods hang off one `Activities` struct (`activities.go`) holding shared dependencies (currently just `Store`), registered as Temporal activities in bulk via `w.RegisterActivity(acts)` in `cmd/worker/main.go`. Workflows reference them via method expressions: `workflow.ExecuteActivity(ctx, (*activity.Activities).ValidateJobActivity, req)`. When adding a new activity that needs a dependency, add it to this struct rather than introducing a second dependency-holder.
- **Failure simulation is a first-class pattern here**, not test-only scaffolding: activities check `req.SimulateFailure` / `req.SimulateStoreFailure` / `req.SimulateDependencyFailure` / `req.SimulateChildFailure` and `activity.GetInfo(ctx).Attempt` to fail deterministically on early attempts and succeed later, so retry/backoff behavior is directly observable in the Temporal UI. Follow this pattern (a boolean/enum request field + attempt-count check + `temporal.NewApplicationError`) when adding new failure-injection points, rather than e.g. random failure or panics.
- **Error surfacing**: `failResult`/`leafMessage` in `data_processing.go` unwrap Temporal's `ActivityError` → `ApplicationError` chain to extract the leaf application error message for `JobResult.Error`. Reuse these helpers rather than re-implementing error unwrapping in a new workflow.
- **Schema migrations** live in `db/migrations/` (Flyway-managed, `V<N>__description.sql` naming) and are applied by the short-lived `flyway` Compose service before `api`/`worker` start (`depends_on: service_completed_successfully`). The integration suite bind-mounts the same directory into its own Flyway container.

## Testing conventions

- Both unit and integration suites use Ginkgo v2 + Gomega. Each package has its own `suite_test.go` with a `TestXxx(t *testing.T)` entrypoint calling `RunSpecs`.
- Unit tests fake collaborators by hand (a small `fakeStore` per package implementing `store.Store`) rather than using a generated/shared mock — follow that pattern for new interfaces.
- Workflow unit tests use the Temporal SDK's test environment and mock activities at the environment level (see `go/internal/workflow/testhelpers_test.go`); they never touch a real store. Child workflows are mocked the same way via `env.OnWorkflow(...)` (args include `ctx` — count them like `OnActivity`); to test parent cancellation propagating to real (non-mocked) children, register the child workflow for real, make its activity mock block via `.After(duration)`, and fire `env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, ...)` before it resolves — see `batch_processing_test.go`.
- Integration tests (`go/tests/integration/`, build tag `integration`) use testcontainers-go to boot the entire stack from scratch per run (two Postgres instances, Temporal server, Flyway, and the Worker + API built from `go/Dockerfile`'s `worker`/`api` targets) — no dependency on `docker compose` being up. `suite_test.go`'s `BeforeSuite` resolves `db/migrations` relative to the test file via `runtime.Caller`, so tests work regardless of the invocation directory.
- A Cursor rule (`.cursor/rules/keep-spec-and-tests-in-sync.mdc`) requires: any new/changed API endpoint, workflow, activity, or request/response field must be reflected in `docs/temporal-spec.md` (targeted edits to the relevant Phase section, not full rewrites) and covered by a new/updated `Describe`/`It` block in `go/tests/integration/` (pattern: POST, assert status/shape, `pollJobUntilDone` if it starts a workflow). Internal-only refactors to `internal/store`, `internal/model`, or `internal/temporalclient` that don't change endpoint behavior do not need an integration test.
