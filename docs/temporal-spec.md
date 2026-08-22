# Temporal Workflow Lab — SDD v1

## 1. Purpose

**Temporal Workflow Lab** is a locally runnable application designed to explore and demonstrate practical Temporal workflow patterns using two independent implementations:

- Go + Temporal Go SDK
- Java + Spring Boot + Temporal Java SDK

The Go implementation will be built first. The Java implementation will reproduce the same functional behavior and experiments.

The application will process configurable jobs through Temporal workflows and will progressively demonstrate:

- Workflow/activity execution
- Activity retries
- Dependency handling
- Child workflows
- Scheduled workflows
- Priority
- Fairness keys
- Rate limiting
- Exponential backoff
- Event coalescing
- Observability and metrics

The application is primarily a **learning and experimentation platform**, not a production workflow engine.

---

# 2. Goals

### Primary goals

1. Learn Temporal deeply through practical experiments.
2. Understand workflow and activity execution semantics.
3. Understand Temporal task queues and worker behavior.
4. Experiment with retries and failure handling.
5. Understand child workflows and workflow dependencies.
6. Experiment with scheduled workflow execution.
7. Explore priority and fairness concepts.
8. Implement rate-limited external API interaction.
9. Experiment with event coalescing.
10. Collect and analyze workflow/activity metrics.
11. Run the entire system locally using Docker.
12. Implement the same application in Go and Java to compare the Temporal SDKs.

### Non-goals

The project will **not** attempt to become:

- A generic workflow engine.
- A replacement for Temporal.
- A production-grade multi-tenant platform.
- A general-purpose scheduling system.
- A complete distributed job queue.

---

# 3. High-Level Architecture

```
                         ┌────────────────────┐
                         │   REST API Server  │
                         │        Go          │
                         └─────────┬──────────┘
                                   │
                                   │ Start / Schedule /
                                   │ Events
                                   ▼
                         ┌────────────────────┐
                         │      Temporal      │
                         │       Server       │
                         └─────────┬──────────┘
                                   │
                            Task Queues
                                   │
                                   ▼
                         ┌────────────────────┐
                         │    Go Worker       │
                         │                    │
                         │ Workflows          │
                         │ Activities         │
                         └─────────┬──────────┘
                                   │
                     ┌─────────────┼──────────────┐
                     ▼             ▼              ▼
                PostgreSQL      Redis        Mock External
                                               Service
```

Supporting infrastructure:

```
Temporal UI
Prometheus
Grafana
Load Generator
```

All components run locally through Docker Compose.

---

# 4. Core Domain

The application revolves around a **Job**.

```
Job
 ├── id
 ├── tenantId
 ├── priority
 ├── fairnessKey
 ├── status
 ├── createdAt
 └── configuration
```

Example:

```json
{
  "id": "job-123",
  "tenantId": "tenant-a",
  "priority": "HIGH",
  "fairnessKey": "tenant-a"
}
```

The exact persistence model can remain minimal during the first implementation.

---

# 5. Core Workflow

The primary workflow is:

```
DataProcessingWorkflow
        │
        ├── ValidateJob
        │
        ├── FetchItems
        │
        ├── ProcessItems
        │
        └── StoreResults
```

The workflow should remain deterministic.

Business logic that performs external I/O must live inside Activities.

### Workflow input

```
JobRequest
```

### Workflow output

```
JobResult
```

Example:

```
JobResult
 ├── jobId
 ├── status
 ├── itemsProcessed
 ├── itemsFailed
 └── duration
```

---

# 6. Activities

Initial activities:

### ValidateJobActivity

Responsibilities:

- Validate input.
- Validate required configuration.
- Reject invalid jobs.

### FetchItemsActivity

Responsibilities:

- Fetch mock data.
- Simulate an external dependency.
- Return items to process.

### ProcessItemsActivity

Responsibilities:

- Process the supplied items.
- Optionally simulate failures.
- Optionally simulate rate limiting.

### StoreResultsActivity

Responsibilities:

- Store processing results.
- Simulate persistence failure when required.

### ProcessBatchActivity

Responsibilities:

- Process the items belonging to one batch, on behalf of a `ProcessBatchWorkflow` child workflow.
- Optionally simulate a failure for a specific batch or for every batch (child-failure experiments).

### StoreBatchActivity

Responsibilities:

- Persist one batch's result into `job_batches`, independently of the final aggregated job result.

---

# 7. Task Queues

Initially use a single task queue:

```
workflow-lab
```

The worker registers:

```
Workflows
Activities
```

Later experiments may introduce separate queues to study:

```
workflow-lab-high
workflow-lab-normal
workflow-lab-low
```

and/or specialized workers.

The SDD should not assume that task queues automatically provide application-level priority or fairness. Those behaviors must be explicitly defined and experimentally verified.

---

# 8. Retry Model

Activities will use Temporal retry policies.

Example conceptual configuration:

```
Initial interval:       1 second
Backoff coefficient:    2
Maximum interval:       30 seconds
Maximum attempts:       5
```

Expected behavior:

```
Attempt 1
   ↓
1 sec
   ↓
Attempt 2
   ↓
2 sec
   ↓
Attempt 3
   ↓
4 sec
   ↓
Attempt 4
   ↓
8 sec
```

The application must distinguish between:

### Retryable errors

Examples:

```
temporary network failure
HTTP 429
HTTP 503
temporary database failure
```

### Non-retryable errors

Examples:

```
invalid input
invalid configuration
authentication failure
malformed request
```

The retry behavior should be observable through Temporal UI and application metrics.

---

# 9. Dependency Handling

The workflow must support explicit activity dependencies.

Example:

```
Validate
   ↓
Fetch
   ↓
Process
   ↓
Store
```

A later experiment will introduce parallel branches:

```
             ┌── ProcessA ──┐
Fetch ───────┤              ├── Aggregate
             └── ProcessB ──┘
```

The application will test:

- Sequential dependencies.
- Parallel execution.
- Failure propagation.
- Optional branches.
- Aggregation after dependent operations.

---

# 10. Child Workflows

Implemented as `BatchProcessingWorkflow`, a third top-level workflow alongside `DataProcessingWorkflow` and `ParallelProcessingWorkflow` (routed via `JobRequest.UseBatchWorkflow`).

```
BatchProcessingWorkflow
        │
        ├── ValidateJob
        ├── FetchItems
        │
        ├── ProcessBatchWorkflow (batch 0)   ─┐
        ├── ProcessBatchWorkflow (batch 1)   ─┼── all children awaited
        └── ProcessBatchWorkflow (batch N)   ─┘
        │
        └── StoreResults (aggregated across all batches)
```

The items returned by `FetchItems` are split into `JobRequest.BatchCount` contiguous batches (default 3, capped at item count so no batch is empty). Each batch is processed by its own `ProcessBatchWorkflow` child workflow execution, started before any is awaited, so Temporal runs them concurrently.

Each child workflow processes one batch.

```
ProcessBatchWorkflow
        │
        ├── ProcessBatchActivity
        └── StoreBatchActivity
```

`StoreBatchActivity` upserts a row into `job_batches` (keyed on `job_id, batch_index`), so each child's completion is independently observable in the database as well as in the Temporal UI.

Experiments implemented:

- **Multiple / parallel child workflows** — `BatchCount` children are started concurrently (fan-out) and awaited in order (fan-in); the first failing child's error fails the parent.
- **Child failure** — `JobRequest.SimulateChildFailure` (`"FIRST"` fails only batch 0, `"ALL"` fails every batch) injects a retryable error into `ProcessBatchActivity` on attempts 1–2, succeeding on attempt 3, so per-child retry behavior is observable independently in the Temporal UI.
- **Parent waiting for children** — the parent blocks on every child future before aggregating and storing the final result.
- **Parent cancellation / child cancellation behavior** — each child is started with `ChildWorkflowOptions.ParentClosePolicy = PARENT_CLOSE_POLICY_REQUEST_CANCEL` (rather than the SDK default `TERMINATE`), so cancelling the parent (`POST /jobs/{id}/cancel`) asks every still-running child to cancel gracefully instead of being abruptly killed. Observable in the Temporal UI as `WorkflowExecutionCancelRequested` events on each child.

---

# 11. Scheduling

The application must support workflows triggered in two ways.

### Immediate trigger

```
POST /jobs
```

starts a workflow immediately.

### Scheduled trigger

The application will support simple recurring schedules such as:

```
Every 1 hour
Every 6 hours
Every 24 hours
```

Temporal's scheduling functionality will be used rather than implementing a custom scheduler.

The application should expose basic operations such as:

```
Create schedule
Pause schedule
Resume schedule
Delete schedule
```

The system must distinguish between:

```
Schedule
     vs
Workflow Execution
```

---

# 12. Priority

Jobs support:

```
HIGH
MEDIUM
LOW
```

Priority is an experimental feature.

The system must measure whether higher-priority work actually receives lower scheduling latency under contention.

Example experiment:

```
10000 LOW priority jobs
+
100 HIGH priority jobs
```

Metrics:

```
queue latency
workflow start latency
completion latency
```

The implementation must document the mechanism used to achieve priority rather than assuming Temporal provides arbitrary application-level priority semantics.

---

# 13. Fairness Key

Each job may specify:

```
fairnessKey
```

Example:

```
tenant-A
tenant-B
tenant-C
```

The purpose is to prevent one workload source from dominating worker capacity.

Example:

```
tenant-A → 10000 jobs
tenant-B → 100 jobs
tenant-C → 100 jobs
```

The experiment will compare:

```
Without fairness
```

against:

```
With fairness
```

Metrics:

```
jobs processed per key
queue latency per key
p95 latency per key
```

The implementation must clearly separate:

- Temporal's scheduling/task dispatch behavior.
- Application-level fairness mechanisms.

---

# 14. Rate Limiting

The application will contain a mock external API.

Example:

```
MockExternalAPI
    limit = 20 requests/sec
```

The workflow may generate substantially more requests.

Example:

```
Worker
  │
  ├── request
  ├── request
  ├── request
  └── ...
       ↓
Mock API
       ↓
HTTP 429
```

The application must handle rate limiting without creating a tight retry loop.

Expected behavior:

```
429
 ↓
backoff
 ↓
retry
 ↓
429
 ↓
backoff
 ↓
retry
```

Backoff should use:

```
Exponential backoff
+
maximum delay
+
jitter
```

Rate-limit metrics:

```
rate_limit_hits
retry_count
backoff_duration
successful_requests
failed_requests
```

---

# 15. Event Coalescing

The application will support events affecting the same entity.

Example:

```
Entity A updated
Entity A updated
Entity A updated
Entity A updated
```

Instead of executing four independent processing operations:

```
Process A
Process A
Process A
Process A
```

the system should experiment with coalescing them into:

```
Process A
```

The initial implementation will use Temporal workflow state/signaling mechanisms where appropriate.

Example:

```
POST /events

{
  "entityId": "customer-123",
  "type": "UPDATED"
}
```

Experiment:

```
100 updates
     ↓
same entity
     ↓
coalescing window
     ↓
single processing operation
```

Metrics:

```
events_received
events_coalesced
workflows_started
processing_operations
```

---

# 16. Observability

The application must expose metrics.

### Workflow metrics

```
workflow_started_total
workflow_completed_total
workflow_failed_total
workflow_cancelled_total
workflow_duration
```

### Activity metrics

```
activity_started_total
activity_completed_total
activity_failed_total
activity_retry_total
activity_duration
```

### Queue metrics

```
task_queue_latency
schedule_to_start_latency
```

### Business metrics

```
jobs_submitted
jobs_completed
jobs_failed
items_processed
items_failed
```

### Feature-specific metrics

```
rate_limit_hit_total
coalesced_event_total
jobs_per_fairness_key
priority_distribution
```

Prometheus will collect metrics.

Grafana will provide local dashboards.

---

# 17. REST API

Initial API:

```
POST   /jobs
GET    /jobs/{id}
POST   /jobs/{id}/cancel

POST   /schedules
GET    /schedules
POST   /schedules/{id}/pause
POST   /schedules/{id}/resume
DELETE /schedules/{id}

POST   /events

GET    /health
```

The API should remain thin.

It should primarily:

```
receive request
    ↓
validate request
    ↓
start/signal Temporal workflow
    ↓
return response
```

Workflow orchestration belongs in Temporal.

---

# 18. Docker Environment

Initial environment:

```
Temporal Server
Temporal UI
Go API
Go Worker
```

Second iteration:

```
PostgreSQL
Redis
```

Third iteration:

```
Prometheus
Grafana
Load Generator
Mock External API
```

Target:

```
docker compose up
```

should eventually start the complete environment.

No cloud services should be required.

---

# 19. Go Project Structure

```
go/
├── cmd/
│   ├── api/
│   └── worker/
│
├── internal/
│   ├── workflow/
│   │   ├── data_processing.go
│   │   ├── batch.go
│   │   └── scheduler.go
│   │
│   ├── activity/
│   │   ├── validation.go
│   │   ├── fetch.go
│   │   ├── processing.go
│   │   └── storage.go
│   │
│   ├── api/
│   ├── service/
│   └── model/
│
├── Dockerfile
└── go.mod
```

The exact package structure may evolve during implementation.

---

# 20. Java Implementation

After the Go version is complete, implement the same system using:

```
Java
Spring Boot
Temporal Java SDK
Micrometer
Prometheus
```

The Java implementation must preserve the same:

- workflow behavior
- activity behavior
- retry semantics
- schedules
- child workflows
- priority experiments
- fairness experiments
- rate-limit behavior
- event coalescing
- metrics
- load scenarios

This allows a meaningful comparison between the Go and Java Temporal SDKs.

---

# 21. Development Phases

### Phase 1 — Infrastructure

```
- [ ] Docker Compose
- [ ] Temporal Server
- [ ] Temporal UI
- [ ] Go API container
- [ ] Go Worker container
```

### Phase 2 — Basic Workflow

```
- [ ] Job API
- [ ] Workflow
- [ ] Activities
- [ ] Task queue
- [ ] Workflow result
```

### Phase 3 — Reliability

```
- [ ] Activity timeouts
- [ ] Retry policies
- [ ] Retryable errors
- [ ] Non-retryable errors
- [ ] Failure propagation
```

### Phase 4 — Dependencies

```
- [x] Sequential dependencies
- [x] Parallel activities
- [x] Aggregation
- [x] Dependency failure experiments
```

### Phase 5 — Child Workflows

```
- [x] Batch workflow
- [x] Parallel child workflows
- [x] Child failure
- [x] Parent cancellation
```

### Phase 6 — Scheduling

```
- [ ] Create schedule
- [ ] Pause schedule
- [ ] Resume schedule
- [ ] Delete schedule
- [ ] Recurring execution
```

### Phase 7 — Scheduling Policies

```
- [ ] Priority
- [ ] Fairness key
- [ ] Contention experiments
```

### Phase 8 — External Dependencies

```
- [ ] Mock API
- [ ] Rate limiting
- [ ] HTTP 429
- [ ] Exponential backoff
- [ ] Jitter
```

### Phase 9 — Event Processing

```
- [ ] Event API
- [ ] Workflow signaling
- [ ] Event coalescing
- [ ] Coalescing metrics
```

### Phase 10 — Observability

```
- [ ] Prometheus
- [ ] Grafana
- [ ] Workflow metrics
- [ ] Activity metrics
- [ ] Queue metrics
- [ ] Business metrics
```

### Phase 11 — Experiments

```
- [ ] Load generator
- [ ] Priority experiment
- [ ] Fairness experiment
- [ ] Retry experiment
- [ ] Rate-limit experiment
- [ ] Child workflow experiment
- [ ] Coalescing experiment
```

### Phase 12 — Java

```
- [ ] Spring Boot API
- [ ] Temporal Java worker
- [ ] Same workflows
- [ ] Same activities
- [ ] Same experiments
- [ ] Go vs Java comparison
```

---

# 22. Definition of Done

The project is complete when:

1. `docker compose up` starts the complete local environment.
2. A job can be submitted through REST.
3. The job executes through a Temporal workflow.
4. Activities execute on workers.
5. Failures trigger configured retries.
6. Dependency failures behave predictably.
7. Child workflows can process batches.
8. Workflows can be scheduled.
9. Priority behavior can be experimentally measured.
10. Fairness behavior can be experimentally measured.
11. Rate-limited APIs are handled with exponential backoff.
12. Events can be coalesced.
13. Metrics are visible in Prometheus/Grafana.
14. Load scenarios can reproduce the experiments.
15. The complete behavior is reproduced in Java/Spring Boot.

---

## 23. Most important development rule

For every Temporal feature we add, we should follow this cycle:

```
Requirement
    ↓
SDD change
    ↓
Implementation
    ↓
Test
    ↓
Run experiment
    ↓
Observe Temporal UI
    ↓
Observe metrics
    ↓
Document what actually happened
```

That last step is particularly important.

The purpose isn't merely to say **"I know how to use Temporal retries."**

It should eventually give you a repository where you can say:

> "I tested 10,000 low-priority jobs and 100 high-priority jobs under worker contention, measured queue latency, and observed how the chosen scheduling mechanism affected the result."
> 

That makes this substantially more valuable as a **Staff-level systems/AI-assisted engineering learning project** than a collection of Temporal code samples.

### What we should build next

With this SDD established, **Phase 1 should be the Go project skeleton + Docker Compose + Temporal Server + Temporal UI + a minimal worker**.

---

## 12. Testing Standards

Every phase of this project **must ship with unit tests**. Tests are not optional and not deferred.

### Framework

All tests in the Go implementation are written using **Ginkgo v2 + Gomega**:

```
github.com/onsi/ginkgo/v2   — BDD test runner
github.com/onsi/gomega      — rich matchers
go.temporal.io/sdk/testsuite — Temporal test harness (no live server required)
```

### Test scope per package

| Package | What is tested | How |
|---------|---------------|-----|
| `internal/activity` | Each activity method: success path, failure-simulation path, edge cases | `testsuite.WorkflowTestSuite.NewTestActivityEnvironment()` |
| `internal/workflow` | Full workflow orchestration: happy path, each failure branch, parallel fan-out/fan-in | `testsuite.WorkflowTestSuite.NewTestWorkflowEnvironment()` with mocked activities |
| `internal/api` | HTTP handlers: valid/invalid requests, store errors, Temporal errors, response shapes | `gin.TestMode` + `httptest.NewRecorder()` with hand-rolled fakes |
| `internal/store` | `DBConfig` defaults and pool settings | Direct struct assertions (no live DB) |

### Rules

1. **Every new activity must have a `_test.go` file** that covers at least: happy path, non-retryable error case (if applicable), and retryable error case (if applicable).
2. **Every new workflow must have a `_test.go` file** that covers: happy path and each failure branch using mocked activities.
3. **Every new HTTP handler must have tests** covering: success, invalid input (400), and server-side errors (500).
4. **No `database/sql`-level integration tests** ship by default — unit tests use fakes. Integration tests require a real PostgreSQL instance and are opt-in (tagged with `//go:build integration`).
5. **Ginkgo suite bootstraps** (`suite_test.go`) live in each package directory. Each file calls `RunSpecs(t, "<Package> Suite")`.
6. **Fakes over mocks** — hand-rolled fakes that implement the interface are preferred over generated mocks for store and client dependencies. This makes tests more readable and avoids generated-code churn.
7. **`env.AssertExpectations(GinkgoT())`** must be called in `AfterEach` for all workflow tests that use `env.OnActivity(...)` to ensure every mocked call was made exactly as expected.

### Running tests

```bash
# Run all unit tests
cd go && go test ./internal/...

# Run a specific package with verbose Ginkgo output
cd go && go test ./internal/activity/... -v

# Run tests matching a label
cd go && go test ./internal/... -run TestActivity
```

I would do that as the next step rather than jumping directly into the full workflow.