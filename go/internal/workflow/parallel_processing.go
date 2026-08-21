package workflow

import (
	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/workflow"
)

// ParallelProcessingWorkflow demonstrates parallel activity execution with aggregation.
//
// Execution graph:
//
//	ValidateJob
//	     ↓
//	FetchItems
//	     ↓
//	┌── ProcessPartA ──┐
//	│                  ├── AggregateResults
//	└── ProcessPartB ──┘        ↓
//	                       StoreResults
//
// The items returned by FetchItems are split evenly between PartA and PartB.
// Both processing activities are dispatched concurrently before either result
// is awaited — this is idiomatic Temporal Go SDK fan-out.
//
// Dependency failure experiment:
//
//	Set req.SimulateDependencyFailure to "PART_A", "PART_B", or "BOTH" to observe
//	retryable failures in one or both parallel branches. Each branch retries
//	independently; the workflow only proceeds to Aggregate once all futures
//	resolve. If a branch exhausts its retries the whole workflow fails.
func ParallelProcessingWorkflow(ctx workflow.Context, req model.JobRequest) (model.JobResult, error) {
	log := workflow.GetLogger(ctx)
	log.Info("ParallelProcessingWorkflow started",
		"jobId", req.JobID,
		"tenantId", req.TenantID,
		"simulateDependencyFailure", req.SimulateDependencyFailure,
	)

	startTime := workflow.Now(ctx)

	// Step 1 — Validate (sequential, non-retryable on bad input)
	validateCtx := workflow.WithActivityOptions(ctx, validateActivityOptions)
	if err := workflow.ExecuteActivity(validateCtx, (*activity.Activities).ValidateJobActivity, req).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 2 — Fetch (sequential; both parallel branches depend on the result)
	fetchCtx := workflow.WithActivityOptions(ctx, fetchActivityOptions)
	var items []activity.Item
	if err := workflow.ExecuteActivity(fetchCtx, (*activity.Activities).FetchItemsActivity, req).Get(ctx, &items); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 3 — Fan-out: split items and dispatch ProcessPartA + ProcessPartB concurrently.
	//
	// Both ExecuteActivity calls are made before either .Get() is called, so
	// Temporal schedules them on the task queue simultaneously. The workflow
	// then blocks on each future in turn.
	mid := len(items) / 2
	if mid == 0 {
		mid = len(items) // guard: all items go to PartA if count < 2
	}
	partAItems := items[:mid]
	partBItems := items[mid:]

	processCtx := workflow.WithActivityOptions(ctx, processActivityOptions)
	futureA := workflow.ExecuteActivity(processCtx, (*activity.Activities).ProcessPartAActivity, req, partAItems)
	futureB := workflow.ExecuteActivity(processCtx, (*activity.Activities).ProcessPartBActivity, req, partBItems)

	// Fan-in: await both futures; collect errors before deciding how to proceed.
	// Blocking on A first does not delay B — Temporal runs both concurrently.
	var partAResult, partBResult activity.ProcessResult
	errA := futureA.Get(ctx, &partAResult)
	errB := futureB.Get(ctx, &partBResult)

	// Surface the first encountered branch failure. If both fail, PartA's error wins.
	if errA != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), errA)
	}
	if errB != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), errB)
	}

	// Step 4 — Aggregate: combine PartA + PartB totals into a single result.
	aggregateCtx := workflow.WithActivityOptions(ctx, processActivityOptions)
	var aggregated activity.ProcessResult
	if err := workflow.ExecuteActivity(aggregateCtx, (*activity.Activities).AggregateResultsActivity, req, partAResult, partBResult).Get(ctx, &aggregated); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 5 — Store (sequential, after full aggregation)
	storeCtx := workflow.WithActivityOptions(ctx, storeActivityOptions)
	if err := workflow.ExecuteActivity(storeCtx, (*activity.Activities).StoreResultsActivity, req, aggregated).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	duration := workflow.Now(ctx).Sub(startTime)
	log.Info("ParallelProcessingWorkflow completed",
		"jobId", req.JobID,
		"itemsProcessed", aggregated.ItemsProcessed,
		"duration", duration,
	)

	return model.JobResult{
		JobID:          req.JobID,
		Status:         model.JobStatusCompleted,
		ItemsProcessed: aggregated.ItemsProcessed,
		ItemsFailed:    aggregated.ItemsFailed,
		Duration:       duration,
	}, nil
}
