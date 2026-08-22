package workflow

import (
	"fmt"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

// DefaultBatchCount is used when JobRequest.BatchCount is unset or non-positive.
const DefaultBatchCount = 3

// BatchProcessingWorkflow demonstrates child workflow orchestration.
//
// Execution graph:
//
//	ValidateJob
//	     ↓
//	FetchItems
//	     ↓
//	┌── ProcessBatchWorkflow(0) ──┐
//	├── ProcessBatchWorkflow(1) ──┼── (all children awaited) ── StoreResults
//	└── ProcessBatchWorkflow(N) ──┘
//
// The items returned by FetchItems are split into req.BatchCount contiguous
// batches (default DefaultBatchCount). Each batch is processed by its own
// ProcessBatchWorkflow child workflow execution; all children are started
// before any is awaited, so Temporal runs them concurrently — the same
// fan-out/fan-in idiom used by ParallelProcessingWorkflow, but with child
// workflow executions instead of local activities.
//
// Parent cancellation / child cancellation behaviour:
//
//	Each child is started with ParentClosePolicy REQUEST_CANCEL rather than the
//	SDK default of TERMINATE. If this workflow is cancelled while children are
//	still running, every running child receives a cancellation request and can
//	unwind gracefully instead of being abruptly killed — observable as
//	"WorkflowExecutionCancelRequested" events on each child in the Temporal UI.
//
// Child failure experiment:
//
//	Set req.SimulateChildFailure to "FIRST" (only batch 0 fails) or "ALL" (every
//	batch fails) to observe per-child retries. A child that exhausts its
//	retries fails its own workflow, which fails the parent as soon as its
//	future is awaited.
func BatchProcessingWorkflow(ctx workflow.Context, req model.JobRequest) (model.JobResult, error) {
	log := workflow.GetLogger(ctx)
	log.Info("BatchProcessingWorkflow started", "jobId", req.JobID, "tenantId", req.TenantID, "batchCount", req.BatchCount)

	startTime := workflow.Now(ctx)

	// Step 1 — Validate
	validateCtx := workflow.WithActivityOptions(ctx, validateActivityOptions)
	if err := workflow.ExecuteActivity(validateCtx, (*activity.Activities).ValidateJobActivity, req).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 2 — Fetch
	fetchCtx := workflow.WithActivityOptions(ctx, fetchActivityOptions)
	var items []activity.Item
	if err := workflow.ExecuteActivity(fetchCtx, (*activity.Activities).FetchItemsActivity, req).Get(ctx, &items); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 3 — Fan-out: split items into batches and start one child workflow per batch.
	batches := splitIntoBatches(items, batchCountFor(req, len(items)))

	futures := make([]workflow.ChildWorkflowFuture, len(batches))
	for i, batch := range batches {
		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:        fmt.Sprintf("%s-batch-%d", req.JobID, i),
			TaskQueue:         TaskQueue,
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		futures[i] = workflow.ExecuteChildWorkflow(cctx, ProcessBatchWorkflow, req, i, batch)
	}

	// Fan-in: await every child in order. The first failing child's error wins.
	var totalProcessed, totalFailed int
	for _, f := range futures {
		var result activity.ProcessResult
		if err := f.Get(ctx, &result); err != nil {
			return failResult(req.JobID, startTime, workflow.Now(ctx), err)
		}
		totalProcessed += result.ItemsProcessed
		totalFailed += result.ItemsFailed
	}

	// Step 4 — Store the aggregated result across all batches.
	aggregated := activity.ProcessResult{JobID: req.JobID, ItemsProcessed: totalProcessed, ItemsFailed: totalFailed}
	storeCtx := workflow.WithActivityOptions(ctx, storeActivityOptions)
	if err := workflow.ExecuteActivity(storeCtx, (*activity.Activities).StoreResultsActivity, req, aggregated).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	duration := workflow.Now(ctx).Sub(startTime)
	log.Info("BatchProcessingWorkflow completed",
		"jobId", req.JobID,
		"batches", len(batches),
		"itemsProcessed", totalProcessed,
		"duration", duration,
	)

	return model.JobResult{
		JobID:          req.JobID,
		Status:         model.JobStatusCompleted,
		ItemsProcessed: totalProcessed,
		ItemsFailed:    totalFailed,
		Duration:       duration,
	}, nil
}

// batchCountFor resolves the effective batch count: DefaultBatchCount when
// unset/non-positive, capped at itemCount so no batch is left empty.
func batchCountFor(req model.JobRequest, itemCount int) int {
	n := req.BatchCount
	if n <= 0 {
		n = DefaultBatchCount
	}
	if n > itemCount {
		n = itemCount
	}
	if n < 1 {
		n = 1
	}
	return n
}

// splitIntoBatches divides items into n contiguous, roughly-equal batches.
// Any remainder is distributed one extra item at a time to the earliest batches.
func splitIntoBatches(items []activity.Item, n int) [][]activity.Item {
	batches := make([][]activity.Item, n)
	base := len(items) / n
	remainder := len(items) % n

	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < remainder {
			size++
		}
		batches[i] = items[start : start+size]
		start += size
	}
	return batches
}
