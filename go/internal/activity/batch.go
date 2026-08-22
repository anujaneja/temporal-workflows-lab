package activity

import (
	"context"
	"fmt"
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"github.com/anuj/temporal-workflows-lab/internal/store"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ProcessBatchActivity processes one batch of items on behalf of a
// ProcessBatchWorkflow child workflow execution.
//
// Failure simulation (controlled by req.SimulateChildFailure):
//   - "ALL":   every batch fails on attempts 1–2, then succeeds on attempt 3.
//   - "FIRST": only batchIndex 0 fails on attempts 1–2, then succeeds on attempt 3.
//   - "" (default): no failure injected.
func (a *Activities) ProcessBatchActivity(ctx context.Context, req model.JobRequest, batchIndex int, items []Item) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	log.Info("ProcessBatchActivity started", "jobId", req.JobID, "batchIndex", batchIndex, "items", len(items), "attempt", info.Attempt)

	shouldFail := req.SimulateChildFailure == "ALL" || (req.SimulateChildFailure == "FIRST" && batchIndex == 0)
	if shouldFail && info.Attempt < 3 {
		msg := fmt.Sprintf("simulated child batch failure for job %s batch %d (attempt %d of max 5)", req.JobID, batchIndex, info.Attempt)
		log.Warn("ProcessBatchActivity injecting retryable failure", "jobId", req.JobID, "batchIndex", batchIndex, "attempt", info.Attempt)
		return ProcessResult{}, temporal.NewApplicationError(msg, "BatchProcessingError", nil)
	}

	result := ProcessResult{
		JobID:          req.JobID,
		ItemsProcessed: len(items),
		ItemsFailed:    0,
	}

	log.Info("ProcessBatchActivity completed", "jobId", req.JobID, "batchIndex", batchIndex, "processed", result.ItemsProcessed)
	return result, nil
}

// StoreBatchActivity persists the result of one batch to the job_batches table.
//
// Unlike StoreResultsActivity (the final, aggregated job result), this records
// per-batch progress, making each child workflow's completion independently
// observable in the database rather than only in the Temporal UI.
func (a *Activities) StoreBatchActivity(ctx context.Context, req model.JobRequest, batchIndex int, result ProcessResult) error {
	log := activity.GetLogger(ctx)
	log.Info("StoreBatchActivity started", "jobId", req.JobID, "batchIndex", batchIndex, "itemsProcessed", result.ItemsProcessed)

	now := time.Now().UTC()
	rec := store.BatchRecord{
		JobID:          req.JobID,
		BatchIndex:     batchIndex,
		Status:         model.JobStatusCompleted,
		ItemsProcessed: result.ItemsProcessed,
		ItemsFailed:    result.ItemsFailed,
		CompletedAt:    &now,
	}

	if err := a.Store.SaveBatchResult(ctx, rec); err != nil {
		return temporal.NewApplicationError(err.Error(), "StoreBatchError", err)
	}

	log.Info("StoreBatchActivity completed", "jobId", req.JobID, "batchIndex", batchIndex)
	return nil
}
