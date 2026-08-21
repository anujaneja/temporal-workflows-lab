package activity

import (
	"context"
	"fmt"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ProcessResult holds the outcome of processing a batch of items.
type ProcessResult struct {
	JobID          string `json:"jobId"`
	ItemsProcessed int    `json:"itemsProcessed"`
	ItemsFailed    int    `json:"itemsFailed"`
}

// ProcessItemsActivity processes the items returned by FetchItemsActivity.
//
// Failure simulation (controlled by req.SimulateFailure):
//   - Attempts 1–2: returns a retryable ApplicationError to exercise Temporal's
//     exponential-backoff retry. Visible in Temporal UI as repeated activity attempts.
//   - Attempt 3+: succeeds normally.
//
// This makes retry behaviour observable without needing an actual broken dependency.
func (a *Activities) ProcessItemsActivity(ctx context.Context, req model.JobRequest, items []Item) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	log.Info("ProcessItemsActivity started", "jobId", req.JobID, "items", len(items), "attempt", info.Attempt)

	if req.SimulateFailure && info.Attempt < 3 {
		msg := fmt.Sprintf("simulated transient processing failure for job %s (attempt %d of max 5)", req.JobID, info.Attempt)
		log.Warn("ProcessItemsActivity injecting retryable failure", "jobId", req.JobID, "attempt", info.Attempt)
		// ApplicationError is retryable by default — Temporal will schedule the next attempt
		// after the configured backoff interval.
		return ProcessResult{}, temporal.NewApplicationError(msg, "TransientProcessingError", nil)
	}

	result := ProcessResult{
		JobID:          req.JobID,
		ItemsProcessed: len(items),
		ItemsFailed:    0,
	}

	log.Info("ProcessItemsActivity completed", "jobId", req.JobID, "processed", result.ItemsProcessed, "attempt", info.Attempt)
	return result, nil
}

// ProcessPartAActivity processes the first partition of items in the parallel branch.
//
// Failure simulation (SimulateDependencyFailure == "PART_A" or "BOTH"):
//   - Attempts 1–2: returns a retryable error to exercise parallel-branch failure propagation.
//   - Attempt 3+: succeeds normally.
//
// Observable in Temporal UI alongside the concurrent ProcessPartBActivity execution.
func (a *Activities) ProcessPartAActivity(ctx context.Context, req model.JobRequest, items []Item) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	log.Info("ProcessPartAActivity started", "jobId", req.JobID, "items", len(items), "attempt", info.Attempt)

	shouldFail := req.SimulateDependencyFailure == "PART_A" || req.SimulateDependencyFailure == "BOTH"
	if shouldFail && info.Attempt < 3 {
		msg := fmt.Sprintf("simulated dependency failure in ProcessPartA for job %s (attempt %d)", req.JobID, info.Attempt)
		log.Warn("ProcessPartAActivity injecting dependency failure", "jobId", req.JobID, "attempt", info.Attempt)
		return ProcessResult{}, temporal.NewApplicationError(msg, "DependencyFailurePartA", nil)
	}

	result := ProcessResult{JobID: req.JobID, ItemsProcessed: len(items)}
	log.Info("ProcessPartAActivity completed", "jobId", req.JobID, "processed", result.ItemsProcessed)
	return result, nil
}

// ProcessPartBActivity processes the second partition of items in the parallel branch.
//
// Failure simulation (SimulateDependencyFailure == "PART_B" or "BOTH"):
//   - Attempts 1–2: returns a retryable error.
//   - Attempt 3+: succeeds normally.
func (a *Activities) ProcessPartBActivity(ctx context.Context, req model.JobRequest, items []Item) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	log.Info("ProcessPartBActivity started", "jobId", req.JobID, "items", len(items), "attempt", info.Attempt)

	shouldFail := req.SimulateDependencyFailure == "PART_B" || req.SimulateDependencyFailure == "BOTH"
	if shouldFail && info.Attempt < 3 {
		msg := fmt.Sprintf("simulated dependency failure in ProcessPartB for job %s (attempt %d)", req.JobID, info.Attempt)
		log.Warn("ProcessPartBActivity injecting dependency failure", "jobId", req.JobID, "attempt", info.Attempt)
		return ProcessResult{}, temporal.NewApplicationError(msg, "DependencyFailurePartB", nil)
	}

	result := ProcessResult{JobID: req.JobID, ItemsProcessed: len(items)}
	log.Info("ProcessPartBActivity completed", "jobId", req.JobID, "processed", result.ItemsProcessed)
	return result, nil
}
