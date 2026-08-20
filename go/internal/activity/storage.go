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

// StoreResultsActivity persists the final processing result to PostgreSQL.
//
// Failure simulation (controlled by req.SimulateStoreFailure):
//   - Attempts 1–2: returns a retryable ApplicationError to simulate a transient DB failure.
//   - Attempt 3+: writes the record and succeeds normally.
func (a *Activities) StoreResultsActivity(ctx context.Context, req model.JobRequest, result ProcessResult) error {
	log := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	log.Info("StoreResultsActivity started", "jobId", req.JobID, "itemsProcessed", result.ItemsProcessed, "attempt", info.Attempt)

	if req.SimulateStoreFailure && info.Attempt < 3 {
		msg := fmt.Sprintf("simulated transient DB failure for job %s (attempt %d of max 5)", req.JobID, info.Attempt)
		log.Warn("StoreResultsActivity injecting retryable failure", "jobId", req.JobID, "attempt", info.Attempt)
		return temporal.NewApplicationError(msg, "TransientStoreError", nil)
	}

	now := time.Now().UTC()
	rec := store.JobRecord{
		ID:             req.JobID,
		TenantID:       req.TenantID,
		Priority:       req.Priority,
		FairnessKey:    req.FairnessKey,
		Status:         model.JobStatusCompleted,
		ItemsProcessed: result.ItemsProcessed,
		ItemsFailed:    result.ItemsFailed,
		CompletedAt:    &now,
	}

	if err := a.Store.SaveJobResult(ctx, rec); err != nil {
		// Real DB errors are also retryable — wrap them explicitly so the error type
		// is visible in Temporal UI alongside the retry count.
		return temporal.NewApplicationError(err.Error(), "StoreError", err)
	}

	log.Info("StoreResultsActivity completed", "jobId", req.JobID)
	return nil
}
