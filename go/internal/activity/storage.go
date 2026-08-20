package activity

import (
	"context"
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"github.com/anuj/temporal-workflows-lab/internal/store"
	"go.temporal.io/sdk/activity"
)

// StoreResultsActivity persists the final processing result to PostgreSQL.
// In Phase 3 it will simulate persistence failures to exercise retry behaviour.
func (a *Activities) StoreResultsActivity(ctx context.Context, req model.JobRequest, result ProcessResult) error {
	log := activity.GetLogger(ctx)
	log.Info("StoreResultsActivity started", "jobId", req.JobID, "itemsProcessed", result.ItemsProcessed)

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
		return err
	}

	log.Info("StoreResultsActivity completed", "jobId", req.JobID)
	return nil
}
