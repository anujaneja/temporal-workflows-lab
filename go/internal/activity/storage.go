package activity

import (
	"context"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
)

// StoreResultsActivity persists the processing results.
// In later phases this will write to PostgreSQL and simulate persistence failures.
func StoreResultsActivity(ctx context.Context, req model.JobRequest, result ProcessResult) error {
	log := activity.GetLogger(ctx)
	log.Info("StoreResultsActivity started", "jobId", req.JobID, "itemsProcessed", result.ItemsProcessed)

	// Phase 2 will persist to PostgreSQL.

	log.Info("StoreResultsActivity completed", "jobId", req.JobID)
	return nil
}
