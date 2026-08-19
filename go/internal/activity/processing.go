package activity

import (
	"context"
	"fmt"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
)

// ProcessResult holds the outcome of processing a batch of items.
type ProcessResult struct {
	JobID          string `json:"jobId"`
	ItemsProcessed int    `json:"itemsProcessed"`
	ItemsFailed    int    `json:"itemsFailed"`
}

// ProcessItemsActivity processes the items returned by FetchItemsActivity.
// In later phases it will simulate failures and rate limiting.
func ProcessItemsActivity(ctx context.Context, req model.JobRequest, items []Item) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	log.Info("ProcessItemsActivity started", "jobId", req.JobID, "items", len(items))

	if req.SimulateFailure {
		return ProcessResult{}, fmt.Errorf("simulated processing failure for job %s", req.JobID)
	}

	result := ProcessResult{
		JobID:          req.JobID,
		ItemsProcessed: len(items),
		ItemsFailed:    0,
	}

	log.Info("ProcessItemsActivity completed", "jobId", req.JobID, "processed", result.ItemsProcessed)
	return result, nil
}
