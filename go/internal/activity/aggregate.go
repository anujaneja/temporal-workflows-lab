package activity

import (
	"context"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
)

// AggregateResultsActivity combines the results from the two parallel processing
// branches (PartA and PartB) into a single ProcessResult.
//
// This activity runs sequentially after both parallel branches have completed,
// demonstrating the fan-in / aggregation step of the dependency graph:
//
//	┌── ProcessPartA ──┐
//	│                  ├── AggregateResults ── StoreResults
//	└── ProcessPartB ──┘
func (a *Activities) AggregateResultsActivity(ctx context.Context, req model.JobRequest, partA, partB ProcessResult) (ProcessResult, error) {
	log := activity.GetLogger(ctx)
	log.Info("AggregateResultsActivity started",
		"jobId", req.JobID,
		"partAProcessed", partA.ItemsProcessed,
		"partBProcessed", partB.ItemsProcessed,
	)

	result := ProcessResult{
		JobID:          req.JobID,
		ItemsProcessed: partA.ItemsProcessed + partB.ItemsProcessed,
		ItemsFailed:    partA.ItemsFailed + partB.ItemsFailed,
	}

	log.Info("AggregateResultsActivity completed",
		"jobId", req.JobID,
		"totalProcessed", result.ItemsProcessed,
		"totalFailed", result.ItemsFailed,
	)
	return result, nil
}
