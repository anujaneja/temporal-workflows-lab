package workflow

import (
	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/workflow"
)

// ProcessBatchWorkflow is a child workflow that processes and stores a single
// batch of items. BatchProcessingWorkflow launches one instance of this per
// batch, running multiple instances concurrently.
//
//	ProcessBatchActivity → StoreBatchActivity
func ProcessBatchWorkflow(ctx workflow.Context, req model.JobRequest, batchIndex int, items []activity.Item) (activity.ProcessResult, error) {
	log := workflow.GetLogger(ctx)
	log.Info("ProcessBatchWorkflow started", "jobId", req.JobID, "batchIndex", batchIndex, "items", len(items))

	processCtx := workflow.WithActivityOptions(ctx, processActivityOptions)
	var result activity.ProcessResult
	if err := workflow.ExecuteActivity(processCtx, (*activity.Activities).ProcessBatchActivity, req, batchIndex, items).Get(ctx, &result); err != nil {
		return activity.ProcessResult{}, err
	}

	storeCtx := workflow.WithActivityOptions(ctx, storeActivityOptions)
	if err := workflow.ExecuteActivity(storeCtx, (*activity.Activities).StoreBatchActivity, req, batchIndex, result).Get(ctx, nil); err != nil {
		return activity.ProcessResult{}, err
	}

	log.Info("ProcessBatchWorkflow completed", "jobId", req.JobID, "batchIndex", batchIndex, "processed", result.ItemsProcessed)
	return result, nil
}
