package workflow

import (
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the single task queue used by the worker and the API client.
// Later phases may introduce separate queues per priority level.
const TaskQueue = "workflow-lab"

// defaultActivityOptions are applied to every activity in this workflow.
// Retry policy will be expanded in Phase 3.
var defaultActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy: &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    30 * time.Second,
		MaximumAttempts:    5,
	},
}

// DataProcessingWorkflow is the primary workflow.
// It orchestrates the four core activities in sequence.
//
// Execution order:
//
//	ValidateJob → FetchItems → ProcessItems → StoreResults
func DataProcessingWorkflow(ctx workflow.Context, req model.JobRequest) (model.JobResult, error) {
	log := workflow.GetLogger(ctx)
	log.Info("DataProcessingWorkflow started", "jobId", req.JobID, "tenantId", req.TenantID, "priority", req.Priority)

	startTime := workflow.Now(ctx)
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// Step 1 — Validate
	if err := workflow.ExecuteActivity(ctx, (*activity.Activities).ValidateJobActivity, req).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 2 — Fetch
	var items []activity.Item
	if err := workflow.ExecuteActivity(ctx, (*activity.Activities).FetchItemsActivity, req).Get(ctx, &items); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 3 — Process
	var processResult activity.ProcessResult
	if err := workflow.ExecuteActivity(ctx, (*activity.Activities).ProcessItemsActivity, req, items).Get(ctx, &processResult); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 4 — Store
	if err := workflow.ExecuteActivity(ctx, (*activity.Activities).StoreResultsActivity, req, processResult).Get(ctx, nil); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	duration := workflow.Now(ctx).Sub(startTime)
	log.Info("DataProcessingWorkflow completed",
		"jobId", req.JobID,
		"itemsProcessed", processResult.ItemsProcessed,
		"duration", duration,
	)

	return model.JobResult{
		JobID:          req.JobID,
		Status:         model.JobStatusCompleted,
		ItemsProcessed: processResult.ItemsProcessed,
		ItemsFailed:    processResult.ItemsFailed,
		Duration:       duration,
	}, nil
}

func failResult(jobID string, start, end time.Time, err error) (model.JobResult, error) {
	return model.JobResult{
		JobID:    jobID,
		Status:   model.JobStatusFailed,
		Duration: end.Sub(start),
		Error:    err.Error(),
	}, err
}
