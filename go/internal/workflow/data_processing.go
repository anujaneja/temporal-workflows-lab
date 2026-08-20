package workflow

import (
	"errors"
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the single task queue used by the worker and the API client.
// Later phases may introduce separate queues per priority level.
const TaskQueue = "workflow-lab"

// standardRetryPolicy is the shared exponential-backoff policy used by all
// activities that make external or DB calls.
//
//	Attempt 1 → wait 1s → Attempt 2 → wait 2s → Attempt 3 → wait 4s → …
var standardRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    30 * time.Second,
	MaximumAttempts:    5,
}

// validateActivityOptions — validation is fast and pure; errors are
// non-retryable by type, so MaximumAttempts is capped at 1.
var validateActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 5 * time.Second,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts: 1,
	},
}

// fetchActivityOptions — simulated external fetch; allow retries for transient failures.
var fetchActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
	RetryPolicy:         standardRetryPolicy,
}

// processActivityOptions — CPU-bound processing; allow retries for transient failures.
var processActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 60 * time.Second,
	RetryPolicy:         standardRetryPolicy,
}

// storeActivityOptions — DB write; allow retries for transient DB failures.
var storeActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
	RetryPolicy:         standardRetryPolicy,
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

	// Step 3 — Process
	processCtx := workflow.WithActivityOptions(ctx, processActivityOptions)
	var processResult activity.ProcessResult
	if err := workflow.ExecuteActivity(processCtx, (*activity.Activities).ProcessItemsActivity, req, items).Get(ctx, &processResult); err != nil {
		return failResult(req.JobID, startTime, workflow.Now(ctx), err)
	}

	// Step 4 — Store
	storeCtx := workflow.WithActivityOptions(ctx, storeActivityOptions)
	if err := workflow.ExecuteActivity(storeCtx, (*activity.Activities).StoreResultsActivity, req, processResult).Get(ctx, nil); err != nil {
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

// failResult constructs a failed JobResult. It unwraps the Temporal ActivityError
// chain to surface the leaf application error message rather than the full wrapper.
func failResult(jobID string, start, end time.Time, err error) (model.JobResult, error) {
	return model.JobResult{
		JobID:    jobID,
		Status:   model.JobStatusFailed,
		Duration: end.Sub(start),
		Error:    leafMessage(err),
	}, err
}

// leafMessage unwraps nested Temporal error wrappers (ActivityError → ApplicationError)
// and returns the innermost message, which is the actual failure reason.
func leafMessage(err error) string {
	for err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) {
			return appErr.Message()
		}
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			break
		}
		err = unwrapped
	}
	return err.Error()
}
