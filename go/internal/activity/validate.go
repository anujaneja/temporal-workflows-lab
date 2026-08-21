package activity

import (
	"context"
	"fmt"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ValidateJobActivity validates the incoming job request.
//
// All validation failures are wrapped as NonRetryableApplicationError so Temporal
// never retries them — bad input will not self-correct on a retry.
func (a *Activities) ValidateJobActivity(ctx context.Context, req model.JobRequest) error {
	log := activity.GetLogger(ctx)
	log.Info("ValidateJobActivity started", "jobId", req.JobID, "tenantId", req.TenantID)

	if err := validateRequest(req); err != nil {
		// NonRetryableApplicationError tells Temporal: do not schedule another attempt.
		return temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", err)
	}

	log.Info("ValidateJobActivity completed", "jobId", req.JobID)
	return nil
}

func validateRequest(req model.JobRequest) error {
	if req.JobID == "" {
		return fmt.Errorf("jobId is required")
	}
	if req.TenantID == "" {
		return fmt.Errorf("tenantId is required")
	}
	if req.Priority != model.PriorityHigh &&
		req.Priority != model.PriorityMedium &&
		req.Priority != model.PriorityLow {
		return fmt.Errorf("invalid priority %q: must be HIGH, MEDIUM, or LOW", req.Priority)
	}
	return nil
}
