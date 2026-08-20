package model

import "time"

type Priority string

const (
	PriorityHigh   Priority = "HIGH"
	PriorityMedium Priority = "MEDIUM"
	PriorityLow    Priority = "LOW"
)

type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusCancelled  JobStatus = "CANCELLED"
)

// Job is the core domain entity.
type Job struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenantId"`
	Priority    Priority  `json:"priority"`
	FairnessKey string    `json:"fairnessKey"`
	Status      JobStatus `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

// JobRequest is the workflow input — submitted via REST and passed directly to the workflow.
type JobRequest struct {
	JobID       string   `json:"jobId"`
	TenantID    string   `json:"tenantId"`
	Priority    Priority `json:"priority"`
	FairnessKey string   `json:"fairnessKey"`
	// ItemCount controls how many mock items the FetchItems activity will return.
	ItemCount int `json:"itemCount"`
	// SimulateFailure injects a retryable transient error into ProcessItemsActivity on
	// attempts 1–2, then succeeds on attempt 3. Use to observe retry behaviour in Temporal UI.
	SimulateFailure bool `json:"simulateFailure,omitempty"`
	// SimulateStoreFailure injects a retryable transient error into StoreResultsActivity on
	// attempts 1–2, then succeeds on attempt 3. Use to observe DB-failure retry behaviour.
	SimulateStoreFailure bool `json:"simulateStoreFailure,omitempty"`
}

// JobResult is the workflow output.
type JobResult struct {
	JobID          string        `json:"jobId"`
	Status         JobStatus     `json:"status"`
	ItemsProcessed int           `json:"itemsProcessed"`
	ItemsFailed    int           `json:"itemsFailed"`
	Duration       time.Duration `json:"duration"`
	Error          string        `json:"error,omitempty"`
}
