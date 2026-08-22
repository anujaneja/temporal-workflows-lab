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
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
	JobStatusCancelled JobStatus = "CANCELLED"
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
	// UseParallelWorkflow routes the job to ParallelProcessingWorkflow instead of the default
	// sequential DataProcessingWorkflow. The parallel workflow splits items into two halves,
	// runs ProcessPartA and ProcessPartB concurrently, then aggregates before storing.
	UseParallelWorkflow bool `json:"useParallelWorkflow,omitempty"`
	// SimulateDependencyFailure injects a retryable failure into the named parallel branch on
	// attempts 1–2, then succeeds on attempt 3. Only applies when UseParallelWorkflow is true.
	// Accepted values: "PART_A" | "PART_B" | "BOTH" | "" (no failure).
	SimulateDependencyFailure string `json:"simulateDependencyFailure,omitempty"`
	// UseBatchWorkflow routes the job to BatchProcessingWorkflow, which splits the fetched
	// items into BatchCount batches and processes each one in its own ProcessBatchWorkflow
	// child workflow execution, run concurrently, before aggregating and storing.
	UseBatchWorkflow bool `json:"useBatchWorkflow,omitempty"`
	// BatchCount controls how many ProcessBatchWorkflow child workflows are started. Only
	// applies when UseBatchWorkflow is true. Non-positive values fall back to a default of 3;
	// values greater than ItemCount are capped to ItemCount so no batch is empty.
	BatchCount int `json:"batchCount,omitempty"`
	// SimulateChildFailure injects a retryable failure into ProcessBatchActivity on attempts
	// 1–2, then succeeds on attempt 3. Only applies when UseBatchWorkflow is true.
	// Accepted values: "FIRST" (only batch 0 fails) | "ALL" (every batch fails) | "" (no failure).
	SimulateChildFailure string `json:"simulateChildFailure,omitempty"`
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
