package store

import (
	"context"
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/model"
)

// JobRecord is the persistence representation of a job across its full lifecycle.
//
// A row is inserted with status=RUNNING when the job is submitted (API layer).
// StoreResultsActivity updates the same row with the final status and results.
type JobRecord struct {
	ID             string
	TenantID       string
	Priority       model.Priority
	FairnessKey    string
	Status         model.JobStatus
	ItemsProcessed int
	ItemsFailed    int
	DurationMs     int64
	Error          string
	// CompletedAt is nil while the job is running and set when it finishes.
	CompletedAt *time.Time
}

// Store abstracts persistence for job lifecycle tracking.
// Implementations can be swapped out or replaced with test doubles.
type Store interface {
	// CreateJob inserts a new job row with status RUNNING at submission time.
	CreateJob(ctx context.Context, rec JobRecord) error

	// SaveJobResult upserts the final result of a completed or failed job.
	// Called by StoreResultsActivity when the workflow finishes.
	SaveJobResult(ctx context.Context, rec JobRecord) error

	// RunInTx executes fn inside a single database transaction.
	// If fn returns an error the transaction is rolled back; otherwise it is committed.
	// The Store passed to fn operates on that open transaction.
	RunInTx(ctx context.Context, fn func(tx Store) error) error

	Close() error
}
