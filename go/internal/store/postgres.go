package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// TODO: change to pgx instead of lib/pq it offers better performance and more features.
	_ "github.com/lib/pq"
	
)

type postgresStore struct {
	db *sql.DB
}

// NewPostgres opens a connection pool to PostgreSQL and verifies it with a ping.
//
// dsn example: postgres://lab:lab@app-postgres:5432/lab?sslmode=disable
func NewPostgres(dsn string) (Store, error) {
	//TODO: we should use a connection pool here to ensure that we don't open too many connections to the database.
	// default value for max open connections and other connection pool settings should be used from a config file. 
	// so, that we can easily change the settings without having to change the code.
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &postgresStore{db: db}, nil
}

// CreateJob inserts a new job row at submission time.
// Only the identifying and classification fields are set; result fields default to zero.
// completed_at is left NULL intentionally — it is set by SaveJobResult on completion.
func (s *postgresStore) CreateJob(ctx context.Context, rec JobRecord) error {
	const q = `
		INSERT INTO jobs (id, tenant_id, priority, fairness_key, status)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := s.db.ExecContext(ctx, q,
		rec.ID,
		rec.TenantID,
		string(rec.Priority),
		rec.FairnessKey,
		string(rec.Status),
	)
	if err != nil {
		return fmt.Errorf("CreateJob %s: %w", rec.ID, err)
	}
	return nil
}

// SaveJobResult upserts the final result into an existing job row.
// If the row does not exist (e.g. DB was wiped while workflow ran) it is created.
func (s *postgresStore) SaveJobResult(ctx context.Context, rec JobRecord) error {
	const q = `
		INSERT INTO jobs (
			id, tenant_id, priority, fairness_key,
			status, items_processed, items_failed,
			duration_ms, error, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			status          = EXCLUDED.status,
			items_processed = EXCLUDED.items_processed,
			items_failed    = EXCLUDED.items_failed,
			duration_ms     = EXCLUDED.duration_ms,
			error           = EXCLUDED.error,
			completed_at    = EXCLUDED.completed_at
	`
	_, err := s.db.ExecContext(ctx, q,
		rec.ID,
		rec.TenantID,
		string(rec.Priority),
		rec.FairnessKey,
		string(rec.Status),
		rec.ItemsProcessed,
		rec.ItemsFailed,
		rec.DurationMs,
		rec.Error,
		rec.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("SaveJobResult %s: %w", rec.ID, err)
	}
	return nil
}

func (s *postgresStore) Close() error {
	return s.db.Close()
}
