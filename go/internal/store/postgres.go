package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBConfig holds tunable connection-pool parameters for PostgreSQL.
// Every field has a production-safe default via DefaultDBConfig.
type DBConfig struct {
	// MaxConns is the hard upper limit on open connections in the pool.
	// Tune this to stay within PostgreSQL's max_connections budget.
	MaxConns int32
	// MinConns is the number of idle connections the pool always keeps open.
	MinConns int32
	// MaxConnLifetime is the maximum age of a connection before it is recycled.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime closes idle connections that exceed this age.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod controls how often idle connections are health-checked.
	HealthCheckPeriod time.Duration
}

// DefaultDBConfig returns safe production defaults.
// MaxConns=10 keeps the footprint small; raise it if you observe pool exhaustion.
func DefaultDBConfig() DBConfig {
	return DBConfig{
		MaxConns:          10,
		MinConns:          2,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: time.Minute,
	}
}

// dbExecutor is satisfied by both *pgxpool.Pool and pgx.Tx, letting the shared
// query helpers work with or without an explicit transaction.
type dbExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// postgresStore is the production Store implementation backed by a pgxpool.
type postgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgres opens a pgxpool connection pool and verifies it with a ping.
//
// DSN example: postgres://lab:lab@app-postgres:5432/lab?sslmode=disable
func NewPostgres(dsn string, cfg DBConfig) (Store, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &postgresStore{pool: pool}, nil
}

func (s *postgresStore) CreateJob(ctx context.Context, rec JobRecord) error {
	return execCreateJob(ctx, s.pool, rec)
}

func (s *postgresStore) SaveJobResult(ctx context.Context, rec JobRecord) error {
	return execSaveJobResult(ctx, s.pool, rec)
}

func (s *postgresStore) SaveBatchResult(ctx context.Context, rec BatchRecord) error {
	return execSaveBatchResult(ctx, s.pool, rec)
}

// RunInTx runs fn inside a single database transaction.
// If fn returns an error the transaction is rolled back; otherwise it is committed.
// The Store passed to fn operates on the same open transaction.
func (s *postgresStore) RunInTx(ctx context.Context, fn func(tx Store) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Rollback is a no-op after a successful Commit (returns ErrTxClosed).
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(&txStore{tx: tx}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *postgresStore) Close() error {
	s.pool.Close()
	return nil
}

// ─── txStore ────────────────────────────────────────────────────────────────

// txStore wraps a pgx.Tx and implements Store for use within RunInTx.
// Nested RunInTx calls reuse the same transaction (no savepoints).
type txStore struct {
	tx pgx.Tx
}

func (t *txStore) CreateJob(ctx context.Context, rec JobRecord) error {
	return execCreateJob(ctx, t.tx, rec)
}

func (t *txStore) SaveJobResult(ctx context.Context, rec JobRecord) error {
	return execSaveJobResult(ctx, t.tx, rec)
}

func (t *txStore) SaveBatchResult(ctx context.Context, rec BatchRecord) error {
	return execSaveBatchResult(ctx, t.tx, rec)
}

func (t *txStore) RunInTx(ctx context.Context, fn func(tx Store) error) error {
	return fn(t)
}

func (t *txStore) Close() error { return nil }

// ─── shared query helpers ────────────────────────────────────────────────────

// execCreateJob inserts a new job row at submission time.
// Only the identifying and classification fields are set; result fields default to zero.
// completed_at is left NULL intentionally — it is set by SaveJobResult on completion.
func execCreateJob(ctx context.Context, e dbExecutor, rec JobRecord) error {
	const q = `
		INSERT INTO jobs (id, tenant_id, priority, fairness_key, status)
		VALUES ($1, $2, $3, $4, $5)
	`
	if _, err := e.Exec(ctx, q,
		rec.ID,
		rec.TenantID,
		string(rec.Priority),
		rec.FairnessKey,
		string(rec.Status),
	); err != nil {
		return fmt.Errorf("CreateJob %s: %w", rec.ID, err)
	}
	return nil
}

// execSaveJobResult upserts the final result into an existing job row.
// If the row does not exist (e.g. DB was wiped while workflow ran) it is created.
func execSaveJobResult(ctx context.Context, e dbExecutor, rec JobRecord) error {
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
	if _, err := e.Exec(ctx, q,
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
	); err != nil {
		return fmt.Errorf("SaveJobResult %s: %w", rec.ID, err)
	}
	return nil
}

// execSaveBatchResult upserts the result of one batch, keyed on (job_id, batch_index).
func execSaveBatchResult(ctx context.Context, e dbExecutor, rec BatchRecord) error {
	const q = `
		INSERT INTO job_batches (
			job_id, batch_index, status, items_processed, items_failed, error, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (job_id, batch_index) DO UPDATE SET
			status          = EXCLUDED.status,
			items_processed = EXCLUDED.items_processed,
			items_failed    = EXCLUDED.items_failed,
			error           = EXCLUDED.error,
			completed_at    = EXCLUDED.completed_at
	`
	if _, err := e.Exec(ctx, q,
		rec.JobID,
		rec.BatchIndex,
		string(rec.Status),
		rec.ItemsProcessed,
		rec.ItemsFailed,
		rec.Error,
		rec.CompletedAt,
	); err != nil {
		return fmt.Errorf("SaveBatchResult %s/%d: %w", rec.JobID, rec.BatchIndex, err)
	}
	return nil
}
