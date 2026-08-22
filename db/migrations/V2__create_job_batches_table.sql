-- V2__create_job_batches_table.sql
-- Creates the job_batches table, which records the per-batch results produced by
-- ProcessBatchWorkflow child workflow executions launched from BatchProcessingWorkflow.
--
-- A row is upserted by StoreBatchActivity when each child workflow completes,
-- keyed on (job_id, batch_index).

CREATE TABLE job_batches (
    job_id          TEXT        NOT NULL,
    batch_index     INT         NOT NULL,
    status          TEXT        NOT NULL,
    items_processed INT         NOT NULL DEFAULT 0,
    items_failed    INT         NOT NULL DEFAULT 0,
    error           TEXT        NOT NULL DEFAULT '',
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, batch_index)
);

CREATE INDEX job_batches_job_id_idx ON job_batches (job_id);
