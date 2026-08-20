-- V1__create_jobs_table.sql
-- Creates the jobs table used to track the full lifecycle of each workflow execution.
--
-- A row is inserted with status=RUNNING when the job is submitted (API layer).
-- StoreResultsActivity updates the same row with the final status and results.

CREATE TABLE jobs (
    id              TEXT        PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    priority        TEXT        NOT NULL,
    fairness_key    TEXT        NOT NULL DEFAULT '',
    status          TEXT        NOT NULL,
    items_processed INT         NOT NULL DEFAULT 0,
    items_failed    INT         NOT NULL DEFAULT 0,
    duration_ms     BIGINT      NOT NULL DEFAULT 0,
    error           TEXT        NOT NULL DEFAULT '',
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX jobs_tenant_id_idx ON jobs (tenant_id);
CREATE INDEX jobs_status_idx    ON jobs (status);
CREATE INDEX jobs_priority_idx  ON jobs (priority);
