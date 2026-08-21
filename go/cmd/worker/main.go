package main

import (
	"log"
	"os"
	"strconv"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/store"
	"github.com/anuj/temporal-workflows-lab/internal/temporalclient"
	"github.com/anuj/temporal-workflows-lab/internal/workflow"
	"go.temporal.io/sdk/worker"
)

func main() {
	temporalHost := envOrDefault("TEMPORAL_HOST", "localhost:7233")
	appDBDSN := envOrDefault("APP_DB_DSN", "postgres://lab:lab@localhost:5433/lab?sslmode=disable")

	dbCfg := store.DefaultDBConfig()
	if v := envOrDefault("DB_MAX_CONNS", ""); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Fatalf("invalid DB_MAX_CONNS %q: %v", v, err)
		}
		dbCfg.MaxConns = int32(n)
	}

	tc, err := temporalclient.Dial(temporalHost)
	if err != nil {
		log.Fatalf("failed to connect to Temporal: %v", err)
	}
	defer tc.Close()

	appStore, err := store.NewPostgres(appDBDSN, dbCfg)
	if err != nil {
		log.Fatalf("failed to connect to app postgres: %v", err)
	}
	defer appStore.Close()

	acts := &activity.Activities{Store: appStore}

	w := worker.New(tc, workflow.TaskQueue, worker.Options{})

	w.RegisterWorkflow(workflow.DataProcessingWorkflow)
	w.RegisterWorkflow(workflow.ParallelProcessingWorkflow)

	// Register all activity methods on the Activities struct at once.
	// Temporal derives activity names from the method names (e.g. "ValidateJobActivity").
	w.RegisterActivity(acts)

	log.Printf("worker starting on task queue %q", workflow.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker exited with error: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
