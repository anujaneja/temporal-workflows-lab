package main

import (
	"log"
	"os"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/temporalclient"
	"github.com/anuj/temporal-workflows-lab/internal/workflow"
	"go.temporal.io/sdk/worker"
)

const taskQueue = "workflow-lab"

func main() {
	temporalHost := envOrDefault("TEMPORAL_HOST", "localhost:7233")

	tc, err := temporalclient.Dial(temporalHost)
	if err != nil {
		log.Fatalf("failed to connect to Temporal: %v", err)
	}
	defer tc.Close()

	w := worker.New(tc, taskQueue, worker.Options{})

	w.RegisterWorkflow(workflow.DataProcessingWorkflow)

	w.RegisterActivity(activity.ValidateJobActivity)
	w.RegisterActivity(activity.FetchItemsActivity)
	w.RegisterActivity(activity.ProcessItemsActivity)
	w.RegisterActivity(activity.StoreResultsActivity)

	log.Printf("worker starting on task queue %q", taskQueue)
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
