package api

import (
	"fmt"
	"net/http"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"github.com/anuj/temporal-workflows-lab/internal/store"
	"github.com/anuj/temporal-workflows-lab/internal/workflow"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Handler holds shared dependencies for all HTTP handlers.
type Handler struct {
	tc    client.Client
	store store.Store
}

func NewHandler(tc client.Client, s store.Store) *Handler {
	return &Handler{tc: tc, store: s}
}

// SubmitJob handles POST /jobs.
//
// Atomicity strategy (best-effort, cross-system):
//  1. Open a DB transaction and insert a RUNNING job record.
//  2. Start the Temporal workflow inside the same transaction scope.
//  3. If workflow creation fails → the transaction is rolled back, leaving the
//     DB clean and returning an error to the caller.
//  4. If both succeed → the transaction is committed.
//
// Note: if the commit itself fails after the workflow has already been accepted
// by Temporal, the job record will be absent until StoreResultsActivity upserts
// it on completion. This is an inherent limitation of dual-write systems and is
// documented here for observability.
func (h *Handler) SubmitJob(c *gin.Context) {
	var req model.JobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.JobID == "" {
		req.JobID = uuid.NewString()
	}
	if req.Priority == "" {
		req.Priority = model.PriorityMedium
	}

	options := client.StartWorkflowOptions{
		ID:        req.JobID,
		TaskQueue: workflow.TaskQueue,
	}

	var run client.WorkflowRun
	if err := h.store.RunInTx(c.Request.Context(), func(tx store.Store) error {
		// Step 1: persist the job record so it is visible before the workflow runs.
		if err := tx.CreateJob(c.Request.Context(), store.JobRecord{
			ID:          req.JobID,
			TenantID:    req.TenantID,
			Priority:    req.Priority,
			FairnessKey: req.FairnessKey,
			Status:      model.JobStatusRunning,
		}); err != nil {
			return fmt.Errorf("create job record: %w", err)
		}

		// Route to ParallelProcessingWorkflow or BatchProcessingWorkflow when the caller
		// requests it; otherwise use the default sequential DataProcessingWorkflow.
		wfFunc := interface{}(workflow.DataProcessingWorkflow)
		switch {
		case req.UseParallelWorkflow:
			wfFunc = workflow.ParallelProcessingWorkflow
		case req.UseBatchWorkflow:
			wfFunc = workflow.BatchProcessingWorkflow
		}
		var err error
		run, err = h.tc.ExecuteWorkflow(c.Request.Context(), options, wfFunc, req)
		return err
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"jobId":      req.JobID,
		"workflowId": run.GetID(),
		"runId":      run.GetRunID(),
		"status":     model.JobStatusRunning,
	})
}

// GetJob handles GET /jobs/:id.
//
// Queries Temporal for the current execution status. Temporal remains the
// source of truth for live workflow state; the DB is the record of completed jobs.
func (h *Handler) GetJob(c *gin.Context) {
	jobID := c.Param("id")

	desc, err := h.tc.DescribeWorkflowExecution(c.Request.Context(), jobID, "")
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found", "jobId": jobID})
		return
	}

	execInfo := desc.GetWorkflowExecutionInfo()
	runID := execInfo.GetExecution().GetRunId()

	switch execInfo.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		c.JSON(http.StatusOK, gin.H{
			"jobId":  jobID,
			"runId":  runID,
			"status": model.JobStatusRunning,
		})

	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		var result model.JobResult
		wfRun := h.tc.GetWorkflow(c.Request.Context(), jobID, runID)
		if err := wfRun.Get(c.Request.Context(), &result); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, result)

	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		wfRun := h.tc.GetWorkflow(c.Request.Context(), jobID, runID)
		runErr := wfRun.Get(c.Request.Context(), nil)
		errMsg := ""
		if runErr != nil {
			errMsg = runErr.Error()
		}
		c.JSON(http.StatusOK, gin.H{
			"jobId":  jobID,
			"status": model.JobStatusFailed,
			"error":  errMsg,
		})

	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		c.JSON(http.StatusOK, gin.H{
			"jobId":  jobID,
			"status": model.JobStatusCancelled,
		})

	default:
		c.JSON(http.StatusOK, gin.H{
			"jobId":  jobID,
			"status": model.JobStatusFailed,
		})
	}
}

// CancelJob handles POST /jobs/:id/cancel.
//
// Sends a cancellation request to the running workflow. The workflow will
// receive a cancellation signal and can clean up before stopping.
func (h *Handler) CancelJob(c *gin.Context) {
	jobID := c.Param("id")

	if err := h.tc.CancelWorkflow(c.Request.Context(), jobID, ""); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jobId":  jobID,
		"status": model.JobStatusCancelled,
	})
}
