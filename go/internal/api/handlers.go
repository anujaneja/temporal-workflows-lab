package api

import (
	"log"
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
// 1. Starts a DataProcessingWorkflow in Temporal (source of truth for execution).
// 2. Inserts a RUNNING row into the jobs table (source of truth for history/audit).
//
// The DB write is best-effort: if it fails the workflow is still running and
// StoreResultsActivity will upsert the row on completion. The failure is logged.
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

	// Route to ParallelProcessingWorkflow when the caller requests it; otherwise
	// use the default sequential DataProcessingWorkflow.
	wfFunc := interface{}(workflow.DataProcessingWorkflow)
	if req.UseParallelWorkflow {
		wfFunc = workflow.ParallelProcessingWorkflow
	}

	run, err := h.tc.ExecuteWorkflow(c.Request.Context(), options, wfFunc, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	//TODO: we should create a transaction here to ensure that the job is created 
	// and the workflow is started atomically. First we should we create the job record in DB
	// and then start the workflow. If the workflow fails, we should roll back the job record.
	// If the workflow succeeds, we should update the job record with the results.
	// If the workflow is cancelled, we should update the job record with the cancellation reason.
	// If the workflow is timed out, we should update the job record with the timeout reason.
	// If the workflow is terminated, we should update the job record with the termination reason.
	// If the workflow is failed, we should update the job record with the failure reason.
	// If the workflow is completed, we should update the job record with the completion results.
	// we should use a transaction here to ensure that the job is created and the workflow is started atomically.
	
	// Record the job in our DB immediately so it is visible before the workflow
	// completes. The row will be updated by StoreResultsActivity on completion.
	if err := h.store.CreateJob(c.Request.Context(), store.JobRecord{
		ID:          req.JobID,
		TenantID:    req.TenantID,
		Priority:    req.Priority,
		FairnessKey: req.FairnessKey,
		Status:      model.JobStatusRunning,
	}); err != nil {
		// Non-fatal: Temporal already accepted the workflow. Log and continue —
		// StoreResultsActivity will upsert the record when the workflow finishes.
		log.Printf("WARN: failed to create job record for %s: %v", req.JobID, err)
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
