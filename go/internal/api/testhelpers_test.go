package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"

	"github.com/gin-gonic/gin"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	internalapi "github.com/anuj/temporal-workflows-lab/internal/api"
	"github.com/anuj/temporal-workflows-lab/internal/store"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// buildRouter wires a Handler into a test gin router.
func buildRouter(tc client.Client, s store.Store) *gin.Engine {
	h := internalapi.NewHandler(tc, s)
	r := gin.New()
	r.POST("/jobs", h.SubmitJob)
	r.GET("/jobs/:id", h.GetJob)
	r.POST("/jobs/:id/cancel", h.CancelJob)
	return r
}

// doRequest fires an HTTP request against the router and returns the recorder.
func doRequest(r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// decodeJSON decodes the response body into a string-keyed map.
func decodeJSON(body string) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(body), &m)
	return m
}

// ─── fakeStore ───────────────────────────────────────────────────────────────

type fakeStore struct {
	createJobErr     error
	saveJobErr       error
	txErr            error // if set, RunInTx returns this without calling fn
	lastCreateJobRec *store.JobRecord
}

func (f *fakeStore) CreateJob(_ context.Context, rec store.JobRecord) error {
	f.lastCreateJobRec = &rec
	return f.createJobErr
}

func (f *fakeStore) SaveJobResult(_ context.Context, _ store.JobRecord) error { return f.saveJobErr }

func (f *fakeStore) SaveBatchResult(_ context.Context, _ store.BatchRecord) error { return nil }

func (f *fakeStore) RunInTx(_ context.Context, fn func(store.Store) error) error {
	if f.txErr != nil {
		return f.txErr
	}
	return fn(f)
}

func (f *fakeStore) Close() error { return nil }

// ─── fakeWorkflowRun ─────────────────────────────────────────────────────────

type fakeWorkflowRun struct {
	id    string
	runID string
	err   error
}

func (f *fakeWorkflowRun) GetID() string                              { return f.id }
func (f *fakeWorkflowRun) GetRunID() string                           { return f.runID }
func (f *fakeWorkflowRun) Get(_ context.Context, _ interface{}) error { return f.err }
func (f *fakeWorkflowRun) GetWithOptions(_ context.Context, _ interface{}, _ client.WorkflowRunGetOptions) error {
	return f.err
}

// ─── fakeTemporalClient ──────────────────────────────────────────────────────

// fakeTemporalClient embeds client.Client to satisfy the full interface.
// Only the methods called by the handler are overridden — any other call panics.
type fakeTemporalClient struct {
	client.Client

	executeWorkflowRun client.WorkflowRun
	executeWorkflowErr error

	describeResponse *workflowservice.DescribeWorkflowExecutionResponse
	describeErr      error

	getWorkflowRun client.WorkflowRun
	cancelErr      error
}

func (f *fakeTemporalClient) ExecuteWorkflow(
	_ context.Context,
	_ client.StartWorkflowOptions,
	_ interface{},
	_ ...interface{},
) (client.WorkflowRun, error) {
	return f.executeWorkflowRun, f.executeWorkflowErr
}

func (f *fakeTemporalClient) DescribeWorkflowExecution(
	_ context.Context, _, _ string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.describeResponse, f.describeErr
}

func (f *fakeTemporalClient) GetWorkflow(_ context.Context, _, _ string) client.WorkflowRun {
	return f.getWorkflowRun
}

func (f *fakeTemporalClient) CancelWorkflow(_ context.Context, _, _ string) error {
	return f.cancelErr
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildDescribeResponse constructs the protobuf response the handler reads from.
func buildDescribeResponse(wfID, runID string, status enumspb.WorkflowExecutionStatus) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wfID,
				RunId:      runID,
			},
			Status: status,
		},
	}
}
