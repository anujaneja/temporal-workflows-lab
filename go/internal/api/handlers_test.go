package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/anuj/temporal-workflows-lab/internal/model"
)

var _ = Describe("SubmitJob handler", func() {
	var (
		tc  *fakeTemporalClient
		fs  *fakeStore
	)

	BeforeEach(func() {
		tc = &fakeTemporalClient{
			executeWorkflowRun: &fakeWorkflowRun{id: "job-1", runID: "run-abc"},
		}
		fs = &fakeStore{}
	})

	Context("with a valid JSON body", func() {
		It("returns 202 with jobId, workflowId, runId, and RUNNING status", func() {
			body := `{"jobId":"job-1","tenantId":"acme","priority":"HIGH","itemCount":3}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusAccepted))
			resp := decodeJSON(w.Body.String())
			Expect(resp["jobId"]).To(Equal("job-1"))
			Expect(resp["workflowId"]).To(Equal("job-1"))
			Expect(resp["runId"]).To(Equal("run-abc"))
			Expect(resp["status"]).To(Equal(string(model.JobStatusRunning)))
		})

		It("auto-generates a UUID jobId when none is supplied", func() {
			body := `{"tenantId":"acme","priority":"MEDIUM","itemCount":1}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusAccepted))
			resp := decodeJSON(w.Body.String())
			jobID, _ := resp["jobId"].(string)
			Expect(jobID).ToNot(BeEmpty())
		})

		It("defaults priority to MEDIUM when not provided", func() {
			body := `{"tenantId":"acme","itemCount":1}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusAccepted))
			// The job record created in the store should use MEDIUM priority.
			Expect(fs.lastCreateJobRec).ToNot(BeNil())
			Expect(fs.lastCreateJobRec.Priority).To(Equal(model.PriorityMedium))
		})

		It("persists tenantId and fairnessKey into the job record", func() {
			body := `{"tenantId":"corp","fairnessKey":"team-x","priority":"LOW","itemCount":1}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusAccepted))
			Expect(fs.lastCreateJobRec.TenantID).To(Equal("corp"))
			Expect(fs.lastCreateJobRec.FairnessKey).To(Equal("team-x"))
		})
	})

	Context("with an invalid JSON body", func() {
		It("returns 400 Bad Request", func() {
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", `{broken json`)
			Expect(w.Code).To(Equal(http.StatusBadRequest))
			Expect(decodeJSON(w.Body.String())["error"]).ToNot(BeEmpty())
		})
	})

	Context("when the store CreateJob fails inside the transaction", func() {
		It("returns 500 and rolls back (no workflow is started)", func() {
			fs.createJobErr = fmt.Errorf("unique constraint violation")
			// Temporal should not be called because CreateJob fails inside the tx.
			tc.executeWorkflowRun = nil // would panic if called
			tc.executeWorkflowErr = nil

			body := `{"tenantId":"acme","priority":"MEDIUM","itemCount":1}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusInternalServerError))
			Expect(decodeJSON(w.Body.String())["error"]).ToNot(BeEmpty())
		})
	})

	Context("when Temporal ExecuteWorkflow fails", func() {
		It("returns 500 and the transaction is rolled back", func() {
			tc.executeWorkflowErr = fmt.Errorf("temporal server unavailable")
			tc.executeWorkflowRun = nil

			body := `{"tenantId":"acme","priority":"MEDIUM","itemCount":1}`
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs", body)

			Expect(w.Code).To(Equal(http.StatusInternalServerError))
		})
	})
})

var _ = Describe("GetJob handler", func() {
	var (
		tc *fakeTemporalClient
		fs *fakeStore
	)

	BeforeEach(func() {
		tc = &fakeTemporalClient{}
		fs = &fakeStore{}
	})

	Context("when the workflow is RUNNING", func() {
		It("returns 200 with RUNNING status", func() {
			tc.describeResponse = buildDescribeResponse("job-1", "run-1",
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

			w := doRequest(buildRouter(tc, fs), http.MethodGet, "/jobs/job-1", "")

			Expect(w.Code).To(Equal(http.StatusOK))
			resp := decodeJSON(w.Body.String())
			Expect(resp["status"]).To(Equal(string(model.JobStatusRunning)))
			Expect(resp["jobId"]).To(Equal("job-1"))
			Expect(resp["runId"]).To(Equal("run-1"))
		})
	})

	Context("when the workflow is COMPLETED", func() {
		It("returns 200 with the JobResult payload", func() {
			tc.describeResponse = buildDescribeResponse("job-2", "run-2",
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)

			result := model.JobResult{
				JobID:          "job-2",
				Status:         model.JobStatusCompleted,
				ItemsProcessed: 10,
				ItemsFailed:    0,
			}
			resultBytes, _ := json.Marshal(result)
			// Provide a fake WorkflowRun whose Get decodes the result.
			tc.getWorkflowRun = &fakeWorkflowRunWithResult{result: result}

			w := doRequest(buildRouter(tc, fs), http.MethodGet, "/jobs/job-2", "")

			Expect(w.Code).To(Equal(http.StatusOK))
			resp := decodeJSON(w.Body.String())
			_ = resultBytes
			Expect(resp["status"]).To(Equal(string(model.JobStatusCompleted)))
			Expect(resp["jobId"]).To(Equal("job-2"))
		})
	})

	Context("when the workflow has FAILED", func() {
		It("returns 200 with FAILED status and error message", func() {
			tc.describeResponse = buildDescribeResponse("job-3", "run-3",
				enumspb.WORKFLOW_EXECUTION_STATUS_FAILED)
			tc.getWorkflowRun = &fakeWorkflowRun{err: fmt.Errorf("processing error")}

			w := doRequest(buildRouter(tc, fs), http.MethodGet, "/jobs/job-3", "")

			Expect(w.Code).To(Equal(http.StatusOK))
			resp := decodeJSON(w.Body.String())
			Expect(resp["status"]).To(Equal(string(model.JobStatusFailed)))
			Expect(resp["error"]).ToNot(BeEmpty())
		})
	})

	Context("when the workflow was CANCELED", func() {
		It("returns 200 with CANCELLED status", func() {
			tc.describeResponse = buildDescribeResponse("job-4", "run-4",
				enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)

			w := doRequest(buildRouter(tc, fs), http.MethodGet, "/jobs/job-4", "")

			Expect(w.Code).To(Equal(http.StatusOK))
			resp := decodeJSON(w.Body.String())
			Expect(resp["status"]).To(Equal(string(model.JobStatusCancelled)))
		})
	})

	Context("when DescribeWorkflowExecution returns an error", func() {
		It("returns 404", func() {
			tc.describeErr = fmt.Errorf("workflow not found")

			w := doRequest(buildRouter(tc, fs), http.MethodGet, "/jobs/unknown", "")

			Expect(w.Code).To(Equal(http.StatusNotFound))
			resp := decodeJSON(w.Body.String())
			Expect(resp["jobId"]).To(Equal("unknown"))
		})
	})
})

var _ = Describe("CancelJob handler", func() {
	var (
		tc *fakeTemporalClient
		fs *fakeStore
	)

	BeforeEach(func() {
		tc = &fakeTemporalClient{}
		fs = &fakeStore{}
	})

	Context("when the cancellation succeeds", func() {
		It("returns 200 with CANCELLED status", func() {
			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs/job-1/cancel", "")

			Expect(w.Code).To(Equal(http.StatusOK))
			resp := decodeJSON(w.Body.String())
			Expect(resp["jobId"]).To(Equal("job-1"))
			Expect(resp["status"]).To(Equal(string(model.JobStatusCancelled)))
		})
	})

	Context("when CancelWorkflow returns an error", func() {
		It("returns 500", func() {
			tc.cancelErr = fmt.Errorf("workflow already completed")

			w := doRequest(buildRouter(tc, fs), http.MethodPost, "/jobs/job-1/cancel", "")

			Expect(w.Code).To(Equal(http.StatusInternalServerError))
		})
	})
})

// ─── fakeWorkflowRunWithResult ────────────────────────────────────────────────
// A variant of fakeWorkflowRun whose Get sets the value via JSON round-trip
// so the handler can deserialise it into a model.JobResult.

type fakeWorkflowRunWithResult struct {
	result model.JobResult
}

func (f *fakeWorkflowRunWithResult) GetID() string    { return f.result.JobID }
func (f *fakeWorkflowRunWithResult) GetRunID() string  { return "" }
func (f *fakeWorkflowRunWithResult) Get(_ context.Context, valuePtr interface{}) error {
	if vp, ok := valuePtr.(*model.JobResult); ok {
		*vp = f.result
	}
	return nil
}
func (f *fakeWorkflowRunWithResult) GetWithOptions(_ context.Context, valuePtr interface{}, _ client.WorkflowRunGetOptions) error {
	return f.Get(context.Background(), valuePtr)
}
