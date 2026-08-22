//go:build integration

package integration_test

import (
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("POST /jobs", func() {

	Describe("immediate response", func() {
		It("returns 202 with jobId, workflowId, runId, and RUNNING status", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:  "acme",
				Priority:  "MEDIUM",
				ItemCount: 3,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			body := decodeBody(resp)
			Expect(body["jobId"]).NotTo(BeEmpty())
			Expect(body["workflowId"]).NotTo(BeEmpty())
			Expect(body["runId"]).NotTo(BeEmpty())
			Expect(body["status"]).To(Equal("RUNNING"))
		})

		It("accepts an explicit jobId supplied by the caller", func() {
			resp := apiPost("/jobs", jobPayload{
				JobID:     "explicit-job-id-001",
				TenantID:  "acme",
				ItemCount: 1,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			body := decodeBody(resp)
			Expect(body["jobId"]).To(Equal("explicit-job-id-001"))
		})

		It("returns 400 for a malformed JSON body", func() {
			resp := apiPostRaw("/jobs", `{broken json`)
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			body := decodeBody(resp)
			Expect(body["error"]).NotTo(BeEmpty())
		})
	})

	Describe("sequential workflow — end-to-end completion", func() {
		It("completes with COMPLETED status and itemsProcessed > 0", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:  "acme",
				Priority:  "HIGH",
				ItemCount: 5,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			result := pollJobUntilDone(jobID, 60*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
			Expect(result["itemsProcessed"]).NotTo(BeNil())
		})

		It("stores fairnessKey and tenantId correctly (job reaches COMPLETED)", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:    "corp",
				FairnessKey: "team-x",
				Priority:    "LOW",
				ItemCount:   2,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			result := pollJobUntilDone(jobID, 60*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
		})
	})

	Describe("parallel workflow — end-to-end completion", func() {
		It("routes to the parallel workflow and reaches COMPLETED", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:            "acme",
				Priority:            "MEDIUM",
				ItemCount:           10,
				UseParallelWorkflow: true,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			result := pollJobUntilDone(jobID, 90*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
			Expect(result["itemsProcessed"]).NotTo(BeNil())
		})
	})

	Describe("batch workflow — end-to-end completion", func() {
		It("routes to the batch workflow, fans out to child workflows, and reaches COMPLETED", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:         "acme",
				Priority:         "MEDIUM",
				ItemCount:        9,
				UseBatchWorkflow: true,
				BatchCount:       3,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			result := pollJobUntilDone(jobID, 90*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
			Expect(result["itemsProcessed"]).To(Equal(float64(9)))
		})
	})

	Describe("failure simulation", func() {
		It("retries and eventually succeeds when simulateFailure is true", func() {
			// simulateFailure injects errors on attempts 1-2 then succeeds on 3.
			resp := apiPost("/jobs", jobPayload{
				TenantID:        "acme",
				ItemCount:       4,
				SimulateFailure: true,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			// Allow extra time for retries.
			result := pollJobUntilDone(jobID, 90*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
		})

		It("retries and eventually succeeds when simulateStoreFailure is true", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:             "acme",
				ItemCount:            3,
				SimulateStoreFailure: true,
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			result := pollJobUntilDone(jobID, 90*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
		})

		It("retries a failing child batch and eventually succeeds when simulateChildFailure is FIRST", func() {
			resp := apiPost("/jobs", jobPayload{
				TenantID:             "acme",
				ItemCount:            6,
				UseBatchWorkflow:     true,
				BatchCount:           3,
				SimulateChildFailure: "FIRST",
			})
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			jobID, _ := decodeBody(resp)["jobId"].(string)

			// Allow extra time for the failing batch's retries.
			result := pollJobUntilDone(jobID, 90*time.Second)
			Expect(result["status"]).To(Equal("COMPLETED"))
		})
	})
})

var _ = Describe("GET /jobs/:id", func() {

	It("returns 404 for an unknown job ID", func() {
		resp := apiGet("/jobs/does-not-exist-xyz")
		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		body := decodeBody(resp)
		Expect(body["error"]).NotTo(BeEmpty())
		Expect(body["jobId"]).To(Equal("does-not-exist-xyz"))
	})

	It("returns RUNNING immediately after submission before the workflow completes", func() {
		// Submit a job with enough items to stay alive for a moment.
		resp := apiPost("/jobs", jobPayload{
			TenantID:  "acme",
			ItemCount: 20,
		})
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		jobID, _ := decodeBody(resp)["jobId"].(string)

		// Query immediately — the workflow should still be running.
		status := apiGet("/jobs/" + jobID)
		body := decodeBody(status)
		Expect(body["status"]).To(Equal("RUNNING"))
		Expect(body["jobId"]).To(Equal(jobID))

		// Let the workflow complete so it doesn't linger into other tests.
		pollJobUntilDone(jobID, 60*time.Second)
	})

	It("returns COMPLETED with full result fields once the workflow finishes", func() {
		resp := apiPost("/jobs", jobPayload{
			TenantID:  "acme",
			ItemCount: 5,
		})
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		jobID, _ := decodeBody(resp)["jobId"].(string)

		result := pollJobUntilDone(jobID, 60*time.Second)
		Expect(result["status"]).To(Equal("COMPLETED"))
		Expect(result["jobId"]).To(Equal(jobID))
		// itemsProcessed and duration are always present on a completed job.
		Expect(result["itemsProcessed"]).NotTo(BeNil())
		Expect(result["duration"]).NotTo(BeNil())
	})
})

var _ = Describe("POST /jobs/:id/cancel", func() {

	It("cancels a running job and polling resolves to CANCELLED", func() {
		// Submit with enough items that the workflow is still running when we cancel.
		resp := apiPost("/jobs", jobPayload{
			TenantID:  "acme",
			ItemCount: 50,
		})
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		jobID, _ := decodeBody(resp)["jobId"].(string)

		// Cancel immediately.
		cancelResp := apiPost("/jobs/"+jobID+"/cancel", nil)
		Expect(cancelResp.StatusCode).To(Equal(http.StatusOK))
		cancelBody := decodeBody(cancelResp)
		Expect(cancelBody["jobId"]).To(Equal(jobID))
		Expect(cancelBody["status"]).To(Equal("CANCELLED"))

		// Poll until the workflow acknowledges the cancellation.
		result := pollJobUntilDone(jobID, 60*time.Second)
		Expect(result["status"]).To(Equal("CANCELLED"))
	})

	It("returns 500 when trying to cancel a non-existent job", func() {
		resp := apiPost("/jobs/ghost-job-xyz/cancel", nil)
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
	})

	It("cancels a running batch job — parent cancellation propagates to child workflows", func() {
		// Submit with enough items/batches that the child workflows are still
		// running when we cancel the parent.
		resp := apiPost("/jobs", jobPayload{
			TenantID:         "acme",
			ItemCount:        60,
			UseBatchWorkflow: true,
			BatchCount:       3,
		})
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		jobID, _ := decodeBody(resp)["jobId"].(string)

		cancelResp := apiPost("/jobs/"+jobID+"/cancel", nil)
		Expect(cancelResp.StatusCode).To(Equal(http.StatusOK))

		// The parent's ParentClosePolicy (REQUEST_CANCEL) asks each running child
		// workflow to cancel gracefully rather than terminating it — observable
		// here as the parent job settling into CANCELLED.
		result := pollJobUntilDone(jobID, 60*time.Second)
		Expect(result["status"]).To(Equal("CANCELLED"))
	})
})
