//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func apiGet(path string) *http.Response {
	resp, err := http.Get(apiBaseURL + path)
	Expect(err).NotTo(HaveOccurred(), "GET %s", path)
	return resp
}

func apiPost(path string, body any) *http.Response {
	b, err := json.Marshal(body)
	Expect(err).NotTo(HaveOccurred())
	resp, err := http.Post(apiBaseURL+path, "application/json", bytes.NewReader(b))
	Expect(err).NotTo(HaveOccurred(), "POST %s", path)
	return resp
}

func apiPostRaw(path, rawBody string) *http.Response {
	resp, err := http.Post(apiBaseURL+path, "application/json", bytes.NewReader([]byte(rawBody)))
	Expect(err).NotTo(HaveOccurred(), "POST %s (raw)", path)
	return resp
}

// decodeBody reads resp.Body and decodes it into a map. Closes the body.
func decodeBody(resp *http.Response) map[string]any {
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var out map[string]any
	Expect(json.Unmarshal(raw, &out)).To(Succeed(), "decode response: %s", string(raw))
	return out
}

// ── Polling helper ────────────────────────────────────────────────────────────

// pollJobUntilDone polls GET /jobs/:id every 500 ms until the status field is
// no longer "RUNNING", or until timeout elapses. Returns the final response
// body as a map. Fails the spec if the deadline is exceeded.
func pollJobUntilDone(jobID string, timeout time.Duration) map[string]any {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp := apiGet(fmt.Sprintf("/jobs/%s", jobID))
		body := decodeBody(resp)
		status, _ := body["status"].(string)
		if status != "RUNNING" {
			return body
		}
		time.Sleep(500 * time.Millisecond)
	}
	Fail(fmt.Sprintf("job %s did not leave RUNNING state within %s", jobID, timeout))
	return nil
}

// ── Request builder ───────────────────────────────────────────────────────────

// jobPayload is a convenience type so test bodies stay readable.
type jobPayload struct {
	JobID                     string `json:"jobId,omitempty"`
	TenantID                  string `json:"tenantId"`
	Priority                  string `json:"priority,omitempty"`
	FairnessKey               string `json:"fairnessKey,omitempty"`
	ItemCount                 int    `json:"itemCount"`
	SimulateFailure           bool   `json:"simulateFailure,omitempty"`
	SimulateStoreFailure      bool   `json:"simulateStoreFailure,omitempty"`
	UseParallelWorkflow       bool   `json:"useParallelWorkflow,omitempty"`
	SimulateDependencyFailure string `json:"simulateDependencyFailure,omitempty"`
	UseBatchWorkflow          bool   `json:"useBatchWorkflow,omitempty"`
	BatchCount                int    `json:"batchCount,omitempty"`
	SimulateChildFailure      string `json:"simulateChildFailure,omitempty"`
}
