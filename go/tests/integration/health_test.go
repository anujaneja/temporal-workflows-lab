//go:build integration

package integration_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GET /health", func() {
	It("returns 200 with status ok when all dependencies are reachable", func() {
		resp := apiGet("/health")
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body := decodeBody(resp)
		Expect(body["status"]).To(Equal("ok"))
		Expect(body["temporal"]).To(Equal("temporal:7233"))
	})
})
