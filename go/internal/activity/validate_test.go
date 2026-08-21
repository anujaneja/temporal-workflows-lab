package activity_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
)

var _ = Describe("ValidateJobActivity", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestActivityEnvironment
		acts *activity.Activities
	)

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestActivityEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterActivity(acts)
	})

	Context("when the request is valid", func() {
		It("returns no error", func() {
			req := model.JobRequest{
				JobID:    "job-valid",
				TenantID: "tenant-1",
				Priority: model.PriorityMedium,
			}
			_, err := env.ExecuteActivity(acts.ValidateJobActivity, req)
			Expect(err).ToNot(HaveOccurred())
		})

		DescribeTable("accepts all valid priorities",
			func(priority model.Priority) {
				req := model.JobRequest{JobID: "j", TenantID: "t", Priority: priority}
				_, err := env.ExecuteActivity(acts.ValidateJobActivity, req)
				Expect(err).ToNot(HaveOccurred())
			},
			Entry("HIGH", model.PriorityHigh),
			Entry("MEDIUM", model.PriorityMedium),
			Entry("LOW", model.PriorityLow),
		)
	})

	Context("when the request is invalid", func() {
		DescribeTable("returns a NonRetryableApplicationError",
			func(req model.JobRequest, expectedMsg string) {
				_, err := env.ExecuteActivity(acts.ValidateJobActivity, req)
				Expect(err).To(HaveOccurred())

				// Temporal wraps the error in ActivityError → ApplicationError.
				var appErr *temporal.ApplicationError
				Expect(errors.As(err, &appErr)).To(BeTrue(),
					"expected the cause to be *temporal.ApplicationError")
				Expect(appErr.NonRetryable()).To(BeTrue(),
					"validation errors must not be retried")
				Expect(appErr.Message()).To(ContainSubstring(expectedMsg))
			},
			Entry("missing JobID",
				model.JobRequest{TenantID: "t", Priority: model.PriorityMedium},
				"jobId is required",
			),
			Entry("missing TenantID",
				model.JobRequest{JobID: "j", Priority: model.PriorityMedium},
				"tenantId is required",
			),
			Entry("invalid priority",
				model.JobRequest{JobID: "j", TenantID: "t", Priority: "INVALID"},
				"invalid priority",
			),
			Entry("empty priority",
				model.JobRequest{JobID: "j", TenantID: "t", Priority: ""},
				"invalid priority",
			),
		)
	})
})
