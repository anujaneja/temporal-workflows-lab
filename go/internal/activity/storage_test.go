package activity_test

import (
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
)

var _ = Describe("StoreResultsActivity", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestActivityEnvironment
		acts *activity.Activities
		fs   *fakeStore
	)

	processResult := activity.ProcessResult{
		JobID:          "job-1",
		ItemsProcessed: 5,
		ItemsFailed:    1,
	}
	baseReq := model.JobRequest{
		JobID:    "job-1",
		TenantID: "tenant-a",
		Priority: model.PriorityHigh,
	}

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		fs = &fakeStore{}
		acts = &activity.Activities{Store: fs}
		env = s.NewTestActivityEnvironment()
		env.RegisterActivity(acts)
	})

	Context("without failure simulation", func() {
		It("calls SaveJobResult and returns no error", func() {
			_, err := env.ExecuteActivity(acts.StoreResultsActivity, baseReq, processResult)
			Expect(err).ToNot(HaveOccurred())

			Expect(fs.savedRec).ToNot(BeNil())
			Expect(fs.savedRec.ID).To(Equal("job-1"))
			Expect(fs.savedRec.Status).To(Equal(model.JobStatusCompleted))
			Expect(fs.savedRec.ItemsProcessed).To(Equal(5))
			Expect(fs.savedRec.ItemsFailed).To(Equal(1))
			Expect(fs.savedRec.CompletedAt).ToNot(BeNil())
		})

		It("persists TenantID, Priority and FairnessKey", func() {
			req := model.JobRequest{
				JobID:       "job-1",
				TenantID:    "acme",
				Priority:    model.PriorityLow,
				FairnessKey: "team-alpha",
			}
			_, err := env.ExecuteActivity(acts.StoreResultsActivity, req, processResult)
			Expect(err).ToNot(HaveOccurred())
			Expect(fs.savedRec.TenantID).To(Equal("acme"))
			Expect(fs.savedRec.Priority).To(Equal(model.PriorityLow))
			Expect(fs.savedRec.FairnessKey).To(Equal("team-alpha"))
		})
	})

	Context("with failure simulation (SimulateStoreFailure=true)", func() {
		It("returns a retryable TransientStoreError on the first attempt", func() {
			req := model.JobRequest{
				JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium,
				SimulateStoreFailure: true,
			}
			_, err := env.ExecuteActivity(acts.StoreResultsActivity, req, processResult)
			Expect(err).To(HaveOccurred())

			var appErr *temporal.ApplicationError
			Expect(errors.As(err, &appErr)).To(BeTrue())
			Expect(appErr.NonRetryable()).To(BeFalse())
			Expect(appErr.Type()).To(Equal("TransientStoreError"))
			Expect(appErr.Message()).To(ContainSubstring("simulated transient DB failure"))
		})
	})

	Context("when the store returns a real error", func() {
		It("wraps the error as a retryable StoreError", func() {
			fs.saveJobErr = fmt.Errorf("connection reset by peer")
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium}

			_, err := env.ExecuteActivity(acts.StoreResultsActivity, req, processResult)
			Expect(err).To(HaveOccurred())

			var appErr *temporal.ApplicationError
			Expect(errors.As(err, &appErr)).To(BeTrue())
			Expect(appErr.Type()).To(Equal("StoreError"))
			Expect(appErr.NonRetryable()).To(BeFalse())
		})
	})
})
