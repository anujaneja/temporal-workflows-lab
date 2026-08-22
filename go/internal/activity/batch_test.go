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

var _ = Describe("ProcessBatchActivity", func() {
	var (
		s     testsuite.WorkflowTestSuite
		env   *testsuite.TestActivityEnvironment
		acts  *activity.Activities
		items []activity.Item
	)

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestActivityEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterActivity(acts)
		items = []activity.Item{
			{ID: "i-1", JobID: "job-1", Data: "d1"},
			{ID: "i-2", JobID: "job-1", Data: "d2"},
		}
	})

	Context("without failure simulation", func() {
		It("returns the processed item count for the given batch", func() {
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium}
			val, err := env.ExecuteActivity(acts.ProcessBatchActivity, req, 0, items)
			Expect(err).ToNot(HaveOccurred())

			var result activity.ProcessResult
			Expect(val.Get(&result)).To(Succeed())
			Expect(result.JobID).To(Equal("job-1"))
			Expect(result.ItemsProcessed).To(Equal(2))
			Expect(result.ItemsFailed).To(Equal(0))
		})
	})

	DescribeTable("failure simulation targets",
		func(simulateVal string, batchIndex int, expectsErr bool) {
			req := model.JobRequest{
				JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium,
				SimulateChildFailure: simulateVal,
			}
			_, err := env.ExecuteActivity(acts.ProcessBatchActivity, req, batchIndex, items)
			if expectsErr {
				Expect(err).To(HaveOccurred())
				var appErr *temporal.ApplicationError
				Expect(errors.As(err, &appErr)).To(BeTrue())
				Expect(appErr.NonRetryable()).To(BeFalse(), "simulated child failure must be retryable")
				Expect(appErr.Type()).To(Equal("BatchProcessingError"))
			} else {
				Expect(err).ToNot(HaveOccurred())
			}
		},
		Entry("ALL → fails batch 0", "ALL", 0, true),
		Entry("ALL → fails batch 1", "ALL", 1, true),
		Entry("FIRST → fails batch 0", "FIRST", 0, true),
		Entry("FIRST → does not fail batch 1", "FIRST", 1, false),
		Entry("empty → no failure", "", 0, false),
	)
})

var _ = Describe("StoreBatchActivity", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestActivityEnvironment
		acts *activity.Activities
		fs   *fakeStore
	)

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestActivityEnvironment()
		fs = &fakeStore{}
		acts = &activity.Activities{Store: fs}
		env.RegisterActivity(acts)
	})

	req := model.JobRequest{JobID: "job-batch", TenantID: "t", Priority: model.PriorityMedium}
	result := activity.ProcessResult{JobID: "job-batch", ItemsProcessed: 3, ItemsFailed: 1}

	It("persists the batch result via the store", func() {
		_, err := env.ExecuteActivity(acts.StoreBatchActivity, req, 2, result)
		Expect(err).ToNot(HaveOccurred())

		Expect(fs.savedBatchRec).ToNot(BeNil())
		Expect(fs.savedBatchRec.JobID).To(Equal("job-batch"))
		Expect(fs.savedBatchRec.BatchIndex).To(Equal(2))
		Expect(fs.savedBatchRec.ItemsProcessed).To(Equal(3))
		Expect(fs.savedBatchRec.ItemsFailed).To(Equal(1))
		Expect(fs.savedBatchRec.Status).To(Equal(model.JobStatusCompleted))
	})

	It("wraps a store error as a retryable StoreBatchError", func() {
		fs.saveBatchErr = errors.New("db unavailable")

		_, err := env.ExecuteActivity(acts.StoreBatchActivity, req, 0, result)
		Expect(err).To(HaveOccurred())

		var appErr *temporal.ApplicationError
		Expect(errors.As(err, &appErr)).To(BeTrue())
		Expect(appErr.NonRetryable()).To(BeFalse())
		Expect(appErr.Type()).To(Equal("StoreBatchError"))
	})
})
