package activity_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
)

var _ = Describe("AggregateResultsActivity", func() {
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

	req := model.JobRequest{
		JobID:    "job-agg",
		TenantID: "t",
		Priority: model.PriorityMedium,
	}

	It("sums ItemsProcessed from both parts", func() {
		partA := activity.ProcessResult{JobID: "job-agg", ItemsProcessed: 3, ItemsFailed: 1}
		partB := activity.ProcessResult{JobID: "job-agg", ItemsProcessed: 4, ItemsFailed: 2}

		val, err := env.ExecuteActivity(acts.AggregateResultsActivity, req, partA, partB)
		Expect(err).ToNot(HaveOccurred())

		var result activity.ProcessResult
		Expect(val.Get(&result)).To(Succeed())
		Expect(result.ItemsProcessed).To(Equal(7))
		Expect(result.ItemsFailed).To(Equal(3))
		Expect(result.JobID).To(Equal("job-agg"))
	})

	It("handles zero counts from both parts", func() {
		partA := activity.ProcessResult{JobID: "job-agg"}
		partB := activity.ProcessResult{JobID: "job-agg"}

		val, err := env.ExecuteActivity(acts.AggregateResultsActivity, req, partA, partB)
		Expect(err).ToNot(HaveOccurred())

		var result activity.ProcessResult
		Expect(val.Get(&result)).To(Succeed())
		Expect(result.ItemsProcessed).To(Equal(0))
		Expect(result.ItemsFailed).To(Equal(0))
	})

	It("handles one part with zero items", func() {
		partA := activity.ProcessResult{JobID: "job-agg", ItemsProcessed: 5}
		partB := activity.ProcessResult{JobID: "job-agg"}

		val, err := env.ExecuteActivity(acts.AggregateResultsActivity, req, partA, partB)
		Expect(err).ToNot(HaveOccurred())

		var result activity.ProcessResult
		Expect(val.Get(&result)).To(Succeed())
		Expect(result.ItemsProcessed).To(Equal(5))
	})
})
