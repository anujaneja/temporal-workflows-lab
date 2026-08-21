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

var _ = Describe("ProcessItemsActivity", func() {
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
		It("succeeds on the first attempt and returns correct counts", func() {
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium}
			val, err := env.ExecuteActivity(acts.ProcessItemsActivity, req, items)
			Expect(err).ToNot(HaveOccurred())

			var result activity.ProcessResult
			Expect(val.Get(&result)).To(Succeed())
			Expect(result.JobID).To(Equal("job-1"))
			Expect(result.ItemsProcessed).To(Equal(2))
			Expect(result.ItemsFailed).To(Equal(0))
		})
	})

	Context("with failure simulation (SimulateFailure=true)", func() {
		// The TestActivityEnvironment runs activities at attempt=1.
		// The simulation fires on attempt < 3, so we always hit the failure path here.
		It("returns a retryable TransientProcessingError", func() {
			req := model.JobRequest{
				JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium,
				SimulateFailure: true,
			}
			_, err := env.ExecuteActivity(acts.ProcessItemsActivity, req, items)
			Expect(err).To(HaveOccurred())

			var appErr *temporal.ApplicationError
			Expect(errors.As(err, &appErr)).To(BeTrue())
			Expect(appErr.NonRetryable()).To(BeFalse(), "simulated failure must be retryable")
			Expect(appErr.Type()).To(Equal("TransientProcessingError"))
			Expect(appErr.Message()).To(ContainSubstring("simulated transient processing failure"))
		})
	})
})

var _ = Describe("ProcessPartAActivity", func() {
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
		items = []activity.Item{{ID: "i-1", JobID: "job-1", Data: "d1"}}
	})

	Context("without failure simulation", func() {
		It("returns the processed item count", func() {
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium}
			val, err := env.ExecuteActivity(acts.ProcessPartAActivity, req, items)
			Expect(err).ToNot(HaveOccurred())

			var result activity.ProcessResult
			Expect(val.Get(&result)).To(Succeed())
			Expect(result.ItemsProcessed).To(Equal(1))
		})
	})

	DescribeTable("failure simulation targets",
		func(simulateVal string, expectsErr bool, errType string) {
			req := model.JobRequest{
				JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium,
				SimulateDependencyFailure: simulateVal,
			}
			_, err := env.ExecuteActivity(acts.ProcessPartAActivity, req, items)
			if expectsErr {
				Expect(err).To(HaveOccurred())
				var appErr *temporal.ApplicationError
				Expect(errors.As(err, &appErr)).To(BeTrue())
				Expect(appErr.NonRetryable()).To(BeFalse())
				Expect(appErr.Type()).To(Equal(errType))
			} else {
				Expect(err).ToNot(HaveOccurred())
			}
		},
		Entry("PART_A → fails PartA", "PART_A", true, "DependencyFailurePartA"),
		Entry("BOTH → fails PartA", "BOTH", true, "DependencyFailurePartA"),
		Entry("PART_B → does not fail PartA", "PART_B", false, ""),
		Entry("empty → no failure", "", false, ""),
	)
})

var _ = Describe("ProcessPartBActivity", func() {
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
		items = []activity.Item{{ID: "i-1", JobID: "job-1", Data: "d1"}}
	})

	Context("without failure simulation", func() {
		It("returns the processed item count", func() {
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium}
			val, err := env.ExecuteActivity(acts.ProcessPartBActivity, req, items)
			Expect(err).ToNot(HaveOccurred())

			var result activity.ProcessResult
			Expect(val.Get(&result)).To(Succeed())
			Expect(result.ItemsProcessed).To(Equal(1))
		})
	})

	DescribeTable("failure simulation targets",
		func(simulateVal string, expectsErr bool, errType string) {
			req := model.JobRequest{
				JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium,
				SimulateDependencyFailure: simulateVal,
			}
			_, err := env.ExecuteActivity(acts.ProcessPartBActivity, req, items)
			if expectsErr {
				Expect(err).To(HaveOccurred())
				var appErr *temporal.ApplicationError
				Expect(errors.As(err, &appErr)).To(BeTrue())
				Expect(appErr.NonRetryable()).To(BeFalse())
				Expect(appErr.Type()).To(Equal(errType))
			} else {
				Expect(err).ToNot(HaveOccurred())
			}
		},
		Entry("PART_B → fails PartB", "PART_B", true, "DependencyFailurePartB"),
		Entry("BOTH → fails PartB", "BOTH", true, "DependencyFailurePartB"),
		Entry("PART_A → does not fail PartB", "PART_A", false, ""),
		Entry("empty → no failure", "", false, ""),
	)
})
