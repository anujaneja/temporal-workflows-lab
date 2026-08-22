package workflow_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"github.com/anuj/temporal-workflows-lab/internal/workflow"
)

var _ = Describe("ProcessBatchWorkflow", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestWorkflowEnvironment
		acts *activity.Activities
	)

	req := model.JobRequest{JobID: "job-pb", TenantID: "tenant-a", Priority: model.PriorityMedium}
	batchItems := []activity.Item{
		{ID: "item-1", JobID: "job-pb", Data: "d1"},
		{ID: "item-2", JobID: "job-pb", Data: "d2"},
	}

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestWorkflowEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterWorkflow(workflow.ProcessBatchWorkflow)
		env.RegisterActivity(acts)
	})

	AfterEach(func() {
		env.AssertExpectations(GinkgoT())
	})

	Context("happy path", func() {
		It("processes then stores the batch and returns the result", func() {
			env.OnActivity(acts.ProcessBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pb", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.StoreBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)

			env.ExecuteWorkflow(workflow.ProcessBatchWorkflow, req, 3, batchItems)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).ToNot(HaveOccurred())

			var result activity.ProcessResult
			Expect(env.GetWorkflowResult(&result)).To(Succeed())
			Expect(result.ItemsProcessed).To(Equal(2))
		})
	})

	Context("when ProcessBatchActivity fails", func() {
		It("fails the child workflow before storing anything", func() {
			env.OnActivity(acts.ProcessBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, temporal.NewApplicationError(
					"simulated child batch failure", "BatchProcessingError", nil,
				))

			env.ExecuteWorkflow(workflow.ProcessBatchWorkflow, req, 0, batchItems)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when StoreBatchActivity fails", func() {
		It("fails the child workflow after processing completes", func() {
			env.OnActivity(acts.ProcessBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pb", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.StoreBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(fmt.Errorf("db unavailable"))

			env.ExecuteWorkflow(workflow.ProcessBatchWorkflow, req, 1, batchItems)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})
})
