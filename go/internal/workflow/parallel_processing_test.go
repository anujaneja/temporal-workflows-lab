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

var _ = Describe("ParallelProcessingWorkflow", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestWorkflowEnvironment
		acts *activity.Activities
	)

	// 4 items → partA gets items[0:2], partB gets items[2:4]
	mockItems := []activity.Item{
		{ID: "item-1", JobID: "job-pp", Data: "d1"},
		{ID: "item-2", JobID: "job-pp", Data: "d2"},
		{ID: "item-3", JobID: "job-pp", Data: "d3"},
		{ID: "item-4", JobID: "job-pp", Data: "d4"},
	}

	defaultReq := model.JobRequest{
		JobID:    "job-pp",
		TenantID: "tenant-b",
		Priority: model.PriorityHigh,
		ItemCount: 4,
	}

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestWorkflowEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterWorkflow(workflow.ParallelProcessingWorkflow)
		env.RegisterActivity(acts)
	})

	AfterEach(func() {
		env.AssertExpectations(GinkgoT())
	})

	Context("happy path", func() {
		It("completes with COMPLETED status and combined item counts", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnActivity(acts.ProcessPartAActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.ProcessPartBActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.AggregateResultsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 4, ItemsFailed: 0}, nil)
			env.OnActivity(acts.StoreResultsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).ToNot(HaveOccurred())

			var result model.JobResult
			Expect(env.GetWorkflowResult(&result)).To(Succeed())
			Expect(result.Status).To(Equal(model.JobStatusCompleted))
			Expect(result.ItemsProcessed).To(Equal(4))
			Expect(result.ItemsFailed).To(Equal(0))
		})
	})

	Context("when ValidateJobActivity fails", func() {
		It("fails the workflow before any parallel work starts", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).
				Return(temporal.NewNonRetryableApplicationError("tenantId is required", "InvalidInput", nil))

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when ProcessPartAActivity fails", func() {
		It("fails the whole workflow", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnActivity(acts.ProcessPartAActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, temporal.NewApplicationError(
					"simulated dependency failure in ProcessPartA", "DependencyFailurePartA", nil,
				))
			env.OnActivity(acts.ProcessPartBActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when ProcessPartBActivity fails", func() {
		It("fails the whole workflow", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnActivity(acts.ProcessPartAActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.ProcessPartBActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, temporal.NewApplicationError(
					"simulated dependency failure in ProcessPartB", "DependencyFailurePartB", nil,
				))

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when both parallel branches fail", func() {
		It("fails the workflow (PartA error takes precedence)", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnActivity(acts.ProcessPartAActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, fmt.Errorf("partA failed"))
			env.OnActivity(acts.ProcessPartBActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, fmt.Errorf("partB failed"))

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when AggregateResultsActivity fails", func() {
		It("fails the workflow after both branches complete", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnActivity(acts.ProcessPartAActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.ProcessPartBActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-pp", ItemsProcessed: 2}, nil)
			env.OnActivity(acts.AggregateResultsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, fmt.Errorf("aggregation failed"))

			env.ExecuteWorkflow(workflow.ParallelProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})
})
