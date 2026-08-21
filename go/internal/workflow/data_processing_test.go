package workflow_test

import (
	"errors"
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

var _ = Describe("DataProcessingWorkflow", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestWorkflowEnvironment
		acts *activity.Activities
	)

	defaultReq := model.JobRequest{
		JobID:     "job-dp-1",
		TenantID:  "tenant-a",
		Priority:  model.PriorityMedium,
		ItemCount: 4,
	}

	mockItems := []activity.Item{
		{ID: "item-1", JobID: "job-dp-1", Data: "d1"},
		{ID: "item-2", JobID: "job-dp-1", Data: "d2"},
		{ID: "item-3", JobID: "job-dp-1", Data: "d3"},
		{ID: "item-4", JobID: "job-dp-1", Data: "d4"},
	}

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestWorkflowEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterWorkflow(workflow.DataProcessingWorkflow)
		env.RegisterActivity(acts)
	})

	AfterEach(func() {
		env.AssertExpectations(GinkgoT())
	})

	Context("happy path", func() {
		It("completes with COMPLETED status and correct item counts", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).
				Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).
				Return(mockItems, nil)
			env.OnActivity(acts.ProcessItemsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-dp-1", ItemsProcessed: 4, ItemsFailed: 0}, nil)
			env.OnActivity(acts.StoreResultsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)

			env.ExecuteWorkflow(workflow.DataProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).ToNot(HaveOccurred())

			var result model.JobResult
			Expect(env.GetWorkflowResult(&result)).To(Succeed())
			Expect(result.Status).To(Equal(model.JobStatusCompleted))
			Expect(result.JobID).To(Equal("job-dp-1"))
			Expect(result.ItemsProcessed).To(Equal(4))
			Expect(result.ItemsFailed).To(Equal(0))
		})
	})

	Context("when ValidateJobActivity fails", func() {
		It("fails the workflow with a NonRetryable error and sets FAILED status", func() {
			validationErr := temporal.NewNonRetryableApplicationError(
				"jobId is required", "InvalidInput", nil,
			)
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).
				Return(validationErr)

			env.ExecuteWorkflow(workflow.DataProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())

			// The workflow itself returns a *model.JobResult via the error path
			// (failResult returns the result AND the error).
			var result model.JobResult
			Expect(env.GetWorkflowResult(&result)).To(HaveOccurred())
			// Verify the error chain contains the application error.
			wfErr := env.GetWorkflowError()
			var appErr *temporal.ApplicationError
			Expect(errors.As(wfErr, &appErr)).To(BeTrue())
			Expect(appErr.Message()).To(ContainSubstring("jobId is required"))
		})
	})

	Context("when FetchItemsActivity fails", func() {
		It("fails the workflow before reaching ProcessItems", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).
				Return(nil, fmt.Errorf("external API unavailable"))

			env.ExecuteWorkflow(workflow.DataProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when ProcessItemsActivity fails", func() {
		It("fails the workflow", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).
				Return(mockItems, nil)
			env.OnActivity(acts.ProcessItemsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, temporal.NewApplicationError(
					"simulated transient processing failure", "TransientProcessingError", nil,
				))

			env.ExecuteWorkflow(workflow.DataProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when StoreResultsActivity fails", func() {
		It("fails the workflow after processing completes", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).
				Return(mockItems, nil)
			env.OnActivity(acts.ProcessItemsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-dp-1", ItemsProcessed: 4}, nil)
			env.OnActivity(acts.StoreResultsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(temporal.NewApplicationError("DB unavailable", "StoreError", nil))

			env.ExecuteWorkflow(workflow.DataProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})
})
