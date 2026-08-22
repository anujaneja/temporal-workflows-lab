package workflow_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
	"github.com/anuj/temporal-workflows-lab/internal/workflow"
)

var _ = Describe("BatchProcessingWorkflow", func() {
	var (
		s    testsuite.WorkflowTestSuite
		env  *testsuite.TestWorkflowEnvironment
		acts *activity.Activities
	)

	// 6 items with the default batch count (3) → two items per batch.
	mockItems := []activity.Item{
		{ID: "item-1", JobID: "job-bp", Data: "d1"},
		{ID: "item-2", JobID: "job-bp", Data: "d2"},
		{ID: "item-3", JobID: "job-bp", Data: "d3"},
		{ID: "item-4", JobID: "job-bp", Data: "d4"},
		{ID: "item-5", JobID: "job-bp", Data: "d5"},
		{ID: "item-6", JobID: "job-bp", Data: "d6"},
	}

	defaultReq := model.JobRequest{
		JobID:     "job-bp",
		TenantID:  "tenant-a",
		Priority:  model.PriorityMedium,
		ItemCount: 6,
	}

	BeforeEach(func() {
		s = testsuite.WorkflowTestSuite{}
		env = s.NewTestWorkflowEnvironment()
		acts = &activity.Activities{Store: &fakeStore{}}
		env.RegisterWorkflow(workflow.BatchProcessingWorkflow)
		env.RegisterActivity(acts)
	})

	AfterEach(func() {
		env.AssertExpectations(GinkgoT())
	})

	Context("happy path", func() {
		It("fans out to DefaultBatchCount children and aggregates their results", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			env.OnWorkflow(workflow.ProcessBatchWorkflow, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{JobID: "job-bp", ItemsProcessed: 2}, nil).
				Times(workflow.DefaultBatchCount)
			env.OnActivity(acts.StoreResultsActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			env.ExecuteWorkflow(workflow.BatchProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).ToNot(HaveOccurred())

			var result model.JobResult
			Expect(env.GetWorkflowResult(&result)).To(Succeed())
			Expect(result.Status).To(Equal(model.JobStatusCompleted))
			Expect(result.ItemsProcessed).To(Equal(2 * workflow.DefaultBatchCount))
		})
	})

	Context("when ValidateJobActivity fails", func() {
		It("fails the workflow before any batch starts", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).
				Return(temporal.NewNonRetryableApplicationError("tenantId is required", "InvalidInput", nil))

			env.ExecuteWorkflow(workflow.BatchProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when FetchItemsActivity fails", func() {
		It("fails the workflow before any batch starts", func() {
			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).
				Return(nil, fmt.Errorf("external API unavailable"))

			env.ExecuteWorkflow(workflow.BatchProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when one child batch fails", func() {
		It("fails the whole parent workflow", func() {
			req := defaultReq
			req.BatchCount = 2
			req.ItemCount = 4

			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems[:4], nil)
			env.OnWorkflow(workflow.ProcessBatchWorkflow, mock.Anything, mock.Anything,
				mock.MatchedBy(func(batchIndex int) bool { return batchIndex == 0 }), mock.Anything).
				Return(activity.ProcessResult{}, temporal.NewApplicationError(
					"simulated child batch failure", "BatchProcessingError", nil,
				))
			env.OnWorkflow(workflow.ProcessBatchWorkflow, mock.Anything, mock.Anything,
				mock.MatchedBy(func(batchIndex int) bool { return batchIndex != 0 }), mock.Anything).
				Return(activity.ProcessResult{JobID: "job-bp", ItemsProcessed: 2}, nil)

			env.ExecuteWorkflow(workflow.BatchProcessingWorkflow, req)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			Expect(env.GetWorkflowError()).To(HaveOccurred())
		})
	})

	Context("when the parent workflow is cancelled while children are still running", func() {
		It("propagates cancellation and the workflow ends as canceled", func() {
			// Use the real ProcessBatchWorkflow (not a mock) so cancellation of the
			// parent context genuinely propagates down into each running child.
			env.RegisterWorkflow(workflow.ProcessBatchWorkflow)

			env.OnActivity(acts.ValidateJobActivity, mock.Anything, mock.Anything).Return(nil)
			env.OnActivity(acts.FetchItemsActivity, mock.Anything, mock.Anything).Return(mockItems, nil)
			// Simulate a long-running batch: the activity "takes" 10s of simulated
			// time, so every child is still in flight when cancellation fires at 1s.
			env.OnActivity(acts.ProcessBatchActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(activity.ProcessResult{}, nil).After(10 * time.Second)

			env.RegisterDelayedCallback(func() {
				env.CancelWorkflow()
			}, time.Second)

			env.ExecuteWorkflow(workflow.BatchProcessingWorkflow, defaultReq)

			Expect(env.IsWorkflowCompleted()).To(BeTrue())
			err := env.GetWorkflowError()
			Expect(err).To(HaveOccurred())
			Expect(temporal.IsCanceledError(err)).To(BeTrue())
		})
	})
})
