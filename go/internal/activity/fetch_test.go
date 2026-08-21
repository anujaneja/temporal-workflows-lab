package activity_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/anuj/temporal-workflows-lab/internal/activity"
	"github.com/anuj/temporal-workflows-lab/internal/model"
)

var _ = Describe("FetchItemsActivity", func() {
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

	decodeItems := func(val converter.EncodedValue) []activity.Item {
		var items []activity.Item
		Expect(val.Get(&items)).To(Succeed())
		return items
	}

	Context("when ItemCount is positive", func() {
		It("returns exactly ItemCount items", func() {
			req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium, ItemCount: 5}
			val, err := env.ExecuteActivity(acts.FetchItemsActivity, req)
			Expect(err).ToNot(HaveOccurred())

			items := decodeItems(val)
			Expect(items).To(HaveLen(5))
		})

		It("sets correct ID and JobID on each item", func() {
			req := model.JobRequest{JobID: "job-abc", TenantID: "t", Priority: model.PriorityMedium, ItemCount: 3}
			val, err := env.ExecuteActivity(acts.FetchItemsActivity, req)
			Expect(err).ToNot(HaveOccurred())

			items := decodeItems(val)
			for i, item := range items {
				Expect(item.JobID).To(Equal("job-abc"))
				Expect(item.ID).To(Equal(fmt.Sprintf("item-job-abc-%d", i+1)))
				Expect(item.Data).To(Equal(fmt.Sprintf("mock-data-%d", i+1)))
			}
		})
	})

	Context("when ItemCount is zero or negative", func() {
		DescribeTable("falls back to default count of 10",
			func(count int) {
				req := model.JobRequest{JobID: "job-1", TenantID: "t", Priority: model.PriorityMedium, ItemCount: count}
				val, err := env.ExecuteActivity(acts.FetchItemsActivity, req)
				Expect(err).ToNot(HaveOccurred())
				Expect(decodeItems(val)).To(HaveLen(10))
			},
			Entry("zero", 0),
			Entry("negative", -5),
		)
	})
})
