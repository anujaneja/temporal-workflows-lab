package activity

import (
	"context"
	"fmt"

	"github.com/anuj/temporal-workflows-lab/internal/model"
	"go.temporal.io/sdk/activity"
)

// Item is the unit of work returned by FetchItemsActivity.
type Item struct {
	ID    string `json:"id"`
	JobID string `json:"jobId"`
	Data  string `json:"data"`
}

// FetchItemsActivity simulates fetching items from an external source.
// In Phase 8 this will call the MockExternalAPI and handle HTTP 429 / rate limiting.
func (a *Activities) FetchItemsActivity(ctx context.Context, req model.JobRequest) ([]Item, error) {
	log := activity.GetLogger(ctx)
	log.Info("FetchItemsActivity started", "jobId", req.JobID, "itemCount", req.ItemCount)

	count := req.ItemCount
	if count <= 0 {
		count = 10
	}

	items := make([]Item, count)
	for i := range items {
		items[i] = Item{
			ID:    fmt.Sprintf("item-%s-%d", req.JobID, i+1),
			JobID: req.JobID,
			Data:  fmt.Sprintf("mock-data-%d", i+1),
		}
	}

	log.Info("FetchItemsActivity completed", "jobId", req.JobID, "itemsFetched", len(items))
	return items, nil
}
