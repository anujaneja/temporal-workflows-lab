package activity

import "github.com/anuj/temporal-workflows-lab/internal/store"

// Activities holds all external dependencies required by the four core activity
// implementations. Using a struct allows dependencies (DB, HTTP clients, etc.)
// to be injected at worker startup rather than through globals.
//
// All exported methods on this struct are registered as Temporal activities.
// The workflow references them via method expressions, e.g.:
//
//	workflow.ExecuteActivity(ctx, (*Activities).ValidateJobActivity, req)
type Activities struct {
	Store store.Store
}
