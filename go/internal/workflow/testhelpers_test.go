package workflow_test

import (
	"context"

	"github.com/anuj/temporal-workflows-lab/internal/store"
)

// fakeStore satisfies store.Store for workflow tests.
// Workflow tests mock activities at the environment level, so the store is
// never actually called — this stub just satisfies the interface.
type fakeStore struct{}

func (f *fakeStore) CreateJob(_ context.Context, _ store.JobRecord) error         { return nil }
func (f *fakeStore) SaveJobResult(_ context.Context, _ store.JobRecord) error     { return nil }
func (f *fakeStore) SaveBatchResult(_ context.Context, _ store.BatchRecord) error { return nil }
func (f *fakeStore) RunInTx(_ context.Context, fn func(store.Store) error) error  { return fn(f) }
func (f *fakeStore) Close() error                                                 { return nil }
