package activity_test

import (
	"context"

	"github.com/anuj/temporal-workflows-lab/internal/store"
)

// fakeStore is a hand-rolled in-memory Store implementation for unit tests.
// It records calls and returns pre-configured errors/results.
type fakeStore struct {
	saveJobErr error
	savedRec   *store.JobRecord
}

func (f *fakeStore) CreateJob(_ context.Context, rec store.JobRecord) error {
	return nil
}

func (f *fakeStore) SaveJobResult(_ context.Context, rec store.JobRecord) error {
	f.savedRec = &rec
	return f.saveJobErr
}

func (f *fakeStore) RunInTx(_ context.Context, fn func(store.Store) error) error {
	return fn(f)
}

func (f *fakeStore) Close() error { return nil }
