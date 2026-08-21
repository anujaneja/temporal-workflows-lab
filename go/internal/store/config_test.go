package store_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/anuj/temporal-workflows-lab/internal/store"
)

var _ = Describe("DefaultDBConfig", func() {
	var cfg store.DBConfig

	BeforeEach(func() {
		cfg = store.DefaultDBConfig()
	})

	It("sets MaxConns to 10 to bound the pool footprint", func() {
		Expect(cfg.MaxConns).To(Equal(int32(10)))
	})

	It("sets MinConns to 2 to keep at least two warm connections", func() {
		Expect(cfg.MinConns).To(Equal(int32(2)))
	})

	It("sets MaxConnLifetime to 30 minutes to recycle long-lived connections", func() {
		Expect(cfg.MaxConnLifetime).To(Equal(30 * time.Minute))
	})

	It("sets MaxConnIdleTime to 5 minutes to trim idle connections", func() {
		Expect(cfg.MaxConnIdleTime).To(Equal(5 * time.Minute))
	})

	It("sets HealthCheckPeriod to 1 minute for regular idle-connection checks", func() {
		Expect(cfg.HealthCheckPeriod).To(Equal(time.Minute))
	})

	It("returns independent copies on each call (no shared mutable state)", func() {
		cfg2 := store.DefaultDBConfig()
		cfg2.MaxConns = 99
		Expect(cfg.MaxConns).To(Equal(int32(10)), "mutating one copy must not affect another")
	})
})
