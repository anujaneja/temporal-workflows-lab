package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	internalapi "github.com/anuj/temporal-workflows-lab/internal/api"
	"github.com/anuj/temporal-workflows-lab/internal/store"
	"github.com/anuj/temporal-workflows-lab/internal/temporalclient"
	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"
)

func main() {
	temporalHost := envOrDefault("TEMPORAL_HOST", "localhost:7233")
	apiPort := envOrDefault("API_PORT", "8081")
	appDBDSN := envOrDefault("APP_DB_DSN", "postgres://lab:lab@localhost:5433/lab?sslmode=disable")

	dbCfg := store.DefaultDBConfig()
	if v := envOrDefault("DB_MAX_CONNS", ""); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Fatalf("invalid DB_MAX_CONNS %q: %v", v, err)
		}
		dbCfg.MaxConns = int32(n)
	}

	tc, err := temporalclient.Dial(temporalHost)
	if err != nil {
		log.Fatalf("failed to connect to Temporal: %v", err)
	}
	defer tc.Close()

	appStore, err := store.NewPostgres(appDBDSN, dbCfg)
	if err != nil {
		log.Fatalf("failed to connect to app postgres: %v", err)
	}
	defer appStore.Close()

	h := internalapi.NewHandler(tc, appStore)

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		_, err := tc.CheckHealth(ctx, &client.CheckHealthRequest{})
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":  "unhealthy",
				"service": "temporal",
				"error":   err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":   "ok",
			"temporal": temporalHost,
		})
	})

	// Job routes — Phase 2
	r.POST("/jobs", h.SubmitJob)
	r.GET("/jobs/:id", h.GetJob)
	r.POST("/jobs/:id/cancel", h.CancelJob)

	log.Printf("API listening on :%s", apiPort)
	if err := r.Run(":" + apiPort); err != nil {
		log.Fatalf("API server failed: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
