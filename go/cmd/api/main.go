package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/anuj/temporal-workflows-lab/internal/temporalclient"
	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"
)

func main() {
	temporalHost := envOrDefault("TEMPORAL_HOST", "localhost:7233")
	apiPort := envOrDefault("API_PORT", "8081")

	tc, err := temporalclient.Dial(temporalHost)
	if err != nil {
		log.Fatalf("failed to connect to Temporal: %v", err)
	}
	defer tc.Close()

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		// Verify Temporal is still reachable.
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
