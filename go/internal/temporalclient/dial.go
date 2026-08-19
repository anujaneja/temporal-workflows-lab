package temporalclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

// Dial connects to the Temporal server at hostPort, retrying until the server
// is reachable or the retry budget is exhausted.
//
// This is necessary because docker compose's healthcheck marks Temporal as
// healthy when the process is up, but the gRPC service may still be
// initialising for a few seconds after that.
func Dial(hostPort string) (client.Client, error) {
	const (
		maxAttempts = 20
		baseDelay   = 2 * time.Second
		maxDelay    = 20 * time.Second
	)

	var (
		tc  client.Client
		err error
	)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		tc, err = client.Dial(client.Options{HostPort: hostPort})
		if err != nil {
			log.Printf("[attempt %d/%d] client.Dial failed: %v — retrying in %s",
				attempt, maxAttempts, err, baseDelay)
			time.Sleep(min(baseDelay*time.Duration(attempt), maxDelay))
			continue
		}

		// client.Dial is lazy — verify the server is actually reachable.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, pingErr := tc.CheckHealth(ctx, &client.CheckHealthRequest{})
		cancel()

		if pingErr != nil {
			tc.Close()
			log.Printf("[attempt %d/%d] health check failed: %v — retrying in %s",
				attempt, maxAttempts, pingErr, baseDelay)
			time.Sleep(min(baseDelay*time.Duration(attempt), maxDelay))
			continue
		}

		log.Printf("connected to Temporal at %s (attempt %d)", hostPort, attempt)
		return tc, nil
	}

	return nil, fmt.Errorf("could not connect to Temporal at %s after %d attempts: %w",
		hostPort, maxAttempts, err)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
