//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobyClient "github.com/moby/moby/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// apiBaseURL is set in BeforeSuite and read by all specs.
var apiBaseURL string

// allContainers holds every container started in BeforeSuite so AfterSuite can
// terminate them in reverse order.
var allContainers []testcontainers.Container

// testNet is the shared bridge network removed in AfterSuite.
var testNet *testcontainers.DockerNetwork

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func() {
	ctx := context.Background()

	// Resolve file-system paths relative to this source file so the suite works
	// regardless of the working directory the test binary is run from.
	_, thisFile, _, _ := runtime.Caller(0)
	// thisFile = …/go/tests/integration/suite_test.go
	goDir := filepath.Join(filepath.Dir(thisFile), "../..")          // …/go
	projectRoot := filepath.Join(goDir, "..")                        // repo root
	migrationsDir := filepath.Join(projectRoot, "db", "migrations")  // …/db/migrations

	// ── 1. Isolated Docker network ────────────────────────────────────────────
	var err error
	testNet, err = network.New(ctx)
	Expect(err).NotTo(HaveOccurred(), "create test network")
	netName := testNet.Name

	// ── 2. Temporal's internal PostgreSQL ────────────────────────────────────
	// Aliased as "temporal-postgres" so the Temporal server can resolve it.
	temporalPG, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16-alpine",
			Env: map[string]string{
				"POSTGRES_USER":     "temporal",
				"POSTGRES_PASSWORD": "temporal",
				"POSTGRES_DB":       "temporal",
			},
			Networks: []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"temporal-postgres"},
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start temporal-postgres")
	allContainers = append(allContainers, temporalPG)

	// ── 3. Application PostgreSQL ─────────────────────────────────────────────
	// Aliased as "app-postgres" so Flyway, API, and Worker can resolve it.
	appPG, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16-alpine",
			Env: map[string]string{
				"POSTGRES_USER":     "lab",
				"POSTGRES_PASSWORD": "lab",
				"POSTGRES_DB":       "lab",
			},
			Networks: []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"app-postgres"},
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start app-postgres")
	allContainers = append(allContainers, appPG)

	// ── 4. Temporal server ────────────────────────────────────────────────────
	// Must start after temporal-postgres is healthy. We wait for port 7233 to
	// accept connections — a reliable signal that the gRPC server is up.
	temporalC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "temporalio/auto-setup:1.25.2",
			Env: map[string]string{
				"DB":             "postgres12",
				"DB_PORT":        "5432",
				"POSTGRES_USER":  "temporal",
				"POSTGRES_PWD":   "temporal",
				"POSTGRES_SEEDS": "temporal-postgres",
			},
			Networks: []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"temporal"},
			},
			ExposedPorts: []string{"7233/tcp"},
			WaitingFor: wait.ForListeningPort("7233/tcp").
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start temporal")
	allContainers = append(allContainers, temporalC)

	// ── 5. Flyway migrations ──────────────────────────────────────────────────
	// Short-lived: runs once and exits 0. We wait for the container to stop.
	flywayC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "flyway/flyway:10-alpine",
			Cmd:   []string{"migrate"},
			Env: map[string]string{
				"FLYWAY_URL":             "jdbc:postgresql://app-postgres:5432/lab",
				"FLYWAY_USER":            "lab",
				"FLYWAY_PASSWORD":        "lab",
				"FLYWAY_CONNECT_RETRIES": "10",
			},
			Networks: []string{netName},
			HostConfigModifier: func(hc *mobycontainer.HostConfig) {
				hc.Binds = append(hc.Binds, migrationsDir+":/flyway/sql:ro")
			},
			WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start flyway")
	allContainers = append(allContainers, flywayC)

	// ── 6. Worker ─────────────────────────────────────────────────────────────
	// Built from go/Dockerfile (target: worker). Depends on Temporal and the
	// migrated app-postgres. We wait for the startup log line.
	workerC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    goDir,
				Dockerfile: "Dockerfile",
				KeepImage:  true, // reuse the built image between runs
				BuildOptionsModifier: func(opts *mobyClient.ImageBuildOptions) {
					opts.Target = "worker"
				},
			},
			Env: map[string]string{
				"TEMPORAL_HOST": "temporal:7233",
				"APP_DB_DSN":    "postgres://lab:lab@app-postgres:5432/lab?sslmode=disable",
			},
			Networks: []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"worker"},
			},
			WaitingFor: wait.ForLog("worker starting on task queue").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start worker")
	allContainers = append(allContainers, workerC)

	// ── 7. API server ─────────────────────────────────────────────────────────
	// Built from go/Dockerfile (target: api). We wait for GET /health → 200
	// before declaring the suite ready — this proves the API, Temporal, and DB
	// are all wired up correctly.
	apiC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    goDir,
				Dockerfile: "Dockerfile",
				KeepImage:  true,
				BuildOptionsModifier: func(opts *mobyClient.ImageBuildOptions) {
					opts.Target = "api"
				},
			},
			Env: map[string]string{
				"TEMPORAL_HOST": "temporal:7233",
				"APP_DB_DSN":    "postgres://lab:lab@app-postgres:5432/lab?sslmode=disable",
				"API_PORT":      "8081",
			},
			ExposedPorts: []string{"8081/tcp"},
			Networks:     []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"api"},
			},
			WaitingFor: wait.ForHTTP("/health").
				WithPort("8081/tcp").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred(), "start api")
	allContainers = append(allContainers, apiC)

	// Derive the host-visible URL the test process uses to hit the API.
	host, err := apiC.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := apiC.MappedPort(ctx, "8081/tcp")
	Expect(err).NotTo(HaveOccurred())
	apiBaseURL = fmt.Sprintf("http://%s:%s", host, port.Port())
})

var _ = AfterSuite(func() {
	ctx := context.Background()
	// Terminate containers newest-first so dependents shut down before their
	// dependencies.
	for i := len(allContainers) - 1; i >= 0; i-- {
		_ = allContainers[i].Terminate(ctx)
	}
	if testNet != nil {
		_ = testNet.Remove(ctx)
	}
})
