# ─── builder ────────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api     ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/worker  ./cmd/worker

# ─── api ────────────────────────────────────────────────────────────────────
FROM scratch AS api
COPY --from=builder /out/api /api
EXPOSE 8081
ENTRYPOINT ["/api"]

# ─── worker ─────────────────────────────────────────────────────────────────
FROM scratch AS worker
COPY --from=builder /out/worker /worker
ENTRYPOINT ["/worker"]
