# syntax=docker/dockerfile:1

# Build stage - Use a glibc-based image like Debian Bookworm
FROM golang:1.24-bookworm AS builder
# Install C build tools and librdkafka for the kafka client
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build ingestor as a static binary
RUN CGO_ENABLED=0 go build -o /bin/ingestor ./cmd/ingestor
# Build publisher with CGO enabled for librdkafka
RUN go build -o /bin/publisher ./cmd/publisher
RUN go build -o /bin/dispatcher ./cmd/dispatcher
RUN CGO_ENABLED=0 go build -o /bin/api ./cmd/api

# Final stage for ingestor
FROM alpine:latest AS ingestor
WORKDIR /app
COPY --from=builder /bin/ingestor .
ENTRYPOINT ["./ingestor"]

# Final stage for publisher
FROM debian:bookworm-slim AS publisher
# The publisher binary is dynamically linked against librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka++1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /bin/publisher .
ENTRYPOINT ["./publisher"]

# Final stage for dispatcher
FROM debian:bookworm-slim AS dispatcher
# The dispatcher binary is also dynamically linked against librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka++1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /bin/dispatcher .
ENTRYPOINT ["./dispatcher"]

# Final stage for api
FROM alpine:latest AS api
WORKDIR /app
COPY --from=builder /bin/api .
ENTRYPOINT ["./api"]