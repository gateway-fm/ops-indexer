FROM golang:1.25-alpine AS builder

# Upgrade system packages
RUN apk update --no-cache && apk upgrade --no-cache

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /indexer ./cmd/indexer

FROM gcr.io/distroless/static-debian12:nonroot@sha256:a9329520abc449e3b14d5bc3a6ffae065bdde0f02667fa10880c49b35c109fd1

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /indexer /indexer

USER nonroot:nonroot

ENTRYPOINT ["/indexer"]
