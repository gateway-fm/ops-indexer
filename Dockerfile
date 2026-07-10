FROM golang:1.26-alpine AS builder

# Upgrade system packages
RUN apk update --no-cache && apk upgrade --no-cache

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /indexer ./cmd/indexer

FROM gcr.io/distroless/static-debian13:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /indexer /indexer
COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md /licenses/

USER nonroot:nonroot

ENTRYPOINT ["/indexer"]
