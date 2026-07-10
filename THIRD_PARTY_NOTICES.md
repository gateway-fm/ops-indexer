# Third-Party Notices

This is a starter notice file for a source-only Apache-2.0 publication. It is
not a complete SBOM and must be regenerated before official binary or container
artifacts are published.

## Source Scope

The repository contains Go source code, protocol definitions, generated Go
stubs, SQL migrations, GitHub Actions workflows, and Docker build materials.

## Notable Go Dependencies

- `github.com/jackc/pgx/v5`
  - License: MIT.
  - Use in this repository: PostgreSQL driver and connection handling.

- `github.com/jackc/tern/v2`
  - License: MIT.
  - Use in this repository: database migration runner.

- `github.com/spf13/viper`
  - License: MIT.
  - Use in this repository: environment and config loading.

- `google.golang.org/grpc`
  - License: Apache-2.0.
  - Use in this repository: gRPC server and client types.

- `google.golang.org/protobuf`
  - License: BSD-3-Clause.
  - Use in this repository: protobuf runtime for generated API stubs.

- `github.com/testcontainers/testcontainers-go`
  - License: MIT.
  - Use in this repository: integration tests.

- Docker and Moby-related modules pulled through testcontainers
  - Licenses vary across Apache-2.0, MIT, BSD-family, and related permissive
    licenses.
  - Use in this repository: test-only container orchestration.

## Container Base Images

The Dockerfile currently builds with `golang:1.26-alpine` and runs on
`gcr.io/distroless/static-debian13:nonroot`. Container distribution should
include scanner-backed SBOMs, base-image license notices, and source-to-image
mapping.

## Required Follow-Up

- Generate a complete dependency license report for Go modules, generated code,
  GitHub Actions, and container base images.
- Replace this starter file with a scanner-backed notice file before the first
  official release artifact.
