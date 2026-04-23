package grpcserver

import (
	"database/sql"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// notFound returns a NotFound status for missing entities.
func notFound(kind, identifier string) error {
	return status.Errorf(codes.NotFound, "%s not found: %s", kind, identifier)
}

// invalidArgument returns InvalidArgument with a short, non-PII message.
func invalidArgument(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// unavailable returns Unavailable for features gated off for this indexer
// config (e.g. OP-Stack endpoints on a non-OP chain).
func unavailable(msg string) error {
	return status.Error(codes.Unavailable, msg)
}

// internalErr wraps a db or IO error as Internal, logging the raw cause but
// not surfacing it to clients. Returns a sanitized gRPC error.
func internalErr(err error, where string) error {
	if err == nil {
		return nil
	}
	// Map common db errors to user-facing codes.
	if errors.Is(err, sql.ErrNoRows) {
		return status.Error(codes.NotFound, "not found")
	}
	// Caller is expected to have logged the original error. Here we return
	// an opaque gRPC error with a short, non-PII location marker.
	return status.Errorf(codes.Internal, "%s: internal error", where)
}

// errEmptyFilter is returned when a List RPC requires at least one filter
// but received none. Translates to InvalidArgument.
var errEmptyFilter = fmt.Errorf("at least one filter must be set")
