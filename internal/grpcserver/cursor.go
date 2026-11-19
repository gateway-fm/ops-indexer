package grpcserver

// V1 continuations are implemented in v1_paging.go. They bind the complete
// unique sort key to the canonical query and selected dataset. The previous
// unbound base64 position format cannot prove which query it belongs to and is
// rejected: clients restart that query without a cursor after upgrading.
