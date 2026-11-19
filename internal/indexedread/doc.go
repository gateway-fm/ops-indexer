// Package indexedread provides fixed, bounded reads of the existing chain
// indexer service. It owns a dedicated SELECT connection, not policy or user storage.
//
// An operator provisions the existing indexer schema and a separate login role.
// The role needs CONNECT, public schema USAGE, and SELECT on the columns in
// schema-columns.json marked select:true. It must not own the database or any
// application object, inherit another role, write application data, create in
// application schemas, grant its privileges onward, or access other application
// relations/functions/sequences. In particular, contracts.abi is excluded;
// approved policy supplies ABI. The reader never grants or migrates anything.
//
// Default PUBLIC TEMP privilege may remain: this adapter never creates temporary
// objects and always uses fully qualified fixed SQL in read-only transactions.
// This is a persistent application authority limit, not a claim that PostgreSQL
// implements a universal sandbox for a stolen database credential. The indexer
// writer, database owner and selected deployment remain operational trust
// dependencies. Connection metadata does not attest chain identity, canonical
// rows, restore history or completeness. Purpose consumers must qualify those
// facts and their current authority independently.
//
// The supported catalogs have either INT or, following indexer migration009,
// BIGINT for all five address_stats and tokens counters listed in the descriptor.
// Mixed counter widths, other counter types and unrelated schema changes are
// rejected. Counters retain their lossless decimal-string projections.
//
// Each snapshot locks the fixed relations through a zero-row read, checks the
// effective role and exact supported catalog, and completes a read-only
// repeatable-read transaction before returning usable data. Unknown completion
// is unavailable, without automatic retry. Fixed purposes share their original
// deadline and query/row/payload budgets with rechecks, bound variable fields in
// SQL before transport and charge owned copies before decoding. No public
// arbitrary-query, transaction, connection or callback API is provided.
package indexedread
