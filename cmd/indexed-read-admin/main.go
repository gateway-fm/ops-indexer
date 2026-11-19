// indexed-read-admin performs offline reader provisioning using indexer-owned
// descriptors. It is never exposed by the serving gRPC service.
package main

import (
	"context"
	"fmt"
	"github.com/gateway-fm/chain-indexer/internal/indexedread"
	"os"
	"time"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--schema-digest" {
		fmt.Println(indexedread.SchemaDigestString())
		return
	}
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "invalid indexed-read-admin arguments")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if indexedread.ProvisionReader(ctx, os.Getenv("DATABASE_URL"), os.Getenv("INDEXED_READ_DSN")) != nil {
		fmt.Fprintln(os.Stderr, "indexed reader provisioning failed")
		os.Exit(1)
	}
	fmt.Println("indexed reader provisioned")
}
