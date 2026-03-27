//go:build !pgstore

package main

import (
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/store"
)

func cmdMigrate(_ store.Config) {
	fmt.Fprintln(os.Stderr, "engram: 'migrate' command requires the pgstore build variant.")
	fmt.Fprintln(os.Stderr, "  Rebuild with: go build -tags pgstore ./cmd/engram/")
	exitFunc(1)
}
