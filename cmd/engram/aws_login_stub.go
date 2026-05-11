//go:build !pgstore

package main

import (
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/store"
)

func cmdAWSLogin(_ store.Config) {
	fmt.Fprintln(os.Stderr, "engram: 'aws-login' command requires the pgstore build tag.")
	fmt.Fprintln(os.Stderr, "  Rebuild with: go build -tags pgstore ./cmd/engram/")
	exitFunc(1)
}
