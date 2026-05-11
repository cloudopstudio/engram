//go:build pgstore

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/Gentleman-Programming/engram/internal/store"
)

// cmdAWSLogin verifies that the configured AWS SSO session is usable for RDS
// IAM authentication. It does NOT perform the SSO login itself — that's
// handled by `aws sso login`, which writes the session to ~/.aws/sso/cache/.
//
// This command:
//  1. Reads aws-region and aws-profile from config (with profile support).
//  2. Attempts to construct an AWSTokenProvider, which calls STS
//     GetCallerIdentity to validate the session.
//  3. Prints the resolved identity, or guides the user to run `aws sso login`.
//
// Usage:
//
//	engram aws-login                       # uses default profile
//	engram aws-login --profile femsa       # uses named profile
func cmdAWSLogin(cfg store.Config) {
	region, awsProfile := store.ResolveAWSAuthExported(cfg.DataDir, cfg.Profile)

	// Pick a connection string that has at least a user@host:port — needed by
	// AWSTokenProvider for token construction. We try database-url from config
	// (with profile fallback) so we don't ask the user to pass it again.
	connStr := os.Getenv("ENGRAM_DATABASE_URL")
	if connStr == "" {
		v, err := config.GetWithProfile(cfg.DataDir, cfg.Profile, "database-url")
		if err != nil || v == "" {
			fatal(fmt.Errorf("aws-login: database-url is not configured.\n  Run: engram config set database-url postgres://USER@HOST:5432/db [--profile %s]", cfg.Profile))
		}
		connStr = v
	}

	if region == "" {
		fmt.Fprintf(os.Stderr, "engram: aws-region not set — relying on ~/.aws/config\n")
	}

	fmt.Fprintf(os.Stderr, "engram aws-login — verifying AWS SSO session\n")
	if awsProfile != "" {
		fmt.Fprintf(os.Stderr, "  AWS profile: %s\n", awsProfile)
	}
	if region != "" {
		fmt.Fprintf(os.Stderr, "  AWS region:  %s\n", region)
	}

	tp, err := store.NewAWSTokenProvider(context.Background(), connStr, region, awsProfile)
	if err != nil {
		// Offer to launch `aws sso login` for the configured profile when the
		// failure looks like a missing/expired SSO session.
		fmt.Fprintf(os.Stderr, "\nengram: %v\n", err)
		if awsProfile != "" {
			fmt.Fprintf(os.Stderr, "\nAttempting: aws sso login --profile %s\n\n", awsProfile)
			cmd := exec.Command("aws", "sso", "login", "--profile", awsProfile)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
			if runErr := cmd.Run(); runErr != nil {
				fatal(fmt.Errorf("aws sso login failed: %w", runErr))
			}
			// Retry once after the user completes SSO.
			tp, err = store.NewAWSTokenProvider(context.Background(), connStr, region, awsProfile)
			if err != nil {
				fatal(err)
			}
		} else {
			fatal(fmt.Errorf("set aws-profile and retry: engram config set aws-profile <name>"))
		}
	}

	// Force a token build so we know the IAM permission rds-db:connect is
	// in place. This catches misconfigured Permission Sets early.
	if _, err := tp.Token(context.Background()); err != nil {
		fatal(fmt.Errorf("aws-login: token generation failed: %w\n  Verify your IAM Permission Set has rds-db:connect on the DB user resource", err))
	}

	identity := tp.Identity()
	fmt.Fprintf(os.Stderr, "\n  AWS SSO session verified.\n  Authenticated as %s\n  Tokens are 15min, generated on demand from your SSO session.\n\n", identity)
}
