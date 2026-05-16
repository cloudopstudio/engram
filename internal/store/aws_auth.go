package store

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
)

// awsTokenLifetime is the lifetime of an RDS IAM auth token. AWS hard-codes
// this to 15 minutes; we cache slightly less to keep refresh ahead of expiry.
const awsTokenLifetime = 15 * time.Minute

// AWSTokenProvider issues short-lived RDS IAM authentication tokens using any
// AWS credential source supported by the SDK (SSO session, environment vars,
// EC2 instance metadata, ECS task role, etc.). Thread-safe.
type AWSTokenProvider struct {
	cfg      aws.Config
	region   string
	endpoint string // host:port — required by BuildAuthToken
	dbUser   string // PostgreSQL user mapped to an IAM identity (rds_iam role)
	profile  string // AWS shared-config profile name (for error messages)

	mu        sync.RWMutex
	token     string
	expiresOn time.Time
	identity  string
}

// NewAWSTokenProvider builds a TokenSource that issues RDS IAM auth tokens for
// the connection described by connStr. It validates the SSO session via STS
// GetCallerIdentity at construction time so that engram can fail fast and
// guide the user to `aws sso login` if the session is expired.
//
// Region resolution order: awsRegion arg → AWS_REGION env → ~/.aws/config.
// Profile resolution order: awsProfile arg → AWS_PROFILE env → "default".
func NewAWSTokenProvider(ctx context.Context, connStr, awsRegion, awsProfile string) (*AWSTokenProvider, error) {
	user, host, port, err := parsePGEndpoint(connStr)
	if err != nil {
		return nil, fmt.Errorf("aws-iam: %w", err)
	}
	if user == "" {
		return nil, fmt.Errorf("aws-iam: connection string must include the PostgreSQL user (postgres://USER@host:5432/db). The user must be mapped to an IAM identity via 'GRANT rds_iam' on RDS.")
	}
	if host == "" {
		return nil, fmt.Errorf("aws-iam: connection string is missing the host")
	}
	if port == 0 {
		port = 5432
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if awsRegion != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(awsRegion))
	}
	if awsProfile != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(awsProfile))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws-iam: load AWS config: %w\n  Run: aws sso login%s", err, awsLoginHint(awsProfile))
	}
	if awsCfg.Region == "" {
		return nil, fmt.Errorf("aws-iam: AWS region is not configured.\n  Run: engram config set aws-region <region>%s\n  Or: export AWS_REGION=<region>", profileFlag(awsProfile))
	}

	tp := &AWSTokenProvider{
		cfg:      awsCfg,
		region:   awsCfg.Region,
		endpoint: net.JoinHostPort(host, strconv.Itoa(port)),
		dbUser:   user,
		profile:  awsProfile,
	}

	// Resolve the caller identity now so the engram.identity GUC has a value
	// from the first connection. This also surfaces an expired SSO session
	// early instead of silently degrading later.
	stsClient := sts.NewFromConfig(awsCfg)
	callerOut, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("aws-iam: get caller identity: %w\n  SSO session may be expired. Run: aws sso login%s", err, awsLoginHint(awsProfile))
	}
	tp.identity = extractAWSIdentity(awsCallerARN(callerOut))

	return tp, nil
}

// Token returns a valid RDS IAM auth token. Tokens are cached and renewed
// before expiry. Concurrent callers share a single refresh.
func (tp *AWSTokenProvider) Token(ctx context.Context) (string, error) {
	tp.mu.RLock()
	if tp.token != "" && time.Until(tp.expiresOn) > tokenRefreshBuffer {
		t := tp.token
		tp.mu.RUnlock()
		return t, nil
	}
	tp.mu.RUnlock()

	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.token != "" && time.Until(tp.expiresOn) > tokenRefreshBuffer {
		return tp.token, nil
	}

	token, err := auth.BuildAuthToken(ctx, tp.endpoint, tp.region, tp.dbUser, tp.cfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("aws-iam: build auth token: %w\n  SSO session may be expired. Run: aws sso login%s", err, awsLoginHint(tp.profile))
	}

	tp.token = token
	tp.expiresOn = time.Now().Add(awsTokenLifetime)
	return token, nil
}

// Identity returns the human identifier extracted from the AWS caller ARN.
// For SSO callers this is the email/UPN; for IAM users it is the username.
func (tp *AWSTokenProvider) Identity() string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.identity
}

// resolveAWSAuth reads aws-region and aws-profile with the standard precedence:
//
//	env var > profile config > root config > ""
//
// It returns whatever was found, possibly empty — NewAWSTokenProvider will
// surface a clear error if a required value is missing.
func resolveAWSAuth(dataDir, profile string) (region, awsProfile string) {
	region = os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	awsProfile = os.Getenv("AWS_PROFILE")

	if dataDir != "" {
		if region == "" {
			if v, err := config.GetWithProfile(dataDir, profile, "aws-region"); err == nil {
				region = v
			}
		}
		if awsProfile == "" {
			if v, err := config.GetWithProfile(dataDir, profile, "aws-profile"); err == nil {
				awsProfile = v
			}
		}
	}
	return region, awsProfile
}

// parsePGEndpoint extracts the user, host, and port from a PostgreSQL
// connection string. Supports both URL form and key=value form via pgx.
func parsePGEndpoint(connStr string) (user, host string, port int, err error) {
	cfg, perr := pgx.ParseConfig(connStr)
	if perr != nil {
		return "", "", 0, fmt.Errorf("parse pg connection string: %w", perr)
	}
	return cfg.User, cfg.Host, int(cfg.Port), nil
}

// awsCallerARN safely dereferences the ARN pointer from STS output.
func awsCallerARN(out *sts.GetCallerIdentityOutput) string {
	if out == nil || out.Arn == nil {
		return ""
	}
	return *out.Arn
}

// extractAWSIdentity pulls the user identifier from an AWS caller ARN.
//
// Examples:
//
//	arn:aws:sts::123:assumed-role/AWSReservedSSO_DevOps_xxx/user@femsa.com
//	  → "user@femsa.com" (SSO email — what we want for engram.identity)
//	arn:aws:iam::123:user/jdoe
//	  → "jdoe"
//	arn:aws:sts::123:assumed-role/EC2Role/i-01234
//	  → "i-01234" (instance id — best we can do for non-human callers)
func extractAWSIdentity(arn string) string {
	if arn == "" {
		return ""
	}
	if i := strings.LastIndex(arn, "/"); i >= 0 && i < len(arn)-1 {
		return arn[i+1:]
	}
	return arn
}

// awsLoginHint formats " --profile <name>" or "" for help messages, mirroring
// profileFlag but with explicit AWS framing.
func awsLoginHint(awsProfile string) string {
	if awsProfile == "" {
		return ""
	}
	return " --profile " + awsProfile
}

// ─── Exported helpers for CLI commands ───────────────────────────────────────

// ResolveAWSAuthExported exposes resolveAWSAuth for cmd/engram callers.
func ResolveAWSAuthExported(dataDir, profile string) (region, awsProfile string) {
	return resolveAWSAuth(dataDir, profile)
}
