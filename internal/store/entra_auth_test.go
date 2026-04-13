//go:build pgstore

package store

import (
	"strings"
	"testing"
	"time"
)

func TestValidateCachedToken_NoCache(t *testing.T) {
	dir := t.TempDir() // empty dir — no token-cache.json

	err := ValidateCachedToken(dir, "")
	if err == nil {
		t.Fatal("expected error when no cached token exists")
	}
	if !strings.Contains(err.Error(), "no cached Azure token") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if !strings.Contains(err.Error(), "engram login") {
		t.Fatalf("error should mention 'engram login': %v", err)
	}
	// Without profile, should NOT contain --profile
	if strings.Contains(err.Error(), "--profile") {
		t.Fatalf("error should not contain --profile when profile is empty: %v", err)
	}
}

func TestValidateCachedToken_NoCacheWithProfile(t *testing.T) {
	dir := t.TempDir()

	err := ValidateCachedToken(dir, "arquitectura")
	if err == nil {
		t.Fatal("expected error when no cached token exists")
	}
	if !strings.Contains(err.Error(), "--profile arquitectura") {
		t.Fatalf("error should contain '--profile arquitectura': %v", err)
	}
}

func TestValidateCachedToken_ExpiredToken(t *testing.T) {
	dir := t.TempDir()

	// Write a token that expired 10 minutes ago — no refresh token.
	expired := time.Now().Add(-10 * time.Minute)
	if err := saveCachedToken(dir, "expired-access-token", "", "", "", expired); err != nil {
		t.Fatalf("saveCachedToken: %v", err)
	}

	err := ValidateCachedToken(dir, "")
	if err == nil {
		t.Fatal("expected error for expired token without refresh token")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Fatalf("error should mention 'expired': %v", err)
	}
	if !strings.Contains(err.Error(), "engram login") {
		t.Fatalf("error should mention 'engram login': %v", err)
	}
}

func TestValidateCachedToken_ExpiredTokenWithRefresh(t *testing.T) {
	dir := t.TempDir()

	// Write a token that expired 10 minutes ago — WITH a refresh token.
	expired := time.Now().Add(-10 * time.Minute)
	if err := saveCachedToken(dir, "expired-access-token", "refresh-token-123", "tenant-id", "client-id", expired); err != nil {
		t.Fatalf("saveCachedToken: %v", err)
	}

	// Should be considered valid because it has a refresh token.
	if err := ValidateCachedToken(dir, ""); err != nil {
		t.Fatalf("expected nil for expired token with refresh token, got: %v", err)
	}
}

func TestValidateCachedToken_ExpiringWithinBuffer(t *testing.T) {
	dir := t.TempDir()

	// Token expires in 3 minutes — within the 5-minute buffer, no refresh token.
	almostExpired := time.Now().Add(3 * time.Minute)
	if err := saveCachedToken(dir, "almost-expired-token", "", "", "", almostExpired); err != nil {
		t.Fatalf("saveCachedToken: %v", err)
	}

	err := ValidateCachedToken(dir, "prod")
	if err == nil {
		t.Fatal("expected error for token expiring within buffer without refresh token")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Fatalf("error should mention 'expired': %v", err)
	}
	if !strings.Contains(err.Error(), "--profile prod") {
		t.Fatalf("error should contain '--profile prod': %v", err)
	}
}

func TestValidateCachedToken_ValidToken(t *testing.T) {
	dir := t.TempDir()

	// Token valid for 1 hour.
	validUntil := time.Now().Add(1 * time.Hour)
	if err := saveCachedToken(dir, "valid-access-token", "", "", "", validUntil); err != nil {
		t.Fatalf("saveCachedToken: %v", err)
	}

	if err := ValidateCachedToken(dir, ""); err != nil {
		t.Fatalf("expected nil for valid token, got: %v", err)
	}
}

func TestValidateCachedToken_ValidTokenWithProfile(t *testing.T) {
	dir := t.TempDir()

	validUntil := time.Now().Add(1 * time.Hour)
	if err := saveCachedToken(dir, "valid-access-token", "", "", "", validUntil); err != nil {
		t.Fatalf("saveCachedToken: %v", err)
	}

	// Profile doesn't matter for a valid token — should return nil.
	if err := ValidateCachedToken(dir, "arquitectura"); err != nil {
		t.Fatalf("expected nil for valid token, got: %v", err)
	}
}

func TestProfileFlag(t *testing.T) {
	if got := profileFlag(""); got != "" {
		t.Fatalf("profileFlag(\"\") = %q, want empty", got)
	}
	if got := profileFlag("dev"); got != " --profile dev" {
		t.Fatalf("profileFlag(\"dev\") = %q, want \" --profile dev\"", got)
	}
}
