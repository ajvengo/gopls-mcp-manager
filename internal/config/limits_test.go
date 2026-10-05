package config

import (
	"strings"
	"testing"
	"time"
)

func TestFromEnvRejectsInvalidSettings(t *testing.T) {
	for _, name := range []string{
		"GOPLS_MANAGER_MAX_OUTSTANDING", "GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE",
		"GOPLS_MANAGER_MAX_LANES", "GOPLS_MANAGER_MAX_CACHE_ENTRIES",
		"GOPLS_MANAGER_MAX_MESSAGE_BYTES", "GOPLS_MANAGER_HTTP_BODY_BUDGET",
		"GOPLS_MANAGER_SSE_BUFFER_BUDGET", "GOPLS_MANAGER_MAX_SERVERS",
		"GOPLS_MANAGER_LOG_TRIM_BYTES", "GOPLS_MANAGER_CACHE_TTL",
		"GOPLS_MANAGER_MAX_OPEN_FILES",
	} {
		for _, value := range []string{"0", "-1", "invalid"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				t.Setenv(name, value)
				if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("invalid %s=%s accepted: %v", name, value, err)
				}
			})
		}
	}
	for _, value := range []string{"-1s", "invalid"} {
		t.Run("execution/"+value, func(t *testing.T) {
			t.Setenv("GOPLS_MANAGER_EXECUTION_TIMEOUT", value)
			if _, err := FromEnv(); err == nil {
				t.Fatal("invalid execution timeout accepted")
			}
		})
	}
	for _, name := range []string{"GOPLS_MANAGER_HTTP_BODY_BUDGET", "GOPLS_MANAGER_SSE_BUFFER_BUDGET"} {
		t.Run(name+"/smaller than message", func(t *testing.T) {
			t.Setenv("GOPLS_MANAGER_MAX_MESSAGE_BYTES", "100")
			t.Setenv(name, "99")
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("budget smaller than one message accepted: %v", err)
			}
		})
	}
}

func TestFromEnvOverrides(t *testing.T) {
	t.Setenv("GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE", "12")
	t.Setenv("GOPLS_MANAGER_MAX_LANES", "2")
	t.Setenv("GOPLS_MANAGER_MAX_CACHE_ENTRIES", "3")
	t.Setenv("GOPLS_MANAGER_CACHE_TTL", "1m")
	t.Setenv("GOPLS_MANAGER_EXECUTION_TIMEOUT", "2m")
	t.Setenv("GOPLS_MANAGER_METRICS", "1")
	want := Default()
	want.PerLane, want.Lanes, want.CacheEntries = 12, 2, 3
	want.CacheTTL, want.Execution, want.Metrics = time.Minute, 2*time.Minute, true
	got, err := FromEnv()
	if err != nil || got != want {
		t.Fatalf("overrides = %+v, %v; want %+v", got, err, want)
	}
	t.Setenv("GOPLS_MANAGER_EXECUTION_TIMEOUT", "0s")
	t.Setenv("GOPLS_MANAGER_METRICS", "true")
	want.Execution, want.Metrics = 0, false
	got, err = FromEnv()
	if err != nil || got != want {
		t.Fatalf("disabled timeout and metrics = %+v, %v; want %+v", got, err, want)
	}
}

func TestWithDefaults(t *testing.T) {
	t.Parallel()
	configured := Limits{PerLane: 1, Session: 2, Lanes: 3, CacheEntries: 4,
		MessageBytes: 5, HTTPBytes: 6, SSEBytes: 7, SharedServers: 8, LogBytes: 9, OpenFiles: 10,
		CacheTTL: time.Minute, Execution: time.Second, Metrics: true}
	partial := Default()
	partial.Execution, partial.Metrics = time.Second, true
	for _, tc := range []struct {
		name     string
		in, want Limits
	}{
		{name: "zero", want: Default()},
		{name: "negative", in: Limits{PerLane: -1, CacheTTL: -1}, want: Default()},
		{name: "configured", in: configured, want: configured},
		{name: "partial", in: Limits{Execution: time.Second, Metrics: true}, want: partial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.in.WithDefaults(); got != tc.want {
				t.Fatalf("WithDefaults(%+v) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}
