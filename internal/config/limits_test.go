package config

import (
	"slices"
	"strings"
	"testing"
)

func TestCallLimitConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "0s", true},
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "2m", true},
		{"GOPLS_MANAGER_EXECUTION_TIMEOUT", "-1s", false},
		{"GOPLS_MANAGER_MAX_OUTSTANDING", "0", false},
		{"GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE", "12", true},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := FromEnv()
			if (err == nil) != tc.valid {
				t.Fatalf("configuration error = %v", err)
			}
		})
	}
}

func TestNewRetentionSettings(t *testing.T) {
	for _, name := range []string{"GOPLS_MANAGER_MAX_LANES", "GOPLS_MANAGER_MAX_CACHE_ENTRIES"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "2")
			limits, err := FromEnv()
			if err != nil || !slices.Contains([]int{limits.Lanes, limits.CacheEntries}, 2) {
				t.Fatalf("limits: %+v, %v", limits, err)
			}
			t.Setenv(name, "0")
			if _, err := FromEnv(); err == nil {
				t.Fatal("zero retention limit accepted")
			}
		})
	}
}

func TestResourceLimitConfiguration(t *testing.T) {
	for _, name := range []string{"GOPLS_MANAGER_MAX_MESSAGE_BYTES", "GOPLS_MANAGER_HTTP_BODY_BUDGET", "GOPLS_MANAGER_SSE_BUFFER_BUDGET", "GOPLS_MANAGER_MAX_SERVERS", "GOPLS_MANAGER_LOG_TRIM_BYTES", "GOPLS_MANAGER_CACHE_TTL"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "0")
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("invalid %s accepted: %v", name, err)
			}
		})
	}
	for _, name := range []string{"GOPLS_MANAGER_HTTP_BODY_BUDGET", "GOPLS_MANAGER_SSE_BUFFER_BUDGET"} {
		t.Run(name+" too small", func(t *testing.T) {
			t.Setenv("GOPLS_MANAGER_MAX_MESSAGE_BYTES", "100")
			t.Setenv(name, "99")
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("small %s accepted: %v", name, err)
			}
		})
	}
}
