// Package config defines resource policy shared by the manager and transports.
package config

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits is the resource policy for a manager and its routers.
type Limits struct {
	PerLane, Session                  int
	Lanes, CacheEntries               int
	Execution                         time.Duration
	CacheTTL                          time.Duration
	MessageBytes, HTTPBytes, SSEBytes int
	// HTTPConnections bounds admitted client sockets before HTTP header parsing.
	HTTPConnections int
	// OwnerSessions bounds stdio sessions and retained HTTP backends together.
	// OwnerOperations bounds queued and unconfirmed upstream work globally.
	OwnerSessions, OwnerOperations int
	OwnerLogBytes                  int
	OwnerRetainedBytes             int
	Idle                           time.Duration
	SharedServers                  int
	LogBytes                       int
	// OpenFiles bounds owner, guardian and watched-tree descriptor reservations.
	OpenFiles int
	// Metrics is GOPLS_MANAGER_METRICS, kept here so the one env var that is
	// not a limit is still read in the one place that reads the environment.
	Metrics bool
	// DirectoryFilters is GOPLS_MANAGER_DIRECTORY_FILTERS: gopls
	// directoryFilters rules, applied after each worktree's go.mod ignore
	// directives to the roots a gopls is told to watch. Nil means none.
	DirectoryFilters []string
}

// Default returns an independent value containing the shipped limits.
func Default() Limits {
	return Limits{PerLane: 128, Session: 1024, Lanes: 64, CacheEntries: 4096, CacheTTL: 5 * time.Minute,
		MessageBytes: 4 << 20, HTTPBytes: 64 << 20, SSEBytes: 64 << 20, SharedServers: 64, LogBytes: 64 << 20,
		OpenFiles: 30000, HTTPConnections: 128, OwnerSessions: 16, OwnerOperations: 1024, OwnerLogBytes: 64 << 20, OwnerRetainedBytes: 64 << 20, Idle: time.Hour}
}

// FromEnv applies and validates GOPLS_MANAGER_* overrides to Default.
func FromEnv() (Limits, error) {
	limits := Default()
	for _, setting := range []struct {
		name   string
		target *int
	}{
		{"GOPLS_MANAGER_MAX_OUTSTANDING", &limits.Session},
		{"GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE", &limits.PerLane},
		{"GOPLS_MANAGER_MAX_LANES", &limits.Lanes},
		{"GOPLS_MANAGER_MAX_CACHE_ENTRIES", &limits.CacheEntries},
		{"GOPLS_MANAGER_MAX_MESSAGE_BYTES", &limits.MessageBytes},
		{"GOPLS_MANAGER_HTTP_BODY_BUDGET", &limits.HTTPBytes},
		{"GOPLS_MANAGER_MAX_HTTP_CONNECTIONS", &limits.HTTPConnections},
		{"GOPLS_MANAGER_MAX_OWNER_SESSIONS", &limits.OwnerSessions},
		{"GOPLS_MANAGER_MAX_OWNER_OPERATIONS", &limits.OwnerOperations},
		{"GOPLS_MANAGER_MAX_LOG_BYTES", &limits.OwnerLogBytes},
		{"GOPLS_MANAGER_RETAINED_MESSAGE_BUDGET", &limits.OwnerRetainedBytes},
		{"GOPLS_MANAGER_SSE_BUFFER_BUDGET", &limits.SSEBytes},
		{"GOPLS_MANAGER_MAX_SERVERS", &limits.SharedServers},
		{"GOPLS_MANAGER_LOG_TRIM_BYTES", &limits.LogBytes},
		{"GOPLS_MANAGER_MAX_OPEN_FILES", &limits.OpenFiles},
	} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				return limits, fmt.Errorf("%s must be a positive integer", setting.name)
			}
			*setting.target = value
		}
	}
	if raw := os.Getenv("GOPLS_MANAGER_EXECUTION_TIMEOUT"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value < 0 {
			return limits, fmt.Errorf("GOPLS_MANAGER_EXECUTION_TIMEOUT must be a nonnegative duration")
		}
		limits.Execution = value
	}
	if raw := os.Getenv("GOPLS_MANAGER_CACHE_TTL"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			return limits, fmt.Errorf("GOPLS_MANAGER_CACHE_TTL must be a positive duration")
		}
		limits.CacheTTL = value
	}
	if raw := os.Getenv("GOPLS_MANAGER_IDLE_TIMEOUT"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			return limits, fmt.Errorf("GOPLS_MANAGER_IDLE_TIMEOUT must be a positive duration")
		}
		limits.Idle = value
	}
	if limits.HTTPBytes < limits.MessageBytes {
		return limits, fmt.Errorf("GOPLS_MANAGER_HTTP_BODY_BUDGET must hold at least one maximum-size message")
	}
	if limits.SSEBytes < limits.MessageBytes {
		return limits, fmt.Errorf("GOPLS_MANAGER_SSE_BUFFER_BUDGET must hold at least one maximum-size message")
	}
	if limits.OwnerRetainedBytes < limits.MessageBytes {
		return limits, fmt.Errorf("GOPLS_MANAGER_RETAINED_MESSAGE_BUDGET must hold at least one maximum-size message")
	}
	limits.Metrics = os.Getenv("GOPLS_MANAGER_METRICS") == "1"
	filters, err := ParseDirectoryFilters(os.Getenv("GOPLS_MANAGER_DIRECTORY_FILTERS"))
	if err != nil {
		return limits, fmt.Errorf("GOPLS_MANAGER_DIRECTORY_FILTERS: %w", err)
	}
	limits.DirectoryFilters = filters
	return limits, nil
}

// MaxDirectoryFilterBytes bounds one rule before regexp compilation.
const MaxDirectoryFilterBytes = 4096

// ParseDirectoryFilters splits a comma-separated list of gopls directoryFilters
// rules and validates each the way gopls does: a leading + or -, and no glob
// operator but a whole "**" segment. Rules come back slash-cleaned, so
// "-docs/" and "-docs" are the same rule. Blank entries are skipped.
func ParseDirectoryFilters(raw string) ([]string, error) {
	if !utf8.ValidString(raw) {
		return nil, fmt.Errorf("directory filters must be valid UTF-8")
	}
	var filters []string
	for _, field := range strings.Split(raw, ",") {
		filter := strings.TrimSpace(field)
		if filter == "" {
			continue
		}
		if len(filter) > MaxDirectoryFilterBytes {
			return nil, fmt.Errorf("directory filter exceeds %d bytes", MaxDirectoryFilterBytes)
		}
		if filter[0] != '+' && filter[0] != '-' {
			return nil, fmt.Errorf("invalid filter %q: must start with + or -", filter)
		}
		for _, seg := range strings.Split(filter[1:], "/") {
			if seg != "**" && strings.ContainsAny(seg, "*?") {
				return nil, fmt.Errorf("invalid filter %q: only whole ** segments are supported", filter)
			}
		}
		filters = append(filters, filter[:1]+strings.TrimPrefix(path.Clean("/"+filter[1:]), "/"))
	}
	return filters, nil
}
