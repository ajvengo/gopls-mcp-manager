// Package config defines resource policy shared by the manager and transports.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Limits is the resource policy for a manager and its routers.
type Limits struct {
	PerLane, Session                  int
	Lanes, CacheEntries               int
	Execution                         time.Duration
	CacheTTL                          time.Duration
	MessageBytes, HTTPBytes, SSEBytes int
	SharedServers                     int
	LogBytes                          int
	// Metrics is GOPLS_MANAGER_METRICS, kept here so the one env var that is
	// not a limit is still read in the one place that reads the environment.
	Metrics bool
}

// WithDefaults fills every unset field from Default, so that nothing downstream
// has to ask whether the value it was handed is populated: a zero field there
// would otherwise mean a cache of no entries, a message limit of no bytes, or a
// server cap of none.
//
// FromEnv needs none of this — it starts from Default and refuses every
// non-positive override. newRouter is the caller that does: the manager it is
// handed may be one a test built field by field, or no manager at all.
//
// Execution is deliberately absent: zero is its shipped value and means no
// execution deadline at all, so there is nothing to fill in.
func (l Limits) WithDefaults() Limits {
	d := Default()
	for _, field := range []struct{ target, fallback *int }{
		{&l.PerLane, &d.PerLane}, {&l.Session, &d.Session},
		{&l.Lanes, &d.Lanes}, {&l.CacheEntries, &d.CacheEntries},
		{&l.MessageBytes, &d.MessageBytes}, {&l.HTTPBytes, &d.HTTPBytes},
		{&l.SSEBytes, &d.SSEBytes}, {&l.SharedServers, &d.SharedServers},
		{&l.LogBytes, &d.LogBytes},
	} {
		if *field.target <= 0 {
			*field.target = *field.fallback
		}
	}
	if l.CacheTTL <= 0 {
		l.CacheTTL = d.CacheTTL
	}
	return l
}

// Default returns an independent value containing the shipped limits.
func Default() Limits {
	return Limits{PerLane: 128, Session: 1024, Lanes: 64, CacheEntries: 4096, CacheTTL: 5 * time.Minute,
		MessageBytes: 4 << 20, HTTPBytes: 64 << 20, SSEBytes: 64 << 20, SharedServers: 64, LogBytes: 64 << 20}
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
		{"GOPLS_MANAGER_SSE_BUFFER_BUDGET", &limits.SSEBytes},
		{"GOPLS_MANAGER_MAX_SERVERS", &limits.SharedServers},
		{"GOPLS_MANAGER_LOG_TRIM_BYTES", &limits.LogBytes},
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
	if limits.HTTPBytes < limits.MessageBytes {
		return limits, fmt.Errorf("GOPLS_MANAGER_HTTP_BODY_BUDGET must hold at least one maximum-size message")
	}
	if limits.SSEBytes < limits.MessageBytes {
		return limits, fmt.Errorf("GOPLS_MANAGER_SSE_BUFFER_BUDGET must hold at least one maximum-size message")
	}
	limits.Metrics = os.Getenv("GOPLS_MANAGER_METRICS") == "1"
	return limits, nil
}
