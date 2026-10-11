// Package protocol adapts MCP messages while preserving opaque client fields.
package protocol

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
)

// WithRootsCapability makes every upstream ask this bridge for its roots. Some
// MCP clients omit the optional capability; without it gopls installs no file
// watcher and newly-created Go files remain invisible until its daemon restarts.
func WithRootsCapability(params jsontext.Value) jsontext.Value {
	rewritten, _ := rewriteNested(params, "capabilities", func(capabilities map[string]jsontext.Value) bool {
		if !absentJSON(capabilities["roots"]) {
			return false
		}
		capabilities["roots"] = jsontext.Value(`{}`)
		return true
	})
	return rewritten
}

// rewriteNested is params with the object under key replaced by what edit made
// of it, and reports whether that happened. Messages we cannot rewrite — ones
// that do not parse, whose nested value is not an object, that edit leaves
// alone, or that will not marshal back — are returned untouched and left for
// the far side to read as the client sent them. An absent nested object is
// created; every other key and field survives verbatim. Names are case-sensitive.
func rewriteNested(params jsontext.Value, key string, edit func(map[string]jsontext.Value) bool) (jsontext.Value, bool) {
	var outer map[string]jsontext.Value
	if err := json.Unmarshal(params, &outer); err != nil || outer == nil {
		return params, false
	}
	nested := make(map[string]jsontext.Value)
	if raw := outer[key]; !absentJSON(raw) {
		if err := json.Unmarshal(raw, &nested); err != nil || nested == nil {
			return params, false
		}
	}
	if !edit(nested) {
		return params, false
	}
	rawNested, err := json.Marshal(nested, json.Deterministic(true))
	if err != nil {
		return params, false
	}
	outer[key] = rawNested

	rewritten, err := json.Marshal(outer, json.Deterministic(true))
	if err != nil {
		return params, false
	}
	return rewritten, true
}

// absentJSON reports whether a client left this value out, spelled either way:
// the key missing entirely, or present and null.
func absentJSON(raw jsontext.Value) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// WithCompleteResultType is a tools/call result carrying the resultType the SDK
// cannot put there itself — the other default answering without next skips, and
// the one a client on protocol revision 2026-07-28 rejects the call for. See
// SPEC for why unmarshalling is the only door into that field.
//
// Preserve an upstream resultType; strict v2 decoding forbids duplicate keys.
// Only the missing default is spliced, leaving opaque result fields intact.
//
// Delete this once the SDK marks CallToolResult complete-capable, or once this
// middleware answers tools/call through next.
func WithCompleteResultType(result jsontext.Value) jsontext.Value {
	body := bytes.TrimLeft(result, " \t\r\n")
	if len(body) == 0 || body[0] != '{' {
		return result // not an object: nothing to splice into, and nothing we own
	}
	var existing struct {
		ResultType jsontext.Value `json:"resultType"`
	}
	if json.Unmarshal(body, &existing) != nil || len(existing.ResultType) != 0 {
		return result
	}
	field := []byte(`{"resultType":"complete",`)
	if rest := bytes.TrimLeft(body[1:], " \t\r\n"); len(rest) > 0 && rest[0] == '}' {
		field = []byte(`{"resultType":"complete"`)
	}
	return append(field, body[1:]...)
}

// WithPhysicalPaths replaces only the path arguments whose physical spelling
// differs. Empty scalars and a nil files slice leave those arguments untouched.
func WithPhysicalPaths(params jsontext.Value, file, dir string, files []string) (jsontext.Value, bool) {
	if file == "" && dir == "" && files == nil {
		return params, false
	}
	return rewriteNested(params, "arguments", func(arguments map[string]jsontext.Value) bool {
		if file != "" {
			arguments["file"], _ = json.Marshal(file)
		}
		if dir != "" {
			arguments["dir"], _ = json.Marshal(dir)
		}
		if files != nil {
			arguments["files"], _ = json.Marshal(files)
		}
		return true
	})
}
