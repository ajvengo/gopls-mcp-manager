// Package protocol adapts MCP messages while preserving opaque client fields.
package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
)

// WithRootsCapability makes every upstream ask this bridge for its roots. Some
// MCP clients omit the optional capability; without it gopls installs no file
// watcher and newly-created Go files remain invisible until its daemon restarts.
func WithRootsCapability(params json.RawMessage) json.RawMessage {
	rewritten, _ := rewriteNested(params, "capabilities", func(capabilities map[string]json.RawMessage) bool {
		rootsKey := jsonKey(capabilities, "roots")
		if !absentJSON(capabilities[rootsKey]) {
			return false
		}
		capabilities[rootsKey] = json.RawMessage(`{}`)
		return true
	})
	return rewritten
}

// rewriteNested is params with the object under key replaced by what edit made
// of it, and reports whether that happened. Messages we cannot rewrite — ones
// that do not parse, whose nested value is not an object, that edit leaves
// alone, or that will not marshal back — are returned untouched and left for
// the far side to read as the client sent them. An absent nested object is
// created; every other key and field survives verbatim. The client's own
// spelling of each key is the one rewritten; see jsonKey.
func rewriteNested(params json.RawMessage, key string, edit func(map[string]json.RawMessage) bool) (json.RawMessage, bool) {
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(params, &outer); err != nil || outer == nil {
		return params, false
	}
	nestedKey := jsonKey(outer, key)
	nested := make(map[string]json.RawMessage)
	if raw := outer[nestedKey]; !absentJSON(raw) {
		if err := json.Unmarshal(raw, &nested); err != nil || nested == nil {
			return params, false
		}
	}
	if !edit(nested) {
		return params, false
	}
	rawNested, err := json.Marshal(nested)
	if err != nil {
		return params, false
	}
	outer[nestedKey] = rawNested

	rewritten, err := json.Marshal(outer)
	if err != nil {
		return params, false
	}
	return rewritten, true
}

// jsonKey is the key in object that a Go decoder would read as name: the exact
// spelling when it is there, and otherwise a case-insensitive match, since
// encoding/json falls back to one. Writing our own spelling beside a client's
// would leave two keys mapping to one field, and which of them gopls took would
// come down to the order they marshalled in — so the capability we add for it
// could silently not be the one it read.
func jsonKey(object map[string]json.RawMessage, name string) string {
	if _, exact := object[name]; exact {
		return name
	}
	for key := range object {
		if strings.EqualFold(key, name) {
			return key
		}
	}
	return name
}

// absentJSON reports whether a client left this value out, spelled either way:
// the key missing entirely, or present and null.
func absentJSON(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// WithCompleteResultType is a tools/call result carrying the resultType the SDK
// cannot put there itself — the other default answering without next skips, and
// the one a client on protocol revision 2026-07-28 rejects the call for. See
// SPEC for why unmarshalling is the only door into that field.
//
// Prepended rather than merged into the object: a duplicate key resolves to the
// last one, so an upstream that answered input_required still overrides this —
// without this middleware having to know which values are allowed, or to take
// the object apart to find out.
//
// Delete this once the SDK marks CallToolResult complete-capable, or once this
// middleware answers tools/call through next.
func WithCompleteResultType(result json.RawMessage) json.RawMessage {
	body := bytes.TrimLeft(result, " \t\r\n")
	if len(body) == 0 || body[0] != '{' {
		return result // not an object: nothing to splice into, and nothing we own
	}
	field := []byte(`{"resultType":"complete",`)
	if rest := bytes.TrimLeft(body[1:], " \t\r\n"); len(rest) > 0 && rest[0] == '}' {
		field = []byte(`{"resultType":"complete"`)
	}
	return append(field, body[1:]...)
}

// WithPhysicalPaths replaces only the path arguments whose physical spelling
// differs. Empty scalars and a nil files slice leave those arguments untouched.
func WithPhysicalPaths(params json.RawMessage, file, dir string, files []string) (json.RawMessage, bool) {
	if file == "" && dir == "" && files == nil {
		return params, false
	}
	return rewriteNested(params, "arguments", func(arguments map[string]json.RawMessage) bool {
		if file != "" {
			arguments[jsonKey(arguments, "file")], _ = json.Marshal(file)
		}
		if dir != "" {
			arguments[jsonKey(arguments, "dir")], _ = json.Marshal(dir)
		}
		if files != nil {
			arguments[jsonKey(arguments, "files")], _ = json.Marshal(files)
		}
		return true
	})
}
