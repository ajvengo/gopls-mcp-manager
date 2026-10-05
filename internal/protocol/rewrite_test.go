package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRootsCapability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "missing capabilities", in: `{}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "null capabilities", in: `{"capabilities":null}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "preserves peers", in: `{"capabilities":{"sampling":{}}}`, want: `{"capabilities":{"roots":{},"sampling":{}}}`},
		{name: "preserves roots", in: `{"capabilities":{"roots":{"listChanged":true}}}`, want: `{"capabilities":{"roots":{"listChanged":true}}}`},
		{name: "replaces null roots", in: `{"capabilities":{"roots":null}}`, want: `{"capabilities":{"roots":{}}}`},
		{name: "invalid parameters", in: `[`, want: `[`},
		{name: "invalid capabilities", in: `{"capabilities":[]}`, want: `{"capabilities":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := string(WithRootsCapability(json.RawMessage(test.in)))
			assert.Equalf(t, test.want, got, "WithRootsCapability(%s) = %s, want %s", test.in, got, test.want)
		})
	}
}

func TestWithPhysicalPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, params, file, dir, want string
		files                         []string
		changed                       bool
	}{
		{name: "untouched", params: `{"arguments":{"file":"/alias"}}`, want: `{"arguments":{"file":"/alias"}}`},
		{name: "file and opaque fields", params: `{"name":"tool","arguments":{"file":"/alias","query":"x"}}`, file: "/physical", want: `{"arguments":{"file":"/physical","query":"x"},"name":"tool"}`, changed: true},
		{name: "directory", params: `{"arguments":{"dir":"/alias"}}`, dir: "/physical", want: `{"arguments":{"dir":"/physical"}}`, changed: true},
		{name: "files", params: `{"arguments":{"files":["/alias","relative"]}}`, files: []string{"/physical", "relative"}, want: `{"arguments":{"files":["/physical","relative"]}}`, changed: true},
		{name: "client key spelling", params: `{"ARGUMENTS":{"FILE":"/alias"}}`, file: "/physical", want: `{"ARGUMENTS":{"FILE":"/physical"}}`, changed: true},
		{name: "missing arguments", params: `{}`, file: "/physical", want: `{"arguments":{"file":"/physical"}}`, changed: true},
		{name: "invalid arguments", params: `{"arguments":[]}`, file: "/physical", want: `{"arguments":[]}`},
		{name: "invalid outer", params: `not json`, file: "/physical", want: `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, changed := WithPhysicalPaths(json.RawMessage(tc.params), tc.file, tc.dir, tc.files)
			require.Truef(t, string(got) == tc.want && changed == tc.changed, "rewritten = %s, %v; want %s, %v", got, changed, tc.want, tc.changed)
		})
	}
}

func TestRewriteNestedRejectsInvalidEdit(t *testing.T) {
	t.Parallel()
	params := json.RawMessage(`{}`)
	got, changed := rewriteNested(params, "arguments", func(object map[string]json.RawMessage) bool {
		object["bad"] = json.RawMessage(`{`)
		return true
	})
	require.Truef(t, !changed && bytes.Equal(got, params), "invalid edit escaped: %s, %v", got, changed)
}

// The splice is byte-level, so it is asserted through the decoder that reads it.
func TestWithCompleteResultType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, result, want string
	}{
		{name: "empty object", result: `{}`, want: "complete"},
		{name: "content preserved", result: `{"content":[{"type":"text","text":"x"}]}`, want: "complete"},
		{name: "leading space", result: "\n {\"content\":[]}", want: "complete"},
		// Last key wins, so an upstream still owns the answer it gave.
		{name: "upstream answered", result: `{"resultType":"input_required"}`, want: "input_required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := new(mcp.CallToolResult)
			err := json.Unmarshal(WithCompleteResultType(json.RawMessage(test.result)), result)
			require.NoErrorf(t, err, "decoding the spliced %s: %v", test.result, err)
			wire, err := json.Marshal(result)
			require.NoError(t, err)
			assert.Truef(t, strings.Contains(string(wire), `"resultType":"`+test.want+`"`), "WithCompleteResultType(%s) reached the client as %s, want resultType %q", test.result, wire, test.want)
		})
	}
	// A message that is not an object is not ours to rewrite.
	for _, result := range []string{`null`, `[]`, `"text"`, ``} {
		got := WithCompleteResultType(json.RawMessage(result))
		assert.Equalf(t, result, string(got), "WithCompleteResultType(%s) = %s, want it forwarded untouched", result, got)
	}
}

// The client's initialize params, rewritten so gopls asks us for its roots
// rather than believing it has none. Arbitrary because a client may send any
// shape it likes.
func FuzzWithRootsCapability(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"capabilities":{}}`))
	f.Add([]byte(`{"capabilities":{"roots":{"listChanged":true}}}`))
	f.Add([]byte(`{"capabilities":null}`))
	f.Add([]byte(`{"capabilities":[]}`))
	f.Add([]byte(`{"capabilities":"nonsense"}`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`not json`))

	f.Fuzz(func(t *testing.T, params []byte) {
		got := WithRootsCapability(params)

		var object map[string]json.RawMessage
		if err := json.Unmarshal(params, &object); err != nil || object == nil {
			// Nothing it can safely rewrite: it must hand the params back as-is
			// rather than inventing a shape the client never sent.
			require.Truef(t, bytes.Equal(got, params), "withRootsCapability rewrote params it could not parse:\n got %s\nwant %s", got, params)
			return
		}
		if bytes.Equal(got, params) {
			// Handed back untouched: capabilities was there but was not an
			// object to add roots to, so there is nothing safe to rewrite.
			return
		}
		require.Truef(t, json.Valid(got), "WithRootsCapability(%s) = %s, which is not valid JSON", params, got)
		var rewritten map[string]json.RawMessage
		err := json.Unmarshal(got, &rewritten)
		require.NoErrorf(t, err, "WithRootsCapability(%s) = %s, no longer an object: %v", params, got, err)
		before := spellingBudget(object, "capabilities")
		after := foldCount(rewritten, "capabilities")
		require.Falsef(t, after > before, "WithRootsCapability(%s) = %s: %d keys fold to \"capabilities\", want at most %d", params, got, after, before)
		// Decoded the way gopls decodes it, rather than through jsonKey: a
		// jsonKey that picked the wrong key would otherwise send this assertion
		// looking under the same wrong key and pass.
		var out struct {
			Capabilities struct {
				Roots json.RawMessage `json:"roots"`
			} `json:"capabilities"`
		}
		err = json.Unmarshal(got, &out)
		require.NoErrorf(t, err, "WithRootsCapability(%s) = %s, which gopls could not decode: %v", params, got, err)
		require.Falsef(t, absentJSON(out.Capabilities.Roots), "WithRootsCapability(%s) = %s: rewritten, but with no roots capability", params, got)
	})
}

// jsonKey is what keeps the rewrite above from adding a second spelling of a
// key the client already sent — two keys mapping to one field, with gopls
// reading whichever marshalled first. Fuzzed directly as well as through
// withRootsCapability, because the hazard is in the object's keys rather than
// in the params around them, and a decoder folds case, so the interesting keys
// are the ones no seed would think to write.
func FuzzJSONKey(f *testing.F) {
	f.Add(`{}`, "capabilities")
	f.Add(`{"capabilities":{}}`, "capabilities")
	f.Add(`{"CApABilities":{}}`, "capabilities")
	f.Add(`{"capabilities":1,"CAPABILITIES":2}`, "capabilities")
	f.Add(`{"roots":null}`, "roots")
	f.Add(`{"":1}`, "")
	f.Add(`{"Ω":1}`, "ω")
	f.Add(`{"other":1}`, "capabilities")

	f.Fuzz(func(t *testing.T, object string, name string) {
		var decoded map[string]json.RawMessage
		if err := json.Unmarshal([]byte(object), &decoded); err != nil || decoded == nil {
			t.Skip() // not an object, so there are no keys to pick between
		}
		key := jsonKey(decoded, name)
		require.Falsef(t, key != name && !strings.EqualFold(key, name), "jsonKey(%s, %q) = %q, a key no decoder would read as %q", object, name, key, name)
		_, present := decoded[key]
		require.Falsef(t, !present && key != name, "jsonKey(%s, %q) = %q, which the object does not have", object, name, key)
		// The property the caller needs: writing under the key it hands back
		// never leaves the object with more spellings of name than it had.
		before := spellingBudget(decoded, name)
		decoded[key] = json.RawMessage(`{}`)
		after := foldCount(decoded, name)
		require.Falsef(t, after > before, "writing under jsonKey(%s, %q) = %q left %d keys folding to it, want at most %d", object, name, key, after, before)
	})
}

// spellingBudget is how many keys may fold to name once something has written
// under it — the assertion both fuzzers above are built around.
//
// encoding/json matches field names case-insensitively, so a second spelling of
// a key the client already sent leaves two keys mapping to one field, and which
// one gopls reads comes down to their order. Adding the key to an object that
// had none is the point; making it ambiguous is the bug. Hence the clamp to 1:
// an object with no spelling is allowed to gain its first, and one already
// ambiguous is allowed to stay exactly as ambiguous as it arrived — dropping it
// would forbid the write these fuzzers exist to permit.
func spellingBudget(object map[string]json.RawMessage, name string) int {
	return max(foldCount(object, name), 1)
}

// foldCount is how many of object's keys a Go decoder would read as name.
func foldCount(object map[string]json.RawMessage, name string) int {
	var n int
	for key := range object {
		if strings.EqualFold(key, name) {
			n++
		}
	}
	return n
}
