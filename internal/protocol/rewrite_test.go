package protocol

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
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
		{name: "case-sensitive names", in: `{"Capabilities":{"Roots":{}}}`, want: `{"Capabilities":{"Roots":{}},"capabilities":{"roots":{}}}`},
		{name: "duplicate names", in: `{"capabilities":{},"capabilities":{}}`, want: `{"capabilities":{},"capabilities":{}}`},
		{name: "invalid UTF-8", in: "{\"text\":\"\xff\"}", want: "{\"text\":\"\xff\"}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := string(WithRootsCapability(jsontext.Value(test.in)))
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
		{name: "case-sensitive names", params: `{"ARGUMENTS":{"FILE":"/alias"}}`, file: "/physical", want: `{"ARGUMENTS":{"FILE":"/alias"},"arguments":{"file":"/physical"}}`, changed: true},
		{name: "missing arguments", params: `{}`, file: "/physical", want: `{"arguments":{"file":"/physical"}}`, changed: true},
		{name: "invalid arguments", params: `{"arguments":[]}`, file: "/physical", want: `{"arguments":[]}`},
		{name: "invalid outer", params: `not json`, file: "/physical", want: `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, changed := WithPhysicalPaths(jsontext.Value(tc.params), tc.file, tc.dir, tc.files)
			require.Truef(t, string(got) == tc.want && changed == tc.changed, "rewritten = %s, %v; want %s, %v", got, changed, tc.want, tc.changed)
		})
	}
}

func TestRewriteNestedRejectsInvalidEdit(t *testing.T) {
	t.Parallel()
	params := jsontext.Value(`{}`)
	got, changed := rewriteNested(params, "arguments", func(object map[string]jsontext.Value) bool {
		object["bad"] = jsontext.Value(`{`)
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
		// An upstream still owns the answer it gave; no duplicate is inserted.
		{name: "upstream answered", result: `{"resultType":"input_required"}`, want: "input_required"},
		{name: "escaped upstream key", result: `{"result\u0054ype":"input_required"}`, want: "input_required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := new(mcp.CallToolResult)
			err := json.Unmarshal(WithCompleteResultType(jsontext.Value(test.result)), result)
			require.NoErrorf(t, err, "decoding the spliced %s: %v", test.result, err)
			wire, err := json.Marshal(result)
			require.NoError(t, err)
			assert.Truef(t, strings.Contains(string(wire), `"resultType":"`+test.want+`"`), "WithCompleteResultType(%s) reached the client as %s, want resultType %q", test.result, wire, test.want)
		})
	}
	// A message that is not an object is not ours to rewrite.
	for _, result := range []string{`null`, `[]`, `"text"`, ``, `{"broken":`, `{"resultType":"complete","resultType":"input_required"}`} {
		got := WithCompleteResultType(jsontext.Value(result))
		assert.Equalf(t, result, string(got), "WithCompleteResultType(%s) = %s, want it forwarded untouched", result, got)
	}
}

// The client's initialize params, rewritten so gopls asks us for its roots
// rather than believing it has none. Arbitrary because a client may send any
// shape it likes.
func FuzzWithRootsCapability(f *testing.F) {
	f.Add([]byte(`{"capabilities":{"roots":null,"opaque":{"roots":false}},"result\u0054ype":"partial"}`))
	f.Add([]byte(`{"arguments":{"file":"/physical.go","files":[]},"resultType":null}`))
	f.Add([]byte(`{"capabilities":{"roots":null},"opaque":[{"a":1}]}`))
	f.Add([]byte(`{"capabilities":{},"capabilities":{"roots":null}}`))
	f.Add([]byte(`{"resultType":"input_required","arguments":{"file":"/alias"}}`))
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
		complete := WithCompleteResultType(params)
		physical, _ := WithPhysicalPaths(params, "/physical/file.go", "", nil)
		if jsontext.Value(params).IsValid() {
			require.True(t, complete.IsValid())
			require.True(t, physical.IsValid())
		} else {
			require.Equal(t, params, []byte(complete))
			require.Equal(t, params, []byte(physical))
		}

		var object map[string]jsontext.Value
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
		require.Truef(t, got.IsValid(), "WithRootsCapability(%s) = %s, which is not valid JSON", params, got)
		var rewritten map[string]jsontext.Value
		err := json.Unmarshal(got, &rewritten)
		require.NoErrorf(t, err, "WithRootsCapability(%s) = %s, no longer an object: %v", params, got, err)
		// Every opaque field other than the exact capabilities key survives.
		for key, value := range object {
			if key != "capabilities" {
				require.JSONEq(t, string(value), string(rewritten[key]))
			}
		}
		var out struct {
			Capabilities struct {
				Roots jsontext.Value `json:"roots"`
			} `json:"capabilities"`
		}
		err = json.Unmarshal(got, &out)
		require.NoErrorf(t, err, "WithRootsCapability(%s) = %s, which gopls could not decode: %v", params, got, err)
		require.Falsef(t, absentJSON(out.Capabilities.Roots), "WithRootsCapability(%s) = %s: rewritten, but with no roots capability", params, got)
	})
}
