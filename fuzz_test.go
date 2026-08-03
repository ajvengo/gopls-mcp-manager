package main

// Fuzz targets for every workflow that takes data this process did not author:
// the map file (hand-editable by design), the client's JSON-RPC params, the
// paths a tool call names, and the worktree strings ports are derived from.
//
// `go test` runs these over their seed corpus only. Fuzzing proper is opt-in:
// go test -fuzz FuzzReadMap -fuzztime 30s

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// The map file is meant to be editable by hand, so readMap's input is arbitrary
// bytes. Nothing it returns may violate the checks it makes, because every
// field it hands back is an argument to kill(2) or to a probe that decides on
// one: a pid of 0 signals this process's own group, a negative pid signals
// every process the user owns, and a port outside our range can only refuse a
// probe and so condemn a live server.
func FuzzReadMap(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("\n\n\n"))
	f.Add([]byte(`{"worktree":"/repo","port":62001,"pid":42}`))
	f.Add([]byte(`{"worktree":"/repo","port":62001,"pid":42,"startedAt":1700000000}`))
	f.Add([]byte(`{"worktree":"","port":62001,"pid":42}`))   // empty worktree
	f.Add([]byte(`{"worktree":"/r","port":1,"pid":42}`))     // port under range
	f.Add([]byte(`{"worktree":"/r","port":70000,"pid":42}`)) // port over range
	f.Add([]byte(`{"worktree":"/r","port":62001,"pid":0}`))  // pid 0: our own group
	f.Add([]byte(`{"worktree":"/r","port":62001,"pid":-1}`)) // pid -1: everything
	f.Add([]byte("not json\n{\"worktree\":\"/r\",\"port\":62001,\"pid\":7}"))
	f.Add([]byte(`{"worktree":"/r","port":62001.9,"pid":7}`))

	f.Fuzz(func(t *testing.T, content []byte) {
		path := newTestManager(t).mapPath
		mustWriteFile(t, path, string(content))
		records, err := readMap(path)
		if err != nil {
			return // a read that failed hands nothing back to act on
		}
		for _, r := range records {
			if r.Worktree == "" {
				t.Errorf("readMap kept a record with no worktree: %#v", r)
			}
			if r.PID <= 0 {
				t.Errorf("readMap kept pid %d, which kill(2) would aim at a group: %#v", r.PID, r)
			}
			if r.Port < firstPort || r.Port > lastPort {
				t.Errorf("readMap kept port %d, outside %d-%d: %#v", r.Port, firstPort, lastPort, r)
			}
		}
	})
}

// One record per line, so any byte in a path — a newline above all — has to
// survive the trip. writeMap refuses what it cannot represent rather than
// writing an approximation, so the property is: it errors, or it round-trips
// exactly. Anything else means a record read back names a worktree nobody
// asked about.
func FuzzMapRoundTrip(f *testing.F) {
	f.Add("/repo/plain", 62001, 42, int64(0))
	f.Add("/repo/with spaces\tand\na quote\"", 62002, 43, int64(1700000000))
	f.Add("/repo/\\backslash\\", 62003, 44, int64(-1))
	f.Add("/repo/юникод/路径", 62004, 45, int64(0))
	f.Add("/repo/\x00null", 62005, 46, int64(0))
	f.Add("/repo/\xff\xfe", 62006, 47, int64(0)) // invalid UTF-8: must be refused

	f.Fuzz(func(t *testing.T, worktree string, port, pid int, startedAt int64) {
		// Normalized into the domain readMap accepts, so that a rejected record
		// never masquerades as a lost one.
		if worktree == "" {
			t.Skip()
		}
		if port < firstPort || port > lastPort || pid <= 0 {
			t.Skip()
		}
		want := []record{{Worktree: worktree, Port: port, PID: pid, StartedAt: startedAt}}

		path := newTestManager(t).mapPath
		if err := writeMap(path, want); err != nil {
			if utf8.ValidString(worktree) {
				t.Fatalf("writeMap refused a representable record: %v", err)
			}
			return
		}
		if !utf8.ValidString(worktree) {
			t.Fatalf("writeMap accepted %q, which JSON cannot represent byte for byte", worktree)
		}
		wantRecords(t, path, "the map did not round-trip", want...)
	})
}

// A port is what a probe is aimed at and what a record claims, so allocation
// must stay inside the range readMap will accept — otherwise a record is
// written that the next read drops, and the gopls behind it is stranded.
func FuzzAllocatePort(f *testing.F) {
	f.Add("/repo/a", 0)
	f.Add("", 0)
	f.Add("/repo/юникод", 7)
	f.Add(strings.Repeat("/deep", 200), 3)
	f.Add("/repo/\xff", 1)

	f.Fuzz(func(t *testing.T, worktree string, taken int) {
		base := basePort(worktree)
		if base < firstPort || base > lastPort {
			t.Fatalf("basePort(%q) = %d, outside %d-%d", worktree, base, firstPort, lastPort)
		}
		if again := basePort(worktree); again != base {
			t.Fatalf("basePort(%q) is not deterministic: %d then %d", worktree, base, again)
		}
		if next := nextPort(base); next < firstPort || next > lastPort {
			t.Fatalf("nextPort(%d) = %d, outside %d-%d", base, next, firstPort, lastPort)
		}

		// Occupy a prefix of the walk, so allocation has to step past it.
		occupied := min(max(taken, 0), 64)
		records := make([]record, 0, occupied)
		for port := base; len(records) < occupied; port = nextPort(port) {
			records = append(records, record{Worktree: worktree, Port: port, PID: 1})
		}
		got, err := allocatePort(worktree, records, func(int) bool { return false })
		if err != nil {
			t.Fatalf("allocatePort with %d of %d ports taken: %v", occupied, portCount, err)
		}
		if got < firstPort || got > lastPort {
			t.Fatalf("allocatePort() = %d, outside %d-%d", got, firstPort, lastPort)
		}
		if slices.ContainsFunc(records, func(r record) bool { return r.Port == got }) {
			t.Fatalf("allocatePort() = %d, a port already claimed", got)
		}
	})
}

// Every path argument a tool call names reaches this, and the answer decides
// which gopls — which index, which worktree — runs the call.
func FuzzContainingDir(f *testing.F) {
	f.Add("/repo/a.go")
	f.Add("/repo/")
	f.Add("/repo/./b/../a.go")
	f.Add("")
	f.Add("relative/a.go")
	f.Add("/")
	f.Add("//")
	f.Add(strings.Repeat("/a", 500))
	f.Add("/repo/\x00/a.go")

	f.Fuzz(func(t *testing.T, path string) {
		got := containingDir(path)
		// The result is a memo key, so two spellings of one directory must not
		// become two entries and two git forks.
		if clean := filepath.Clean(got); clean != got {
			t.Fatalf("containingDir(%q) = %q, which is not cleaned (%q)", path, got, clean)
		}
		if filepath.IsAbs(path) && !filepath.IsAbs(got) {
			t.Fatalf("containingDir(%q) = %q, relative for an absolute argument", path, got)
		}
	})
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
		got := withRootsCapability(params)

		var object map[string]json.RawMessage
		if err := json.Unmarshal(params, &object); err != nil || object == nil {
			// Nothing it can safely rewrite: it must hand the params back as-is
			// rather than inventing a shape the client never sent.
			if !bytes.Equal(got, params) {
				t.Fatalf("withRootsCapability rewrote params it could not parse:\n got %s\nwant %s", got, params)
			}
			return
		}
		if bytes.Equal(got, params) {
			// Handed back untouched: capabilities was there but was not an
			// object to add roots to, so there is nothing safe to rewrite.
			return
		}
		if !json.Valid(got) {
			t.Fatalf("withRootsCapability(%s) = %s, which is not valid JSON", params, got)
		}
		var rewritten map[string]json.RawMessage
		if err := json.Unmarshal(got, &rewritten); err != nil {
			t.Fatalf("withRootsCapability(%s) = %s, no longer an object: %v", params, got, err)
		}
		// encoding/json matches field names case-insensitively, so a second
		// spelling of a key the client already sent leaves two keys mapping to
		// one field — and which one gopls reads comes down to their order.
		// Adding the key to an object that had none is the point; making it
		// ambiguous is the bug. An input already ambiguous stays its own.
		before := max(foldCount(object, "capabilities"), 1)
		if after := foldCount(rewritten, "capabilities"); after > before {
			t.Fatalf("withRootsCapability(%s) = %s: %d keys fold to \"capabilities\", want at most %d", params, got, after, before)
		}
		// Decoded the way gopls decodes it, rather than through jsonKey: a
		// jsonKey that picked the wrong key would otherwise send this assertion
		// looking under the same wrong key and pass.
		var out struct {
			Capabilities struct {
				Roots json.RawMessage `json:"roots"`
			} `json:"capabilities"`
		}
		if err := json.Unmarshal(got, &out); err != nil {
			t.Fatalf("withRootsCapability(%s) = %s, which gopls could not decode: %v", params, got, err)
		}
		if absentJSON(out.Capabilities.Roots) {
			t.Fatalf("withRootsCapability(%s) = %s: rewritten, but with no roots capability", params, got)
		}
	})
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

// The routing decision itself: which worktrees a tools/call names. Resolution
// shells out to git, so the property asserted is the one that holds whatever it
// answers — and the one the memos in front of it could actually break: asking
// twice must answer twice the same, or a call would route somewhere its own
// repeat did not.
func FuzzToolCallWorktrees(f *testing.F) {
	f.Add([]byte(`{"arguments":{"file":"/repo/a.go"}}`))
	f.Add([]byte(`{"arguments":{"dir":"/repo"}}`))
	f.Add([]byte(`{"arguments":{"files":["/repo/a.go","/repo/b.go"]}}`))
	f.Add([]byte(`{"arguments":{"file":"relative.go"}}`))
	f.Add([]byte(`{"arguments":{"files":[]}}`))
	f.Add([]byte(`{"arguments":{"files":null}}`))
	f.Add([]byte(`{"arguments":{"file":123}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`garbage`))

	f.Fuzz(func(t *testing.T, params []byte) {
		r := newTestRouter(t, testHome)
		first := r.toolCallWorktrees(params)
		if again := r.toolCallWorktrees(params); !slices.Equal(first, again) {
			t.Fatalf("toolCallWorktrees(%s) = %q, then %q from the memo", params, first, again)
		}
	})
}

// notifications/cancelled carries an id minted by the client, matched against
// the ids in flight (R7). Two ids are tracked under different worktrees, so the
// answer has to be the one belonging to the id named — a target that returned
// the wrong lane, or any lane at all for an id nobody owes, would show here.
func FuzzCancelTarget(f *testing.F) {
	f.Add([]byte(`{"requestId":1}`))
	f.Add([]byte(`{"requestId":2}`))
	f.Add([]byte(`{"requestId":3}`))
	f.Add([]byte(`{"requestId":"abc"}`))
	f.Add([]byte(`{"requestId":null}`))
	f.Add([]byte(`{"requestId":{"nested":true}}`))
	f.Add([]byte(`{"requestId":1e400}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`garbage`))

	const otherWorktree = "/tmp/other"
	f.Fuzz(func(t *testing.T, params []byte) {
		r := newTestRouter(t, testHome)
		r.track(mustID(t, float64(1)), nil, testWorktree)
		r.track(mustID(t, float64(2)), nil, otherWorktree)

		var cancelled struct {
			RequestID any `json:"requestId"`
		}
		_ = json.Unmarshal(params, &cancelled)
		// Only a JSON number names one of the two tracked ids; every other shape
		// — a string, an object, nothing at all — is owed by nobody.
		var want string
		switch id, _ := cancelled.RequestID.(float64); id {
		case 1:
			want = testWorktree
		case 2:
			want = otherWorktree
		}
		if got := r.cancelTarget(params); got != want {
			t.Fatalf("cancelTarget(%s) = %q, want %q", params, got, want)
		}
	})
}

// A request is routed on its method and params together, and target's answer is
// the lane that will run it. It must always name one.
func FuzzTargetNamesALane(f *testing.F) {
	f.Add("tools/call", []byte(`{"arguments":{"file":"/repo/a.go"}}`))
	f.Add("tools/call", []byte(`{"arguments":{"query":"Foo"}}`))
	f.Add("tools/list", []byte(`{}`))
	f.Add("notifications/cancelled", []byte(`{"requestId":1}`))
	f.Add("initialize", []byte(`{}`))
	f.Add("", []byte(``))

	f.Fuzz(func(t *testing.T, method string, params []byte) {
		r := newTestRouter(t, testHome)
		got, err := r.target(&jsonrpc.Request{Method: method, Params: params})
		if err != nil {
			// The only refusal is a call spanning worktrees, which names them.
			if got != "" {
				t.Fatalf("target() refused with %q, want no worktree alongside the error", got)
			}
			return
		}
		if got == "" {
			t.Fatalf("target(%q, %s) = %q with no error: no lane would own it", method, params, got)
		}
	})
}
