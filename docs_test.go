package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// A test or fuzz target, as declared in Go and as cited in the docs.
	declaredPattern = regexp.MustCompile(`func ((?:Test|Fuzz)[A-Za-z0-9_]*)\(`)
	citedPattern    = regexp.MustCompile("`((?:Test|Fuzz)[A-Za-z0-9_]*)`")
	// A table row's name, and the citation naming one or more of them.
	rowPattern      = regexp.MustCompile(`name:\s*"([^"]*)"`)
	citedRowPattern = regexp.MustCompile(`\(cases? ("[^)]*")\)`)
	quotedPattern   = regexp.MustCompile(`"([^"]*)"`)
	spacePattern    = regexp.MustCompile(`\s+`)
)

// SPEC.md and README.md name the test behind each invariant, which is worth
// something only while the names still resolve. Eight had already drifted when
// this check was written: folding four recordAlive tests and three worktreeOf
// tests into tables renamed them, and prose cannot fail to compile.
//
// Case names are checked too, since a table's rows are where the invariants
// ended up. Whitespace is collapsed first, because a citation — and a quoted
// case name inside it — may be wrapped across lines.
func TestDocsNameTestsThatExist(t *testing.T) {
	t.Parallel()

	declared := make(map[string]bool)
	rows := make(map[string]bool)
	testFiles, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	internalFiles, err := filepath.Glob("internal/*/*_test.go")
	require.NoError(t, err)
	testFiles = append(testFiles, internalFiles...)
	for _, path := range testFiles {
		source := mustReadString(t, path)
		for _, m := range declaredPattern.FindAllStringSubmatch(source, -1) {
			declared[m[1]] = true
		}
		for _, m := range rowPattern.FindAllStringSubmatch(source, -1) {
			rows[m[1]] = true
		}
	}

	// Both sides are counted, because a pattern that matched nothing would let
	// this pass on anything — which is the failure it exists to prevent.
	var citedTests, citedRows int
	for _, doc := range []string{"SPEC.md", "README.md"} {
		prose := spacePattern.ReplaceAllString(mustReadString(t, doc), " ")
		for _, m := range citedPattern.FindAllStringSubmatch(prose, -1) {
			citedTests++
			assert.Truef(t, declared[m[1]], "%s names %s, which no test declares", doc, m[1])
		}
		for _, citation := range citedRowPattern.FindAllStringSubmatch(prose, -1) {
			for _, m := range quotedPattern.FindAllStringSubmatch(citation[1], -1) {
				citedRows++
				assert.Truef(t, rows[m[1]], "%s names case %q, which no table row declares", doc, m[1])
			}
		}
	}
	switch {
	case len(declared) == 0:
		require.FailNow(t, "no tests found, so this check would pass on anything")
	case citedTests == 0:
		require.FailNow(t, "no test citation found in the docs, so this check would pass on anything")
	case citedRows == 0:
		require.FailNow(t, "no case citation found in the docs, so the case check would pass on anything")
	}
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}
