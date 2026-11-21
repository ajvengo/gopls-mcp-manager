//go:build integration

package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type auditOwnerProcess struct {
	command *exec.Cmd
	done    chan struct{}
	output  bytes.Buffer
}

func startAuditOwner(t *testing.T, ctx context.Context, binary string, m *manager) *auditOwnerProcess {
	t.Helper()
	path, err := supervisorSocket(m)
	require.NoError(t, err)
	p := &auditOwnerProcess{command: exec.Command(binary, "supervisor"), done: make(chan struct{})}
	p.command.Stdout, p.command.Stderr = &p.output, &p.output
	require.NoError(t, p.command.Start())
	go func() {
		_ = p.command.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		defer func() { _ = os.Remove(filepath.Dir(path)) }()
		_ = p.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			assert.Fail(t, "disposable owner did not shut down", p.output.String())
		}
	})
	require.Eventually(t, func() bool {
		return supervisorPing(ctx, m) == nil
	}, 10*time.Second, 20*time.Millisecond)
	return p
}

func expectAuditCleanup(t *testing.T, m *manager) {
	t.Helper()
	t.Cleanup(func() {
		assert.Eventually(t, func() bool {
			count, err := guardianManifestCount(guardianStateDir(m))
			return err == nil && count == 0
		}, 15*time.Second, 20*time.Millisecond, "disposable guardian residue")
	})
}

func useAuditHome(t *testing.T, home string) *manager {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "env", "-json", "GOCACHE", "GOMODCACHE", "GOPATH").Output()
	require.NoError(t, err)
	var caches map[string]string
	require.NoError(t, json.Unmarshal(out, &caches))
	for name, value := range caches {
		t.Setenv(name, value)
	}
	t.Setenv("HOME", home)
	t.Setenv("GOPLSCACHE", filepath.Join(home, "gopls-cache"))
	m, err := newManager()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(m.mapPath), 0o700))
	return m
}

func auditEnsure(ctx context.Context, m *manager, worktree string) error {
	var output bytes.Buffer
	if err := querySupervisor(ctx, m, "/ensure", worktree, &output); err != nil {
		return err
	}
	_, err := strconv.Atoi(strings.TrimSpace(output.String()))
	return err
}

func auditSnapshot(ctx context.Context, m *manager) (supervisorSnapshot, error) {
	var output bytes.Buffer
	if err := querySupervisor(ctx, m, "/status", "", &output); err != nil {
		return supervisorSnapshot{}, err
	}
	var snapshot supervisorSnapshot
	err := json.Unmarshal(output.Bytes(), &snapshot)
	return snapshot, err
}

type auditHook struct {
	file, before, statement string
}

// Instrument only disposable builds so release binaries have no crash switches.
func buildRuntimeAuditBinary(t *testing.T, ctx context.Context, output string, hooks []auditHook, flags ...string) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{}
	read := func(name string) string {
		if source, ok := files[name]; ok {
			return source
		}
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		return string(source)
	}
	for _, hook := range hooks {
		source := read(hook.file)
		require.Equal(t, 1, strings.Count(source, hook.before), "ambiguous audit checkpoint in %s", hook.file)
		files[hook.file] = strings.Replace(source, hook.before, hook.statement+"\n"+hook.before, 1)
	}
	files["main.go"] = read("main.go") + `
func init() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300_000_000_000)
		defer cancel()
		<-ctx.Done()
		os.Exit(124)
	}()
}

func auditCrash(point string) {
	if point == os.Getenv("GOPLS_AUDIT_CRASH") {
		if err := os.WriteFile(os.Getenv("GOPLS_AUDIT_MARKER"), []byte(point), 0o600); err != nil {
			panic(err)
		}
		os.Exit(93)
	}
}

func auditStateWrite() {
	path := os.Getenv("GOPLS_AUDIT_STATE_WRITE")
	if path == "" {
		return
	}
	updated := []record{{Worktree: "/after", Port: firstPort, PID: 2}}
	if os.Getenv("GOPLS_AUDIT_CRASH") == "retry-directory-sync" {
		m := &manager{mapPath: path}
		_, err := m.withMap(context.Background(), func([]record) ([]record, error) { return updated, nil })
		if err == nil || err.Error() != "audit directory sync failure" {
			panic(fmt.Sprintf("expected a directory sync failure, got %v", err))
		}
		if err := os.Setenv("GOPLS_AUDIT_RETRY", "1"); err != nil {
			panic(err)
		}
		if _, err := m.withMap(context.Background(), func([]record) ([]record, error) { return updated, nil }); err != nil {
			panic(err)
		}
	} else if err := writeMap(path, updated); err != nil {
		panic(err)
	}
	os.Exit(0)
}
`
	replacements := make(map[string]string, len(files))
	for name, source := range files {
		original, err := filepath.Abs(name)
		require.NoError(t, err)
		replacement := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(replacement, []byte(source), 0o600))
		replacements[original] = replacement
	}
	overlay, err := json.Marshal(map[string]any{"Replace": replacements})
	require.NoError(t, err)
	overlayPath := filepath.Join(dir, "overlay.json")
	require.NoError(t, os.WriteFile(overlayPath, overlay, 0o600))
	args := append([]string{"build", "-overlay=" + overlayPath, "-o", output}, flags...)
	out, err := exec.CommandContext(ctx, "go", append(args, ".")...).CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func TestAuditRealGoplsRegistryCrashBoundaries(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "manager")
	hooks := []auditHook{
		{"main.go", "if err := run(os.Args, os.Stdout);", "auditStateWrite()"},
		{"registry.go", "if err := tmp.Sync();", `auditCrash("after-write")`},
		{"registry.go", "if err := os.Rename(tmp.Name(), path);", `auditCrash("after-file-sync")`},
		{"registry.go", "return syncStateDirectory(dir)", `auditCrash("after-rename")`},
		{"registry.go", "return dir.Sync()", `
if os.Getenv("GOPLS_AUDIT_CRASH") == "retry-directory-sync" {
	if os.Getenv("GOPLS_AUDIT_RETRY") != "1" {
		return fmt.Errorf("audit directory sync failure")
	}
	if err := dir.Sync(); err != nil { return err }
	auditCrash("retry-directory-sync")
}
if os.Getenv("GOPLS_AUDIT_CRASH") == "after-directory-sync" {
	if err := dir.Sync(); err == nil { auditCrash("after-directory-sync") }
}`},
	}
	buildRuntimeAuditBinary(t, t.Context(), binary, hooks)
	for _, tc := range []struct{ point, worktree string }{
		{"after-write", "/before"},
		{"after-file-sync", "/before"},
		{"after-rename", "/after"},
		{"after-directory-sync", "/after"},
		{"retry-directory-sync", "/after"},
	} {
		t.Run(tc.point, func(t *testing.T) {
			dir := t.TempDir()
			path, marker := filepath.Join(dir, "map"), filepath.Join(dir, "hit")
			require.NoError(t, writeMap(path, []record{{Worktree: "/before", Port: firstPort, PID: 1}}))
			command := exec.CommandContext(t.Context(), binary)
			command.Env = append(os.Environ(), "GOPLS_AUDIT_STATE_WRITE="+path, "GOPLS_AUDIT_MARKER="+marker,
				"GOPLS_AUDIT_CRASH="+tc.point, "GOPLS_AUDIT_RETRY=")
			out, err := command.CombinedOutput()
			require.Error(t, err, "%s", out)
			require.Equal(t, 93, command.ProcessState.ExitCode(), "%s", out)
			hit, err := os.ReadFile(marker)
			require.NoError(t, err)
			require.Equal(t, tc.point, string(hit))
			records, intact, err := readMap(path)
			require.NoError(t, err)
			require.True(t, intact)
			require.Len(t, records, 1)
			require.Equal(t, tc.worktree, records[0].Worktree)
		})
	}
}

func TestAuditRealGoplsLifecycleCrashBoundaries(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "manager")
	hooks := []auditHook{
		{"guardian.go", "controlRead, controlWrite, err := os.Pipe()", `auditCrash("before-guardian-start")`},
		{"guardian.go", "record.ChildPID = child.Process.Pid", `auditCrash("after-child-start")`},
		{"guardian.go", "quota, err := strconv.Atoi(args[0])", `
if os.Getenv("GOPLS_AUDIT_CRASH") == "after-child-start" {
	time.Sleep(500_000_000)
	if err := os.WriteFile(os.Getenv("GOPLS_AUDIT_MARKER")+".child", []byte("exiting"), 0o600); err != nil {
		panic(err)
	}
	os.Exit(0)
}`},
		{"supervisor.go", `defer s.m.timed("readiness", time.Now())`, `auditCrash("after-registry-publication")`},
		{"supervisor.go", "s.mu.Lock()\n\tdefer s.mu.Unlock()\n\tg.cleaning = false", `if err == nil { auditCrash("after-registry-cleanup") }`},
	}
	buildRuntimeAuditBinary(t, t.Context(), binary, hooks)
	root, linked := auditWorktrees(t)
	for _, point := range []string{"before-guardian-start", "after-child-start", "after-registry-publication", "after-registry-cleanup"} {
		t.Run(point, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			home := t.TempDir()
			if point == "after-child-start" {
				t.Setenv("GOPLS_MANAGER_MAX_SERVERS", "1")
			}
			m := useAuditHome(t, home)
			marker := filepath.Join(home, "crash")
			t.Setenv("GOPLS_AUDIT_MARKER", marker)
			t.Setenv("GOPLS_AUDIT_CRASH", point)
			owner := startAuditOwner(t, ctx, binary, m)
			if point == "after-registry-cleanup" {
				require.NoError(t, auditEnsure(ctx, m, root))
				out, err := exec.CommandContext(ctx, binary, "restart", "--timeout=15s").CombinedOutput()
				require.Error(t, err, "%s", out)
			} else {
				require.Error(t, auditEnsure(ctx, m, root))
			}
			hit, err := os.ReadFile(marker)
			require.NoError(t, err)
			require.Equal(t, point, string(hit))
			if point == "after-child-start" {
				require.Eventually(t, func() bool {
					_, err := os.Stat(marker + ".child")
					return err == nil
				}, 5*time.Second, 20*time.Millisecond)
				snapshot, err := auditSnapshot(ctx, m)
				require.NoError(t, err)
				require.Len(t, snapshot.Generations, 1)
				require.Equal(t, string(generationTerminating), snapshot.Generations[0].State)
				require.ErrorContains(t, auditEnsure(ctx, m, linked), "shared gopls server limit reached")
				check, stop := context.WithTimeout(ctx, 200*time.Millisecond)
				defer stop()
				require.ErrorContains(t, guardianRecovery(check, guardianStateDir(m)), "exit is unconfirmed")
				return
			}
			select {
			case <-owner.done:
				require.Equal(t, 93, owner.command.ProcessState.ExitCode(), owner.output.String())
			case <-ctx.Done():
				t.Fatal("owner failed to exit at the marked boundary")
			}
			records, _, err := readMap(m.mapPath)
			require.NoError(t, err)
			if point == "after-registry-publication" {
				require.Len(t, records, 1)
			} else {
				require.Empty(t, records)
			}
			require.NoError(t, guardianRecovery(ctx, guardianStateDir(m)))
			t.Setenv("GOPLS_AUDIT_CRASH", "")
			expectAuditCleanup(t, m)
			startAuditOwner(t, ctx, binary, m)
			require.NoError(t, auditEnsure(ctx, m, root))
			after := mustReadMap(t, m.mapPath)
			require.Len(t, after, 1)
			if len(records) != 0 {
				require.NotEqual(t, records[0].Generation, after[0].Generation)
			}
		})
	}
}
