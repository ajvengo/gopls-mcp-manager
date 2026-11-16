package main

import (
	"context"
	"crypto/rand"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The process census and actual child/descendant lifetimes require real OS
// processes, so these fault regressions intentionally stay outside synctest.
func TestGuardianFailedInitialObservationRetainsUncertainty(t *testing.T) {
	for _, firstSnapshot := range []string{"exit 1", "exit 0"} {
		t.Run(firstSnapshot, func(t *testing.T) {
			for _, mode := range []string{"normal", "with-escape"} {
				t.Run(mode, func(t *testing.T) {
					dir := t.TempDir()
					marker := filepath.Join(dir, "first-snapshot-failed")
					shim := "#!/bin/sh\nif [ ! -e \"$GPM_GUARDIAN_PS_MARKER\" ]; then\n  : > \"$GPM_GUARDIAN_PS_MARKER\"\n  " + firstSnapshot + "\nfi\nexec /bin/ps \"$@\"\n"
					require.NoError(t, os.WriteFile(filepath.Join(dir, "ps"), []byte(shim), 0o700))
					t.Setenv("GPM_GUARDIAN_PS_MARKER", marker)
					t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
					fixture := newGuardianFixture(t, mode)
					require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
					require.FileExists(t, marker)
					select {
					case <-fixture.child.ConfirmedDone:
						require.FailNow(t, "missing ancestry evidence was treated as confirmed cleanup")
					case <-fixture.child.UncertainDone:
					case <-time.After(5 * time.Second):
						require.FailNow(t, "failed census did not report uncertainty")
					}
					record, err := guardianReadRecord(fixture.child.StatePath)
					require.NoError(t, err, "missing observation must retain its durable reservation")
					require.Contains(t, record.Uncertain, "initial descendant observation failed")
					if mode == "with-escape" {
						data, err := os.ReadFile(fixture.stop + ".desc.pid")
						require.NoError(t, err)
						pid, err := strconv.Atoi(string(data))
						require.NoError(t, err)
						require.NoError(t, syscall.Kill(pid, 0), "escaped disposable helper must still be alive")
					}
					select {
					case <-fixture.child.ConfirmedDone:
						require.FailNow(t, "later group observation erased initial uncertainty")
					default:
					}
					// Fixture cleanup uses only its private stop markers and observes every
					// helper exiting; it never signals a descendant by a rediscovered PID.
				})
			}
		})
	}
}

// TestGuardianHelperProcess is used only by explicitly marked disposable
// subprocesses. It does not touch the real registry or signal foreign groups.
func TestGuardianHelperProcess(t *testing.T) {
	if os.Getenv("GOPLS_GUARDIAN_TEST_HELPER") != "1" {
		return
	}
	// Test runners can be killed before Cleanup runs. Every disposable helper,
	// including escaped descendants, must still have a finite lifetime.
	watchdog := time.AfterFunc(5*time.Minute, func() { os.Exit(124) })
	defer watchdog.Stop()
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) == 0 {
		os.Exit(2)
	}
	if args[0] == "pre-fork-reservation" {
		if len(args) != 4 {
			os.Exit(2)
		}
		dir, token := args[1], args[2]
		port, err := strconv.Atoi(args[3])
		if err != nil {
			os.Exit(2)
		}
		lease, err := guardianAcquireLease(dir, port)
		if err != nil {
			os.Exit(3)
		}
		defer func() { _ = lease.Close() }()
		record := guardianRecord{Version: guardianProtocol, Token: token, Worktree: dir,
			Port: port, MaxOpenFiles: 512, Scope: "pre-fork regression"}
		if err := guardianWriteRecord(filepath.Join(dir, "guardian-"+token+".json"), record); err != nil {
			os.Exit(3)
		}
		if err := os.WriteFile(filepath.Join(dir, "reserved.ready"), nil, 0o600); err != nil {
			os.Exit(3)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	if args[0] == "guardian-child" {
		if err := guardianExecChild(args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		os.Exit(0)
	}
	if args[0] == "guardian" {
		if err := runGuardian(nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		if len(args) == 2 {
			if err := os.WriteFile(args[1]+".ready", nil, 0o600); err != nil {
				os.Exit(3)
			}
			for {
				if _, err := os.Stat(args[1]); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		os.Exit(0)
	}
	if len(args) < 2 {
		os.Exit(2)
	}
	mode, stop := args[0], args[1]
	if mode == "ignore" || mode == "descendant" {
		signalGuardianTestIgnore()
	}
	if mode == "with-descendant" || mode == "with-escape" {
		command := exec.Command(os.Args[0], "-test.run=^TestGuardianHelperProcess$", "--", "descendant", stop+".desc")
		command.Env = os.Environ()
		if mode == "with-escape" {
			command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		}
		if err := command.Start(); err != nil {
			os.Exit(4)
		}
		if err := os.WriteFile(stop+".desc.pid", []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
			os.Exit(4)
		}
		go func() { _ = command.Wait() }()
		for {
			if _, err := os.Stat(stop + ".desc.ready"); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		os.Exit(5)
	}
	if err := os.WriteFile(stop+".limits", fmt.Appendf(nil, "%d/%d", limit.Cur, limit.Max), 0o600); err != nil {
		os.Exit(5)
	}
	if mode == "fd-quota" {
		var files []*os.File
		exhausted := false
		for range 1024 {
			file, err := os.Open(os.DevNull)
			if err != nil {
				exhausted = errors.Is(err, syscall.EMFILE)
				break
			}
			files = append(files, file)
		}
		for _, file := range files {
			_ = file.Close()
		}
		if err := os.WriteFile(stop+".fd", fmt.Appendf(nil, "%d/%t", len(files), exhausted), 0o600); err != nil {
			os.Exit(5)
		}
	}
	if err := os.WriteFile(stop+".ready", []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		os.Exit(5)
	}
	for {
		if _, err := os.Stat(stop); err == nil {
			os.Exit(0)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type guardianFixture struct {
	child   *guardianChild
	command *exec.Cmd
	config  guardianConfig
	stop    string
}

func newGuardianFixture(t *testing.T, mode string, guardianArgs ...string) guardianFixture {
	t.Helper()
	dir := t.TempDir()
	binary, err := os.Executable()
	require.NoError(t, err)
	stop := filepath.Join(dir, "stop")
	config := guardianConfig{Version: guardianProtocol, Worktree: dir, Port: firstPort,
		Token: rand.Text(), Command: binary, MaxOpenFiles: 512,
		ExecArgs: []string{"-test.run=^TestGuardianHelperProcess$", "--", "guardian-child"},
		Args:     []string{"-test.run=^TestGuardianHelperProcess$", "--", mode, stop}}
	config.StatePath = filepath.Join(dir, "state", "guardian-"+config.Token+".json")
	command := exec.Command(binary, append([]string{"-test.run=^TestGuardianHelperProcess$", "--", "guardian"}, guardianArgs...)...)
	command.Env = append(os.Environ(), "GOPLS_GUARDIAN_TEST_HELPER=1")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	child, err := startGuardianCommand(ctx, config, io.Discard, command)
	require.NoError(t, err)
	fixture := guardianFixture{child: child, command: command, config: config, stop: stop}
	t.Cleanup(func() {
		assert.NoError(t, os.WriteFile(stop, nil, 0o600))
		assert.NoError(t, os.WriteFile(stop+".desc", nil, 0o600))
		assert.NoError(t, child.CloseOwner())
		pids := []int{child.Process.Pid}
		if data, err := os.ReadFile(stop + ".desc.pid"); err == nil {
			pid, parseErr := strconv.Atoi(string(data))
			if assert.NoError(t, parseErr) && assert.Positive(t, pid) {
				pids = append(pids, pid)
			}
		} else {
			assert.ErrorIs(t, err, os.ErrNotExist)
		}
		// Keep stop files alive until every disposable helper observes them.
		// Guardian disappearance alone says nothing about escaped descendants.
		for _, pid := range pids {
			assert.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) },
				5*time.Second, 10*time.Millisecond, "disposable child/descendant must exit before TempDir cleanup")
		}
		if !assert.Eventually(t, func() bool { return errors.Is(syscall.Kill(child.GuardianPID, 0), syscall.ESRCH) },
			5*time.Second, 10*time.Millisecond, "disposable guardian must exit") {
			// This is the direct subprocess handle created by this fixture,
			// never a PID rediscovered from a manifest.
			killErr := command.Process.Kill()
			if !errors.Is(killErr, os.ErrProcessDone) {
				assert.NoError(t, killErr)
			}
			assert.Eventually(t, func() bool { return errors.Is(syscall.Kill(child.GuardianPID, 0), syscall.ESRCH) },
				5*time.Second, 10*time.Millisecond, "forced disposable guardian teardown must be reaped")
		}
	})
	require.Eventually(t, func() bool { _, err := os.Stat(stop + ".ready"); return err == nil }, 5*time.Second, 10*time.Millisecond)
	return fixture
}

func signalGuardianTestIgnore() { signal.Ignore(syscall.SIGTERM) }

func TestGuardianOwnerLossStopsAndReapsGroup(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "normal")
	data, err := os.ReadFile(fixture.child.StatePath)
	require.NoError(t, err)
	var record guardianRecord
	require.NoError(t, json.Unmarshal(data, &record))
	require.Equal(t, fixture.child.Process.Pid, record.ChildPID)
	require.Equal(t, fixture.child.GuardianPID, record.GuardianPID)
	require.NoError(t, fixture.child.CloseOwner())
	select {
	case <-fixture.child.ConfirmedDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "owner loss did not confirm group cleanup")
	}
	require.NoFileExists(t, fixture.child.StatePath)
}

func TestGuardianTERMUsesAnchoredGroup(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "normal")
	require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
	select {
	case <-fixture.child.ConfirmedDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "TERM did not confirm child/group cleanup")
	}
	require.ErrorIs(t, fixture.child.Signal(syscall.SIGTERM), os.ErrProcessDone)
}

func TestGuardianIgnoredTERMRetainsReservation(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "ignore")
	require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
	select {
	case <-fixture.child.ConfirmedDone:
		require.FailNow(t, "ignored TERM was reported as exit")
	case <-time.After(150 * time.Millisecond):
	}
	require.FileExists(t, fixture.child.StatePath)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, guardianRecovery(ctx, filepath.Dir(fixture.child.StatePath)))
	require.NoError(t, fixture.child.Signal(syscall.SIGKILL), "explicit failed-start escalation is delegated to the owned child")
	select {
	case <-fixture.child.ConfirmedDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "owned child kill was not reaped")
	}
}

func TestGuardianCrashIsNotChildExitProof(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "ignore")
	require.NoError(t, fixture.command.Process.Kill(), "only the test's owned guardian is killed")
	select {
	case <-fixture.child.UncertainDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "guardian crash was not reported")
	}
	select {
	case <-fixture.child.ConfirmedDone:
		require.FailNow(t, "guardian EOF was treated as child exit proof")
	default:
	}
	require.FileExists(t, fixture.child.StatePath)
	require.Error(t, fixture.child.Signal(syscall.SIGTERM))
}

func TestGuardianWaitAndDescendantExitAreSeparate(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "with-descendant")
	require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
	select {
	case <-fixture.child.ConfirmedDone:
		require.FailNow(t, "live descendant did not retain the group reservation")
	case <-time.After(150 * time.Millisecond):
	}
	require.FileExists(t, fixture.child.StatePath)
	require.NoError(t, os.WriteFile(fixture.stop+".desc", nil, 0o600))
	select {
	case <-fixture.child.ConfirmedDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "descendant disappearance was not confirmed")
	}
}

func TestGuardianDetectedEscapedDescendantIsUncertain(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "with-escape")
	require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
	select {
	case <-fixture.child.UncertainDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "escaped descendant was not reported as uncertainty")
	}
	select {
	case <-fixture.child.ConfirmedDone:
		require.FailNow(t, "escaped descendant was claimed as cleaned up")
	default:
	}
	require.FileExists(t, fixture.child.StatePath)
}

func TestGuardianManifestExistsBeforeHelperCanRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary, err := os.Executable()
	require.NoError(t, err)
	config := guardianConfig{Version: guardianProtocol, Worktree: dir, Port: firstPort,
		Token: rand.Text(), Command: binary, MaxOpenFiles: 512,
		ExecArgs: []string{"-test.run=^TestGuardianHelperProcess$", "--", "guardian-child"}, Args: []string{"unused"}}
	config.StatePath = filepath.Join(dir, "guardian-"+config.Token+".json")
	// A helper that exits without reading config models failure before its
	// first durable PID update. The reservation must already be discoverable.
	command := exec.Command(binary, "-test.run=^$")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = startGuardianCommand(ctx, config, io.Discard, command)
	require.Error(t, err)
	require.FileExists(t, config.StatePath)
	data, err := os.ReadFile(config.StatePath)
	require.NoError(t, err)
	var record guardianRecord
	require.NoError(t, json.Unmarshal(data, &record))
	require.Zero(t, record.ChildPID)
}

func TestGuardianStartFailureReleasesProvenUnstartedReservation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	config := guardianConfig{Version: guardianProtocol, Worktree: dir, Port: firstPort,
		Token: rand.Text(), Command: "unused", MaxOpenFiles: 512, ExecArgs: []string{"unused"}}
	config.StatePath = filepath.Join(dir, "guardian-"+config.Token+".json")
	_, err := startGuardianCommand(t.Context(), config, io.Discard, exec.Command(filepath.Join(dir, "missing-helper")))
	require.Error(t, err)
	require.NoFileExists(t, config.StatePath)
}

func TestGuardianRecoveryDoesNotGuessFromDeadPIDs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, guardianWriteRecord(filepath.Join(dir, "guardian-uncertain.json"), guardianRecord{Version: guardianProtocol}))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, guardianRecovery(ctx, dir))
	require.NoError(t, guardianRecovery(t.Context(), filepath.Join(dir, "missing")))
}

func TestGuardianConfigAndTelemetryPolicy(t *testing.T) {
	t.Parallel()
	env := guardianEnvironment([]string{"UNRELATED=value", "GO_TELEMETRY_CHILD=1", "GO_TELEMETRY_CHILD_UPLOAD=1", "TEST_TELEMETRY_DIR=foreign"}, "/private/telemetry")
	require.Equal(t, []string{"UNRELATED=value", "GO_TELEMETRY_CHILD=2", "TEST_TELEMETRY_DIR=/private/telemetry"}, env)
	require.Error(t, guardianCheckTelemetry(os.Args[0]), "a test binary is not the supported gopls executable")
	require.Error(t, guardianCheckTelemetry(filepath.Join(t.TempDir(), "missing")))
	require.Error(t, runGuardian([]string{"unexpected"}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := startGuardian(ctx, t.TempDir(), firstPort, t.TempDir(), io.Discard, 512)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGuardianTelemetryCompatibility(t *testing.T) {
	t.Parallel()
	for _, release := range []struct{ name, version, sum string }{
		{"gopls v0.23.0", "v0.0.0-20260625142307-59b4966ccb57", "h1:nwGZBCt+FnXUrGsj5vjzAsEmkcaFvd82BbOjECiFYZc="},
		{"gopls v0.24.0", "v0.0.0-20260924152758-ed294f943157", "h1:DftxASHKl6JolFPqoN+2WrFO53JvkHwRXvFqf8gsivA="},
	} {
		t.Run(release.name, func(t *testing.T) {
			t.Parallel()
			for _, scenario := range []string{"supported", "unknown version", "wrong checksum", "crossed pair", "replacement", "wrong module", "wrong binary", "missing dependency"} {
				t.Run(scenario, func(t *testing.T) {
					t.Parallel()
					dep := &debug.Module{Path: "golang.org/x/telemetry", Version: release.version, Sum: release.sum}
					build := &debug.BuildInfo{Path: "golang.org/x/tools/gopls", Deps: []*debug.Module{dep}}
					switch scenario {
					case "unknown version":
						dep.Version = "v0.0.0-unknown"
					case "wrong checksum":
						dep.Sum = "h1:unknown"
					case "crossed pair":
						if release.name == "gopls v0.23.0" {
							dep.Version = "v0.0.0-20260924152758-ed294f943157"
						} else {
							dep.Version = "v0.0.0-20260625142307-59b4966ccb57"
						}
					case "replacement":
						copy := *dep
						dep.Replace = &copy
					case "wrong module":
						dep.Path = "example.com/telemetry"
					case "wrong binary":
						build.Path = "example.com/gopls"
					case "missing dependency":
						build.Deps = nil
					}
					err := guardianValidateTelemetry(build)
					if scenario == "supported" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				})
			}
		})
	}
}

func TestGuardianSnapshotAndDetection(t *testing.T) {
	t.Parallel()
	rows, err := guardianParseSnapshot("100 1 100\n101 100 100\n102 101 102\n")
	require.NoError(t, err)
	require.Equal(t, 1, guardianGroupMembers(rows, 100))
	require.True(t, guardianEscaped(rows, 101, 100))
	require.False(t, guardianEscaped(rows[:2], 101, 100))
	for _, malformed := range []string{"1 2", "a 2 3", "1 -2 3", "1 2 3 4"} {
		_, err := guardianParseSnapshot(malformed)
		require.Error(t, err)
	}
	var output guardianOutput
	_, err = output.Write(make([]byte, 2<<20))
	require.NoError(t, err)
	_, err = output.Write([]byte("x"))
	require.Error(t, err)
}

func FuzzGuardianSnapshot(f *testing.F) {
	f.Add("100 1 100\n101 100 100\n")
	f.Add("invalid")
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > 2<<20 {
			t.Skip()
		}
		rows, err := guardianParseSnapshot(text)
		if err != nil {
			return
		}
		for _, row := range rows {
			assert.Positive(t, row.pid)
			assert.GreaterOrEqual(t, row.parent, 0)
			assert.GreaterOrEqual(t, row.group, 0)
		}
		assert.LessOrEqual(t, guardianGroupMembers(rows, 100), len(rows))
	})
}

func TestGuardianPrivateTelemetryMarkerIsBounded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, err := guardianTelemetryEnvironment("relative")
	require.Error(t, err)
	_, err = guardianTelemetryEnvironment(dir)
	require.NoError(t, err)
	_, err = guardianTelemetryEnvironment(dir)
	require.NoError(t, err, "the same one-file marker is reused")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "telemetry-off", "mode"), []byte("local\n"), 0o600))
	_, err = guardianTelemetryEnvironment(dir)
	require.Error(t, err, "never silently repair or adopt an unexpected telemetry mode")
}

func TestGuardianKernelDescriptorQuota(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "fd-quota")
	limits, err := os.ReadFile(fixture.stop + ".limits")
	require.NoError(t, err)
	require.Equal(t, "512/512", string(limits), "actual child inherits the reserved hard and soft quota")
	data, err := os.ReadFile(fixture.stop + ".fd")
	require.NoError(t, err)
	var opened int
	var exhausted bool
	_, err = fmt.Sscanf(string(data), "%d/%t", &opened, &exhausted)
	require.NoError(t, err)
	require.True(t, exhausted, "the kernel must reject further opens with EMFILE")
	require.Positive(t, opened)
	require.Less(t, opened, 512)
}

func TestGuardianPreForkCrashReconcilesOnlyAfterLeaseRelease(t *testing.T) {
	t.Parallel()
	dir, token := t.TempDir(), rand.Text()
	command := exec.Command(os.Args[0], "-test.run=^TestGuardianHelperProcess$", "--",
		"pre-fork-reservation", dir, token, strconv.Itoa(firstPort))
	command.Env = append(os.Environ(), "GOPLS_GUARDIAN_TEST_HELPER=1")
	require.NoError(t, command.Start())
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, "reserved.ready"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	path := filepath.Join(dir, "guardian-"+token+".json")
	record, err := guardianReadRecord(path)
	require.NoError(t, err)
	require.Zero(t, record.GuardianPID)
	require.Zero(t, record.ChildPID)
	lockPath := filepath.Join(dir, "leases", "port-"+strconv.Itoa(firstPort)+".lock")
	before, err := os.Stat(lockPath)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, guardianRecovery(ctx, dir), context.DeadlineExceeded,
		"both-zero metadata cannot override a live pre-start actor's lease")
	require.FileExists(t, path)
	require.NoError(t, command.Process.Kill(), "only this disposable owned pre-fork actor is killed")
	require.Error(t, command.Wait())
	require.NoError(t, guardianRecovery(t.Context(), dir),
		"released lifetime lease proves no actor can subsequently spawn")
	require.NoFileExists(t, path)
	after, err := os.Stat(lockPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "the stable lock inode must not be unlinked")
}

func TestGuardianInheritedLifetimeLeaseSurvivesParentCopyClosing(t *testing.T) {
	t.Parallel()
	fixture := newGuardianFixture(t, "normal")
	lease, err := guardianAcquireLease(filepath.Dir(fixture.child.StatePath), fixture.config.Port)
	if lease != nil {
		_ = lease.Close()
	}
	require.True(t, errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN),
		"the returned guardian must still own the inherited lifetime lease")
}

func TestGuardianRecoveryKeepsPositivePIDAndLegacyUncertainty(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                           string
		version, guardianPID, childPID int
	}{
		{"guardian only", guardianProtocol, 1, 0},
		{"child only", guardianProtocol, 0, 1},
		{"legacy no lease", 1, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir, token := t.TempDir(), rand.Text()
			path := filepath.Join(dir, "guardian-"+token+".json")
			require.NoError(t, guardianWriteRecord(path, guardianRecord{Version: test.version,
				Token: token, Worktree: dir, Port: firstPort, MaxOpenFiles: 512,
				GuardianPID: test.guardianPID, ChildPID: test.childPID}))
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			require.Error(t, guardianRecovery(ctx, dir))
			require.FileExists(t, path, "no age or PID-liveness guess permits cleanup")
		})
	}
}

func TestGuardianPartialStartReturnsRetainedCapacityContract(t *testing.T) {
	t.Parallel()
	dir, token := t.TempDir(), rand.Text()
	config := guardianConfig{Version: guardianProtocol, Worktree: dir, Port: firstPort,
		Token: token, Command: os.Args[0], Args: []string{"unused"},
		ExecArgs: []string{"unused"}, MaxOpenFiles: 512,
		StatePath: filepath.Join(dir, "guardian-"+token+".json")}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// This owned helper dies without reading config or acknowledging a child.
	// Returning nil,error would let callers incorrectly refund its reservation.
	child, err := startGuardianCommand(ctx, config, io.Discard, exec.Command(os.Args[0], "-test.run=^$"))
	require.Error(t, err)
	require.NotNil(t, child)
	var retained *guardianStartError
	require.ErrorAs(t, err, &retained)
	require.Same(t, child, retained.Child)
	require.Equal(t, config.StatePath, retained.StatePath)
	require.Positive(t, retained.GuardianPID)
	require.Zero(t, child.Process.Pid, "unknown metadata must not invent an actual child PID")
	require.Eventually(t, func() bool {
		released, err := retained.Released()
		return err == nil && released
	}, 5*time.Second, 10*time.Millisecond,
		"explicit lease/manifest proof permits eventual reservation refund")
	require.NoFileExists(t, config.StatePath)
}

func TestGuardianLeaseAndManifestFIFOsAreRefused(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "leases"), 0o700))
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, "leases", "port-"+strconv.Itoa(firstPort)+".lock"), 0o600))
	_, err := guardianAcquireLease(dir, firstPort)
	require.Error(t, err)
	path := filepath.Join(dir, "guardian-"+rand.Text()+".json")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	_, err = guardianReadRecord(path)
	require.Error(t, err)
}

// Real OS pipe framing is an integration boundary and stays outside synctest.
func TestGuardianStartupErrorStreamCanStillConfirmCleanup(t *testing.T) {
	t.Parallel()
	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = controlRead.Close(); _ = controlWrite.Close() }()
	statusRead, statusWrite, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = statusRead.Close(); _ = statusWrite.Close() }()
	controller := &guardianController{control: controlWrite, status: statusRead,
		confirmed: make(chan struct{}), uncertain: make(chan struct{}), acks: make(chan guardianEvent, 1)}
	started := make(chan guardianEvent, 1)
	go controller.read(started)
	guardianSend(statusWrite, guardianEvent{Kind: "startup-error", Error: "durable update failed"})
	require.Equal(t, "startup-error", (<-started).Kind)
	guardianSend(statusWrite, guardianEvent{Kind: "confirmed"})
	controller.observeGuardianExit()
	select {
	case <-controller.confirmed:
	case <-time.After(time.Second):
		require.FailNow(t, "startup error discarded subsequent explicit exit proof")
	}
}

// Real OS pipe framing is an integration boundary and stays outside synctest.
func TestGuardianCleanupProofDoesNotReleaseBeforeGuardianExit(t *testing.T) {
	t.Parallel()
	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = controlRead.Close(); _ = controlWrite.Close() }()
	statusRead, statusWrite, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = statusRead.Close(); _ = statusWrite.Close() }()
	controller := &guardianController{control: controlWrite, status: statusRead,
		confirmed: make(chan struct{}), uncertain: make(chan struct{}), acks: make(chan guardianEvent, 1)}
	started := make(chan guardianEvent, 1)
	readerDone := make(chan struct{})
	go func() { controller.read(started); close(readerDone) }()
	guardianSend(statusWrite, guardianEvent{Kind: "started", PID: 1})
	require.Equal(t, "started", (<-started).Kind)
	guardianSend(statusWrite, guardianEvent{Kind: "confirmed"})
	select {
	case <-readerDone:
	case <-time.After(time.Second):
		require.FailNow(t, "cleanup frame was not processed")
	}
	select {
	case <-controller.confirmed:
		require.FailNow(t, "resident guardian capacity was released before Wait proof")
	default:
	}
	controller.observeGuardianExit()
	select {
	case <-controller.confirmed:
	default:
		require.FailNow(t, "both exit proofs must release capacity")
	}
}

func TestGuardianProofAndRemovedManifestKeepLiveGuardianCapacity(t *testing.T) {
	t.Parallel()
	exit := filepath.Join(t.TempDir(), "guardian-exit")
	fixture := newGuardianFixture(t, "normal", exit)
	t.Cleanup(func() { _ = os.WriteFile(exit, nil, 0o600) })
	require.NoError(t, fixture.child.Signal(syscall.SIGTERM))
	require.Eventually(t, func() bool {
		_, err := os.Stat(exit + ".ready")
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "guardian service must finish its explicit cleanup proof")
	require.NoFileExists(t, fixture.child.StatePath)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, guardianRecovery(ctx, filepath.Dir(fixture.child.StatePath)), context.DeadlineExceeded,
		"restart cannot refund resident guardian capacity on manifest absence alone")
	select {
	case <-fixture.child.ConfirmedDone:
		require.FailNow(t, "a live resident guardian was reported reaped")
	default:
	}
	retained := &guardianStartError{StatePath: fixture.child.StatePath,
		Child: fixture.child, port: fixture.config.Port}
	released, err := retained.Released()
	require.NoError(t, err)
	require.False(t, released, "missing manifest cannot bypass the live guardian's lifetime lease")
	require.NoError(t, os.WriteFile(exit, nil, 0o600))
	select {
	case <-fixture.child.ConfirmedDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "guardian OS exit was not reaped")
	}
	released, err = retained.Released()
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, guardianRecovery(t.Context(), filepath.Dir(fixture.child.StatePath)))
}

func TestGuardianCompletionGateIsDeterministic(t *testing.T) {
	for _, proofFirst := range []bool{true, false} {
		name := "exit before proof"
		if proofFirst {
			name = "proof before exit"
		}
		t.Run(name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				controller := &guardianController{confirmed: make(chan struct{}), uncertain: make(chan struct{})}
				go func() {
					if proofFirst {
						controller.observeCleanupProof()
					} else {
						controller.observeGuardianExit()
					}
				}()
				synctest.Wait()
				select {
				case <-controller.confirmed:
					require.FailNow(t, "one proof cannot release the generation")
				default:
				}
				go func() {
					if proofFirst {
						controller.observeGuardianExit()
					} else {
						controller.observeCleanupProof()
					}
				}()
				synctest.Wait()
				select {
				case <-controller.confirmed:
				default:
					require.FailNow(t, "both proofs must release the generation")
				}
			})
		})
	}
}

func TestGuardianTelemetryRejectsFIFO(t *testing.T) {
	if stateDir := os.Getenv("GPM_REVIEW_FIFO_CHILD"); stateDir != "" {
		_, err := guardianTelemetryEnvironment(stateDir)
		require.Error(t, err)
		return
	}
	t.Parallel()
	stateDir := t.TempDir()
	dir := filepath.Join(stateDir, "telemetry-off")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, "mode"), 0o600))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGuardianTelemetryRejectsFIFO$")
	child.Env = append(os.Environ(), "GPM_REVIEW_FIFO_CHILD="+stateDir)
	out, err := child.CombinedOutput()
	require.NoError(t, ctx.Err(), "telemetry mode FIFO blocked instead of being refused")
	require.NoError(t, err, "%s", out)
}

func TestGuardianFailedStartCleanup(t *testing.T) {
	for _, mode := range []string{"already reaped", "TERM", "KILL", "kill refused", "missing proof"} {
		t.Run(mode, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				proof := make(chan struct{})
				if mode == "already reaped" {
					close(proof)
				}
				var signals []os.Signal
				child := &guardianChild{ConfirmedDone: proof, Signal: func(signal os.Signal) error {
					signals = append(signals, signal)
					if mode == "TERM" || mode == "KILL" && signal == syscall.SIGKILL {
						close(proof)
					}
					if mode == "kill refused" && signal == syscall.SIGKILL {
						return os.ErrPermission
					}
					return nil
				}}
				before := time.Now()
				err := child.stopFailedStart()
				switch mode {
				case "already reaped":
					require.NoError(t, err)
					require.Empty(t, signals)
					require.Zero(t, time.Since(before))
				case "TERM":
					require.NoError(t, err)
					require.Equal(t, []os.Signal{syscall.SIGTERM}, signals)
					require.Zero(t, time.Since(before))
				case "KILL":
					require.NoError(t, err)
					require.Equal(t, []os.Signal{syscall.SIGTERM, syscall.SIGKILL}, signals)
					require.Equal(t, exitGrace, time.Since(before))
				case "kill refused":
					require.ErrorIs(t, err, os.ErrPermission)
					require.Equal(t, exitGrace, time.Since(before))
				case "missing proof":
					require.ErrorContains(t, err, "not confirmed cleanup")
					require.Equal(t, 2*exitGrace, time.Since(before))
				}
			})
		})
	}
}
