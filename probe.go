package main

import (
	"context"
	"errors"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// A short probe bounds observation cost. A timeout is uncertainty, never
// permission to discard or kill a shared server.
var probeClient = &http.Client{Timeout: 500 * time.Millisecond}

// endpointProbe reports whether the MCP endpoint on port answered, and whether
// a negative answer is conclusive. Only a refused connection is, because only
// that one means nobody is listening; §8 has the rest of the argument.
func endpointProbe(ctx context.Context, port int) (alive, conclusive bool) {
	// The url is ours and fixed, so NewRequest cannot reject it.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, mcpURL(port), nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := probeClient.Do(req)
	if err != nil {
		// Refusal is named, not inferred from "not a timeout": every other way
		// a dial can fail — descriptors gone, host unreachable, our own context
		// cancelled — says something about this process, not about whether a
		// gopls is listening, and inferring death from it condemns live servers.
		return false, errors.Is(err, syscall.ECONNREFUSED)
	}
	defer func() { _ = resp.Body.Close() }()
	alive = resp.StatusCode == http.StatusOK && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	return alive, false
}

// withinStartGrace reports whether r names a gopls forked so recently that it
// may not have bound its port yet. Two callers need to know: a sweep, which
// must not reap it (L7), and an ensure that did not start it, which must not
// hand its caller a port that still refuses (P6). `ensure` holds the map lock
// only up to the fork now (§9), which is what puts this window in another
// process's reach at all.
//
// A timestamp in the future gets no grace, since the file is meant to be
// editable by hand and this field is the one way to make a record immortal.
func withinStartGrace(r record) bool {
	age := time.Since(time.Unix(r.StartedAt, 0))
	return age >= 0 && age < startGrace
}

// recordAlive classifies a snapshot without signalling or changing the map.
// Refused endpoints belonging to our process require termination; inconclusive
// probes retain the record. A terminating record is removed only after its
// process is gone or its PID demonstrably belongs to a different process.
func recordAlive(ctx context.Context, r record) probeVerdict {
	if ctx.Err() != nil {
		return probeUncertain
	}
	err := syscall.Kill(r.PID, 0)
	if err != nil && !errors.Is(err, syscall.EPERM) {
		return probeGone
	}
	if r.Terminating {
		return identityVerdict(ctx, r)
	}
	// Asked after the pid, not before: the grace is for a process that exists
	// and has not bound yet, and a start that crashed instead is not that. Held
	// off until the timestamp expired, such a record would answer every ensure
	// for its worktree with a dead port for the rest of the window.
	if withinStartGrace(r) {
		return probeLive
	}
	alive, conclusive := endpointProbe(ctx, r.Port)
	if alive {
		return probeLive
	}
	// One question left, and identity answers it whichever way the probe went:
	// may this process be signalled, and — where the probe proved nothing — is
	// the record worth another sweep at all. Keeping an unproven record on the
	// probe alone would be a one-way door, since nothing ever revisits a record
	// kept as alive: a port some unrelated listener took over, after a reboot
	// recycled the pid too, would leave this worktree pointed at it until the
	// map is edited by hand.
	verdict := identityVerdict(ctx, r)
	// A probe that proved nothing cannot license a signal on its own: identity
	// only says the process may be signalled, not that it needs to be. Kept
	// uncertain, the record comes back to a later sweep with a conclusive probe.
	if verdict == probeTerminate && !conclusive {
		return probeUncertain
	}
	return verdict
}

// identityVerdict asks whether the process r names is still one of ours, and
// says what that alone is worth: ours may be signalled, somebody else's means
// the record names nothing this tool manages, and no answer at all leaves the
// record for a later sweep rather than acting on a guess.
func identityVerdict(ctx context.Context, r record) probeVerdict {
	ours, err := isOurGopls(ctx, r.PID, r.Port)
	if err != nil || ctx.Err() != nil {
		return probeUncertain
	}
	if !ours {
		return probeGone
	}
	return probeTerminate
}

// Probing never mutates the registry or signals a process. Termination is an
// action requiring a durable record and a second identity check.
type probeVerdict uint8

const (
	probeUncertain probeVerdict = iota
	probeLive
	probeTerminate
	probeGone
)

// isOurGopls checks both executable spelling and the recorded listen address.
// A PID alone is not identity because the registry survives reboots. Queries
// run outside the registry lock and have a one-second ceiling.
func isOurGopls(ctx context.Context, pid, port int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	return matchesGopls(string(out), port), err
}

// matchesGopls reports whether a command line is one of ours listening on port.
// Named once because two callers ask it — this one, which decides whether a
// server may be signalled, and status, which reports the same judgement. Two
// spellings could disagree, and then status would vouch for a process the sweep
// declines to touch.
func matchesGopls(command string, port int) bool {
	return strings.Contains(command, goplsBinary) && strings.Contains(command, mcpAddress(port))
}
