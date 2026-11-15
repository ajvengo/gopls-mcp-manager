package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(os.Args, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "gopls-mcp-manager:", err)
		os.Exit(1)
	}
}

// The command line and stdout are arguments rather than read off the process,
// so that the argument grammar below is reachable from a test — every path past
// it forks a gopls or shells out to git, and the grammar is the part that
// decides which.
func run(args []string, stdout io.Writer) error {
	if len(args) > 1 {
		switch args[1] {
		case "internal-guardian":
			return runGuardian(args[2:])
		case "internal-restart-check":
			return runRestartCheck(stdout)
		case "internal-restart-child":
			return runRestartChild()
		case "restart", "reload":
			return restartCommand(args[2:], args[1] == "reload", stdout)
		}
	}
	command := "bridge"
	if len(args) > 1 {
		command = args[1]
	}
	usage := fmt.Errorf("usage: %s [bridge|ensure] [worktree-path] | http [listen-address [worktree-path]] | supervisor | list | status | trim-logs | restart [--hot] [--binary path] [--timeout duration] | reload [--binary path] [--timeout duration]", args[0])
	switch command {
	case "http":
		if len(args) > 4 {
			return usage
		}
	case "list", "status", "trim-logs", "supervisor":
		if len(args) != 2 {
			return usage
		}
	case "bridge", "ensure":
		if len(args) > 3 {
			return usage
		}
	default:
		return usage
	}

	m, err := newManager()
	if err != nil {
		return err
	}
	// Cover startup and list's lock wait as well as the running bridge.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if command == "supervisor" {
		return runSupervisor(ctx, m)
	}
	if command == "trim-logs" {
		logs, err := m.logs(ctx, true)
		if err != nil {
			return err
		}
		return json.MarshalEncode(jsontext.NewEncoder(stdout), logs)
	}
	if command == "status" {
		return m.status(ctx, stdout)
	}
	if command == "list" {
		if err := supervisorPing(ctx, m); err == nil {
			return querySupervisor(ctx, m, "/list", "", stdout)
		} else if !supervisorAbsent(err) {
			return err
		}
		return m.list(ctx, stdout)
	}
	worktreeArg := "."
	if command == "http" {
		if len(args) > 3 {
			worktreeArg = args[3]
		}
	} else if len(args) > 2 {
		worktreeArg = args[2]
	}
	worktree, err := worktreePath(ctx, worktreeArg)
	if err != nil {
		return err
	}
	if err := ensureSupervisor(ctx, m); err != nil {
		return err
	}
	if command == "ensure" {
		return querySupervisor(ctx, m, "/ensure", worktree, stdout)
	}
	if command == "http" {
		address := "127.0.0.1:6099"
		if len(args) > 2 {
			address = args[2]
		}
		return proxyHTTPViaSupervisor(ctx, m, worktree, address, stdout)
	}

	return restartBridge(ctx, m, worktree, os.Stdin, os.Stdout)
}

// Acquisition probes only the requested worktree. Sweep independently of
