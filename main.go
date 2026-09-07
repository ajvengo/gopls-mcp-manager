package main

import (
	"context"
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
	command := "bridge"
	if len(args) > 1 {
		command = args[1]
	}
	usage := fmt.Errorf("usage: %s [bridge|ensure] [worktree-path] | list", args[0])
	switch command {
	case "list":
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
	if command == "list" {
		return m.list(ctx, stdout)
	}
	worktreeArg := "."
	if len(args) > 2 {
		worktreeArg = args[2]
	}
	worktree, err := worktreePath(ctx, worktreeArg)
	if err != nil {
		return err
	}
	// Starting the home gopls up front keeps a broken install a startup error
	// rather than a failed initialize halfway into a session.
	port, err := m.ensure(ctx, worktree)
	if err != nil {
		return err
	}
	if command == "ensure" {
		_, err := fmt.Fprintln(stdout, port)
		return err
	}

	return bridge(ctx, m, worktree)
}
