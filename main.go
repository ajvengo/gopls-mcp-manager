package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gopls-mcp-manager:", err)
		os.Exit(1)
	}
}

func run() error {
	command := "bridge"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	usage := fmt.Errorf("usage: %s [bridge|ensure] [absolute-worktree-path] | list", os.Args[0])
	switch command {
	case "list":
		if len(os.Args) != 2 {
			return usage
		}
	case "bridge", "ensure":
		if len(os.Args) > 3 {
			return usage
		}
	default:
		return usage
	}

	m, err := newManager()
	if err != nil {
		return err
	}
	if command == "list" {
		return m.list(os.Stdout)
	}
	worktreeArg := "."
	if len(os.Args) > 2 {
		worktreeArg = os.Args[2]
	}
	worktree, err := worktreePath(worktreeArg)
	if err != nil {
		return err
	}
	// Starting the home gopls up front keeps a broken install a startup error
	// rather than a failed initialize halfway into a session.
	port, err := m.ensure(worktree)
	if err != nil {
		return err
	}
	if command == "ensure" {
		fmt.Println(port)
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return bridge(ctx, m, worktree)
}
