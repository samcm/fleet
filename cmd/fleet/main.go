// Command fleet runs headless ACP coding-agent workers and exposes them to
// Claude Code as MCP tools and to scripts as subcommands.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/samcm/fleet/internal/fleet"
	"github.com/samcm/fleet/internal/mcpserver"
)

func main() {
	if err := root().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "fleet:", err)
		os.Exit(1)
	}
}

func root() *cobra.Command {
	home, _ := os.UserHomeDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	cmd := &cobra.Command{Use: "fleet", Short: "headless ACP coding-agent workers", SilenceUsage: true}

	cmd.AddCommand(&cobra.Command{
		Use: "serve", Short: "run the daemon in the foreground",
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, err := fleet.NewDaemon(home, logger)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			return d.Serve(ctx)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use: "mcp", Short: "serve MCP over stdio (for Claude Code)",
		RunE: func(cmd *cobra.Command, _ []string) error { return mcpserver.Run(ctx, home) },
	})

	client := fleet.NewClient(home)
	withDaemon := func(f func(cmd *cobra.Command, args []string) (string, error)) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if err := client.Ensure(ctx); err != nil {
				return err
			}

			out, err := f(cmd, args)
			if err != nil {
				return err
			}

			fmt.Print(out)

			return nil
		}
	}

	var spec fleet.Spec
	var briefPath string

	spawn := &cobra.Command{
		Use: "spawn", Short: "start a worker",
		RunE: withDaemon(func(_ *cobra.Command, _ []string) (string, error) {
			if briefPath != "" {
				b, err := os.ReadFile(briefPath)
				if err != nil {
					return "", err
				}

				spec.Brief = string(b)
			}

			return client.Spawn(ctx, spec)
		}),
	}
	spawn.Flags().StringVar(&spec.Agent, "agent", "omp", "agent: omp or oracle")
	spawn.Flags().StringVar(&spec.Model, "model", "", "full provider/model id")
	spawn.Flags().StringVar(&spec.Thinking, "thinking", "", "thinking level")
	spawn.Flags().StringVar(&spec.Cwd, "cwd", "", "absolute working directory")
	spawn.Flags().StringVar(&spec.Label, "label", "", "one line, at most 15 words")
	spawn.Flags().StringVar(&briefPath, "brief", "", "path to the brief file")
	spawn.Flags().BoolVar(&spec.Writes, "writes", false, "allow edits")
	spawn.Flags().IntVar(&spec.Minutes, "minutes", 0, "budget per turn (default 25, oracle 10)")
	cmd.AddCommand(spawn)

	var all bool

	ls := &cobra.Command{
		Use: "ls", Short: "list workers",
		RunE: withDaemon(func(_ *cobra.Command, _ []string) (string, error) { return client.Ls(ctx, all) }),
	}
	ls.Flags().BoolVar(&all, "all", false, "include finished workers")
	cmd.AddCommand(ls)

	cmd.AddCommand(&cobra.Command{
		Use: "say <id> <message...>", Short: "send a follow-up", Args: cobra.MinimumNArgs(2),
		RunE: withDaemon(func(_ *cobra.Command, args []string) (string, error) {
			return client.Say(ctx, args[0], strings.Join(args[1:], " "))
		}),
	})
	cmd.AddCommand(&cobra.Command{
		Use: "result <id>", Short: "latest reply and footer", Args: cobra.ExactArgs(1),
		RunE: withDaemon(func(_ *cobra.Command, args []string) (string, error) { return client.Result(ctx, args[0]) }),
	})
	cmd.AddCommand(&cobra.Command{
		Use: "stop <id>", Short: "end a worker", Args: cobra.ExactArgs(1),
		RunE: withDaemon(func(_ *cobra.Command, args []string) (string, error) { return client.Stop(ctx, args[0]) }),
	})

	var waitTimeout, watchTimeout int

	waitCmd := &cobra.Command{
		Use: "wait <id>", Short: "block until the worker is final (exit 3 on timeout)", Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := client.Ensure(ctx); err != nil {
				return err
			}

			out, err := client.Wait(ctx, args[0], time.Duration(waitTimeout)*time.Second)
			fmt.Print(out)

			if errors.Is(err, fleet.ErrStillRunning) {
				fmt.Printf("%s still running after %ds\n", args[0], waitTimeout)
				os.Exit(3)
			}

			return err
		},
	}
	waitCmd.Flags().IntVar(&waitTimeout, "timeout", 1800, "seconds to wait")
	cmd.AddCommand(waitCmd)

	watchCmd := &cobra.Command{
		Use: "watch [ids...]", Short: "print a line per state change or flag until all are final",
		RunE: func(_ *cobra.Command, args []string) error {
			if err := client.Ensure(ctx); err != nil {
				return err
			}

			return client.Watch(ctx, args, time.Duration(watchTimeout)*time.Second, os.Stdout)
		},
	}
	watchCmd.Flags().IntVar(&watchTimeout, "timeout", 3600, "seconds before giving up")
	cmd.AddCommand(watchCmd)

	var tail int

	logCmd := &cobra.Command{
		Use: "log <id>", Short: "recent events", Args: cobra.ExactArgs(1),
		RunE: withDaemon(func(_ *cobra.Command, args []string) (string, error) { return client.Log(ctx, args[0], tail) }),
	}
	logCmd.Flags().IntVar(&tail, "tail", 30, "events to show")
	cmd.AddCommand(logCmd)

	return cmd
}
