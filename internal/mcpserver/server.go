// Package mcpserver exposes the fleet daemon to an MCP client over stdio.
package mcpserver

import (
	"context"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/samcm/fleet/internal/fleet"
)

type spawnInput struct {
	Agent     string `json:"agent,omitempty" jsonschema:"agent to run: omp (default, full coding agent with tools) or oracle (bare one-shot: no tools, no context files, the brief must carry everything)"`
	Model     string `json:"model" jsonschema:"full provider/model id exactly as omp names it, e.g. openai-codex/gpt-5.6-terra"`
	Thinking  string `json:"thinking" jsonschema:"thinking level on the model's ladder: off minimal low medium high xhigh max"`
	Cwd       string `json:"cwd,omitempty" jsonschema:"absolute path of the repo or worktree the worker edits; ignored for oracle"`
	Label     string `json:"label" jsonschema:"what this worker is for, one line, at most 15 words; shown in fleet_ls"`
	Brief     string `json:"brief,omitempty" jsonschema:"the full brief text; or use brief_path"`
	BriefPath string `json:"brief_path,omitempty" jsonschema:"absolute path of a file holding the brief"`
	Writes    bool   `json:"writes,omitempty" jsonschema:"true only for a worker that edits files; otherwise edit/delete/move tool calls are refused"`
	Minutes   int    `json:"minutes,omitempty" jsonschema:"wall-clock budget per turn in minutes (default 25, oracle 10); the worker is told its hard stop"`
}

type idInput struct {
	ID string `json:"id" jsonschema:"worker id from fleet_spawn or fleet_ls, like w-1a2b3c"`
}

type sayInput struct {
	ID      string `json:"id" jsonschema:"worker id"`
	Message string `json:"message" jsonschema:"follow-up prompt for the same session: a correction, a review to act on, or 'continue'"`
}

type lsInput struct {
	All bool `json:"all,omitempty" jsonschema:"include workers finished more than 30 minutes ago"`
}

type logInput struct {
	ID   string `json:"id" jsonschema:"worker id"`
	Tail int    `json:"tail,omitempty" jsonschema:"number of recent events to show (default 30)"`
}

// Run serves MCP over stdio until the client disconnects.
func Run(ctx context.Context, home string) error {
	client := fleet.NewClient(home)
	if err := client.Ensure(ctx); err != nil {
		return err
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "fleet", Version: "0.1.0"}, nil)

	text := func(s string, err error) (*mcp.CallToolResult, any, error) {
		if err != nil {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}}}, nil, nil
		}

		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}, nil, nil
	}

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_spawn",
		Description: "Start one headless coding-agent worker on the given model and return its id. The worker runs on its own; " +
			"nothing is pushed back to you. Check on it with fleet_ls, read its reply with fleet_result, steer it with fleet_say, end it with fleet_stop. " +
			"A worker that dies at launch (bad model, quota, auth) shows FAILED or QUOTA in fleet_ls within seconds.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in spawnInput) (*mcp.CallToolResult, any, error) {
		brief := in.Brief
		if in.BriefPath != "" {
			b, err := os.ReadFile(in.BriefPath)
			if err != nil {
				return text("", fmt.Errorf("read brief_path: %w", err))
			}

			brief = string(b)
		}

		return text(client.Spawn(ctx, fleet.Spec{
			Agent: in.Agent, Model: in.Model, Thinking: in.Thinking, Cwd: in.Cwd, Label: in.Label,
			Brief: brief, Writes: in.Writes, Minutes: in.Minutes,
		}))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_ls",
		Description: "One entry per worker: state, elapsed/budget, model, label, then what it is doing right now (tool in flight and for how long, " +
			"or seconds silent, plus its last words) and token usage. A FLAG prefix means NO-TURN, STALLED or LOOPING. Cheap; call it whenever you want to know.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in lsInput) (*mcp.CallToolResult, any, error) {
		return text(client.Ls(ctx, in.All))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "fleet_result",
		Description: "The worker's latest reply in full, followed by a footer with its final state, usage and detail. While it is still running you get the partial output so far.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in idInput) (*mcp.CallToolResult, any, error) {
		return text(client.Result(ctx, in.ID))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_say",
		Description: "Send a follow-up prompt into the worker's live session: review findings to fix, a correction, or 'continue'. " +
			"Runs at once if the worker is idle, otherwise queues behind the current turn. Keeps the worker's context and cache; far cheaper than a new worker.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in sayInput) (*mcp.CallToolResult, any, error) {
		return text(client.Say(ctx, in.ID, in.Message))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "fleet_stop",
		Description: "Cancel the worker's current turn and end its process. Its partial reply stays readable with fleet_result.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in idInput) (*mcp.CallToolResult, any, error) {
		return text(client.Stop(ctx, in.ID))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "fleet_log",
		Description: "The worker's recent events: tool calls with durations, permission decisions, turn ends, process exit. Use it when fleet_ls looks wrong before deciding to stop.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in logInput) (*mcp.CallToolResult, any, error) {
		return text(client.Log(ctx, in.ID, in.Tail))
	})

	return server.Run(ctx, &mcp.StdioTransport{})
}
