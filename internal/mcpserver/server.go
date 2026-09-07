// Package mcpserver exposes the fleet daemon to an MCP client over stdio.
package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/samcm/fleet/internal/fleet"
)

type spawnInput struct {
	Agent     string `json:"agent,omitempty" jsonschema:"agent to run: omp (default, full coding agent with tools) or oracle (bare one-shot: no tools, no context files, the brief must carry everything)"`
	Model     string `json:"model" jsonschema:"full provider/model id exactly as fleet_models lists it, e.g. anthropic/claude-opus-5; a bare name is refused"`
	Thinking  string `json:"thinking" jsonschema:"thinking level on the model's ladder: off minimal low medium high xhigh max"`
	Account   string `json:"account,omitempty" jsonschema:"named account from ~/.fleet/accounts.yaml whose OAuth identity the worker is pinned to; empty applies the provider default from that file; balanced turns the pin off"`
	Cwd       string `json:"cwd,omitempty" jsonschema:"absolute path of the repo or worktree the worker edits; ignored for oracle"`
	Label     string `json:"label" jsonschema:"what this worker is for, one line, at most 15 words; shown in fleet_ls"`
	Brief     string `json:"brief,omitempty" jsonschema:"the full brief text; or use brief_path"`
	BriefPath string `json:"brief_path,omitempty" jsonschema:"absolute path of a file holding the brief"`
	Writes    bool   `json:"writes,omitempty" jsonschema:"true only for a worker that edits files; otherwise edit/delete/move tool calls are refused"`
	Minutes   int    `json:"minutes,omitempty" jsonschema:"wall-clock budget per turn in minutes (default 25, oracle 10); fleet cuts the turn at the budget and the worker is never told it"`
}

type idInput struct {
	ID string `json:"id" jsonschema:"worker id from fleet_spawn or fleet_ls, like w-1a2b3c"`
}

type resultInput struct {
	ID   string `json:"id" jsonschema:"worker id"`
	Turn string `json:"turn,omitempty" jsonschema:"which turn's reply: empty or 0 for the latest (live while running), a turn number for an earlier one, or all for every turn concatenated"`
}

type sayInput struct {
	ID      string `json:"id" jsonschema:"worker id"`
	Message string `json:"message" jsonschema:"follow-up prompt for the same session: a correction, a review to act on, or 'continue'"`
}

type lsInput struct {
	All bool `json:"all,omitempty" jsonschema:"include workers finished more than 30 minutes ago"`
}

type modelsInput struct {
	All bool `json:"all,omitempty" jsonschema:"every model in the omp catalog, not only the roster"`
}

type logInput struct {
	ID   string `json:"id" jsonschema:"worker id"`
	Tail int    `json:"tail,omitempty" jsonschema:"number of recent events to show (default 30)"`
}

type waitInput struct {
	ID      string `json:"id" jsonschema:"worker id"`
	Seconds int    `json:"seconds,omitempty" jsonschema:"how long to block for the worker to finish, capped at 240 (default 60)"`
}

// Run serves MCP over stdio until the client disconnects.
func Run(ctx context.Context, home, version string) error {
	client := fleet.NewClient(home)
	if err := client.Ensure(ctx); err != nil {
		return err
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "fleet", Version: version}, nil)

	text := func(s string, err error) (*mcp.CallToolResult, any, error) {
		if err != nil {
			// A failed tool call is reported to the caller in the result, not as
			// a protocol error, which would drop the message.
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}}}, nil, nil //nolint:nilerr // tool errors travel in the result
		}

		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}, nil, nil
	}

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_spawn",
		Description: "Start one headless coding-agent worker on the given model and return its id. The worker runs on its own; " +
			"nothing is pushed back to you. Check on it with fleet_ls, read its reply with fleet_result, steer it with fleet_say, end it with fleet_stop. " +
			"A worker that dies at launch (bad model, quota, auth) shows FAILED or QUOTA in fleet_ls within seconds; a turn whose whole reply is a provider quota error also ends QUOTA. " +
			"To be woken when it finishes, run `fleet wait <id> --timeout 5400` as a background shell command; it exits when the worker is final.",
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
			Agent: in.Agent, Model: in.Model, Thinking: in.Thinking, Account: in.Account, Cwd: in.Cwd,
			Label: in.Label, Brief: brief, Writes: in.Writes, Minutes: in.Minutes,
		}))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_ls",
		Description: "One entry per worker: state, elapsed/budget, model, label, then what it is doing right now (tool in flight and for how long, " +
			"or seconds silent, plus its last words) and token usage. States: STARTING RUNNING DONE TIMEOUT STOPPED FAILED QUOTA. " +
			"A FLAG prefix is the only thing to react to: NO-TURN (3 min with no event: dead provider or auth hang; stop and relaunch elsewhere), " +
			"STALLED (15 min silent with no tool call in flight: read fleet_log, then nudge with fleet_say or stop), " +
			"LOOPING (same tool title in 4 of the last 6 calls: stop, tighten the brief, respawn), " +
			"NO-TOOLS (10 min into a turn with no tool call: it is reasoning about files it never opened; stop and respawn). Cheap; call it whenever you want to know.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in lsInput) (*mcp.CallToolResult, any, error) {
		return text(client.Ls(ctx, in.All))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_models",
		Description: "The model roster joined live with the omp catalog: tier, the thinking levels allowed (first is the default; fleet_spawn refuses others), the ladder it supports, " +
			"price per million tokens in and out (quota for subscription models), measured tokens per second, context size and the operator's note. " +
			"Call it before choosing a model. The roster is ~/.fleet/roster.yaml; change it there. Pass all=true for the whole catalog.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in modelsInput) (*mcp.CallToolResult, any, error) {
		return text(fleet.Models(ctx, home, in.All))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_result",
		Description: "A worker's reply in full, followed by a footer with its state, usage and the list of turns. " +
			"While it is still running you get the partial output so far. Earlier turns stay readable: pass turn=N for one or turn=all for every turn.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in resultInput) (*mcp.CallToolResult, any, error) {
		return text(client.Result(ctx, in.ID, in.Turn))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_say",
		Description: "Send a follow-up prompt into the worker's live session: review findings to fix, a correction, or 'continue'. " +
			"Runs at once if the worker is idle, otherwise queues behind the current turn. Keeps the worker's context and cache; far cheaper than a new worker. " +
			"A worker idle for an hour after its last turn has been released and says so; spawn a new one then.",
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

	mcp.AddTool(server, &mcp.Tool{
		Name: "fleet_wait",
		Description: "Block until the worker is final or seconds pass (capped at 240), then return its fleet_ls entry, or \"still running\". " +
			"For anything longer, run `fleet wait <id> --timeout 5400` as a background shell command instead: it exits the moment the worker is final, which wakes you.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in waitInput) (*mcp.CallToolResult, any, error) {
		seconds := in.Seconds
		if seconds <= 0 {
			seconds = 60
		}

		seconds = min(seconds, 240)

		entry, err := client.Wait(ctx, in.ID, time.Duration(seconds)*time.Second)
		if errors.Is(err, fleet.ErrStillRunning) {
			return text(fmt.Sprintf("%s still running after %ds\n%s", in.ID, seconds, entry), nil)
		}

		return text(entry, err)
	})

	return server.Run(ctx, &mcp.StdioTransport{})
}
