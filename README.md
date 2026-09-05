# fleet

Run headless coding-agent workers over the [Agent Client Protocol](https://agentclientprotocol.com), and drive them from Claude Code as MCP tools or from a shell as a CLI.

One worker is one agent process on the model you name, held by a daemon that outlives the session that spawned it. You spawn work, go do something else, and collect the reply later. Worker state comes from the ACP event stream rather than from parsing transcripts, so `fleet ls` reports what a worker is doing right now, not what it last printed.

This exists to fan work out to models other than the one you are talking to. Claude Code's built-in subagents all run on your Anthropic account; a fleet worker runs whatever the agent binary can reach.

## Requirements

- Go 1.25 or newer to build.
- An ACP-capable agent binary. The built-in agent definitions target [`omp`](https://www.npmjs.com/package/@oh-my-pi/pi-coding-agent) (`omp acp`), found on `PATH` or at the usual install locations. Any other ACP agent works through `agents.json`.
- Linux or macOS. The daemon uses unix sockets.

## Install

```sh
go install github.com/samcm/fleet/cmd/fleet@latest
```

Or download a binary from the [releases page](https://github.com/samcm/fleet/releases) and put it on your `PATH`.

Register it with Claude Code as an MCP server:

```sh
claude mcp add --scope user fleet -- fleet mcp
```

That exposes `fleet_spawn`, `fleet_ls`, `fleet_result`, `fleet_say`, `fleet_stop`, `fleet_log` and `fleet_wait`. The daemon starts on its own the first time any client talks to it, so there is nothing to run by hand and nothing to add to your login items.

## Use

```sh
fleet spawn --model openai-codex/gpt-5.6-terra --thinking xhigh \
  --cwd /abs/repo --label "port the parser" --brief /abs/brief.md --writes --minutes 25

fleet ls                  # state, elapsed/budget, model, label, current activity, tokens, context fill, cost
fleet result <id>         # the reply; --turn N for an earlier turn, --turn all for every turn
fleet say <id> "<msg>"    # follow-up on the same live session; queues if the worker is busy
fleet stop <id>
fleet log <id> --tail 50
```

Workers are asynchronous and nothing is pushed to you. To be woken when one finishes, run its wait as a background command:

```sh
fleet wait <id> --timeout 5400      # blocks until final, exit 3 on timeout
fleet watch [ids...] --timeout 3600 # one line per state change or flag
```

Inside a tool call, `fleet_wait {id, seconds}` does the same for up to 240 seconds and returns either the `ls` entry or "still running".

A worker ends in `DONE`, `TIMEOUT`, `STOPPED`, `FAILED` or `QUOTA`. While running it can carry a flag: `NO-TURN` (no event for 180s), `STALLED` (silent for 900s), or `LOOPING` (the same tool title in four of the last six calls).

### Read-only and writing workers

`--writes` decides how much the worker is allowed to do. Without it the agent runs with `--approval-mode always-ask` and fleet refuses every edit, delete and move request, so a review worker cannot touch the tree. With it the agent runs unattended and may edit freely. Each turn gets the `--minutes` budget, and the brief tells the worker its hard stop.

### Briefs

`--brief` takes a file, not a string. A worker sees only its brief: no conversation history, no prior context. Say what to do, which paths matter, what to verify, and what to report back.

## Configuration

Everything lives under `~/.fleet`:

| Path | What it is |
|---|---|
| `agents.json` | Agent definitions, overlaid on the built-ins |
| `rules.md` | Appended to the system prompt of every `omp` worker |
| `bare.md`, `bare.yml` | System prompt and config for the toolless `oracle` agent |
| `workers/<id>/` | `meta.json`, `brief.md`, `events.jsonl`, `promptN.md`, `resultN.md`, `stderr.log`, and the host's `launch.json`, `host.log`, `host.sock`, `acp.jsonl` |

Two agents are built in: `omp`, the normal worker with tools, and `oracle`, a toolless one-shot for questions that need a model rather than a workspace (roughly 400 tokens of overhead). Add your own, or replace a built-in by reusing its name:

```json
{
  "my-agent": {
    "argv": ["/abs/path/to/agent", "acp", "--flag"],
    "env": ["KEY=value"],
    "bare": false
  }
}
```

Set `bare` when the agent has no tools, which tells fleet the working directory is irrelevant and the brief must carry everything.

## Surviving a restart

Each worker's agent runs under its own host process (`fleet host <workerdir>`, started detached by the daemon), so restarting the daemon does not kill live workers.

The host journals every agent stdout line to `acp.jsonl` with a stream position and forwards it to the daemon. The worker persists the position it has handled (`ack_seq`), plus the in-flight prompt's call id and deadline. A restarted daemon dials `host.sock`, sends `{"resume":N}`, and the host replays the journal past that point before going live, so the turn's response and any permission requests in the gap are handled exactly once. A worker whose host is gone is marked `FAILED`.

## Development

```sh
go test ./...           # hermetic; a fake ACP agent stands in for a real one
go test -race ./...
go build ./...
```

Tests use temp homes under `/tmp` because macOS caps unix socket paths at 104 bytes.

## License

MIT. See [LICENSE](LICENSE).
