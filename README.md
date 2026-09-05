# fleet

Headless coding-agent workers over the [Agent Client Protocol](https://agentclientprotocol.com), driven from Claude Code as MCP tools or from a shell as a CLI.

One worker is one agent process on the model you name, held by a daemon that outlives the session that spawned it. State comes from the ACP event stream, not from parsing transcripts, so `fleet ls` shows what a worker is doing now. Nothing is pushed to you; you ask.

It exists to run work on models other than the one you are talking to. Claude Code's built-in subagents all run on your Anthropic account.

## Requirements

- Go 1.25+ to build. Linux or macOS; the daemon uses unix sockets.
- An ACP agent binary. The built-in definitions target [`omp`](https://www.npmjs.com/package/@oh-my-pi/pi-coding-agent) (`omp acp`), found on `PATH` or the usual install paths. Others work via `agents.json`.

fleet holds no credentials. The agent uses whatever providers it is already configured for; a model it cannot reach fails the spawn in about a second.

## Install

```sh
go install github.com/samcm/fleet/cmd/fleet@latest
```

Or take a binary from [releases](https://github.com/samcm/fleet/releases).

```sh
claude mcp add --scope user fleet -- fleet mcp
```

That exposes `fleet_spawn`, `fleet_ls`, `fleet_result`, `fleet_say`, `fleet_stop`, `fleet_log` and `fleet_wait`. The daemon auto-starts on first contact.

## First worker

A worker reads its brief from a file, and sees nothing else: no conversation history, no earlier workers. Say what to do, which paths, what to verify, what to report.

```sh
echo 'Read internal/fleet/daemon.go. In under 200 words, explain how a worker
gets from STARTING to RUNNING, naming the functions. Do not edit anything.' > /tmp/brief.md

fleet spawn --model openai-codex/gpt-5.6-terra --thinking high \
  --cwd "$PWD" --label "explain startup" --brief /tmp/brief.md --minutes 10

fleet wait w-4e94b3 --timeout 900
fleet result w-4e94b3
```

Without `--writes` the agent runs with `--approval-mode always-ask` and fleet refuses every edit, delete and move, so the worker cannot touch the tree. It cannot run tests either.

`--model` takes a full `provider/model` selector; `omp models` lists them. `--thinking` takes an effort level that model supports, which varies — `omp models --json` has the ladder per model.

## Commands

```sh
fleet spawn [flags]     # --model --thinking --cwd --label --brief --writes --minutes --agent
fleet ls [--all]        # state, elapsed/budget, model, label, current activity, tokens, context fill, cost
fleet result <id>       # --turn N for an earlier turn, --turn all for every turn
fleet say <id> "<msg>"  # follow-up on the live session; keeps its context and cache
fleet stop <id>
fleet log <id> [--tail N]
fleet wait <id> [--timeout 1800]       # blocks until final, exit 3 on timeout
fleet watch [ids...] [--timeout 3600]  # a line per state change or flag
fleet version
```

`--minutes` is the per-turn budget, default 25 (10 for `oracle`). `--agent` defaults to `omp`.

To be woken, run `fleet wait` as a background command, or `fleet watch` for several. Both take a timeout.

## States

Final: `DONE`, `TIMEOUT`, `STOPPED`, `FAILED`, `QUOTA`. `DONE` means the worker stopped, not that the work is right.

Flags on a running worker: `NO-TURN` (no event for 180s), `STALLED` (900s silent with no tool call in flight), `LOOPING` (same tool title in four of the last six calls).

`FAILED` or `QUOTA` in the first status line is the launch dying — unknown model, unsupported effort, auth, or a provider limit. Fix the spec rather than retrying it unchanged. After launch, `fleet log <id>` and `~/.fleet/workers/<id>/stderr.log` have the detail. `TIMEOUT` keeps its result and its context, so `fleet say "continue"` is usually better than respawning.

## Shared trees

Workers in one tree cannot see each other:

- No `git stash`, `checkout`, `reset`, `clean`, `restore` or branch switching. Read old versions with `git show <ref>:<path>`.
- The tree does not compile while parallel editors run. Tell them to skip building and testing, and gate once at integration.
- No concurrent project-wide test runs; they corrupt each other's caches. Name the checks each worker runs.

A worktree per worker avoids all of it.

## Configuration

Under `~/.fleet`:

| Path | What it is |
|---|---|
| `agents.json` | Agent definitions, overlaid on the built-ins |
| `rules.md` | Appended to every `omp` worker's system prompt |
| `bare.md`, `bare.yml` | System prompt and config for `oracle` |
| `workers/<id>/` | `meta.json`, `brief.md`, `events.jsonl`, `promptN.md`, `resultN.md`, `stderr.log`, and the host's `launch.json`, `host.log`, `host.sock`, `acp.jsonl` |

Built-ins: `omp`, the normal worker with tools, and `oracle`, a toolless one-shot for questions that need a model rather than a workspace (~400 tokens of overhead).

```json
{
  "my-agent": {
    "argv": ["/abs/path/to/agent", "acp", "--flag"],
    "env": ["KEY=value"],
    "bare": false
  }
}
```

`bare` tells fleet the agent has no tools and the working directory is irrelevant.

## Surviving a restart

Each worker's agent runs under its own detached host process (`fleet host <workerdir>`), so restarting the daemon does not kill live workers.

The host journals agent stdout to `acp.jsonl` with a stream position and forwards it to the daemon. The worker persists the position it has handled (`ack_seq`) plus the in-flight prompt's call id and deadline. A restarted daemon dials `host.sock`, sends `{"resume":N}`, and the host replays past that point before going live, so the turn's response and any permission requests in the gap are handled exactly once. A worker whose host is gone is marked `FAILED`.

## Development

```sh
go test -race ./...
```

Hermetic: a fake ACP agent in `internal/fleet/testdata/fakeagent` stands in for a real one. Temp homes live under `/tmp` because macOS caps unix socket paths at 104 bytes.

## License

MIT.
