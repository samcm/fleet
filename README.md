# fleet

Headless coding-agent workers over the [Agent Client Protocol](https://agentclientprotocol.com), for a coding agent to drive: Claude Code reaches them as MCP tools, or as a CLI through its shell.

One worker is one agent process on the model you name, held by a daemon that outlives the session that spawned it. State comes from the ACP event stream, not from parsing transcripts, so `fleet ls` shows what a worker is doing now. Nothing is pushed to you; you ask.

## Why

Claude Code's built-in subagents all run on your Anthropic account, so "get a second opinion from another vendor" is not something it can do. Its subagents also die with the session that spawned them, which rules out an hour-long build.

fleet gives one Claude Code session a set of long-lived workers on whatever models the agent binary can reach, and a way to check on them without sitting there.

## How I use it

I never run these commands. Claude Code does, and I talk to Claude.

The whole interface is a sentence naming a model and a role:

> use fleet to consult gpt-5.6-sol and kimi k3 at max to critique the plan

> get a fleet agent to do the grunt work and get fable to review it

> do all the followups now, use fleet with k3 for some of the grunt work

Claude writes the brief, picks the budget, spawns the worker, arms a `fleet wait` in the background so it gets woken, reads the result, and sends review findings back with `fleet say` until the piece is clean. I see the summary.

That shapes the design. The MCP tools and the CLI do the same things because the caller is an agent either way: the CLI is what it reaches for from a shell tool, and `fleet wait` blocking until a worker is final is how an agent gets a wake-up without polling. Nothing is pushed, because there is nobody watching a screen.

The loop it settles into: split the work into pieces, one writing worker per piece on a strong builder, then a read-only reviewer on a *different vendor*, then findings back to the builder, repeat. Five rounds on one piece is normal. That last part is the point — a model reviewing its own work agrees with itself.

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

`--model` takes a full `provider/model` selector and a bare model name is refused at spawn; `fleet models` lists the roster and `omp models` the whole catalog. `--thinking` takes an effort level that model supports, which varies — `omp models --json` has the ladder per model.

`--account` pins the worker to one named OAuth account from `~/.fleet/accounts.yaml`, read again on every spawn: `accounts` maps a name to `{provider, identity}`, where `identity` is omp's OAuth identity key in the form `email:<x>|org:<y>`, and `defaults` maps a provider to the account a spawn that names none gets — no default for that provider means no pin. The provider is the part of `--model` before the slash. `--account balanced` turns the pin off and leaves omp's own balancing in charge.

The identity never reaches the worker's argv or environment: fleet writes it into a pool file in the worker directory and points omp at that file with `OMP_AUTH_BROKER_ACCOUNT_POOL_FILE`, so the process sees only that one account for that provider.

## Commands

```sh
fleet spawn [flags]     # --model --thinking --account --cwd --label --brief --writes --minutes --agent
fleet ls [--all]        # state, elapsed/budget, model, label, current activity, tokens, context fill, cost
fleet models [--all]    # the roster joined with the live omp catalog: tier, thinking, ladder, price, speed, context
fleet result <id>       # --turn N for an earlier turn, --turn all for every turn
fleet say <id> "<msg>"  # follow-up on the live session; keeps its context and cache
fleet stop <id>
fleet log <id> [--tail N]
fleet wait <id> [--timeout 1800]       # blocks until final, exit 3 on timeout
fleet watch [ids...] [--timeout 3600]  # a line per state change or flag
fleet dashboard [--addr 127.0.0.1:7770] # the status wall, for a browser
fleet version
```

`--minutes` is the per-turn budget, default 25 (10 for `oracle`); fleet cuts the turn there and the worker is never told the clock. `--agent` defaults to `omp`.

`~/.fleet/roster.yaml` is the model roster: `models` maps a full selector to `{thinking, tier, tps, note}`; `thinking` is one level or a list of allowed levels, first the default, and `fleet spawn` refuses a level outside it. `fleet models` and the `fleet_models` MCP tool print it against the live catalog (ladder, price, context), so the operator's view of the models lives in one file and the catalog facts come from omp.

To be woken, run `fleet wait` as a background command, or `fleet watch` for several. Both take a timeout.

`fleet dashboard` serves a 1920×1080 status wall for a screen: one lane per worker `ls` would show, with the provider mark, the repository its tree belongs to (a worktree is named after its main checkout), model and thinking level, what it is doing now (the tool call in flight, else the last sentence it wrote, else the first line of its reply once it is done), a hairline of elapsed against the turn budget, context fill, tokens and cost; under it, a day of concurrency, tokens and spend by model, and an hour of tool calls per minute. It is read-only, polls the daemon every three seconds, and listens on loopback unless `--addr` says otherwise. The agent reports tokens and cost over ACP only when a turn ends, so during a turn the daemon reads them from the session journal omp writes (`sessions/<tree>/<time>_<session id>.jsonl` under the agent directory), every two seconds; context fill still arrives at turn end.

## States

Final: `DONE`, `TIMEOUT`, `STOPPED`, `FAILED`, `QUOTA`. `DONE` means the worker stopped, not that the work is right.

Flags on a running worker: `NO-TURN` (no event for 180s), `STALLED` (900s silent with no tool call in flight), `LOOPING` (same tool title in four of the last six calls), `NO-TOOLS` (ten minutes into a turn with no tool call at all; the seat is reasoning about files it never opened). A turn whose whole reply is a provider quota error ends `QUOTA`, not `DONE`.

`FAILED` or `QUOTA` in the first status line is the launch dying — unknown model, unsupported effort, auth, or a provider limit. Fix the spec rather than retrying it unchanged. After launch, `fleet log <id>` and `~/.fleet/workers/<id>/stderr.log` have the detail. `TIMEOUT` keeps its result and its context, so `fleet say "continue"` is usually better than respawning. A worker that sits `DONE` or `TIMEOUT` for an hour with no follow-up is released: its agent process ends, its files and its `fleet ls --all` entry stay, and `fleet say` then asks for a new worker.

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

The host journals agent stdout to `acp.jsonl` with a stream position and forwards it to the daemon. The worker persists the position it has handled (`ack_seq`) plus the in-flight prompt's call id and deadline. A restarted daemon dials `host.sock`, sends `{"resume":N}`, and the host replays past that point before going live, so the turn's response and any permission requests in the gap are handled exactly once. A worker whose host is gone is marked `FAILED`; a worker the daemon gives up on at startup has its host ended.

## Development

```sh
go test -race ./...
```

Hermetic: a fake ACP agent in `internal/fleet/testdata/fakeagent` stands in for a real one. Temp homes live under `/tmp` because macOS caps unix socket paths at 104 bytes.

## License

MIT.
