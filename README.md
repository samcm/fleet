# fleet

Run headless coding-agent workers over the [Agent Client Protocol](https://agentclientprotocol.com), and drive them from Claude Code as MCP tools or from a shell as a CLI.

One worker is one agent process on the model you name, held by a daemon that outlives the session that spawned it. You spawn work, go do something else, and collect the reply later. Worker state comes from the ACP event stream rather than from parsing transcripts, so `fleet ls` reports what a worker is doing right now, not what it last printed.

This exists to fan work out to models other than the one you are talking to. Claude Code's built-in subagents all run on your Anthropic account; a fleet worker runs whatever the agent binary can reach.

## Requirements

- Go 1.25 or newer to build.
- An ACP-capable agent binary. The built-in agent definitions target [`omp`](https://www.npmjs.com/package/@oh-my-pi/pi-coding-agent) (`omp acp`), found on `PATH` or at the usual install locations. Any other ACP agent works through `agents.json`.
- Linux or macOS. The daemon uses unix sockets.

fleet does not handle model credentials. It launches the agent binary, and the agent uses whatever providers it is already configured for. Before spawning anything, check that the model you want appears in `omp models` and that `omp usage` lists the account behind it. A worker naming a model the agent cannot reach fails within a second or two.

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

## First worker

A worker reads a brief from a file. Write one:

```sh
cat > /tmp/brief.md <<'EOF'
Read internal/fleet/daemon.go and explain, in under 200 words, how a worker
gets from STARTING to RUNNING. Name the functions involved. Do not edit
anything.
EOF
```

Pick a model the agent can reach, then spawn:

```sh
fleet spawn \
  --model openai-codex/gpt-5.6-terra \
  --thinking high \
  --cwd "$PWD" \
  --label "explain worker startup" \
  --brief /tmp/brief.md \
  --minutes 10
```

That prints a worker id such as `w-4e94b3` and returns immediately. Check on it, wait for it, then read the reply:

```sh
fleet ls                      # what it is doing right now
fleet wait w-4e94b3 --timeout 900
fleet result w-4e94b3
```

The worker above cannot edit anything, because `--writes` was not passed. Add `--writes` when you want it to change the tree.

## Choosing a model

`--model` takes a full `provider/model` selector, exactly as the agent names it. List what is available:

```sh
omp models              # every model, grouped by provider
omp models openai-codex # one provider
```

`--thinking` takes one of the effort levels that model supports, which differ per model: usually some subset of `low`, `medium`, `high`, `xhigh` and `max`. `omp models --json` reports the ladder for each. Naming a level a model does not support fails the spawn.

## Writing a brief

The brief is the whole interface to a worker. It sees the brief and the working directory, and nothing else: no conversation history, no prior context, no memory of earlier workers. Anything you leave implicit is simply absent.

A brief that works usually says:

- what to do, concretely, and what "done" looks like
- which paths matter, as absolute paths
- what to verify, and which command proves it
- what to report back
- what not to touch

Briefs are files rather than strings because they are usually longer than a shell line, and because keeping them on disk means you can diff and reuse them.

## Using workers well

These are the habits that make a fleet of workers cheaper and better than doing the work in one place. They are about judgement, not flags.

**Spawn only when it pays.** A worker costs a brief to write, tokens to run, and wall clock to wait for. Four lines in a file you already have open are yours to change.

**Start cheap and escalate on evidence.** Do not try to predict which model a slice needs. Run the cheapest one that should clear the bar, review the result, and escalate only when the review shows it fell short. Predicting spends the expensive model every time in case it was needed; escalating spends it only when it was.

**Escalate, do not race.** Running a cheap worker and an expensive worker on the same slice at once is not starting cheap. You pay for both and read one.

**Review across vendors.** The reviewer should be a different vendor from the one that produced the code, and ideally a stronger model. A model reviewing its own output agrees with itself. When a review matters, run the same review on two vendors and compare, rather than splitting one review into thirds — splitting removes the disagreement that makes review worth doing.

**Do not cut review to save budget.** Reviews can run on cheap models; the rework they prevent runs on the expensive one.

**Treat agreement as a reason to look closer.** Two workers missing the same requirement usually means the brief was ambiguous, not that the requirement is met.

**Guard your own context.** Everything you read yourself stays in your window and is re-read on every later turn. Send log trawls, grep sweeps and build output to a worker that returns the conclusion, not the transcript.

**Follow up rather than relaunch.** `fleet say` keeps the worker's context and prompt cache, so sending review findings back to the worker that wrote the code is much cheaper than spawning a fresh one to redo it.

**Arm one wake-up and go do something else.** Nothing is pushed to you. Run `fleet wait` in the background for a single worker, or `fleet watch` for several, and give either a timeout. Polling `fleet ls` in a loop burns tokens to learn nothing.

### Shared trees

Several workers in one working directory need rules, because they cannot see each other:

- No worker may run `git stash`, `checkout`, `reset`, `clean`, `restore`, or switch branches. Another worker's uncommitted work is in that tree. To read an earlier version, use `git show <ref>:<path>`.
- While parallel workers are editing, the tree does not compile. Say so in the brief, tell them to skip building and testing, and gate once at integration when they have all finished.
- Do not have workers run the project-wide test suite concurrently; parallel runs corrupt each other's caches. Name the specific checks each worker should run, or run them yourself afterwards.

Giving each worker its own git worktree avoids all of this, at the cost of a checkout per worker.

## Reference

```sh
fleet spawn [flags]     # --model --thinking --cwd --label --brief --writes --minutes --agent
fleet ls [--all]        # state, elapsed/budget, model, label, current activity, tokens, context fill, cost
fleet result <id>       # the reply; --turn N for an earlier turn, --turn all for every turn
fleet say <id> "<msg>"  # follow-up on the same live session; queues if the worker is busy
fleet stop <id>
fleet log <id> [--tail N]
fleet wait <id> [--timeout 1800]      # blocks until final, exit 3 on timeout
fleet watch [ids...] [--timeout 3600] # one line per state change or flag
fleet version
```

`--minutes` is the budget for each turn, defaulting to 25 (10 for `oracle`), and the brief tells the worker its hard stop. `--agent` selects the agent definition, defaulting to `omp`.

Nothing is pushed to you. To be woken when a worker finishes, run its wait as a background command; it exits when the worker is final. Inside a tool call, `fleet_wait {id, seconds}` does the same for up to 240 seconds and returns either the `ls` entry or "still running".

### States and flags

A worker ends in `DONE`, `TIMEOUT`, `STOPPED`, `FAILED` or `QUOTA`. While running it can carry a flag: `NO-TURN`, `STALLED` or `LOOPING`. A flag is a hint that a worker needs looking at, not a state.

| Outcome | What it means | What to do |
|---|---|---|
| `FAILED` / `QUOTA` at launch | The agent rejected the spawn: unknown model, unsupported effort, auth, or a provider refusing on limits | Fix the spec or pick another provider. Never retry the same spawn unchanged |
| `NO-TURN` | Running 3 minutes with no event at all | Usually a dead provider or an auth hang. Stop it and relaunch elsewhere |
| `STALLED` | 15 minutes with no event and no tool call in flight | Read `fleet log` first: a long generation streams text and does not stall. Then nudge with `say`, or stop |
| `LOOPING` | The same tool title in four of the last six calls | Stop it, tighten the brief, spawn again |
| `TIMEOUT` | The turn hit its budget | `fleet result` still has what it produced. If the work was converging, `fleet say "continue"` reuses the loaded context rather than starting over |
| `DONE` | The turn ended normally | Read the result, then verify it yourself |

`DONE` means the worker stopped, not that the work is right. Run the touched tests yourself before believing a report.

### Read-only and writing workers

`--writes` decides how much a worker may do. Without it the agent runs with `--approval-mode always-ask` and fleet refuses every edit, delete and move request, so a review worker cannot touch the tree. With it the agent runs unattended and may edit freely.

Read-only is the right default for review, audit and explanation work. Note that a read-only worker cannot run tests either, so ask it for findings, not for proof it ran something.

## Configuration

Everything lives under `~/.fleet`:

| Path | What it is |
|---|---|
| `agents.json` | Agent definitions, overlaid on the built-ins |
| `rules.md` | Appended to the system prompt of every `omp` worker |
| `bare.md`, `bare.yml` | System prompt and config for the toolless `oracle` agent |
| `workers/<id>/` | `meta.json`, `brief.md`, `events.jsonl`, `promptN.md`, `resultN.md`, `stderr.log`, and the host's `launch.json`, `host.log`, `host.sock`, `acp.jsonl` |

Two agents are built in. `omp` is the normal worker with tools and a working directory. `oracle` is a toolless one-shot for questions that need a model rather than a workspace: it has no tools at all, so the brief must carry everything it needs, and it costs roughly 400 tokens of overhead.

Add your own, or replace a built-in by reusing its name:

```json
{
  "my-agent": {
    "argv": ["/abs/path/to/agent", "acp", "--flag"],
    "env": ["KEY=value"],
    "bare": false
  }
}
```

Set `bare` when the agent has no tools, which tells fleet the working directory is irrelevant.

## When something goes wrong

A worker that dies in the first seconds usually names its reason in `fleet ls`. `FAILED` on launch means the agent rejected the spawn: most often an unknown model selector, an effort level the model does not support, or an agent that cannot authenticate. `QUOTA` means the provider refused on limits rather than on anything you did.

For anything after launch, `fleet log <id>` shows recent events, and `~/.fleet/workers/<id>/stderr.log` has whatever the agent printed. A worker whose model id was simply out of date is the common case: `omp models refresh` re-reads the catalog, and an agent binary too old to know a model will reject it no matter what fleet sends.

## Surviving a restart

Each worker's agent runs under its own host process (`fleet host <workerdir>`, started detached by the daemon), so restarting the daemon does not kill live workers.

The host journals every agent stdout line to `acp.jsonl` with a stream position and forwards it to the daemon. The worker persists the position it has handled (`ack_seq`), plus the in-flight prompt's call id and deadline. A restarted daemon dials `host.sock`, sends `{"resume":N}`, and the host replays the journal past that point before going live, so the turn's response and any permission requests in the gap are handled exactly once. A worker whose host is gone is marked `FAILED`.

## Development

```sh
go test ./...
go test -race ./...
go build ./...
```

Tests are hermetic: a fake ACP agent under `internal/fleet/testdata/fakeagent` stands in for a real one, so nothing reaches a model. Temp homes live under `/tmp` because macOS caps unix socket paths at 104 bytes.

## License

MIT. See [LICENSE](LICENSE).
