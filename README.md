# fleet

Headless coding-agent workers over the Agent Client Protocol, exposed to Claude Code as MCP tools and to scripts as a CLI.

A daemon (`fleet serve`, auto-started, unix socket at `~/.fleet/fleet.sock`) owns one worker each. Every worker's agent runs under a per-worker host process (`fleet host <workerdir>`, a hidden subcommand the daemon starts detached), so a daemon restart does not kill live workers: on startup the daemon reattaches to every surviving host over `<workerdir>/host.sock` and resumes the in-flight turn where it left off. State comes from the ACP event stream, not from transcripts. Nothing is pushed to the caller; it asks.

```
fleet spawn --model openai-codex/gpt-5.6-terra --thinking xhigh --cwd /abs/repo --label "..." --brief /abs/brief.md [--writes] [--minutes 25]
fleet spawn --agent oracle --model ... --thinking ... --label "..." --brief /abs/q.md   # bare one-shot, ~400 tokens of overhead
fleet ls [--all]        # one entry per worker: state, elapsed/budget, model, label, what it is doing now, tokens, live context fill and session cost, FLAG NO-TURN|STALLED|LOOPING
fleet result <id> [--turn N|--turn all]   # latest reply + footer; --turn N reads an earlier turn, all concatenates every turn
fleet say <id> <msg>    # follow-up on the same live session (queues if busy)
fleet stop <id>
fleet log <id> [--tail N]
fleet wait <id> [--timeout 1800]    # block until final; prints the entry; exit 3 on timeout — run it as a background shell command to be woken when the worker is final
fleet watch [ids...] [--timeout 3600]  # one line per state change or flag; exits when all watched are final
fleet mcp               # MCP over stdio: fleet_spawn fleet_ls fleet_result fleet_say fleet_stop fleet_log fleet_wait
```

Nothing is pushed to the caller, so the wake-up path is `fleet wait <id> --timeout 5400` run as a background shell command: it exits when the worker is final (exit 3 on timeout), and the shell notifies you. Inside a tool call, `fleet_wait {id, seconds}` does the same for up to 240 seconds and returns the ls entry or "still running".

States: STARTING RUNNING DONE TIMEOUT STOPPED FAILED QUOTA. Read-only workers run omp with `--approval-mode always-ask` and edit/delete/move permission requests are refused; writing workers run `yolo`. Each turn gets the `--minutes` budget and the brief carries the hard-stop time. The agent's `usage_update` notifications are tracked live: `fleet ls` shows the context window fill and cumulative session cost (`ctx 177k/1.0M $7.09`) next to the per-turn token totals, and the result footer carries the same line.

Agents live in `~/.fleet/agents.json` (`{"name": {"argv": [...], "env": [...], "bare": false}}`) overlaid on the built-in `omp` and `oracle`. Shared prompt files: `~/.fleet/rules.md` (appended to every omp worker), `~/.fleet/bare.md` and `bare.yml` (oracle system prompt and config overlay). Per-worker files: `~/.fleet/workers/<id>/{meta.json,brief.md,events.jsonl,promptN.md,resultN.md,stderr.log}` plus the host's `launch.json`, `host.log`, `host.sock` and `acp.jsonl`. Each turn saves its prompt (`promptN.md`) and its reply (`resultN.md`), so earlier turns stay readable after a follow-up: `fleet result <id> --turn N`, `fleet_result` with `turn=N`, or `turn=all` for everything; the result footer lists every turn with its state and size.

Restart survival, in short: the host journals every agent stdout line to `acp.jsonl` with a stream position and forwards it to the daemon; the worker persists the position it has handled (`ack_seq` in meta.json) plus the in-flight prompt's call id and deadline. A restarted daemon dials `host.sock`, sends `{"resume":N}` with that position, and the host replays the journal past it before going live — so the turn's `session/prompt` response and any mid-gap permission requests are handled exactly once. Workers whose host is gone are marked FAILED ("daemon restarted while the worker was running") as before.
