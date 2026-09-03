# fleet

Headless coding-agent workers over the Agent Client Protocol, exposed to Claude Code as MCP tools and to scripts as a CLI.

A daemon (`fleet serve`, auto-started, unix socket at `~/.fleet/fleet.sock`) owns one `omp acp` process per worker. State comes from the ACP event stream, not from transcripts. Nothing is pushed to the caller; it asks.

```
fleet spawn --model openai-codex/gpt-5.6-terra --thinking xhigh --cwd /abs/repo --label "..." --brief /abs/brief.md [--writes] [--minutes 25]
fleet spawn --agent oracle --model ... --thinking ... --label "..." --brief /abs/q.md   # bare one-shot, ~400 tokens of overhead
fleet ls [--all]        # one entry per worker: state, elapsed/budget, model, label, what it is doing now, tokens, FLAG NO-TURN|STALLED|LOOPING
fleet result <id> [--turn N|--turn all]   # latest reply + footer; --turn N reads an earlier turn, all concatenates every turn
fleet say <id> <msg>    # follow-up on the same live session (queues if busy)
fleet stop <id>
fleet log <id> [--tail N]
fleet wait <id> [--timeout 1800]    # block until final; prints the entry; exit 3 on timeout — the caller's one wake-up
fleet watch [ids...] [--timeout 3600]  # one line per state change or flag; exits when all watched are final
fleet mcp               # MCP over stdio: fleet_spawn fleet_ls fleet_result fleet_say fleet_stop fleet_log
```

States: STARTING RUNNING DONE TIMEOUT STOPPED FAILED QUOTA. Read-only workers run omp with `--approval-mode always-ask` and edit/delete/move permission requests are refused; writing workers run `yolo`. Each turn gets the `--minutes` budget and the brief carries the hard-stop time.

Agents live in `~/.fleet/agents.json` (`{"name": {"argv": [...], "env": [...], "bare": false}}`) overlaid on the built-in `omp` and `oracle`. Shared prompt files: `~/.fleet/rules.md` (appended to every omp worker), `~/.fleet/bare.md` and `bare.yml` (oracle system prompt and config overlay). Per-worker files: `~/.fleet/workers/<id>/{meta.json,brief.md,events.jsonl,promptN.md,resultN.md,stderr.log}`. Each turn saves its prompt (`promptN.md`) and its reply (`resultN.md`), so earlier turns stay readable after a follow-up: `fleet result <id> --turn N`, `fleet_result` with `turn=N`, or `turn=all` for everything; the result footer lists every turn with its state and size.
