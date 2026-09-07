package fleet

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/samcm/fleet/internal/acp"
)

// State is a worker's lifecycle state.
type State string

const (
	StateStarting State = "STARTING"
	StateRunning  State = "RUNNING"
	StateDone     State = "DONE"
	StateTimeout  State = "TIMEOUT"
	StateStopped  State = "STOPPED"
	StateFailed   State = "FAILED"
	StateQuota    State = "QUOTA"
)

// Terminal reports whether the state can no longer change on its own.
func (s State) Terminal() bool { return s != StateStarting && s != StateRunning }

// Spec is what a caller asks for when spawning a worker.
type Spec struct {
	Agent    string `json:"agent"`
	Model    string `json:"model"`
	Thinking string `json:"thinking"`
	Account  string `json:"account"`
	Cwd      string `json:"cwd"`
	Label    string `json:"label"`
	Brief    string `json:"brief"`
	Writes   bool   `json:"writes"`
	Minutes  int    `json:"minutes"`
}

// Meta is the persisted description of a worker.
type Meta struct {
	ID              string     `json:"id"`
	Spec            Spec       `json:"spec"`
	Started         time.Time  `json:"started"`
	SessionID       string     `json:"session_id,omitempty"`
	PID             int        `json:"pid,omitempty"`
	State           State      `json:"state"`
	Detail          string     `json:"detail,omitempty"`
	Turns           int        `json:"turns"`
	TurnLog         []TurnInfo `json:"turn_log,omitempty"`
	Usage           Usage      `json:"usage"`
	AccountIdentity string     `json:"account_identity,omitempty"`
	// ContextUsed and ContextSize are the agent's latest reported context
	// window fill in tokens; CostUSD is the latest reported session cost and
	// CostAt when it was reported.
	ContextUsed int64     `json:"context_used,omitempty"`
	ContextSize int64     `json:"context_size,omitempty"`
	CostUSD     float64   `json:"cost_usd,omitempty"`
	CostAt      time.Time `json:"cost_at,omitzero"`
	Ended       time.Time `json:"ended,omitempty"`
	// Restart survival: AckSeq is the host stream position handled so far,
	// NextRPCID keeps request ids unique across reattaches, and TurnCallID
	// with TurnDeadline describe the in-flight prompt call to resume.
	AckSeq       int64     `json:"ack_seq,omitempty"`
	NextRPCID    int64     `json:"next_rpc_id,omitempty"`
	TurnCallID   int64     `json:"turn_call_id,omitempty"`
	TurnDeadline time.Time `json:"turn_deadline,omitempty"`
}

// TurnInfo records one finished turn: how it ended, when it ran and what it
// consumed, so past turns stay listable and chartable after the in-memory
// text of earlier turns is gone. CostUSD is the session's cumulative cost as
// reported at the end of the turn, like Meta.CostUSD; the turn's own share is
// the difference from the previous entry.
type TurnInfo struct {
	State   State     `json:"state"`
	Chars   int       `json:"chars"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Usage   Usage     `json:"usage"`
	CostUSD float64   `json:"cost_usd,omitempty"`
}

// Usage is cumulative token usage across turns.
type Usage struct {
	Input      int64 `json:"input"`
	CachedRead int64 `json:"cached_read"`
	Output     int64 `json:"output"`
}

// toolCall is one tool call the agent has made: Title is the agent's own
// description of it and Target the file, command or pattern it named.
type toolCall struct {
	ID      string
	Title   string
	Kind    string
	Target  string
	Started time.Time
}

// Worker owns one ACP agent session, reached through the worker's host.
type Worker struct {
	dir      string
	agent    Agent
	logger   *slog.Logger
	home     string
	hostArgv []string
	calls    *callLog
	// project names the repository the worker's tree belongs to.
	project string

	mu         sync.Mutex
	meta       Meta
	client     *acp.Client
	turnCancel context.CancelFunc
	turnStart  time.Time
	lastEvent  time.Time
	inFlight   map[string]toolCall
	lastCall   toolCall
	turnCalls  int
	recent     []string
	turnText   strings.Builder
	turnFile   *os.File
	lastText   string
	// narration is the tail of what the agent has written this turn, its
	// reasoning and its reply alike, so its latest sentence can be quoted.
	narration  strings.Builder
	live       liveUsage
	pendingSay string
	stderrPath string
	events     *os.File
	closed     bool
	released   bool
}

// liveUsage is what the agent's own session journal shows the worker has
// spent so far. omp reports cost and tokens over ACP only when a turn ends,
// but appends every model reply's usage to the journal as it arrives, so the
// journal is read behind an offset to price a running turn as it goes.
type liveUsage struct {
	path   string
	offset int64
	cost   float64
	tokens int64
}

// narrationKeep bounds narration; once it grows past four times this, only
// the last narrationKeep bytes are kept.
const narrationKeep = 1024

// idleAfter is how long a finished worker keeps its agent for a follow-up.
// After that the daemon releases it: the process ends, the files and the ls
// entry stay, and fleet say asks for a new worker.
const idleAfter = time.Hour

// noToolsAfter is how long a tool-bearing seat may run without one tool
// call before ls flags it.
const noToolsAfter = 10 * time.Minute

var quotaPattern = regexp.MustCompile(`(?i)usage limit|out of credits|need a Grok subscription|insufficient_quota|billing|rate.?limit`)

func newWorker(dir string, meta Meta, agent Agent, home string, hostArgv []string, calls *callLog, logger *slog.Logger) (*Worker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	events, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		dir:        dir,
		agent:      agent,
		logger:     logger.With(slog.String("worker", meta.ID)),
		home:       home,
		hostArgv:   hostArgv,
		calls:      calls,
		meta:       meta,
		inFlight:   make(map[string]toolCall),
		stderrPath: filepath.Join(dir, "stderr.log"),
		events:     events,
	}

	if !agent.Bare {
		w.project = projectOf(meta.Spec.Cwd)
	}

	if err := os.WriteFile(filepath.Join(dir, "brief.md"), []byte(meta.Spec.Brief), 0o644); err != nil {
		return nil, err
	}

	w.saveMeta()

	return w, nil
}

// Meta returns a snapshot of the persisted description.
func (w *Worker) Meta() Meta {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.meta
}

func (w *Worker) saveMeta() {
	b, err := json.MarshalIndent(w.meta, "", "  ")
	if err != nil {
		return
	}

	_ = os.WriteFile(filepath.Join(w.dir, "meta.json"), b, 0o644)
}

func (w *Worker) event(kind string, fields map[string]any) {
	rec := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339Nano), "kind": kind}
	for k, v := range fields {
		rec[k] = v
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return
	}

	_, _ = w.events.Write(append(b, '\n'))
}

// start launches the process, opens the session and sends the brief. Errors
// before the first prompt are fatal and visible in the worker's state.
func (w *Worker) start(ctx context.Context) error {
	if w.agent.Bare {
		if err := prepareBareAgentDir(w.home); err != nil {
			return w.fail(StateFailed, "prepare bare agent dir: "+err.Error())
		}
	}

	cwd := w.meta.Spec.Cwd
	if w.agent.Bare {
		cwd = filepath.Join(Root(w.home), "barehome")
	}

	argv := append([]string(nil), w.agent.Argv...)
	if !w.agent.Bare {
		// omp only asks permission for tools its approval mode gates; always-ask routes
		// every call through onRequest so a read-only worker's edits can be refused.
		mode := "always-ask"
		if w.meta.Spec.Writes {
			mode = "yolo"
		}

		argv = append(argv, "--approval-mode", mode)
	}

	env := append([]string(nil), w.agent.Env...)
	if w.meta.Spec.Account != "" {
		provider, _, _ := strings.Cut(w.meta.Spec.Model, "/")
		pool, err := json.Marshal(map[string][]string{provider: {w.meta.AccountIdentity}})
		if err != nil {
			return w.fail(StateFailed, "encode account pool: "+err.Error())
		}

		path, err := filepath.Abs(filepath.Join(w.dir, "accounts.json"))
		if err != nil {
			return w.fail(StateFailed, "resolve account pool path: "+err.Error())
		}

		if err := os.WriteFile(path, pool, 0o600); err != nil {
			return w.fail(StateFailed, "write account pool: "+err.Error())
		}

		env = append(env, "OMP_AUTH_BROKER_ACCOUNT_POOL_FILE="+path)
	}

	hostArgv := append(append([]string(nil), w.hostArgv...), w.dir)

	client, err := acp.Launch(hostArgv, w.dir, acp.LaunchSpec{Argv: argv, Env: env, Cwd: cwd, StderrPath: w.stderrPath})
	if err != nil {
		return w.fail(StateFailed, err.Error())
	}

	w.mu.Lock()
	w.client = client
	w.meta.PID = client.PID()
	w.mu.Unlock()

	go w.eventLoop()

	initCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if err := client.Call(initCtx, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{"fs": map[string]bool{"readTextFile": false, "writeTextFile": false}, "terminal": false},
	}, nil); err != nil {
		return w.fail(StateFailed, "initialize: "+err.Error())
	}

	var session struct {
		SessionID     string `json:"sessionId"`
		ConfigOptions []struct {
			ID string `json:"id"`
		} `json:"configOptions"`
	}

	if err := client.Call(initCtx, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}, &session); err != nil {
		return w.fail(StateFailed, "session/new: "+err.Error())
	}

	w.mu.Lock()
	w.meta.SessionID = session.SessionID
	w.mu.Unlock()

	for _, opt := range []struct{ id, value string }{{"model", w.meta.Spec.Model}, {"thinking", w.meta.Spec.Thinking}} {
		if opt.value == "" {
			continue
		}

		if err := client.Call(initCtx, "session/set_config_option", map[string]any{
			"sessionId": session.SessionID, "configId": opt.id, "value": opt.value,
		}, nil); err != nil {
			return w.fail(StateFailed, fmt.Sprintf("set %s=%s: %v", opt.id, opt.value, err))
		}
	}

	w.saveMeta()
	w.prompt(w.meta.Spec.Brief)

	return nil
}

// prompt starts one turn. The budget applies per turn. The in-flight call's
// id and deadline are persisted before the prompt is sent so a restarted
// daemon can resume the same call.
func (w *Worker) prompt(text string) {
	w.mu.Lock()

	if w.closed || w.meta.State.Terminal() && w.meta.State != StateDone && w.meta.State != StateTimeout {
		w.mu.Unlock()

		return
	}

	client := w.client
	callID := client.Reserve()
	deadline := time.Now().Add(time.Duration(w.meta.Spec.Minutes) * time.Minute)
	turnCtx, cancel := context.WithDeadline(context.Background(), deadline)

	w.turnCancel = cancel
	w.turnStart = time.Now()
	w.lastEvent = time.Time{}
	w.turnText.Reset()
	w.narration.Reset()
	w.inFlight = make(map[string]toolCall)
	w.lastCall = toolCall{}
	w.turnCalls = 0
	w.recent = nil
	w.meta.State = StateRunning
	w.meta.Detail = ""
	w.meta.Turns++
	w.meta.TurnCallID = callID
	w.meta.TurnDeadline = deadline
	w.meta.NextRPCID = callID
	sessionID := w.meta.SessionID
	turn := w.meta.Turns
	w.saveMeta()
	w.mu.Unlock()

	w.event("prompt", map[string]any{"chars": len(text)})
	_ = os.WriteFile(filepath.Join(w.dir, fmt.Sprintf("prompt%d.md", turn)), []byte(text), 0o644)

	// The turn's reply accumulates in resultN.md as chunks arrive, so a
	// reattached daemon reloads it instead of losing the pre-restart text.
	turnFile, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("result%d.md", turn)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err == nil {
		w.mu.Lock()
		w.turnFile = turnFile
		w.mu.Unlock()
	}

	go w.runTurn(turnCtx, cancel, callID, sessionID, text)
}

// promptResult is the answer to one session/prompt call.
type promptResult struct {
	StopReason string `json:"stopReason"`
	Usage      *struct {
		InputTokens      int64 `json:"inputTokens"`
		OutputTokens     int64 `json:"outputTokens"`
		CachedReadTokens int64 `json:"cachedReadTokens"`
	} `json:"usage"`
}

// runTurn waits for one prompt call to end and finishes the turn. A non-empty
// text sends a fresh prompt; an empty one resumes the call sent before a
// daemon restart.
func (w *Worker) runTurn(ctx context.Context, cancel context.CancelFunc, callID int64, sessionID, text string) {
	defer cancel()

	client := w.client
	res := &promptResult{}

	_ = client.Expect(callID)

	var err error
	var ackSeq int64

	if text != "" {
		if err = client.Send(callID, "session/prompt", map[string]any{
			"sessionId": sessionID,
			"prompt":    []map[string]string{{"type": "text", "text": text}},
		}); err != nil {
			err = fmt.Errorf("session/prompt: write: %w", err)
		}
	}

	if err == nil {
		ackSeq, err = client.AwaitCall(ctx, callID, res)
		if err != nil && !errors.Is(err, acp.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("session/prompt: %w", err)
		}
	}

	w.finishTurn(callID, ackSeq, err, res, sessionID)
}

// finishTurn records the end of the current turn and starts a queued one.
func (w *Worker) finishTurn(callID, ackSeq int64, err error, res *promptResult, sessionID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if errors.Is(err, acp.ErrClosed) && !w.client.Exited() {
		// The daemon lost or handed over the host link without the agent
		// exiting; the next daemon resumes this turn from meta.
		return
	}

	began := w.turnBegan()

	var turnUsage Usage

	if res.Usage != nil {
		turnUsage = Usage{Input: res.Usage.InputTokens, CachedRead: res.Usage.CachedReadTokens, Output: res.Usage.OutputTokens}
		w.meta.Usage.Input += turnUsage.Input
		w.meta.Usage.Output += turnUsage.Output
		w.meta.Usage.CachedRead += turnUsage.CachedRead
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		w.meta.State = StateTimeout
		w.meta.Detail = fmt.Sprintf("hit the %d minute budget; reply below is what the turn produced so far", w.meta.Spec.Minutes)
		_ = w.client.Notify("session/cancel", map[string]any{"sessionId": sessionID})
	case errors.Is(err, context.Canceled):
		w.meta.State = StateStopped
		w.meta.Detail = "stopped by request"
	case errors.Is(err, acp.ErrClosed):
		w.meta.State = StateFailed
		w.meta.Detail = "agent process exited mid-turn"
		w.classifyExit()
	case err != nil:
		w.meta.State = StateFailed
		w.meta.Detail = err.Error()
	default:
		w.meta.State, w.meta.Detail = replyState(w.turnText.String(), res.StopReason)
	}

	if ackSeq > w.meta.AckSeq {
		w.meta.AckSeq = ackSeq
	}

	w.meta.TurnCallID = 0
	w.meta.TurnDeadline = time.Time{}
	w.meta.Ended = time.Now()
	w.event("turn_end", map[string]any{"state": string(w.meta.State), "detail": w.meta.Detail})

	if w.turnFile != nil {
		_ = w.turnFile.Close()
		w.turnFile = nil
	} else {
		_ = os.WriteFile(filepath.Join(w.dir, fmt.Sprintf("result%d.md", w.meta.Turns)), []byte(w.turnText.String()), 0o644)
	}

	w.meta.TurnLog = append(w.meta.TurnLog, TurnInfo{
		State: w.meta.State, Chars: w.turnText.Len(),
		Started: began, Ended: w.meta.Ended, Usage: turnUsage, CostUSD: w.meta.CostUSD,
	})
	w.saveMeta()

	if w.pendingSay != "" && w.meta.State == StateDone {
		next := w.pendingSay
		w.pendingSay = ""

		go w.prompt(next)
	}
}

// replyState classifies a finished turn. Providers report an exhausted
// subscription as a normal end_turn whose whole reply is the error line, so
// a short reply matching the quota pattern is QUOTA rather than DONE.
func replyState(reply, stopReason string) (State, string) {
	reply = strings.TrimSpace(reply)
	if len(reply) < 300 && quotaPattern.MatchString(reply) {
		return StateQuota, reply
	}

	return StateDone, "stopReason " + stopReason
}

// turnBegan is when the current turn was prompted. turnStart is reset on a
// daemon reattach so the resumed turn gets a fresh silence grace period, but
// the persisted deadline still fixes the original prompt time; must hold mu.
func (w *Worker) turnBegan() time.Time {
	if !w.meta.TurnDeadline.IsZero() {
		return w.meta.TurnDeadline.Add(-time.Duration(w.meta.Spec.Minutes) * time.Minute)
	}

	return w.turnStart
}

func (w *Worker) eventLoop() {
	client := w.client

	for {
		select {
		case n, ok := <-client.Notifications():
			if !ok || client.Detached() {
				w.onConnEnd()

				return
			}

			w.onNotification(n)
			w.ack(n.Seq)
		case req, ok := <-client.Requests():
			if !ok || client.Detached() {
				w.onConnEnd()

				return
			}

			w.onRequest(req)
			w.ack(req.Seq)
		}
	}
}

// ack persists the host stream position after a message has been handled, so
// a restarted daemon replays only what this worker never saw.
func (w *Worker) ack(seq int64) {
	if seq <= 0 {
		return
	}

	w.mu.Lock()

	if seq > w.meta.AckSeq {
		w.meta.AckSeq = seq
		w.saveMeta()
	}

	w.mu.Unlock()
}

// attach reconnects a worker whose host outlived the previous daemon. The
// in-flight turn, if any, resumes with its remaining budget.
func (w *Worker) attach(client *acp.Client) {
	w.mu.Lock()
	w.client = client
	client.SetNextID(w.meta.NextRPCID)

	callID := w.meta.TurnCallID
	deadline := w.meta.TurnDeadline
	sessionID := w.meta.SessionID

	// Reload the latest turn's reply: an in-flight turn keeps appending to
	// it, and a finished worker's last words stay quotable after a restart.
	if w.meta.Turns > 0 {
		resultPath := filepath.Join(w.dir, fmt.Sprintf("result%d.md", w.meta.Turns))
		if b, err := os.ReadFile(resultPath); err == nil {
			w.turnText.Write(b)
			w.lastText = tailString(w.turnText.String(), 200)
		}

		if callID != 0 {
			if f, err := os.OpenFile(resultPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
				w.turnFile = f
			}

			w.turnStart = time.Now()
			w.recoverCalls()
		}
	}

	w.mu.Unlock()

	w.event("reattached", map[string]any{"ack_seq": w.meta.AckSeq})
	w.logger.Info("reattached to worker host", slog.Int64("ack_seq", w.meta.AckSeq))

	go w.eventLoop()

	if callID != 0 {
		turnCtx, cancel := context.WithDeadline(context.Background(), deadline)

		w.mu.Lock()
		w.turnCancel = cancel
		w.mu.Unlock()

		go w.runTurn(turnCtx, cancel, callID, sessionID, "")
	}
}

// recoverCalls rebuilds the current turn's tool-call state from the event
// journal after a reattach: the calls started since the last prompt and not
// finished are in flight again, and the turn's counters cover the whole turn
// rather than the part after the restart. A call's start record carries its
// id and title under the tool's own kind; must hold mu.
func (w *Worker) recoverCalls() {
	f, err := os.Open(filepath.Join(w.dir, "events.jsonl"))
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	var starts []toolCall

	// latest is the time of the turn's last tool event, so the silence
	// counters pick up where the previous daemon left them.
	var latest time.Time

	done := make(map[string]bool)

	for sc.Scan() {
		var ev struct {
			TS     time.Time `json:"ts"`
			Kind   string    `json:"kind"`
			ID     string    `json:"id"`
			Title  string    `json:"title"`
			Target string    `json:"target"`
		}

		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}

		switch {
		case ev.Kind == "prompt":
			starts = starts[:0]
			clear(done)
			latest = time.Time{}
		case ev.Kind == "tool_done":
			done[ev.ID] = true
			latest = ev.TS
		case ev.ID != "" && ev.Title != "":
			starts = append(starts, toolCall{ID: ev.ID, Title: ev.Title, Kind: ev.Kind, Target: ev.Target, Started: ev.TS})
			latest = ev.TS
		}
	}

	if !latest.IsZero() {
		w.lastEvent = latest
	}

	for _, call := range starts {
		if !done[call.ID] {
			w.inFlight[call.ID] = call
		}

		w.lastCall = call
		w.turnCalls++
		w.recent = append(w.recent, call.Title)

		if len(w.recent) > 6 {
			w.recent = w.recent[1:]
		}
	}
}

// detach drops the host connection when the daemon shuts down; the agent
// keeps running for the next daemon to resume.
func (w *Worker) detach() {
	w.mu.Lock()
	client := w.client
	w.mu.Unlock()

	if client != nil {
		client.Close()
	}
}

// onConnEnd handles the host link ending: the agent's exit is final; anything
// else means the daemon lost or handed over the link and must go quiet
// without touching the persisted state.
func (w *Worker) onConnEnd() {
	if w.client.Exited() {
		w.onExit()

		return
	}

	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()

	w.event("detached", nil)
}

func (w *Worker) onExit() {
	<-w.client.Done()

	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true

	if !w.meta.State.Terminal() {
		w.meta.State = StateFailed
		w.meta.Detail = "agent process exited"
		w.classifyExit()
		w.meta.Ended = time.Now()
		w.saveMeta()
	}

	if err := w.client.ExitError(); err != nil {
		w.event("exit", map[string]any{"error": err.Error()})
	} else {
		w.event("exit", nil)
	}
}

// classifyExit refines FAILED using the process exit error and stderr; must hold mu.
func (w *Worker) classifyExit() {
	if err := w.client.ExitError(); err != nil {
		w.meta.Detail += " (" + err.Error() + ")"
	}

	tail := stderrTail(w.stderrPath, 2000)
	if quotaPattern.MatchString(tail) {
		w.meta.State = StateQuota
		w.meta.Detail = "provider refused the model; relaunch on another vendor: " + lastLine(tail)
	} else if tail != "" {
		w.meta.Detail += ": " + lastLine(tail)
	}
}

func (w *Worker) onNotification(n acp.Notification) {
	if n.Method != "session/update" {
		return
	}

	var p struct {
		Update struct {
			SessionUpdate string `json:"sessionUpdate"`
			ToolCallID    string `json:"toolCallId"`
			Title         string `json:"title"`
			Kind          string `json:"kind"`
			Status        string `json:"status"`
			Used          int64  `json:"used"`
			Size          int64  `json:"size"`
			Cost          *struct {
				Amount   float64 `json:"amount"`
				Currency string  `json:"currency"`
			} `json:"cost"`
			// RawInput is the tool's argument object; the keys that name a
			// file, command, pattern or address are what a reader wants.
			RawInput struct {
				Path    string `json:"path"`
				Command string `json:"command"`
				Pattern string `json:"pattern"`
				URL     string `json:"url"`
			} `json:"rawInput"`
			Content json.RawMessage
		} `json:"update"`
	}

	if err := json.Unmarshal(n.Params, &p); err != nil {
		return
	}

	u := p.Update

	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastEvent = time.Now()

	switch u.SessionUpdate {
	case "tool_call":
		in := u.RawInput
		call := toolCall{ID: u.ToolCallID, Title: u.Title, Kind: u.Kind, Started: w.lastEvent}
		call.Target = cmp.Or(in.Path, in.Command, in.Pattern, in.URL)
		w.inFlight[u.ToolCallID] = call
		w.lastCall = call
		w.turnCalls++
		w.calls.add(w.lastEvent)
		w.recent = append(w.recent, u.Title)

		if len(w.recent) > 6 {
			w.recent = w.recent[1:]
		}

		fields := map[string]any{"id": u.ToolCallID, "title": u.Title, "kind": u.Kind}
		if call.Target != "" {
			fields["target"] = call.Target
		}

		w.event("tool_call", fields)
	case "tool_call_update":
		if u.Status == "completed" || u.Status == "failed" || u.Status == "cancelled" {
			if tc, ok := w.inFlight[u.ToolCallID]; ok {
				w.event("tool_done", map[string]any{"id": u.ToolCallID, "title": tc.Title, "status": u.Status, "secs": int(time.Since(tc.Started).Seconds())})
				delete(w.inFlight, u.ToolCallID)
			}
		}
	case "agent_message_chunk", "agent_thought_chunk":
		var c struct {
			Text string `json:"text"`
		}

		if err := json.Unmarshal(u.Content, &c); err != nil || c.Text == "" {
			return
		}

		w.narration.WriteString(c.Text)

		if w.narration.Len() > 4*narrationKeep {
			tail := tailString(w.narration.String(), narrationKeep)
			w.narration.Reset()
			w.narration.WriteString(tail)
		}

		if u.SessionUpdate == "agent_message_chunk" {
			w.turnText.WriteString(c.Text)

			if w.turnFile != nil {
				_, _ = w.turnFile.WriteString(c.Text)
			}

			w.lastText = tailString(w.turnText.String(), 200)
		}
	case "usage_update":
		// used/size are the context window fill; cost.amount is the cumulative
		// session cost so far, so the latest update replaces the stored values.
		w.meta.ContextUsed = u.Used
		w.meta.ContextSize = u.Size

		if u.Cost != nil {
			w.meta.CostUSD = u.Cost.Amount
			w.meta.CostAt = w.lastEvent
		}

		w.saveMeta()
		w.event("usage", map[string]any{"used": u.Used, "size": u.Size, "cost_usd": w.meta.CostUSD})
	}
}

func (w *Worker) onRequest(req acp.Request) {
	if req.Method != "session/request_permission" {
		_ = w.client.RespondError(req, -32601, "unsupported")

		return
	}

	var p struct {
		ToolCall struct {
			Title string `json:"title"`
			Kind  string `json:"kind"`
		} `json:"toolCall"`
		Options []struct {
			OptionID string `json:"optionId"`
			Kind     string `json:"kind"`
		} `json:"options"`
	}

	if err := json.Unmarshal(req.Params, &p); err != nil {
		_ = w.client.RespondError(req, -32602, "bad params")

		return
	}

	mutating := p.ToolCall.Kind == "edit" || p.ToolCall.Kind == "delete" || p.ToolCall.Kind == "move"
	want := "allow"

	if mutating && !w.meta.Spec.Writes {
		want = "reject"
	}

	for _, o := range p.Options {
		if strings.HasPrefix(o.Kind, want) {
			w.event("permission", map[string]any{"title": p.ToolCall.Title, "kind": p.ToolCall.Kind, "decision": o.Kind})
			_ = w.client.Respond(req, map[string]any{"outcome": map[string]string{"outcome": "selected", "optionId": o.OptionID}})

			return
		}
	}

	_ = w.client.Respond(req, map[string]any{"outcome": map[string]string{"outcome": "cancelled"}})
}

// Say queues a follow-up prompt on the same session. It runs at once when the
// worker is idle, or after the current turn ends.
func (w *Worker) Say(text string) (string, error) {
	w.mu.Lock()

	if w.closed {
		released := w.released
		w.mu.Unlock()

		if released {
			return "", fmt.Errorf("agent released after %dm idle; spawn a new worker", int(idleAfter.Minutes()))
		}

		return "", errors.New("agent process has exited; spawn a new worker")
	}

	switch w.meta.State {
	case StateRunning, StateStarting:
		w.pendingSay = text
		w.mu.Unlock()

		return "queued behind the current turn", nil
	case StateDone, StateTimeout:
		w.mu.Unlock()
		w.prompt(text)

		return "new turn started", nil
	default:
		w.mu.Unlock()

		return "", fmt.Errorf("worker is %s; spawn a new one", w.meta.State)
	}
}

// Stop cancels the current turn and ends the process.
func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.turnCancel
	client := w.client
	sessionID := w.meta.SessionID
	running := !w.meta.State.Terminal()
	w.mu.Unlock()

	if client == nil {
		return
	}

	if running {
		_ = client.Notify("session/cancel", map[string]any{"sessionId": sessionID})

		if cancel != nil {
			cancel()
		}
	}

	time.AfterFunc(5*time.Second, client.Kill)
}

// releaseIdle ends the agent of a worker that has sat DONE or TIMEOUT for
// idleAfter with nothing queued, and reports whether it did. The state, the
// files and the ls entry stay; only the process goes.
func (w *Worker) releaseIdle(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.client == nil || w.pendingSay != "" {
		return false
	}

	if w.meta.State != StateDone && w.meta.State != StateTimeout {
		return false
	}

	if w.meta.Ended.IsZero() || now.Sub(w.meta.Ended) < idleAfter {
		return false
	}

	w.closed = true
	w.released = true
	w.meta.Detail += fmt.Sprintf("; released after %dm idle", int(idleAfter.Minutes()))
	w.saveMeta()
	w.event("released", map[string]any{"idle": fmtDur(now.Sub(w.meta.Ended))})
	w.client.Kill()

	return true
}

// refreshUsage folds the replies the session journal has gained since the
// last call into the worker's cost and token figures. A running turn is the
// only time the journal is ahead of what the agent has reported.
func (w *Worker) refreshUsage(now time.Time) {
	w.mu.Lock()
	state, sessionID, path, offset := w.meta.State, w.meta.SessionID, w.live.path, w.live.offset
	w.mu.Unlock()

	if state != StateRunning || sessionID == "" {
		return
	}

	if path == "" {
		if path = w.journalPath(sessionID); path == "" {
			return
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return
	}

	// A journal shorter than where it was last read has been rewritten;
	// start over on it.
	reset := st.Size() < offset
	if reset {
		offset = 0
	}

	if st.Size() == offset {
		return
	}

	buf := make([]byte, st.Size()-offset)

	n, err := f.ReadAt(buf, offset)
	if err != nil && n == 0 {
		return
	}

	// Only whole lines count; the last one may still be being written.
	end := bytes.LastIndexByte(buf[:n], '\n')
	if end < 0 {
		return
	}

	cost, tokens := journalUsage(buf[:end])

	w.mu.Lock()
	defer w.mu.Unlock()

	if reset {
		w.live.cost, w.live.tokens = 0, 0
	}

	w.live.path = path
	w.live.offset = offset + int64(end) + 1
	w.live.cost += cost
	w.live.tokens += tokens

	if w.live.cost > w.meta.CostUSD {
		w.meta.CostUSD = w.live.cost
		w.meta.CostAt = now
	}
}

// journalUsage sums the cost and tokens of the assistant replies in a run of
// whole session journal lines.
func journalUsage(lines []byte) (cost float64, tokens int64) {
	for line := range bytes.SplitSeq(lines, []byte{'\n'}) {
		if !bytes.Contains(line, []byte(`"assistant"`)) || !bytes.Contains(line, []byte(`"usage"`)) {
			continue
		}

		var rec struct {
			Type    string `json:"type"`
			Message struct {
				Role  string `json:"role"`
				Usage struct {
					Input      int64 `json:"input"`
					Output     int64 `json:"output"`
					CacheRead  int64 `json:"cacheRead"`
					CacheWrite int64 `json:"cacheWrite"`
					Cost       struct {
						Total float64 `json:"total"`
					} `json:"cost"`
				} `json:"usage"`
			} `json:"message"`
		}

		if json.Unmarshal(line, &rec) != nil || rec.Type != "message" || rec.Message.Role != "assistant" {
			continue
		}

		u := rec.Message.Usage
		cost += u.Cost.Total
		tokens += u.Input + u.Output + u.CacheRead + u.CacheWrite
	}

	return cost, tokens
}

// journalPath finds the session journal omp keeps for the worker's session:
// <agent dir>/sessions/<tree>/<time>_<session id>.jsonl, under the agent
// directory the agent definition, the environment or omp's default names.
func (w *Worker) journalPath(sessionID string) string {
	dir := filepath.Join(w.home, ".omp", "agent")

	if env := os.Getenv("PI_CODING_AGENT_DIR"); env != "" {
		dir = env
	}

	for _, kv := range w.agent.Env {
		if v, ok := strings.CutPrefix(kv, "PI_CODING_AGENT_DIR="); ok {
			dir = v
		}
	}

	matches, _ := filepath.Glob(filepath.Join(dir, "sessions", "*", "*_"+sessionID+".jsonl"))
	if len(matches) == 0 {
		return ""
	}

	return matches[0]
}

// Now describes what the worker is doing, plus any flag. Tool, Title and
// Target name the oldest tool call in flight, when there is one. Since is
// how long that call has run, or, with nothing in flight, how long the
// worker has been silent since its last event or its prompt.
type Now struct {
	Line   string
	Flag   string
	Tool   string
	Title  string
	Target string
	Since  time.Duration
}

func (w *Worker) now() Now {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.nowLocked()
}

// nowLocked is now for a caller that already holds mu.
func (w *Worker) nowLocked() Now {
	var n Now

	if w.meta.State != StateRunning {
		if w.meta.Detail != "" {
			n.Line = w.meta.Detail
		}

		return n
	}

	since := time.Since(w.turnStart)
	n.Since = since

	if w.lastEvent.IsZero() {
		n.Line = fmt.Sprintf("no event yet, %s since prompt", fmtDur(since))

		if since > 180*time.Second {
			n.Flag = "NO-TURN"
		}

		return n
	}

	if len(w.inFlight) > 0 {
		var oldest toolCall
		for _, tc := range w.inFlight {
			if oldest.ID == "" || tc.Started.Before(oldest.Started) {
				oldest = tc
			}
		}

		n.Tool = oldest.Kind
		n.Title = oldest.Title
		n.Target = oldest.Target
		n.Since = time.Since(oldest.Started)
		n.Line = fmt.Sprintf("in %s for %s", truncate(oldest.Title, 90), fmtDur(n.Since))
	} else {
		idle := time.Since(w.lastEvent)
		n.Since = idle
		n.Line = fmt.Sprintf("silent %s, no tool call in flight", fmtDur(idle))

		if idle > 900*time.Second {
			n.Flag = "STALLED"
		}
	}

	// A seat that has read nothing after ten minutes is reasoning about files
	// it never opened; its report, if one comes, is not grounded in the tree.
	if n.Flag == "" && !w.agent.Bare && w.turnCalls == 0 && since > noToolsAfter {
		n.Flag = "NO-TOOLS"
	}

	if n.Flag == "" && len(w.recent) >= 4 {
		counts := map[string]int{}
		for _, t := range w.recent {
			counts[t]++

			if counts[t] >= 4 {
				n.Flag = "LOOPING"
			}
		}
	}

	if w.lastText != "" {
		n.Line += " | last: " + strings.ReplaceAll(truncate(w.lastText, 90), "\n", " ")
	}

	return n
}

// fail records a startup failure so it is visible in ls, and ends the process.
func (w *Worker) fail(state State, detail string) error {
	w.mu.Lock()
	w.meta.State = state
	w.meta.Detail = detail
	w.meta.Ended = time.Now()
	client := w.client
	w.saveMeta()
	w.mu.Unlock()

	w.event("failed", map[string]any{"detail": detail})

	if client != nil {
		client.Kill()
	}

	return errors.New(detail)
}
