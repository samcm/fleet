package fleet

import (
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
	Cwd      string `json:"cwd"`
	Label    string `json:"label"`
	Brief    string `json:"brief"`
	Writes   bool   `json:"writes"`
	Minutes  int    `json:"minutes"`
}

// Meta is the persisted description of a worker.
type Meta struct {
	ID        string    `json:"id"`
	Spec      Spec      `json:"spec"`
	Started   time.Time `json:"started"`
	SessionID string    `json:"session_id,omitempty"`
	PID       int       `json:"pid,omitempty"`
	State     State     `json:"state"`
	Detail    string    `json:"detail,omitempty"`
	Turns     int       `json:"turns"`
	Usage     Usage     `json:"usage"`
	Ended     time.Time `json:"ended,omitempty"`
}

// Usage is cumulative token usage across turns.
type Usage struct {
	Input      int64 `json:"input"`
	CachedRead int64 `json:"cached_read"`
	Output     int64 `json:"output"`
}

type toolCall struct {
	ID      string
	Title   string
	Kind    string
	Started time.Time
}

// Worker owns one ACP agent process and its session.
type Worker struct {
	dir    string
	agent  Agent
	logger *slog.Logger
	home   string

	mu         sync.Mutex
	meta       Meta
	client     *acp.Client
	turnCancel context.CancelFunc
	turnStart  time.Time
	lastEvent  time.Time
	inFlight   map[string]toolCall
	recent     []string
	turnText   strings.Builder
	lastText   string
	pendingSay string
	stderrPath string
	events     *os.File
	closed     bool
}

var quotaPattern = regexp.MustCompile(`(?i)usage limit|out of credits|need a Grok subscription|insufficient_quota|billing|rate.?limit`)

func newWorker(dir string, meta Meta, agent Agent, home string, logger *slog.Logger) (*Worker, error) {
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
		meta:       meta,
		inFlight:   make(map[string]toolCall),
		stderrPath: filepath.Join(dir, "stderr.log"),
		events:     events,
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

	client, err := acp.Start(context.Background(), argv, w.agent.Env, cwd, w.stderrPath)
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

// prompt starts one turn. The budget applies per turn.
func (w *Worker) prompt(text string) {
	w.mu.Lock()

	if w.closed || w.meta.State.Terminal() && w.meta.State != StateDone && w.meta.State != StateTimeout {
		w.mu.Unlock()

		return
	}

	turnCtx, cancel := context.WithTimeout(context.Background(), time.Duration(w.meta.Spec.Minutes)*time.Minute)
	w.turnCancel = cancel
	w.turnStart = time.Now()
	w.lastEvent = time.Time{}
	w.turnText.Reset()
	w.inFlight = make(map[string]toolCall)
	w.recent = nil
	w.meta.State = StateRunning
	w.meta.Detail = ""
	w.meta.Turns++
	sessionID := w.meta.SessionID
	client := w.client
	w.saveMeta()
	w.mu.Unlock()

	w.event("prompt", map[string]any{"chars": len(text)})

	if !w.agent.Bare {
		text += deadlineNote(w.meta.Spec.Minutes)
	}

	go func() {
		defer cancel()

		var res struct {
			StopReason string `json:"stopReason"`
			Usage      *struct {
				InputTokens      int64 `json:"inputTokens"`
				OutputTokens     int64 `json:"outputTokens"`
				CachedReadTokens int64 `json:"cachedReadTokens"`
			} `json:"usage"`
		}

		err := client.Call(turnCtx, "session/prompt", map[string]any{
			"sessionId": sessionID,
			"prompt":    []map[string]string{{"type": "text", "text": text}},
		}, &res)

		w.mu.Lock()
		defer w.mu.Unlock()

		if res.Usage != nil {
			w.meta.Usage.Input += res.Usage.InputTokens
			w.meta.Usage.Output += res.Usage.OutputTokens
			w.meta.Usage.CachedRead += res.Usage.CachedReadTokens
		}

		switch {
		case errors.Is(err, context.DeadlineExceeded):
			w.meta.State = StateTimeout
			w.meta.Detail = fmt.Sprintf("hit the %d minute budget; reply below is what the turn produced so far", w.meta.Spec.Minutes)
			_ = client.Notify("session/cancel", map[string]any{"sessionId": sessionID})
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
			w.meta.State = StateDone
			w.meta.Detail = "stopReason " + res.StopReason
		}

		w.meta.Ended = time.Now()
		w.event("turn_end", map[string]any{"state": string(w.meta.State), "detail": w.meta.Detail})
		_ = os.WriteFile(filepath.Join(w.dir, fmt.Sprintf("result%d.md", w.meta.Turns)), []byte(w.turnText.String()), 0o644)
		w.saveMeta()

		if w.pendingSay != "" && w.meta.State == StateDone {
			next := w.pendingSay
			w.pendingSay = ""

			go w.prompt(next)
		}
	}()
}

func deadlineNote(minutes int) string {
	reserve := 3
	if minutes < 15 {
		reserve = max(minutes/5, 1)
	}

	stop := time.Now().UTC().Add(time.Duration(minutes) * time.Minute)
	report := stop.Add(-time.Duration(reserve) * time.Minute)

	return fmt.Sprintf("\n---\nfleet: wall-clock budget %d minutes, hard stop at %s UTC. Be in a committed, reported state by %s UTC; check `date -u` if unsure. Anything still unreported at the hard stop is lost.\n",
		minutes, stop.Format("15:04"), report.Format("15:04"))
}

func (w *Worker) eventLoop() {
	client := w.client

	for {
		select {
		case n, ok := <-client.Notifications():
			if !ok {
				w.onExit()

				return
			}

			w.onNotification(n)
		case req, ok := <-client.Requests():
			if !ok {
				w.onExit()

				return
			}

			w.onRequest(req)
		}
	}
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
			Content       json.RawMessage
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
		w.inFlight[u.ToolCallID] = toolCall{ID: u.ToolCallID, Title: u.Title, Kind: u.Kind, Started: time.Now()}
		w.recent = append(w.recent, u.Title)

		if len(w.recent) > 6 {
			w.recent = w.recent[1:]
		}

		w.event("tool_call", map[string]any{"id": u.ToolCallID, "title": u.Title, "kind": u.Kind})
	case "tool_call_update":
		if u.Status == "completed" || u.Status == "failed" || u.Status == "cancelled" {
			if tc, ok := w.inFlight[u.ToolCallID]; ok {
				w.event("tool_done", map[string]any{"id": u.ToolCallID, "title": tc.Title, "status": u.Status, "secs": int(time.Since(tc.Started).Seconds())})
				delete(w.inFlight, u.ToolCallID)
			}
		}
	case "agent_message_chunk":
		var c struct {
			Text string `json:"text"`
		}

		if err := json.Unmarshal(u.Content, &c); err == nil && c.Text != "" {
			w.turnText.WriteString(c.Text)

			w.lastText = tailString(w.turnText.String(), 200)
		}
	case "usage_update":
		w.event("usage", map[string]any{"raw": json.RawMessage(n.Params)})
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
		w.mu.Unlock()

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

// Result returns the text of the latest turn.
func (w *Worker) Result() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.turnText.String()
}

// Now describes what the worker is doing, plus any flag.
type Now struct {
	Line string
	Flag string
}

func (w *Worker) now() Now {
	w.mu.Lock()
	defer w.mu.Unlock()

	var n Now

	if w.meta.State != StateRunning {
		if w.meta.Detail != "" {
			n.Line = w.meta.Detail
		}

		return n
	}

	since := time.Since(w.turnStart)

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

		n.Line = fmt.Sprintf("in %s for %s", truncate(oldest.Title, 90), fmtDur(time.Since(oldest.Started)))
	} else {
		idle := time.Since(w.lastEvent)
		n.Line = fmt.Sprintf("silent %s, no tool call in flight", fmtDur(idle))

		if idle > 900*time.Second {
			n.Flag = "STALLED"
		}
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

func stderrTail(path string, n int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return ""
	}

	off := st.Size() - n
	if off < 0 {
		off = 0
	}

	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil && st.Size()-off > 0 {
		return ""
	}

	var lines []string
	for _, l := range strings.Split(string(buf), "\n") {
		l = strings.TrimSpace(l)
		if l != "" && l != "Working..." {
			lines = append(lines, l)
		}
	}

	return strings.Join(lines, "\n")
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")

	return truncate(lines[len(lines)-1], 160)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "…"
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[len(s)-n:]
}

func fmtDur(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}

	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
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
