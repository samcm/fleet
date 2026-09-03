package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Root returns ~/.fleet.
func Root(home string) string { return filepath.Join(home, ".fleet") }

// SocketPath returns the daemon's unix socket path.
func SocketPath(home string) string { return filepath.Join(Root(home), "fleet.sock") }

// Daemon owns the live workers and serves them over a unix socket.
type Daemon struct {
	home   string
	logger *slog.Logger
	agents map[string]Agent

	mu      sync.Mutex
	workers map[string]*Worker
	history map[string]Meta
}

// NewDaemon loads agent definitions and the metadata of past workers.
func NewDaemon(home string, logger *slog.Logger) (*Daemon, error) {
	agents, err := LoadAgents(home)
	if err != nil {
		return nil, err
	}

	d := &Daemon{
		home:    home,
		logger:  logger.WithGroup("fleet"),
		agents:  agents,
		workers: make(map[string]*Worker),
		history: make(map[string]Meta),
	}

	entries, _ := os.ReadDir(filepath.Join(Root(home), "workers"))
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(Root(home), "workers", e.Name(), "meta.json"))
		if err != nil {
			continue
		}

		var m Meta
		if json.Unmarshal(b, &m) == nil {
			if !m.State.Terminal() {
				m.State = StateFailed
				m.Detail = "daemon restarted while the worker was running"
			}

			d.history[m.ID] = m
		}
	}

	return d, nil
}

// Serve listens on the unix socket until ctx is cancelled.
func (d *Daemon) Serve(ctx context.Context) error {
	sock := SocketPath(d.home)
	if err := os.MkdirAll(filepath.Dir(sock), 0o755); err != nil {
		return err
	}

	_ = os.Remove(sock)

	ln, err := net.Listen("unix", sock)
	if err != nil {
		return fmt.Errorf("listen %s: %w", sock, err)
	}

	_ = os.WriteFile(filepath.Join(Root(d.home), "fleet.pid"), []byte(strconv.Itoa(os.Getpid())), 0o644)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /spawn", d.handleSpawn)
	mux.HandleFunc("GET /ls", d.handleLs)
	mux.HandleFunc("POST /say", d.handleSay)
	mux.HandleFunc("GET /result", d.handleResult)
	mux.HandleFunc("POST /stop", d.handleStop)
	mux.HandleFunc("GET /log", d.handleLog)
	mux.HandleFunc("GET /workers", d.handleWorkers)
	mux.HandleFunc("GET /wait", d.handleWait)
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}()

	d.logger.Info("fleet daemon listening", slog.String("socket", sock))

	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func writeErr(w http.ResponseWriter, code int, err error) {
	http.Error(w, err.Error(), code)
}

func (d *Daemon) handleSpawn(w http.ResponseWriter, r *http.Request) {
	var spec Spec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, 400, err)

		return
	}

	id, err := d.Spawn(r.Context(), spec)
	if err != nil {
		writeErr(w, 400, err)

		return
	}

	time.Sleep(1500 * time.Millisecond)

	_, _ = fmt.Fprintf(w, "spawned %s\n%s", id, d.Ls(false, id))
}

// Spawn validates the spec and starts a worker.
func (d *Daemon) Spawn(ctx context.Context, spec Spec) (string, error) {
	if spec.Agent == "" {
		spec.Agent = "omp"
	}

	agent, ok := d.agents[spec.Agent]
	if !ok {
		return "", fmt.Errorf("unknown agent %q (have: %s)", spec.Agent, strings.Join(d.agentNames(), ", "))
	}

	switch {
	case spec.Model == "":
		return "", errors.New("model is required (full provider/model id)")
	case spec.Thinking == "":
		return "", errors.New("thinking is required")
	case strings.TrimSpace(spec.Brief) == "":
		return "", errors.New("brief is required")
	case spec.Label == "" || len(strings.Fields(spec.Label)) > 15 || strings.ContainsAny(spec.Label, "\n\t"):
		return "", errors.New("label is required: one line, at most 15 words")
	case !agent.Bare && (spec.Cwd == "" || !filepath.IsAbs(spec.Cwd)):
		return "", errors.New("cwd must be an absolute path")
	case agent.Bare && spec.Writes:
		return "", errors.New("a bare agent has no tools; drop writes")
	}

	if !agent.Bare {
		if st, err := os.Stat(spec.Cwd); err != nil || !st.IsDir() {
			return "", fmt.Errorf("cwd %s is not a directory", spec.Cwd)
		}
	}

	if spec.Minutes <= 0 {
		spec.Minutes = 25
		if agent.Bare {
			spec.Minutes = 10
		}
	}

	id := newID()
	meta := Meta{ID: id, Spec: spec, Started: time.Now(), State: StateStarting}

	wk, err := newWorker(filepath.Join(Root(d.home), "workers", id), meta, agent, d.home, d.logger)
	if err != nil {
		return "", err
	}

	d.mu.Lock()
	d.workers[id] = wk
	d.mu.Unlock()

	go func() {
		if err := wk.start(context.Background()); err != nil {
			d.logger.Warn("worker failed to start", slog.String("worker", id), slog.String("err", err.Error()))
		}
	}()

	return id, nil
}

func (d *Daemon) agentNames() []string {
	names := make([]string, 0, len(d.agents))
	for n := range d.agents {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func newID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)

	return "w-" + hex.EncodeToString(b)
}

func (d *Daemon) worker(id string) (*Worker, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	wk, ok := d.workers[id]
	if !ok {
		if _, hist := d.history[id]; hist {
			return nil, fmt.Errorf("%s finished before the daemon restarted; its files are in %s", id, filepath.Join(Root(d.home), "workers", id))
		}

		return nil, fmt.Errorf("no such worker %s", id)
	}

	return wk, nil
}

func (d *Daemon) handleLs(w http.ResponseWriter, r *http.Request) {
	all := r.URL.Query().Get("all") == "1"
	_, _ = w.Write([]byte(d.Ls(all, "")))
}

// Ls renders one entry per worker: live ones always, finished ones with all.
func (d *Daemon) Ls(all bool, only string) string {
	d.mu.Lock()

	type row struct {
		meta Meta
		now  Now
	}

	rows := make([]row, 0, len(d.workers)+len(d.history))

	for _, wk := range d.workers {
		m := wk.Meta()
		if only != "" && m.ID != only {
			continue
		}

		if !all && m.State.Terminal() && time.Since(m.Ended) > 30*time.Minute {
			continue
		}

		rows = append(rows, row{meta: m, now: wk.now()})
	}

	if all && only == "" {
		for _, m := range d.history {
			rows = append(rows, row{meta: m, now: Now{Line: m.Detail}})
		}
	}

	d.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool { return rows[i].meta.Started.After(rows[j].meta.Started) })

	if len(rows) > 40 {
		rows = rows[:40]
	}

	var b strings.Builder

	if len(rows) == 0 {
		return "no workers\n"
	}

	_, _ = fmt.Fprintf(&b, "%-8s %-9s %-9s %-7s %-26s %-6s %s\n", "STATE", "ID", "ELAPSED", "AGENT", "MODEL", "THINK", "LABEL")

	for _, r := range rows {
		m := r.meta
		end := time.Now()

		if !m.Ended.IsZero() {
			end = m.Ended
		}

		elapsed := fmt.Sprintf("%s/%dm", fmtDur(end.Sub(m.Started)), m.Spec.Minutes)
		model := m.Spec.Model

		if i := strings.Index(model, "/"); i >= 0 {
			model = model[i+1:]
		}

		_, _ = fmt.Fprintf(&b, "%-8s %-9s %-9s %-7s %-26s %-6s %s\n", m.State, m.ID, elapsed, m.Spec.Agent, truncate(model, 26), m.Spec.Thinking, m.Spec.Label)

		detail := r.now.Line
		if r.now.Flag != "" {
			detail = "FLAG " + r.now.Flag + " — " + detail
		}

		tok := fmt.Sprintf("tok in %s cached %s out %s", human(m.Usage.Input), human(m.Usage.CachedRead), human(m.Usage.Output))
		if m.Turns > 1 {
			tok += fmt.Sprintf(", turn %d", m.Turns)
		}

		if detail != "" {
			_, _ = fmt.Fprintf(&b, "%18s%s | %s\n", "", detail, tok)
		} else {
			_, _ = fmt.Fprintf(&b, "%18s%s\n", "", tok)
		}
	}

	return b.String()
}

func human(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1000)
	default:
		return strconv.FormatInt(n, 10)
	}
}

func (d *Daemon) handleSay(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)

		return
	}

	wk, err := d.worker(req.ID)
	if err != nil {
		writeErr(w, 404, err)

		return
	}

	status, err := wk.Say(req.Message)
	if err != nil {
		writeErr(w, 409, err)

		return
	}

	_, _ = fmt.Fprintf(w, "%s: %s\n", req.ID, status)
}

func (d *Daemon) handleResult(w http.ResponseWriter, r *http.Request) {
	wk, err := d.worker(r.URL.Query().Get("id"))
	if err != nil {
		writeErr(w, 404, err)

		return
	}

	m := wk.Meta()
	text := wk.Result()

	if m.State == StateRunning || m.State == StateStarting {
		_, _ = fmt.Fprintf(w, "%s is still %s; partial output so far:\n\n%s\n", m.ID, m.State, text)

		return
	}

	_, _ = fmt.Fprintf(w, "%s\n\n---- fleet ----\n%s", text, d.Ls(true, m.ID))
}

func (d *Daemon) handleStop(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)

		return
	}

	wk, err := d.worker(req.ID)
	if err != nil {
		writeErr(w, 404, err)

		return
	}

	wk.Stop()
	time.Sleep(500 * time.Millisecond)

	_, _ = w.Write([]byte(d.Ls(true, req.ID)))
}

func (d *Daemon) handleLog(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))

	if tail <= 0 {
		tail = 30
	}

	b, err := os.ReadFile(filepath.Join(Root(d.home), "workers", id, "events.jsonl"))
	if err != nil {
		writeErr(w, 404, fmt.Errorf("no events for %s", id))

		return
	}

	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}

	for _, l := range lines {
		var ev map[string]any
		if json.Unmarshal([]byte(l), &ev) != nil {
			continue
		}

		ts, _ := ev["ts"].(string)
		if len(ts) >= 19 {
			ts = ts[11:19]
		}

		kind, _ := ev["kind"].(string)
		delete(ev, "ts")
		delete(ev, "kind")

		rest, _ := json.Marshal(ev)
		_, _ = fmt.Fprintf(w, "%s  %-11s %s\n", ts, kind, truncate(string(rest), 200))
	}
}

// Status is the machine-readable form of one ls entry.
type Status struct {
	ID    string `json:"id"`
	State State  `json:"state"`
	Flag  string `json:"flag,omitempty"`
	Label string `json:"label"`
	Line  string `json:"line"`
	Entry string `json:"entry"`
}

// Statuses snapshots every live worker.
func (d *Daemon) Statuses() []Status {
	d.mu.Lock()
	workers := make([]*Worker, 0, len(d.workers))

	for _, wk := range d.workers {
		workers = append(workers, wk)
	}
	d.mu.Unlock()

	out := make([]Status, 0, len(workers))

	for _, wk := range workers {
		m := wk.Meta()
		n := wk.now()
		out = append(out, Status{ID: m.ID, State: m.State, Flag: n.Flag, Label: m.Spec.Label, Line: n.Line, Entry: d.Ls(true, m.ID)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (d *Daemon) handleWorkers(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d.Statuses())
}

// handleWait blocks until the worker is in a final state or the timeout (capped
// at 300 s) passes. 200 = final, 202 = still running; the body is the ls entry.
func (d *Daemon) handleWait(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))

	if timeout <= 0 || timeout > 300 {
		timeout = 300
	}

	wk, err := d.worker(id)
	if err != nil {
		writeErr(w, 404, err)

		return
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)

	for {
		m := wk.Meta()
		if m.State.Terminal() {
			_, _ = w.Write([]byte(d.Ls(true, id)))

			return
		}

		if time.Now().After(deadline) || r.Context().Err() != nil {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(d.Ls(true, id)))

			return
		}

		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}
}
