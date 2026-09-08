package fleet

import (
	"bufio"
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// The dashboard's time axes: tool calls over the last hour in one-minute
// bins; concurrency, tokens and spend over the last day in fifteen-minute
// bins.
const (
	hourBins   = 60
	hourBin    = time.Minute
	hourWindow = hourBins * hourBin

	dayBins   = 96
	dayBin    = 15 * time.Minute
	dayWindow = dayBins * dayBin
)

// callLog remembers when tool calls started, across every worker, for the
// dashboard's calls-per-minute series. Entries fall off the front as they
// leave the window.
type callLog struct {
	mu    sync.Mutex
	times []time.Time
}

func (c *callLog) add(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.times = append(c.times, t)

	cutoff := t.Add(-hourWindow)
	drop := 0

	for drop < len(c.times) && c.times[drop].Before(cutoff) {
		drop++
	}

	c.times = c.times[drop:]
}

// recall reloads the tool calls after cutoff from one worker's event journal,
// so the series survives a daemon restart. A call's start record carries the
// call id and the agent's title for it; the event kind is the tool's own
// kind, so the records are told apart from the tool_done ones by shape. Call
// sort once every worker has been recalled.
func (c *callLog) recall(dir string, cutoff time.Time) {
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	c.mu.Lock()
	defer c.mu.Unlock()

	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.Contains(line, []byte(`"title"`)) || bytes.Contains(line, []byte(`"tool_done"`)) {
			continue
		}

		var ev struct {
			TS time.Time `json:"ts"`
			ID string    `json:"id"`
		}

		if json.Unmarshal(line, &ev) != nil || ev.ID == "" {
			continue
		}

		if ev.TS.After(cutoff) {
			c.times = append(c.times, ev.TS)
		}
	}
}

func (c *callLog) sort() {
	c.mu.Lock()
	defer c.mu.Unlock()

	sort.Slice(c.times, func(i, j int) bool { return c.times[i].Before(c.times[j]) })
}

// snapshot copies the entries at or after cutoff, oldest first.
func (c *callLog) snapshot(cutoff time.Time) []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for i < len(c.times) && c.times[i].Before(cutoff) {
		i++
	}

	return append([]time.Time(nil), c.times[i:]...)
}

// DashboardWorker is one worker as the dashboard shows it.
type DashboardWorker struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Agent string `json:"agent"`
	Bare  bool   `json:"bare,omitempty"`
	// Project is the repository the worker's tree belongs to; empty for a
	// bare agent, which has no tree.
	Project  string `json:"project,omitempty"`
	Writes   bool   `json:"writes"`
	Model    string `json:"model"`
	Thinking string `json:"thinking"`
	State    State  `json:"state"`
	Flag     string `json:"flag,omitempty"`
	// Turn counts turns so far; TurnStarted is when the latest was prompted
	// and BudgetMin its per-turn budget. Ended is set for a final worker only.
	Turn        int       `json:"turn"`
	TurnStarted time.Time `json:"turn_started,omitzero"`
	BudgetMin   int       `json:"budget_min"`
	Started     time.Time `json:"started"`
	Ended       time.Time `json:"ended,omitzero"`
	ContextUsed int64     `json:"context_used"`
	ContextSize int64     `json:"context_size"`
	// Tokens is everything the model read and wrote across finished turns.
	Tokens  int64   `json:"tokens"`
	CostUSD float64 `json:"cost_usd"`
	// Tool, Title and Target name the tool call in flight, or with ToolLive
	// false the last one made this turn. Said is the latest sentence the
	// agent wrote this turn, reasoning or reply. SinceSec is how long the
	// call has run, or how long the worker has been silent. Line is the
	// reply's first line for a worker that finished cleanly (empty when it
	// said nothing), otherwise the detail of how it ended.
	Tool     string `json:"tool,omitempty"`
	Title    string `json:"title,omitempty"`
	Target   string `json:"target,omitempty"`
	ToolLive bool   `json:"tool_live,omitempty"`
	Said     string `json:"said,omitempty"`
	SinceSec int    `json:"since_sec"`
	Line     string `json:"line,omitempty"`
}

// Series is the dashboard's history, oldest bin first. Calls cover the last
// hour in HourBinSec-wide bins. Concurrency, Tokens and Spend cover the last
// day in DayBinSec-wide bins; Tokens and Spend are per bin per model,
// aligned with Models.
type Series struct {
	End         time.Time   `json:"end"`
	HourBinSec  int         `json:"hour_bin_sec"`
	Calls       []int       `json:"calls"`
	CallsNow    int         `json:"calls_now"`
	DayBinSec   int         `json:"day_bin_sec"`
	Models      []string    `json:"models"`
	Concurrency []int       `json:"concurrency"`
	Tokens      [][]int64   `json:"tokens"`
	Spend       [][]float64 `json:"spend"`
}

// Dashboard is the snapshot behind the status wall.
type Dashboard struct {
	Now     time.Time         `json:"now"`
	Started time.Time         `json:"started"`
	Workers []DashboardWorker `json:"workers"`
	Series  Series            `json:"series"`
}

// dashboard snapshots the worker for the status wall.
func (w *Worker) dashboard() (DashboardWorker, Meta) {
	w.mu.Lock()
	defer w.mu.Unlock()

	m := w.meta
	n := w.nowLocked()

	began := w.turnBegan()
	if m.State.Terminal() {
		began = m.Started
		if n := len(m.TurnLog); n > 0 && !m.TurnLog[n-1].Started.IsZero() {
			began = m.TurnLog[n-1].Started
		}
	}

	row := DashboardWorker{
		ID: m.ID, Label: m.Spec.Label, Agent: m.Spec.Agent, Bare: w.agent.Bare, Project: w.project, Writes: m.Spec.Writes,
		Model: m.Spec.Model, Thinking: m.Spec.Thinking, State: m.State, Flag: n.Flag,
		Turn: m.Turns, TurnStarted: began, BudgetMin: m.Spec.Minutes, Started: m.Started,
		ContextUsed: m.ContextUsed, ContextSize: m.ContextSize,
		Tokens:   max(m.Usage.Input+m.Usage.CachedRead+m.Usage.Output, w.live.tokens),
		CostUSD:  m.CostUSD,
		SinceSec: int(n.Since.Seconds()),
	}

	// A shell or script call's title and command can run to many lines;
	// the first says what it is.
	switch {
	case m.State == StateRunning && n.Tool != "":
		row.Tool, row.Title, row.Target, row.ToolLive = n.Tool, firstLine(n.Title), firstLine(n.Target), true
	case m.State == StateRunning:
		row.Tool, row.Title, row.Target = w.lastCall.Kind, firstLine(w.lastCall.Title), firstLine(w.lastCall.Target)
		row.Said = lastSentence(w.narration.String())
	case m.State == StateDone:
		row.Ended = m.Ended
		row.Line = firstLine(w.turnText.String())
	case m.State.Terminal():
		row.Ended = m.Ended
		row.Line = m.Detail
	}

	return row, m
}

// firstLine is the first non-empty line of a reply, without any markdown
// heading or list marker, as the one-line summary of what a worker said.
func firstLine(reply string) string {
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#*-> "))
		if line != "" {
			return truncate(line, 140)
		}
	}

	return ""
}

// Dashboard snapshots every worker the default ls shows, plus the series
// over every worker the daemon knows.
func (d *Daemon) Dashboard() Dashboard {
	now := time.Now()
	cutoff := now.Add(-dayWindow)

	var (
		rows  []DashboardWorker
		spans []turnSpan
	)

	d.mu.Lock()

	for _, wk := range d.workers {
		row, m := wk.dashboard()

		// A running turn's tokens are whatever the row shows beyond the
		// finished turns' total.
		var inFlight time.Time
		var liveTokens int64
		if m.State == StateRunning {
			inFlight = row.TurnStarted
			liveTokens = row.Tokens - (m.Usage.Input + m.Usage.CachedRead + m.Usage.Output)
		}

		spans = appendTurnSpans(spans, m, inFlight, liveTokens, now)

		if m.State.Terminal() && now.Sub(m.Ended) > 30*time.Minute {
			continue
		}

		rows = append(rows, row)
	}

	for _, m := range d.history {
		if !m.Ended.After(cutoff) {
			continue
		}

		spans = appendTurnSpans(spans, m, time.Time{}, 0, now)
	}

	d.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool {
		if a, b := rows[i].State.Terminal(), rows[j].State.Terminal(); a != b {
			return b
		}

		return rows[i].Started.After(rows[j].Started)
	})

	if rows == nil {
		rows = []DashboardWorker{}
	}

	return Dashboard{
		Now: now, Started: d.started, Workers: rows,
		Series: buildSeries(spans, d.calls.snapshot(now.Add(-hourWindow)), now),
	}
}

func (d *Daemon) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d.Dashboard())
}

// turnSpan is one turn's time range and what it consumed: the unit the
// series are built from.
type turnSpan struct {
	worker string
	model  string
	start  time.Time
	end    time.Time
	tokens int64
	cost   float64
}

// appendTurnSpans adds a worker's finished turns and, when inFlight is set,
// the turn running since then until now. Cost is recorded cumulatively at the
// end of each turn; whatever the worker's total exceeds its last turn's
// figure by was reported after that turn closed, and belongs to the running
// turn when it came in during it, else to that last turn. Turns logged
// before they carried timing cannot be placed one by one; the worker's whole
// life stands in for them.
func appendTurnSpans(spans []turnSpan, m Meta, inFlight time.Time, liveTokens int64, now time.Time) []turnSpan {
	first := len(spans)
	timed := len(m.TurnLog) > 0

	for _, t := range m.TurnLog {
		if t.Started.IsZero() {
			timed = false

			break
		}
	}

	var prev float64

	switch {
	case timed:
		for _, t := range m.TurnLog {
			spans = append(spans, turnSpan{
				worker: m.ID, model: m.Spec.Model, start: t.Started, end: t.Ended,
				tokens: t.Usage.Input + t.Usage.CachedRead + t.Usage.Output, cost: max(t.CostUSD-prev, 0),
			})
			prev = max(t.CostUSD, prev)
		}
	case len(m.TurnLog) > 0 && !m.Ended.IsZero():
		spans = append(spans, turnSpan{
			worker: m.ID, model: m.Spec.Model, start: m.Started, end: m.Ended,
			tokens: m.Usage.Input + m.Usage.CachedRead + m.Usage.Output, cost: m.CostUSD,
		})
		prev = m.CostUSD
	}

	if !inFlight.IsZero() {
		spans = append(spans, turnSpan{worker: m.ID, model: m.Spec.Model, start: inFlight, end: now, tokens: max(liveTokens, 0)})
	}

	if remainder := m.CostUSD - prev; remainder > 0 && len(spans) > first {
		last := len(spans) - 1
		if !inFlight.IsZero() && !m.CostAt.After(inFlight) && last > first {
			last--
		}

		spans[last].cost += remainder
	}

	return spans
}

// spread visits every one of the n bins of width bin from start that sp
// touches, with the fraction of the span that falls inside the bin. A span
// of no length sits wholly in the bin holding its start.
func spread(sp turnSpan, start time.Time, bin time.Duration, n int, visit func(i int, share float64)) {
	end := start.Add(time.Duration(n) * bin)

	lo, hi := sp.start, sp.end
	if lo.Before(start) {
		lo = start
	}

	if hi.After(end) {
		hi = end
	}

	if hi.Before(lo) || !lo.Before(end) {
		return
	}

	dur := sp.end.Sub(sp.start)
	first := int(lo.Sub(start) / bin)
	last := min(int(hi.Sub(start)/bin), n-1)

	for i := first; i <= last; i++ {
		b0 := start.Add(time.Duration(i) * bin)
		b1 := b0.Add(bin)

		l, h := lo, hi
		if b0.After(l) {
			l = b0
		}

		if b1.Before(h) {
			h = b1
		}

		switch {
		case h.After(l):
			visit(i, float64(h.Sub(l))/float64(dur))
		case dur == 0 && i == first:
			visit(i, 1)
		}
	}
}

// buildSeries bins the spans and calls into the windows ending now. A turn's
// tokens and cost are spread over its bins in proportion to the time it
// spent in each; the agent reports them once, at the end of the turn.
// Spans of one worker must be adjacent so its concurrency counts once per bin.
func buildSeries(spans []turnSpan, calls []time.Time, now time.Time) Series {
	hourStart := now.Add(-hourWindow)
	dayStart := now.Add(-dayWindow)

	models := make([]string, 0, 4)
	index := make(map[string]int)

	for _, sp := range spans {
		if !sp.end.After(dayStart) || !sp.start.Before(now) {
			continue
		}

		if _, ok := index[sp.model]; !ok {
			index[sp.model] = 0
			models = append(models, sp.model)
		}
	}

	sort.Strings(models)

	for i, m := range models {
		index[m] = i
	}

	s := Series{
		End: now, HourBinSec: int(hourBin.Seconds()), Calls: make([]int, hourBins),
		DayBinSec: int(dayBin.Seconds()), Models: models, Concurrency: make([]int, dayBins),
		Tokens: make([][]int64, dayBins), Spend: make([][]float64, dayBins),
	}

	for i := range dayBins {
		s.Tokens[i] = make([]int64, len(models))
		s.Spend[i] = make([]float64, len(models))
	}

	seen := make([]string, dayBins)

	for _, sp := range spans {
		mi, ok := index[sp.model]
		if !ok {
			continue
		}

		// Tokens are whole: each bin takes the rounded cumulative share less
		// what earlier bins took, so the bins sum to the turn's total.
		var cum float64
		var given int64

		spread(sp, dayStart, dayBin, dayBins, func(i int, share float64) {
			if seen[i] != sp.worker {
				seen[i] = sp.worker
				s.Concurrency[i]++
			}

			cum += share
			want := int64(math.Round(float64(sp.tokens) * cum))
			s.Tokens[i][mi] += want - given
			given = want
			s.Spend[i][mi] += sp.cost * share
		})
	}

	lastMinute := now.Add(-time.Minute)

	for _, t := range calls {
		if t.Before(hourStart) || !t.Before(now) {
			continue
		}

		s.Calls[int(t.Sub(hourStart)/hourBin)]++

		if t.After(lastMinute) {
			s.CallsNow++
		}
	}

	return s
}
