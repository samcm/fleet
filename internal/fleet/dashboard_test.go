package fleet

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// TestBuildSeriesSpreadsTurnsAndDedupesConcurrency pins the binning rules: a
// turn's tokens and cost land in its bins in proportion to the time spent in
// each and the token bins sum to the turn's total, one worker counts once per
// bin however many turns it had there, a finished turn's late usage goes to
// that turn while cost reported during a running turn goes to it, a worker
// whose turns carry no timing is spread over its life, and calls bin by
// start time on the hour axis.
func TestBuildSeriesSpreadsTurnsAndDedupesConcurrency(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	hourStart := now.Add(-hourWindow)
	dayStart := now.Add(-dayWindow)
	last := dayBins - 1

	// Worker a: one twenty-minute turn ending now, a quarter of it in the
	// second-last bin, cost reported late (1.00 at turn end, 1.50 on the
	// worker), a token count that does not split into whole quarters.
	a := Meta{ID: "w-a", Spec: Spec{Model: "openai-codex/gpt-6-astra"}, CostUSD: 1.5, TurnLog: []TurnInfo{
		{Started: now.Add(-20 * time.Minute), Ended: now, Usage: Usage{Input: 102, CachedRead: 200, Output: 300}, CostUSD: 1},
	}}

	// Worker b: two turns inside the last bin that must count as one worker,
	// then a turn in flight since a minute ago that has reported 0.25 of cost.
	b := Meta{ID: "w-b", Spec: Spec{Model: "anthropic/claude-opus-5"}, CostUSD: 1.25, CostAt: now.Add(-50 * time.Second), TurnLog: []TurnInfo{
		{Started: now.Add(-10 * time.Minute), Ended: now.Add(-9 * time.Minute), CostUSD: 0.5},
		{Started: now.Add(-8 * time.Minute), Ended: now.Add(-7 * time.Minute), CostUSD: 1},
	}}

	// Worker c: logged before turns carried timing, half an hour of life
	// covering bins 40 and 41.
	c := Meta{ID: "w-c", Spec: Spec{Model: "kimi-code/k3"}, CostUSD: 4, Usage: Usage{Output: 1000},
		Started: dayStart.Add(10 * time.Hour), Ended: dayStart.Add(10*time.Hour + 30*time.Minute), TurnLog: []TurnInfo{{State: StateDone}}}

	// Worker d: in its first turn for two minutes, cost reported with no
	// time recorded and 7000 tokens read live from its journal; both must
	// stay with d, not leak to c's spans.
	d := Meta{ID: "w-d", Spec: Spec{Model: "kimi-code/k3"}, CostUSD: 0.5}

	var spans []turnSpan
	spans = appendTurnSpans(spans, a, time.Time{}, 0, now)
	spans = appendTurnSpans(spans, b, now.Add(-time.Minute), 0, now)
	spans = appendTurnSpans(spans, c, time.Time{}, 0, now)
	spans = appendTurnSpans(spans, d, now.Add(-2*time.Minute), 7000, now)

	calls := []time.Time{
		hourStart.Add(-time.Second), // before the window: dropped
		hourStart,                   // first bin
		now.Add(-90 * time.Second),  // bin 58
		now.Add(-30 * time.Second),  // bin 59 and the last minute
		now.Add(time.Second),        // after now: nowhere
	}

	s := buildSeries(spans, calls, now)

	want := []string{"anthropic/claude-opus-5", "kimi-code/k3", "openai-codex/gpt-6-astra"}
	if len(s.Models) != 3 || s.Models[0] != want[0] || s.Models[1] != want[1] || s.Models[2] != want[2] {
		t.Fatalf("models %v, want %v", s.Models, want)
	}

	if len(s.Tokens) != dayBins || len(s.Concurrency) != dayBins || len(s.Calls) != hourBins || s.DayBinSec != 900 || s.HourBinSec != 60 {
		t.Fatalf("axes: day %d/%d bins of %ds, hour %d bins of %ds", len(s.Tokens), len(s.Concurrency), s.DayBinSec, len(s.Calls), s.HourBinSec)
	}

	opus, k3, astra := 0, 1, 2

	if s.Tokens[last-1][astra] != 151 || s.Tokens[last][astra] != 451 {
		t.Errorf("astra tokens split %d + %d, want 151 + 451 (602 conserved)", s.Tokens[last-1][astra], s.Tokens[last][astra])
	}

	if got := s.Spend[last-1][astra] + s.Spend[last][astra]; math.Abs(got-1.5) > 1e-9 || math.Abs(s.Spend[last-1][astra]-0.375) > 1e-9 {
		t.Errorf("astra spend %.4f then %.4f, want 0.375 then 1.125 (late usage attributed)", s.Spend[last-1][astra], s.Spend[last][astra])
	}

	if math.Abs(s.Spend[last][opus]-1.25) > 1e-9 || s.Spend[last-1][opus] != 0 {
		t.Errorf("opus spend last=%.4f prev=%.4f, want 1.25 (two turns plus the running turn's 0.25) and 0", s.Spend[last][opus], s.Spend[last-1][opus])
	}

	if got := spentSince(spans, now.Add(-30*time.Minute)); math.Abs(got-3.25) > 1e-9 {
		t.Errorf("spent in the last half hour %.4f, want 3.25 (c ended earlier)", got)
	}

	if math.Abs(s.Spend[last][k3]-0.5) > 1e-9 || math.Abs(s.Spend[41][k3]-2) > 1e-9 || s.Tokens[last][k3] != 7000 {
		t.Errorf("k3 last bin spend=%.4f tokens=%d, bin41 spend=%.4f; want 0.5 and 7000 (d's running turn) and 2.0 (c untouched)", s.Spend[last][k3], s.Tokens[last][k3], s.Spend[41][k3])
	}

	if s.Tokens[40][k3] != 500 || s.Tokens[41][k3] != 500 || s.Tokens[42][k3] != 0 || math.Abs(s.Spend[40][k3]-2) > 1e-9 {
		t.Errorf("k3 life spread: tokens bin40=%d bin41=%d bin42=%d spend bin40=%.4f, want 500 500 0 2.0", s.Tokens[40][k3], s.Tokens[41][k3], s.Tokens[42][k3], s.Spend[40][k3])
	}

	if s.Concurrency[last] != 3 || s.Concurrency[last-1] != 1 || s.Concurrency[last-2] != 0 || s.Concurrency[40] != 1 || s.Concurrency[42] != 0 {
		t.Errorf("concurrency tail %v bin40=%d, want [0 1 3] and 1", s.Concurrency[last-2:], s.Concurrency[40])
	}

	if s.Calls[0] != 1 || s.Calls[58] != 1 || s.Calls[59] != 1 || s.CallsNow != 1 {
		t.Errorf("calls bins first=%d 58=%d 59=%d now=%d, want 1 1 1 1", s.Calls[0], s.Calls[58], s.Calls[59], s.CallsNow)
	}
}

// TestDashboardReportsFinishedTurn runs one fake turn and reads the snapshot
// over the socket: the row carries the spec and usage, the turn is logged
// with its timing, and the series place the turn and its tool calls now.
func TestDashboardReportsFinishedTurn(t *testing.T) {
	d, home := newTestDaemon(t)
	stop := startServe(t, d)
	defer stop()

	id := spawnFake(t, d, "duration=500ms permdelay=50ms tag=one tools=3", true)

	m := waitFinal(t, d, id, waitTimeout)
	if m.State != StateDone {
		t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
	}

	if len(m.TurnLog) != 1 || m.TurnLog[0].Started.IsZero() || !m.TurnLog[0].Ended.After(m.TurnLog[0].Started) {
		t.Fatalf("turn log %+v, want one timed turn", m.TurnLog)
	}

	if turn := m.TurnLog[0]; turn.Usage != (Usage{Input: 100, CachedRead: 10, Output: 50}) || turn.CostUSD != 7.0859 {
		t.Errorf("turn usage %+v cost %v, want 100/10/50 and 7.0859", turn.Usage, turn.CostUSD)
	}

	body, err := NewClient(home).Dashboard(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var dash Dashboard
	if err := json.Unmarshal(body, &dash); err != nil {
		t.Fatal(err)
	}

	if len(dash.Workers) != 1 {
		t.Fatalf("workers %+v, want the one spawned", dash.Workers)
	}

	row := dash.Workers[0]
	if row.ID != id || row.Model != "test/model" || row.Thinking != "high" || !row.Writes || row.State != StateDone {
		t.Errorf("row %+v", row)
	}

	if row.Tokens != 160 || row.CostUSD != 7.0859 || row.ContextUsed != 176878 || row.Ended.IsZero() || row.TurnStarted.IsZero() {
		t.Errorf("row usage %+v", row)
	}

	// A finished worker is summarised by its reply's first line, and names
	// no tool since nothing is in flight.
	if row.Line != "alpha beta tag=one gamma perm=allowed" || row.Tool != "" || row.ToolLive {
		t.Errorf("row activity line=%q tool=%q live=%v", row.Line, row.Tool, row.ToolLive)
	}

	if dash.SpentToday != 7.0859 || dash.Started.IsZero() {
		t.Errorf("spent today %v started %v", dash.SpentToday, dash.Started)
	}

	s := dash.Series
	if len(s.Models) != 1 || s.Models[0] != "test/model" || len(s.Tokens) != dayBins || len(s.Calls) != hourBins {
		t.Fatalf("series models %v day bins %d hour bins %d", s.Models, len(s.Tokens), len(s.Calls))
	}

	day, hour := dayBins-1, hourBins-1
	if s.Concurrency[day] != 1 || s.Tokens[day][0] != 160 || math.Abs(s.Spend[day][0]-7.0859) > 1e-9 {
		t.Errorf("last day bin concurrency=%d tokens=%d spend=%v, want 1 160 7.0859", s.Concurrency[day], s.Tokens[day][0], s.Spend[day][0])
	}

	// The fake agent made three tool calls this turn.
	if s.Calls[hour] != 3 || s.CallsNow != 3 {
		t.Errorf("calls last bin=%d now=%d, want 3 3", s.Calls[hour], s.CallsNow)
	}

	// A fresh daemon rebuilds the calls series from the journal and, having
	// reattached the finished worker, still has its reply to quote.
	stop()

	d2 := newDaemonOn(t, home)
	if got := len(d2.calls.snapshot(time.Time{})); got != 3 {
		t.Errorf("recalled %d tool calls after restart, want 3", got)
	}

	wk, err := d2.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	if row, _ := wk.dashboard(); row.Line != "alpha beta tag=one gamma perm=allowed" {
		t.Errorf("reattached row line %q, want the reply's first line", row.Line)
	}

	if text := wk.Result(); !strings.Contains(text, "gamma perm=allowed") {
		t.Errorf("reattached result %q lost the saved reply", text)
	}
}

// TestReattachRecoversInFlightCall drops the daemon while a tool call is
// open and proves the next daemon still knows about it: the call is in
// flight again with its title and target, and the turn's call count covers
// the whole turn.
func TestReattachRecoversInFlightCall(t *testing.T) {
	d1, home := newTestDaemon(t)
	stop1 := startServe(t, d1)

	id := spawnFake(t, d1, "duration=20s permdelay=100ms tools=2 hang=1", true)

	wk1, err := d1.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, waitTimeout, func() bool { return wk1.now().Tool == "read" }, "the open tool call")

	stop1()

	d2 := newDaemonOn(t, home)

	wk2, err := d2.worker(id)
	if err != nil {
		t.Fatalf("worker not reattached: %v", err)
	}

	if n := wk2.now(); n.Tool != "read" || n.Title != "read file 1" || n.Target != "file1.go" {
		t.Errorf("after reattach now=%+v, want the open read of file1.go", n)
	}

	if row, _ := wk2.dashboard(); !row.ToolLive || row.Title != "read file 1" || row.Target != "file1.go" {
		t.Errorf("after reattach row tool=%q live=%v target=%q", row.Title, row.ToolLive, row.Target)
	}

	wk2.mu.Lock()
	calls := wk2.turnCalls
	wk2.mu.Unlock()

	if calls != 2 {
		t.Errorf("turn calls after reattach %d, want 2", calls)
	}

	wk2.Stop()
}
