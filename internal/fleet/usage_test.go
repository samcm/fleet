package fleet

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRefreshUsageTailsJournal reads a running worker's session journal
// behind an offset: whole assistant replies are priced as they land, a reply
// still being written waits for its newline, and the agent's own end-of-turn
// figure is never lowered by the journal.
func TestRefreshUsageTailsJournal(t *testing.T) {
	home := t.TempDir()
	agentDir := filepath.Join(home, "agent")
	journalDir := filepath.Join(agentDir, "sessions", "-some-tree")

	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}

	journal := filepath.Join(journalDir, "2026-09-07T11-25-49-352Z_sid-1.jsonl")

	reply := func(cost float64, output int64) string {
		return fmt.Sprintf(`{"type":"message","message":{"role":"assistant","usage":{"input":10,"output":%d,"cacheRead":100,"cacheWrite":5,"cost":{"total":%g}}}}`, output, cost) + "\n"
	}

	user := `{"type":"message","message":{"role":"user","content":"an assistant said usage"}}` + "\n"

	if err := os.WriteFile(journal, []byte(user+reply(0.25, 20)+`{"type":"message","message":{"role":"assistant","usage":{"cost":{"total":9`), 0o644); err != nil {
		t.Fatal(err)
	}

	agent := Agent{Argv: []string{"agent"}, Env: []string{"PI_CODING_AGENT_DIR=" + agentDir}}
	meta := Meta{ID: "w-t", Spec: Spec{Model: "test/model", Cwd: home}, State: StateRunning, SessionID: "sid-1"}

	w, err := newWorker(filepath.Join(home, "w-t"), meta, agent, home, nil, &callLog{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	w.refreshUsage(now)

	if m := w.Meta(); m.CostUSD != 0.25 || !m.CostAt.Equal(now) || w.live.tokens != 135 {
		t.Fatalf("after one whole reply: cost %v at %v, tokens %d; want 0.25 at now, 135", m.CostUSD, m.CostAt, w.live.tokens)
	}

	// The cut-off reply completes, and another follows.
	f, err := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteString(`.5}}}}` + "\n" + reply(1, 40)); err != nil {
		t.Fatal(err)
	}

	_ = f.Close()

	w.refreshUsage(now.Add(time.Second))

	if m := w.Meta(); m.CostUSD != 10.75 || w.live.tokens != 290 {
		t.Fatalf("after the journal grew: cost %v tokens %d; want 10.75 and 290", m.CostUSD, w.live.tokens)
	}

	// The agent's turn-end figure is authoritative; a lower journal total
	// does not pull it back down.
	w.mu.Lock()
	w.meta.CostUSD = 11
	w.mu.Unlock()

	w.refreshUsage(now.Add(2 * time.Second))

	if m := w.Meta(); m.CostUSD != 11 {
		t.Errorf("journal lowered the reported cost to %v", m.CostUSD)
	}
}
