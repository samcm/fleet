package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSpawnRefusesBareModelID(t *testing.T) {
	d, _ := newTestDaemon(t)

	_, err := d.Spawn(context.Background(), Spec{
		Agent: "fake", Model: "claude-opus-5", Thinking: "high",
		Cwd: t.TempDir(), Label: "bare id", Brief: "anything", Minutes: 5,
	})
	if err == nil || !strings.Contains(err.Error(), "provider/model") {
		t.Fatalf("bare model id was accepted, err=%v", err)
	}
}

func TestReplyState(t *testing.T) {
	cases := []struct {
		name, reply string
		want        State
	}{
		{"codex usage limit", "Codex error event: The usage limit has been reached (code=usage_limit_reached)", StateQuota},
		{"grok subscription", "You need a Grok subscription to continue.", StateQuota},
		{"normal report", "## Report\n\nChanged three files; tests green. No rate limit was hit during the bench run, and the billing path is untouched." + strings.Repeat(" more detail", 40), StateDone},
		{"short normal reply", "Done. Nothing to report.", StateDone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, detail := replyState(tc.reply, "end_turn")
			if state != tc.want {
				t.Fatalf("state %s (%s), want %s", state, detail, tc.want)
			}
		})
	}
}

func TestNoToolsFlag(t *testing.T) {
	running := func(age time.Duration, calls int, bare bool) *Worker {
		return &Worker{
			meta:      Meta{State: StateRunning},
			agent:     Agent{Bare: bare},
			turnStart: time.Now().Add(-age),
			lastEvent: time.Now().Add(-time.Second),
			inFlight:  map[string]toolCall{},
			turnCalls: calls,
		}
	}

	cases := []struct {
		name string
		w    *Worker
		want string
	}{
		{"no calls after the grace", running(11*time.Minute, 0, false), "NO-TOOLS"},
		{"no calls inside the grace", running(5*time.Minute, 0, false), ""},
		{"one call is enough", running(11*time.Minute, 1, false), ""},
		{"bare agents have no tools", running(11*time.Minute, 0, true), ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.w.now().Flag; got != tc.want {
				t.Fatalf("flag %q, want %q", got, tc.want)
			}
		})
	}
}
