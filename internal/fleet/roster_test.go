package fleet

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestLoadRoster(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(Root(home), 0o755); err != nil {
		t.Fatal(err)
	}

	if r, err := LoadRoster(home); err != nil || len(r.Models) != 0 {
		t.Fatalf("missing roster: %+v, %v; want empty, nil", r, err)
	}

	doc := "models:\n  anthropic/claude-opus-5:\n    thinking: [high, xhigh]\n    tier: A\n    tps: 83\n    note: real code\n  kimi-code/k3:\n    thinking: max\n"
	if err := os.WriteFile(RosterPath(home), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := LoadRoster(home)
	if err != nil {
		t.Fatal(err)
	}

	got := r.Models["anthropic/claude-opus-5"]
	if got.Thinking.String() != "high xhigh" || got.Tier != "A" || got.TPS != 83 || got.Note != "real code" {
		t.Fatalf("loaded entry %+v", got)
	}

	if k3 := r.Models["kimi-code/k3"]; k3.Thinking.String() != "max" {
		t.Fatalf("scalar thinking loaded as %+v", k3)
	}

	if _, ok := r.Allows("anthropic/claude-opus-5", "max"); ok {
		t.Fatal("max allowed for a model limited to high and xhigh")
	}

	if _, ok := r.Allows("anthropic/claude-opus-5", "high"); !ok {
		t.Fatal("high refused for a model that lists it")
	}

	if _, ok := r.Allows("unlisted/model", "max"); !ok {
		t.Fatal("an unlisted model must allow any level")
	}
}

func TestSpawnRefusesThinkingOutsideTheRoster(t *testing.T) {
	d, home := newTestDaemon(t)

	doc := "models:\n  test/model:\n    thinking: [high]\n"
	if err := os.WriteFile(RosterPath(home), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := d.Spawn(context.Background(), Spec{
		Agent: "fake", Model: "test/model", Thinking: "max",
		Cwd: t.TempDir(), Label: "thinking gate", Brief: "anything", Minutes: 5,
	})
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("thinking outside the roster was accepted, err=%v", err)
	}
}

func TestRenderModels(t *testing.T) {
	catalog := []CatalogModel{
		{Selector: "anthropic/claude-opus-5", ContextWindow: 1_000_000, Thinking: []string{"low", "high", "max"}},
		{Selector: "openai-codex/gpt-6-astra", ContextWindow: 272_000, Thinking: []string{"low", "xhigh"}},
		{Selector: "other/model", ContextWindow: 8_000},
	}
	catalog[0].Cost.Input, catalog[0].Cost.Output = 5, 25

	roster := Roster{
		Models: map[string]RosterEntry{
			"anthropic/claude-opus-5":  {Thinking: Levels{"xhigh"}, Tier: "A", TPS: 83, Note: "real code"},
			"openai-codex/gpt-6-astra": {Thinking: Levels{"xhigh"}, Tier: "S"},
			"kimi-code/k3":             {Thinking: Levels{"max"}},
		},
	}

	out := renderModels(catalog, roster, false)

	for _, want := range []string{
		"anthropic/claude-opus-5", "A", "low high max", "5/25", "83", "1.0M", "real code",
		"openai-codex/gpt-6-astra", "S", "quota", "272k",
		"kimi-code/k3", "NOT IN OMP CATALOG",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("roster view missing %q:\n%s", want, out)
		}
	}

	if strings.Contains(out, "other/model") {
		t.Errorf("roster view lists a non-roster model:\n%s", out)
	}

	if all := renderModels(catalog, roster, true); !strings.Contains(all, "other/model") {
		t.Errorf("all view misses a catalog model:\n%s", all)
	}
}

func TestTierRank(t *testing.T) {
	tiers := []string{"S", "A", "B", "C", ""}
	for i := 1; i < len(tiers); i++ {
		if tierRank(tiers[i-1]) >= tierRank(tiers[i]) {
			t.Fatalf("tier %q does not rank above %q", tiers[i-1], tiers[i])
		}
	}
}
