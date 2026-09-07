package fleet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// Levels is the thinking levels a model may run at, first entry the default.
// In YAML it is a sequence or, for one level, a plain scalar.
type Levels []string

// UnmarshalYAML accepts `thinking: xhigh` as well as `thinking: [high, xhigh]`.
func (l *Levels) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		*l = Levels{node.Value}

		return nil
	}

	var list []string
	if err := node.Decode(&list); err != nil {
		return err
	}

	*l = list

	return nil
}

func (l Levels) String() string { return strings.Join(l, " ") }

// RosterEntry is the operator's standing view of one model: the thinking
// levels it may run at, a tier rank, measured output speed in tokens per
// second, and a note. The catalog supplies the rest.
type RosterEntry struct {
	Thinking Levels `yaml:"thinking,omitempty"`
	Tier     string `yaml:"tier,omitempty"`
	TPS      int    `yaml:"tps,omitempty"`
	Note     string `yaml:"note,omitempty"`
}

// Roster is the models in use, keyed by full selector. It is one file so the
// CLI and the MCP tools read the same copy.
type Roster struct {
	Models map[string]RosterEntry `yaml:"models"`
}

// RosterPath returns where the roster lives.
func RosterPath(home string) string { return filepath.Join(Root(home), "roster.yaml") }

// LoadRoster reads the roster; a missing file is an empty roster.
func LoadRoster(home string) (Roster, error) {
	var r Roster

	b, err := os.ReadFile(RosterPath(home))
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}

	if err != nil {
		return r, fmt.Errorf("read roster: %w", err)
	}

	if err := yaml.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("parse %s: %w", RosterPath(home), err)
	}

	return r, nil
}

// Allows reports whether the roster permits the model at that thinking level.
// A model absent from the roster, or listed without levels, allows any.
func (r Roster) Allows(selector, thinking string) (Levels, bool) {
	entry, ok := r.Models[selector]
	if !ok || len(entry.Thinking) == 0 {
		return nil, true
	}

	return entry.Thinking, slices.Contains(entry.Thinking, thinking)
}
