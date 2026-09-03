package fleet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Agent describes how to launch one ACP agent process.
type Agent struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env,omitempty"`
	// Bare agents have no tools; cwd is irrelevant and the brief must carry everything.
	Bare bool `json:"bare,omitempty"`
}

func defaultAgents(home string) map[string]Agent {
	omp := filepath.Join(home, ".bun", "bin", "omp")
	runs := Root(home)

	return map[string]Agent{
		"omp": {
			Argv: []string{omp, "acp", "--no-skills", "--no-title", "--append-system-prompt", filepath.Join(runs, "rules.md")},
		},
		"oracle": {
			Argv: []string{omp, "acp", "--no-tools", "--no-skills", "--no-rules", "--no-extensions", "--no-lsp", "--no-prewalk",
				"--config", filepath.Join(runs, "bare.yml"), "--system-prompt", filepath.Join(runs, "bare.md")},
			Env:  []string{"HOME=" + filepath.Join(runs, "barehome"), "PI_CODING_AGENT_DIR=" + filepath.Join(runs, "bareagent")},
			Bare: true,
		},
	}
}

// LoadAgents returns the built-in agents overlaid with ~/.fleet/agents.json.
func LoadAgents(home string) (map[string]Agent, error) {
	agents := defaultAgents(home)

	b, err := os.ReadFile(filepath.Join(home, ".fleet", "agents.json"))
	if errors.Is(err, os.ErrNotExist) {
		return agents, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read agents.json: %w", err)
	}

	var custom map[string]Agent
	if err := json.Unmarshal(b, &custom); err != nil {
		return nil, fmt.Errorf("parse agents.json: %w", err)
	}

	for name, a := range custom {
		agents[name] = a
	}

	return agents, nil
}

// prepareBareAgentDir mirrors ~/.omp/agent into ~/.fleet/bareagent without
// mcp.json: --no-tools keeps MCP tools, and with no read/write tool omp inlines
// every MCP schema into the prompt.
func prepareBareAgentDir(home string) error {
	src := filepath.Join(home, ".omp", "agent")
	dst := filepath.Join(Root(home), "bareagent")

	if err := os.RemoveAll(dst); err != nil {
		return err
	}

	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(Root(home), "barehome"), 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.Name() == "mcp.json" {
			continue
		}

		if err := os.Symlink(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}

	return nil
}
