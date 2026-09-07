package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

// TestProjectOf names a tree after its checkout, and a worktree after the
// checkout it belongs to, from any directory inside either.
func TestProjectOf(t *testing.T) {
	root := t.TempDir()

	main := filepath.Join(root, "farplane")
	worktree := filepath.Join(root, "farplane-cleanup-2026-09-07")
	nested := filepath.Join(main, ".claude", "worktrees", "reaper")
	loose := filepath.Join(root, "scratch", "notes")

	for _, dir := range []string{
		filepath.Join(main, ".git", "worktrees", "cleanup"),
		filepath.Join(main, ".git", "worktrees", "reaper"),
		filepath.Join(worktree, "cmd", "farplane"),
		filepath.Join(nested, "internal"),
		loose,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	gitFile := "gitdir: " + filepath.Join(main, ".git", "worktrees", "cleanup") + "\n"
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte(gitFile), 0o644); err != nil {
		t.Fatal(err)
	}

	// A worktree under the checkout's own tree, as coding agents make them,
	// may point at its gitdir relatively.
	if err := os.WriteFile(filepath.Join(nested, ".git"), []byte("gitdir: ../../../.git/worktrees/reaper"), 0o644); err != nil {
		t.Fatal(err)
	}

	for cwd, want := range map[string]string{
		main:                                   "farplane",
		filepath.Join(main, "internal", "api"): "farplane",
		filepath.Join(worktree, "cmd", "farplane"): "farplane",
		filepath.Join(nested, "internal"):          "farplane",
		loose:                                      "notes",
	} {
		if got := projectOf(cwd); got != want {
			t.Errorf("projectOf(%s) = %q, want %q", cwd, got, want)
		}
	}
}

func TestLastSentence(t *testing.T) {
	for in, want := range map[string]string{
		"":                                 "",
		"The user wants me to run":         "The user wants me to run",
		"Checked the daemon. Now the host": "Checked the daemon.",
		"- First point!\n\nSecond thought? Third": "Second thought?",
		"## Findings\nNone of the six touch it.":  "None of the six touch it.",
		"**Preserving rationale for doc moves**":  "Preserving rationale for doc moves",
	} {
		if got := lastSentence(in); got != want {
			t.Errorf("lastSentence(%q) = %q, want %q", in, got, want)
		}
	}
}
