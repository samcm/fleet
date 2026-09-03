package fleet

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Result returns the text of the latest turn.
func (w *Worker) Result() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.turnText.String()
}

// ResultTurn returns one turn's reply text. An empty turn, "0", or the number
// of the latest turn gives the live text; "all" concatenates every turn; any
// other number reads that turn's saved reply.
func (w *Worker) ResultTurn(turn string) (string, error) {
	turn = strings.TrimSpace(turn)

	if turn == "" || turn == "0" {
		return w.Result(), nil
	}

	if turn == "all" {
		return w.allTurns(), nil
	}

	n, err := strconv.Atoi(turn)
	if err != nil || n < 1 {
		return "", fmt.Errorf("turn must be a turn number, 0, or all (got %q)", turn)
	}

	w.mu.Lock()
	turns := w.meta.Turns
	w.mu.Unlock()

	if n == turns {
		return w.Result(), nil
	}

	if n > turns {
		return "", fmt.Errorf("no turn %d yet (%d so far)", n, turns)
	}

	b, err := os.ReadFile(filepath.Join(w.dir, fmt.Sprintf("result%d.md", n)))
	if err != nil {
		return "", fmt.Errorf("turn %d has no saved text: %w", n, err)
	}

	return string(b), nil
}

// allTurns concatenates every turn's reply behind a separator naming the turn
// and the start of its prompt.
func (w *Worker) allTurns() string {
	w.mu.Lock()
	turns := w.meta.Turns
	w.mu.Unlock()

	blocks := make([]string, 0, turns)

	for i := 1; i <= turns; i++ {
		prompt := ""
		if b, err := os.ReadFile(filepath.Join(w.dir, fmt.Sprintf("prompt%d.md", i))); err == nil {
			prompt = truncate(strings.ReplaceAll(strings.TrimSpace(string(b)), "\n", " "), 120)
		}

		var text string
		if i == turns {
			text = w.Result()
		} else if b, err := os.ReadFile(filepath.Join(w.dir, fmt.Sprintf("result%d.md", i))); err == nil {
			text = string(b)
		}

		blocks = append(blocks, fmt.Sprintf("---- turn %d (prompt: %s) ----\n%s", i, prompt, text))
	}

	return strings.Join(blocks, "\n\n")
}

// TurnsLine lists every turn with its end state and reply size for the result
// footer. The latest turn reports live values until it ends.
func (w *Worker) TurnsLine() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.meta.Turns == 0 {
		return ""
	}

	parts := make([]string, 0, w.meta.Turns)

	for i := 1; i <= w.meta.Turns; i++ {
		state, chars := w.meta.State, w.turnText.Len()
		if i-1 < len(w.meta.TurnLog) {
			state, chars = w.meta.TurnLog[i-1].State, w.meta.TurnLog[i-1].Chars
		}

		parts = append(parts, fmt.Sprintf("%d %s %s chars", i, state, humanChars(int64(chars))))
	}

	return "turns: " + strings.Join(parts, " · ") + " (fleet_result turn=N for an earlier one)"
}

// humanChars renders a character count compactly.
func humanChars(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}
