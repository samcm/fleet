package fleet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

func stderrTail(path string, n int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return ""
	}

	off := st.Size() - n
	if off < 0 {
		off = 0
	}

	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil && st.Size()-off > 0 {
		return ""
	}

	var lines []string
	for _, l := range strings.Split(string(buf), "\n") {
		l = strings.TrimSpace(l)
		if l != "" && l != "Working..." {
			lines = append(lines, l)
		}
	}

	return strings.Join(lines, "\n")
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")

	return truncate(lines[len(lines)-1], 160)
}

// truncate cuts s to at most n bytes on a rune boundary, marking the cut.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}

	return s[:n] + "…"
}

// tailString keeps the last n bytes of s, starting on a rune boundary.
func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}

	i := len(s) - n
	for i < len(s) && !utf8.RuneStart(s[i]) {
		i++
	}

	return s[i:]
}

func fmtDur(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}

	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
}

// projectOf names the repository a working directory belongs to. It walks up
// to the nearest git checkout; a worktree, whose .git is a file pointing into
// the main checkout's .git/worktrees, is named after the main checkout. A
// directory under no checkout is named after itself.
func projectOf(cwd string) string {
	for dir := cwd; ; dir = filepath.Dir(dir) {
		st, err := os.Stat(filepath.Join(dir, ".git"))
		if err == nil {
			if !st.IsDir() {
				if main := worktreeMain(filepath.Join(dir, ".git")); main != "" {
					return filepath.Base(main)
				}
			}

			return filepath.Base(dir)
		}

		if filepath.Dir(dir) == dir {
			return filepath.Base(cwd)
		}
	}
}

// worktreeMain reads a worktree's .git file ("gitdir: <main>/.git/worktrees/<name>")
// and returns the main checkout's directory, or "" when the file is something else.
func worktreeMain(gitFile string) string {
	b, err := os.ReadFile(gitFile)
	if err != nil {
		return ""
	}

	gitdir, ok := strings.CutPrefix(strings.TrimSpace(string(b)), "gitdir: ")
	if !ok {
		return ""
	}

	if !filepath.IsAbs(gitdir) {
		gitdir = filepath.Join(filepath.Dir(gitFile), gitdir)
	}

	main, _, ok := strings.Cut(filepath.Clean(gitdir), string(filepath.Separator)+".git"+string(filepath.Separator)+"worktrees"+string(filepath.Separator))
	if !ok {
		return ""
	}

	return main
}

// lastSentence is the latest whole sentence on the latest line of a stream
// of prose, or, until one has finished, the tail of what has been written.
// Markdown list, heading and emphasis markers are dropped so it reads as
// plain text.
func lastSentence(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		s = s[i+1:]
	}

	s = strings.Join(strings.Fields(s), " ")

	end := strings.LastIndexAny(s, ".!?")
	if end < 0 {
		if len(s) > 140 {
			return "…" + tailString(s, 140)
		}

		return plain(s)
	}

	start := strings.LastIndexAny(s[:end], ".!?") + 1

	return truncate(plain(s[start:end+1]), 160)
}

// plain strips the markdown markers that open or close a line of prose.
func plain(s string) string {
	return strings.TrimRight(strings.TrimLeft(s, " #*_`->"), " *_`")
}
