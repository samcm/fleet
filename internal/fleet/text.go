package fleet

import (
	"fmt"
	"os"
	"strings"
	"time"
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "…"
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[len(s)-n:]
}

func fmtDur(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}

	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
}
