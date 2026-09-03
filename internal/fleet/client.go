package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// Client talks to the daemon over its unix socket, starting it when absent.
type Client struct {
	home string
	http *http.Client
}

// NewClient returns a client for the daemon under home.
func NewClient(home string) *Client {
	sock := SocketPath(home)

	return &Client{
		home: home,
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer

					return d.DialContext(ctx, "unix", sock)
				},
			},
		},
	}
}

// Ensure makes sure a daemon is answering, launching `fleet serve` detached if not.
func (c *Client) Ensure(ctx context.Context) error {
	if c.ping(ctx) == nil {
		return nil
	}

	self, err := os.Executable()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(Root(c.home), 0o755); err != nil {
		return err
	}

	logf, err := os.OpenFile(Root(c.home)+"/daemon.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer logf.Close()

	cmd := exec.Command(self, "serve")
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}

	_ = cmd.Process.Release()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.ping(ctx) == nil {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return errors.New("daemon did not answer within 5s; see ~/.fleet/daemon.log")
}

func (c *Client) ping(ctx context.Context) error {
	_, err := c.get(ctx, "/ping", nil)

	return err
}

func (c *Client) get(ctx context.Context, path string, q url.Values) (string, error) {
	u := "http://fleet" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}

	return c.do(req)
}

func (c *Client) post(ctx context.Context, path string, body any) (string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://fleet"+path, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")

	return c.do(req)
}

func (c *Client) do(req *http.Request) (string, error) {
	s, _, err := c.doStatus(req)

	return s, err
}

func (c *Client) doStatus(req *http.Request) (string, int, error) {
	res, err := c.http.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return "", 0, err
	}

	if res.StatusCode >= 400 {
		return "", res.StatusCode, errors.New(string(bytes.TrimSpace(b)))
	}

	return string(b), res.StatusCode, nil
}

// ErrStillRunning is returned by Wait when the timeout passes first.
var ErrStillRunning = errors.New("still running")

// waitPollStep is the longest single long poll Wait issues. It stays well
// under the HTTP client timeout so a poll always returns 200 or 202.
const waitPollStep = 45 * time.Second

// Wait blocks until the worker is final or timeout passes, returning its ls entry.
func (c *Client) Wait(ctx context.Context, id string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)

	for {
		left := time.Until(deadline)
		if left <= 0 {
			entry, _ := c.get(ctx, "/ls", url.Values{"all": {"1"}})

			return entryFor(entry, id), ErrStillRunning
		}

		// Each long poll must finish inside the HTTP client's own timeout, or
		// the client cancels it and the wait ends with a transport error.
		step := min(left, waitPollStep)
		seconds := max(1, int(math.Ceil(step.Seconds())))
		q := url.Values{"id": {id}, "timeout": {fmt.Sprint(seconds)}}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fleet/wait?"+q.Encode(), nil)
		if err != nil {
			return "", err
		}

		body, code, err := c.doStatus(req)
		if err != nil {
			return "", err
		}

		if code == http.StatusOK {
			return body, nil
		}
	}
}

func entryFor(ls, id string) string {
	lines := strings.Split(ls, "\n")
	for i, l := range lines {
		if strings.Contains(l, " "+id+" ") {
			out := l
			if i+1 < len(lines) {
				out += "\n" + lines[i+1]
			}

			return out + "\n"
		}
	}

	return ls
}

// Statuses returns the machine-readable snapshot of live workers.
func (c *Client) Statuses(ctx context.Context) ([]Status, error) {
	body, err := c.get(ctx, "/workers", nil)
	if err != nil {
		return nil, err
	}

	var out []Status
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return nil, err
	}

	return out, nil
}

// Watch prints one line per state change or flag until every watched worker is
// final or timeout passes. With no ids it watches every worker live at start.
func (c *Client) Watch(ctx context.Context, ids []string, timeout time.Duration, out io.Writer) error {
	deadline := time.Now().Add(timeout)
	watch := map[string]bool{}

	for _, id := range ids {
		watch[id] = true
	}

	last := map[string]string{}

	for {
		statuses, err := c.Statuses(ctx)
		if err != nil {
			return err
		}

		if len(watch) == 0 {
			for _, st := range statuses {
				watch[st.ID] = true
			}
		}

		allFinal := len(watch) > 0

		for _, st := range statuses {
			if !watch[st.ID] {
				continue
			}

			key := string(st.State) + "|" + st.Flag
			if last[st.ID] != key {
				last[st.ID] = key
				mark := string(st.State)

				if st.Flag != "" {
					mark = "FLAG " + st.Flag
				}

				_, _ = fmt.Fprintf(out, "%s %s %s — %s | %s\n", time.Now().Format("15:04:05"), st.ID, mark, st.Label, st.Line)
			}

			if !st.State.Terminal() {
				allFinal = false
			}
		}

		if allFinal || time.Now().After(deadline) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// Spawn starts a worker and returns its first status lines.
func (c *Client) Spawn(ctx context.Context, spec Spec) (string, error) {
	return c.post(ctx, "/spawn", spec)
}

// Ls lists workers.
func (c *Client) Ls(ctx context.Context, all bool) (string, error) {
	q := url.Values{}
	if all {
		q.Set("all", "1")
	}

	return c.get(ctx, "/ls", q)
}

// Say sends a follow-up to a worker.
func (c *Client) Say(ctx context.Context, id, message string) (string, error) {
	return c.post(ctx, "/say", map[string]string{"id": id, "message": message})
}

// Result returns a worker's latest reply and footer.
func (c *Client) Result(ctx context.Context, id string) (string, error) {
	return c.get(ctx, "/result", url.Values{"id": {id}})
}

// Stop ends a worker.
func (c *Client) Stop(ctx context.Context, id string) (string, error) {
	return c.post(ctx, "/stop", map[string]string{"id": id})
}

// Log returns the last events of a worker.
func (c *Client) Log(ctx context.Context, id string, tail int) (string, error) {
	return c.get(ctx, "/log", url.Values{"id": {id}, "tail": {fmt.Sprint(tail)}})
}
