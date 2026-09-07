// Package acp is a minimal Agent Client Protocol client: JSON-RPC 2.0 lines
// carried between the daemon and one agent process per worker. A per-worker
// host process owns the agent (see ServeHost), so the agent survives a daemon
// restart; the client below is the daemon's side of that link.
package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Notification is a JSON-RPC notification from the agent (no id). Seq is the
// host's stream position of the line that carried it.
type Notification struct {
	Seq    int64
	Method string
	Params json.RawMessage
}

// Request is a JSON-RPC request from the agent that the client must answer.
// Seq is the host's stream position of the line that carried it.
type Request struct {
	Seq    int64
	ID     json.RawMessage
	Method string
	Params json.RawMessage
}

// CallResult is the answer to a client request, tagged with the stream
// position of the line that carried it.
type CallResult struct {
	Seq    int64
	Result json.RawMessage
	Err    error
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError is the JSON-RPC error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// ErrClosed is returned for requests outstanding when the link to the agent's
// host ends. Exited reports whether the agent itself exited.
var ErrClosed = errors.New("acp: link to the agent's host closed")

// Client is the daemon's handle on one worker's agent, connected through the
// worker's host socket.
type Client struct {
	conn net.Conn
	pid  int

	writeMu sync.Mutex
	nextID  atomic.Int64
	seq     atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]chan CallResult

	notifications chan Notification
	requests      chan Request
	done          chan struct{}
	exited        atomic.Bool
	ended         atomic.Bool
	exitErr       error
	finishOnce    sync.Once
}

// connect dials the worker's host, resumes the stream after resumeSeq, and
// pre-registers expect ids as pending so replayed responses are not missed.
func connect(workerdir string, resumeSeq int64, expect []int64) (*Client, error) {
	conn, err := net.DialTimeout("unix", sockPath(workerdir), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial host: %w", err)
	}

	c := &Client{
		conn:          conn,
		pending:       make(map[int64]chan CallResult),
		notifications: make(chan Notification, 1024),
		requests:      make(chan Request, 16),
		done:          make(chan struct{}),
	}

	for _, id := range expect {
		c.pending[id] = make(chan CallResult, 1)
	}

	c.seq.Store(resumeSeq)

	if err := c.writeFrame(inboundFrame{Resume: &resumeSeq}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("resume handshake: %w", err)
	}

	// The host answers every resume with the agent's pid before any replay.
	// The buffered reader is shared with the read loop so bytes read ahead
	// during the handshake are not lost.
	br := bufio.NewReader(conn)

	if err := c.readPID(br); err != nil {
		_ = conn.Close()
		return nil, err
	}

	go c.readLoop(br)

	return c, nil
}

func (c *Client) readPID(br *bufio.Reader) error {
	if err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	defer c.conn.SetReadDeadline(time.Time{}) //nolint:errcheck // best-effort reset

	line, err := br.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read pid frame: %w", err)
	}

	var f outboundFrame
	if err := json.Unmarshal(line, &f); err != nil || f.PID == 0 {
		return fmt.Errorf("host did not introduce its agent (got %s)", line)
	}

	c.pid = f.PID

	return nil
}

// LaunchSpec describes how the host starts the agent; persisted as
// launch.json in the worker directory so the host needs no other input.
type LaunchSpec struct {
	Argv       []string `json:"argv"`
	Env        []string `json:"env"`
	Cwd        string   `json:"cwd"`
	StderrPath string   `json:"stderr_path"`
}

// Launch writes launch.json into workerdir, starts the host process detached,
// and returns a client once the host's socket answers. The host outlives the
// caller: it keeps the agent alive across a daemon restart.
func Launch(hostArgv []string, workerdir string, launch LaunchSpec) (*Client, error) {
	if len(hostArgv) == 0 {
		return nil, errors.New("acp: empty host argv")
	}

	if len(launch.Argv) == 0 {
		return nil, errors.New("acp: empty agent argv")
	}

	b, err := json.Marshal(launch)
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(workerdir, "launch.json"), b, 0o644); err != nil {
		return nil, err
	}

	logf, err := os.OpenFile(filepath.Join(workerdir, "host.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	defer logf.Close()

	cmd := exec.Command(hostArgv[0], hostArgv[1:]...)
	cmd.Stdin = nil
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start host: %w", err)
	}

	// The host is in its own session and outlives this process. Waiting on
	// it only reaps the exit status, so a finished host is not left a zombie.
	go func() { _ = cmd.Wait() }()

	deadline := time.Now().Add(10 * time.Second)

	for {
		client, err := connect(workerdir, 0, nil)
		if err == nil {
			return client, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("host did not answer within 10s (see %s): %w", filepath.Join(workerdir, "host.log"), err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Attach connects to the host of a worker the daemon has lost, resuming the
// stream after resumeSeq. expect lists request ids still in flight from
// before the loss; their replayed responses are routed to Expect channels.
func Attach(workerdir string, resumeSeq int64, expect ...int64) (*Client, error) {
	return connect(workerdir, resumeSeq, expect)
}

// KillHost attaches to a worker's host only to end its agent, and waits up to
// wait for the exit. It reports whether a host answered: one that does not is
// already gone, which is the outcome the caller wants. Nothing is replayed,
// since a client that only kills has no use for the stream.
func KillHost(workerdir string, wait time.Duration) (bool, error) {
	client, err := connect(workerdir, math.MaxInt64, nil)
	if err != nil {
		return false, nil //nolint:nilerr // no host answers: nothing to kill
	}
	defer client.Close()

	client.Kill()

	select {
	case <-client.Done():
		return true, nil
	case <-time.After(wait):
		return true, fmt.Errorf("kill host: agent of %s did not exit within %s", workerdir, wait)
	}
}

// Notifications delivers agent notifications in stream order. Closed when the
// link ends.
func (c *Client) Notifications() <-chan Notification { return c.notifications }

// Requests delivers agent-to-client requests. Closed when the link ends.
func (c *Client) Requests() <-chan Request { return c.requests }

// Done is closed when the link to the host has ended, by the agent's exit or
// by losing the host.
func (c *Client) Done() <-chan struct{} { return c.done }

// Exited reports whether the link ended because the agent process exited.
func (c *Client) Exited() bool { return c.exited.Load() }

// Detached reports whether the link ended without the agent exiting: the host
// replaced this connection or the daemon is shutting down.
func (c *Client) Detached() bool { return c.ended.Load() && !c.exited.Load() }

// ExitError reports the agent's exit error once Done is closed.
func (c *Client) ExitError() error { return c.exitErr }

// PID returns the agent process id, learned during the handshake.
func (c *Client) PID() int { return c.pid }

// Seq returns the highest stream position the host has sent so far.
func (c *Client) Seq() int64 { return c.seq.Load() }

func (c *Client) readLoop(r io.Reader) {
	scanner := newScanner(r)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var f outboundFrame
		if err := json.Unmarshal(line, &f); err != nil {
			continue
		}

		switch {
		case f.Exit:
			c.exited.Store(true)

			if f.Error != "" {
				c.exitErr = errors.New(f.Error)
			}

			c.finish()

			return
		case f.Line != "":
			c.seq.Store(f.Seq)
			c.dispatch(f.Seq, f.Line)
		}
	}

	c.finish()
}

func (c *Client) dispatch(seq int64, line string) {
	var msg rpcMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return
	}

	switch {
	case msg.Method != "" && len(msg.ID) > 0:
		c.requests <- Request{Seq: seq, ID: msg.ID, Method: msg.Method, Params: msg.Params}
	case msg.Method != "":
		select {
		case c.notifications <- Notification{Seq: seq, Method: msg.Method, Params: msg.Params}:
		default:
		}
	case len(msg.ID) > 0:
		var id int64
		if err := json.Unmarshal(msg.ID, &id); err != nil {
			return
		}

		c.pendingMu.Lock()
		ch, ok := c.pending[id]
		delete(c.pending, id)
		c.pendingMu.Unlock()

		if ok {
			// msg.Error is a typed pointer; keep a nil result a nil error.
			var callErr error
			if msg.Error != nil {
				callErr = msg.Error
			}

			ch <- CallResult{Seq: seq, Result: msg.Result, Err: callErr}
		}
	}
}

// finish ends the link exactly once: outstanding calls fail with ErrClosed
// and every channel closes.
func (c *Client) finish() {
	c.finishOnce.Do(func() {
		c.ended.Store(true)

		c.pendingMu.Lock()
		for id, ch := range c.pending {
			ch <- CallResult{Err: ErrClosed}
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()

		close(c.notifications)
		close(c.requests)
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *Client) writeFrame(f inboundFrame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_, err = c.conn.Write(append(b, '\n'))

	return err
}

// writeLine ships one raw JSON-RPC line to the agent through the host.
func (c *Client) writeLine(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return c.writeFrame(inboundFrame{Line: string(b)})
}

// Reserve allocates the next request id without sending anything.
func (c *Client) Reserve() int64 { return c.nextID.Add(1) }

// SetNextID raises the id counter so ids issued before a restart never
// repeat. It never lowers the counter.
func (c *Client) SetNextID(n int64) {
	for {
		cur := c.nextID.Load()
		if n <= cur || c.nextID.CompareAndSwap(cur, n) {
			return
		}
	}
}

// Expect registers id as in flight and returns the channel its result arrives
// on. An id already registered (e.g. by Attach) returns the existing channel.
func (c *Client) Expect(id int64) <-chan CallResult {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	ch, ok := c.pending[id]
	if !ok {
		ch = make(chan CallResult, 1)
		c.pending[id] = ch
	}

	return ch
}

// Send writes a request with an id from Reserve. Pair it with Expect before
// sending so a fast response cannot be missed.
func (c *Client) Send(id int64, method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}

	return c.writeLine(rpcMessage{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method, Params: raw})
}

// AwaitCall waits for the response to a request registered with Expect and
// unpacks it like Call. It returns the stream position of the response.
func (c *Client) AwaitCall(ctx context.Context, id int64, result any) (int64, error) {
	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	c.pendingMu.Unlock()

	if !ok {
		return 0, fmt.Errorf("acp: request %d is not pending", id)
	}

	select {
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()

		return 0, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			if errors.Is(res.Err, ErrClosed) {
				return res.Seq, ErrClosed
			}

			return res.Seq, res.Err
		}

		if result != nil && len(res.Result) > 0 {
			return res.Seq, json.Unmarshal(res.Result, result)
		}

		return res.Seq, nil
	}
}

// Call sends a request and waits for its response or ctx cancellation.
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	id := c.Reserve()
	_ = c.Expect(id)

	if err := c.Send(id, method, params); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()

		return fmt.Errorf("%s: write: %w", method, err)
	}

	_, err := c.AwaitCall(ctx, id, result)
	if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", method, err)
	}

	return err
}

// Notify sends a notification (no response expected).
func (c *Client) Notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}

	return c.writeLine(rpcMessage{JSONRPC: "2.0", Method: method, Params: raw})
}

// Respond answers an agent-to-client request.
func (c *Client) Respond(req Request, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return c.writeLine(rpcMessage{JSONRPC: "2.0", ID: req.ID, Result: raw})
}

// RespondError answers an agent-to-client request with an error.
func (c *Client) RespondError(req Request, code int, message string) error {
	return c.writeLine(rpcMessage{JSONRPC: "2.0", ID: req.ID, Error: &RPCError{Code: code, Message: message}})
}

// Kill asks the host to terminate the agent's process group.
func (c *Client) Kill() { _ = c.writeFrame(inboundFrame{Kill: true}) }

// Close drops the connection to the host. The agent keeps running; a later
// Attach resumes it.
func (c *Client) Close() { c.finish() }
