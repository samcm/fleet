// Package acp is a minimal Agent Client Protocol client: JSON-RPC 2.0 over the
// stdio of an agent process, with notifications and agent-to-client requests
// surfaced to the caller.
package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
)

// Notification is a JSON-RPC notification from the agent (no id).
type Notification struct {
	Method string
	Params json.RawMessage
}

// Request is a JSON-RPC request from the agent that the client must answer.
type Request struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
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

// ErrClosed is returned when the agent process has exited.
var ErrClosed = errors.New("acp: agent process closed")

// Client drives one agent process.
type Client struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *os.File

	writeMu sync.Mutex
	nextID  atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]chan rpcMessage

	notifications chan Notification
	requests      chan Request
	done          chan struct{}
	exitErr       error
}

// Start launches argv with the given environment additions and working
// directory, and begins reading its stdout. stderr is appended to stderrPath.
func Start(ctx context.Context, argv []string, env []string, dir, stderrPath string) (*Client, error) {
	if len(argv) == 0 {
		return nil, errors.New("acp: empty argv")
	}

	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open stderr log: %w", err)
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stderr.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		stderr.Close()
		return nil, fmt.Errorf("start %s: %w", argv[0], err)
	}

	c := &Client{
		cmd:           cmd,
		stdin:         stdin,
		stderr:        stderr,
		pending:       make(map[int64]chan rpcMessage),
		notifications: make(chan Notification, 1024),
		requests:      make(chan Request, 16),
		done:          make(chan struct{}),
	}

	go c.readLoop(stdout)

	return c, nil
}

// Notifications delivers agent notifications in order. Closed when the process exits.
func (c *Client) Notifications() <-chan Notification { return c.notifications }

// Requests delivers agent-to-client requests. Closed when the process exits.
func (c *Client) Requests() <-chan Request { return c.requests }

// Done is closed when the agent process has exited.
func (c *Client) Done() <-chan struct{} { return c.done }

// ExitError reports the process exit error once Done is closed.
func (c *Client) ExitError() error { return c.exitErr }

// PID returns the agent process id.
func (c *Client) PID() int { return c.cmd.Process.Pid }

func (c *Client) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var msg rpcMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		switch {
		case msg.Method != "" && len(msg.ID) > 0:
			c.requests <- Request{ID: msg.ID, Method: msg.Method, Params: msg.Params}
		case msg.Method != "":
			select {
			case c.notifications <- Notification{Method: msg.Method, Params: msg.Params}:
			default:
			}
		case len(msg.ID) > 0:
			var id int64
			if err := json.Unmarshal(msg.ID, &id); err != nil {
				continue
			}

			c.pendingMu.Lock()
			ch, ok := c.pending[id]
			delete(c.pending, id)
			c.pendingMu.Unlock()

			if ok {
				ch <- msg
			}
		}
	}

	c.exitErr = c.cmd.Wait()
	c.stderr.Close()

	c.pendingMu.Lock()
	for id, ch := range c.pending {
		ch <- rpcMessage{Error: &RPCError{Code: -1, Message: ErrClosed.Error()}}
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	close(c.notifications)
	close(c.requests)
	close(c.done)
}

func (c *Client) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_, err = c.stdin.Write(append(b, '\n'))

	return err
}

// Call sends a request and waits for its response or ctx cancellation.
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	id := c.nextID.Add(1)
	ch := make(chan rpcMessage, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}

	if err := c.write(rpcMessage{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method, Params: raw}); err != nil {
		return fmt.Errorf("%s: write: %w", method, err)
	}

	select {
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()

		return ctx.Err()
	case msg := <-ch:
		if msg.Error != nil {
			if msg.Error.Message == ErrClosed.Error() {
				return ErrClosed
			}

			return fmt.Errorf("%s: %w", method, msg.Error)
		}

		if result != nil && len(msg.Result) > 0 {
			return json.Unmarshal(msg.Result, result)
		}

		return nil
	}
}

// Notify sends a notification (no response expected).
func (c *Client) Notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}

	return c.write(rpcMessage{JSONRPC: "2.0", Method: method, Params: raw})
}

// Respond answers an agent-to-client request.
func (c *Client) Respond(req Request, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return c.write(rpcMessage{JSONRPC: "2.0", ID: req.ID, Result: raw})
}

// RespondError answers an agent-to-client request with an error.
func (c *Client) RespondError(req Request, code int, message string) error {
	return c.write(rpcMessage{JSONRPC: "2.0", ID: req.ID, Error: &RPCError{Code: code, Message: message}})
}

// Kill terminates the agent process group.
func (c *Client) Kill() {
	if c.cmd.Process != nil {
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGTERM)
	}
}
