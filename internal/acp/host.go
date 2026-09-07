package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// inboundFrame is a daemon-to-host message: a resume handshake, one raw
// JSON-RPC line for the agent's stdin, or a kill order.
type inboundFrame struct {
	Resume *int64 `json:"resume,omitempty"`
	Line   string `json:"line,omitempty"`
	Kill   bool   `json:"kill,omitempty"`
}

// outboundFrame is a host-to-daemon message: the agent's pid, one recorded
// agent stdout line with its stream position, or the agent's exit.
type outboundFrame struct {
	Seq   int64  `json:"seq,omitempty"`
	Line  string `json:"line,omitempty"`
	PID   int    `json:"pid,omitempty"`
	Exit  bool   `json:"exit,omitempty"`
	Error string `json:"error,omitempty"`
}

func sockPath(workerdir string) string { return filepath.Join(workerdir, "host.sock") }

func newScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)

	return scanner
}

// host owns one agent process and bridges its stdio to one daemon connection
// at a time. Every agent stdout line is journaled to acp.jsonl with a stream
// position before it is forwarded, so a reattaching daemon replays exactly
// what it has not consumed.
type host struct {
	dir string

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stderr  *os.File
	stdout  io.ReadCloser
	journal *os.File
	ln      net.Listener

	// mu guards conn and every write to it; stdout pumping, replay and exit
	// delivery serialize on it so a daemon never sees lines out of order.
	mu        sync.Mutex
	conn      net.Conn
	seq       int64
	exited    bool
	exitErr   error
	delivered bool

	shutdownCh  chan struct{}
	shutdownNow sync.Once

	// exitedCh closes once the agent has exited and its tree is swept.
	exitedCh chan struct{}

	// doomed lists the agent's descendants at the moment a kill order went
	// out. They reparent to init when the agent exits, so they are found
	// before and swept after.
	doomed []int
}

// ServeHost runs the agent described by <workerdir>/launch.json and serves it
// on <workerdir>/host.sock until the agent's exit has been delivered to a
// daemon (or two hours pass with no daemon collecting it).
func ServeHost(ctx context.Context, workerdir string) error {
	b, err := os.ReadFile(filepath.Join(workerdir, "launch.json"))
	if err != nil {
		return fmt.Errorf("read launch.json: %w", err)
	}

	var launch LaunchSpec
	if err := json.Unmarshal(b, &launch); err != nil {
		return fmt.Errorf("parse launch.json: %w", err)
	}

	if len(launch.Argv) == 0 {
		return errors.New("launch.json: empty argv")
	}

	h := &host{dir: workerdir, shutdownCh: make(chan struct{}), exitedCh: make(chan struct{})}

	if err := h.startAgent(launch); err != nil {
		return err
	}

	sock := sockPath(workerdir)
	_ = os.Remove(sock)

	ln, err := net.Listen("unix", sock)
	if err != nil {
		return fmt.Errorf("listen %s: %w", sock, err)
	}

	h.ln = ln

	go h.pumpStdout()
	go h.waitAgent()

	go func() {
		<-ctx.Done()
		h.term()

		// Stay for the sweep of the agent's tree; leaving early would leave
		// the tree behind.
		select {
		case <-h.exitedCh:
		case <-time.After(termGrace + 5*time.Second):
		}

		h.shutdown()
	}()

	h.log("listening on %s", sock)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Accept only fails here once shutdown has closed the listener, so
			// the error describes the close, not a fault worth reporting.
			<-h.shutdownCh

			return nil //nolint:nilerr // a closed listener is a clean stop
		}

		h.handleConn(conn)
	}
}

// startAgent launches the agent exactly once, in its own process group so a
// kill order reaches everything that stays in it.
func (h *host) startAgent(launch LaunchSpec) error {
	stderr, err := os.OpenFile(launch.StderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open stderr log: %w", err)
	}

	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.Cwd
	cmd.Env = append(os.Environ(), launch.Env...)
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = stderr.Close()

		return fmt.Errorf("stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stderr.Close()

		return fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stderr.Close()

		return fmt.Errorf("start %s: %w", launch.Argv[0], err)
	}

	journal, err := os.OpenFile(filepath.Join(h.dir, "acp.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = stderr.Close()

		return fmt.Errorf("open journal: %w", err)
	}

	h.cmd, h.stdin, h.stderr, h.stdout, h.journal = cmd, stdin, stderr, stdout, journal
	h.log("agent pid %d: %v", cmd.Process.Pid, launch.Argv)

	return nil
}

func (h *host) log(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "host %s: %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// pumpStdout journals every agent line with the next stream position, then
// forwards it to the connected daemon.
func (h *host) pumpStdout() {
	scanner := newScanner(h.stdout)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		h.mu.Lock()
		h.seq++

		frame, err := json.Marshal(outboundFrame{Seq: h.seq, Line: string(line)})
		if err == nil {
			_, _ = h.journal.Write(append(frame, '\n'))
			h.writeConnLocked(frame)
		}

		h.mu.Unlock()
	}
}

// waitAgent delivers the agent's exit to the connected daemon and shuts the
// host down. With no daemon connected it waits up to two hours for one to
// collect the exit.
func (h *host) waitAgent() {
	err := h.cmd.Wait()

	h.mu.Lock()
	h.exited = true
	h.exitErr = err
	h.mu.Unlock()

	h.log("agent exited: %v", err)
	h.sweep()
	close(h.exitedCh)

	if h.deliverExit() {
		h.shutdown()

		return
	}

	select {
	case <-h.shutdownCh:
	case <-time.After(2 * time.Hour):
		h.log("no daemon collected the exit within 2h; exiting")
		h.shutdown()
	}
}

// deliverExit sends the exit frame if a daemon is connected and has not heard
// it yet. It reports whether the exit is delivered.
func (h *host) deliverExit() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.deliverExitLocked()
}

func (h *host) deliverExitLocked() bool {
	if h.delivered || h.conn == nil {
		return h.delivered
	}

	errStr := ""
	if h.exitErr != nil {
		errStr = h.exitErr.Error()
	}

	frame, _ := json.Marshal(outboundFrame{Exit: true, Error: errStr})
	if !h.writeConnLocked(frame) {
		return false
	}

	h.delivered = true

	return true
}

// handleConn runs the resume handshake on a fresh daemon connection: it
// replaces any previous connection, introduces the agent, replays journaled
// lines past the daemon's position, then hands the connection to a reader.
func (h *host) handleConn(conn net.Conn) {
	scanner := newScanner(conn)

	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	ok := scanner.Scan()
	_ = conn.SetReadDeadline(time.Time{})

	if !ok {
		_ = conn.Close()

		return
	}

	var f inboundFrame
	if err := json.Unmarshal(scanner.Bytes(), &f); err != nil || f.Resume == nil {
		h.log("connection without resume handshake; dropped")
		_ = conn.Close()

		return
	}

	h.mu.Lock()

	if h.conn != nil {
		// A second connection replaces the first.
		_ = h.conn.Close()
	}

	h.conn = conn

	pidFrame, _ := json.Marshal(outboundFrame{PID: h.cmd.Process.Pid})
	h.writeConnLocked(pidFrame)
	h.replayLocked(*f.Resume, h.seq)
	delivered := !h.exited || h.deliverExitLocked()

	h.mu.Unlock()

	if !delivered {
		return
	}

	if h.exited {
		h.shutdown()

		return
	}

	go h.readDaemon(conn, scanner)
}

// replayLocked resends journaled lines in (from, to]; to is the position when
// the handshake started, so lines journaled during the replay are left for
// live forwarding. Callers must hold mu.
func (h *host) replayLocked(from, to int64) {
	journal, err := os.Open(filepath.Join(h.dir, "acp.jsonl"))
	if err != nil {
		return
	}
	defer journal.Close()

	scanner := newScanner(journal)

	for scanner.Scan() {
		line := scanner.Bytes()

		var f outboundFrame
		if err := json.Unmarshal(line, &f); err != nil || f.Seq <= from {
			continue
		}

		if f.Seq > to {
			return
		}

		if !h.writeConnLocked(line) {
			return
		}
	}
}

// readDaemon forwards daemon lines to the agent's stdin and acts on kill
// orders until the daemon goes away; the agent keeps running for a later
// connection.
func (h *host) readDaemon(conn net.Conn, scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var f inboundFrame
		if err := json.Unmarshal(line, &f); err != nil {
			continue
		}

		switch {
		case f.Kill:
			h.mu.Lock()
			exited := h.exited
			h.mu.Unlock()

			if !exited {
				h.term()

				break // the agent's exit is delivered through the usual path
			}

			h.deliverExit()
			h.shutdown()

			return
		case f.Line != "":
			_, _ = h.stdin.Write([]byte(f.Line + "\n"))
		}
	}

	h.mu.Lock()

	if h.conn == conn {
		h.conn = nil
	}

	h.mu.Unlock()
	_ = conn.Close()
}

// writeConnLocked writes one frame to the connected daemon, dropping the
// connection on error. Callers must hold mu.
func (h *host) writeConnLocked(frame []byte) bool {
	if h.conn == nil {
		return false
	}

	if _, err := h.conn.Write(append(frame, '\n')); err != nil {
		_ = h.conn.Close()
		h.conn = nil

		return false
	}

	return true
}

func (h *host) shutdown() {
	h.shutdownNow.Do(func() {
		close(h.shutdownCh)
		_ = h.ln.Close()

		h.mu.Lock()

		if h.conn != nil {
			_ = h.conn.Close()
		}

		h.mu.Unlock()

		_ = h.journal.Close()
		_ = h.stderr.Close()
	})
}

// termGrace is how long the agent's tree gets to leave on SIGTERM before it
// is killed outright.
const termGrace = 15 * time.Second

// term asks the agent's tree to exit and, if the agent is still there after
// termGrace, kills it outright.
func (h *host) term() {
	h.killAgent(syscall.SIGTERM)

	time.AfterFunc(termGrace, func() {
		h.mu.Lock()
		exited := h.exited
		h.mu.Unlock()

		if !exited {
			h.log("agent ignored SIGTERM for %s; sending SIGKILL", termGrace)
			h.killAgent(syscall.SIGKILL)
		}
	})
}

// killAgent signals the agent's process group and every process under the
// agent. omp starts some of its workers in their own process groups, so the
// group signal alone leaves them behind; they are listed while the agent is
// still their ancestor and remembered for sweep.
func (h *host) killAgent(sig syscall.Signal) {
	if h.cmd.Process == nil {
		return
	}

	pid := h.cmd.Process.Pid
	tree := descendants(pid)

	h.mu.Lock()
	h.doomed = append(h.doomed, tree...)
	h.mu.Unlock()

	_ = syscall.Kill(-pid, sig)

	for _, p := range tree {
		_ = syscall.Kill(p, sig)
	}
}

// sweep waits briefly for the processes killAgent listed to follow the
// agent out, then kills the ones that did not.
func (h *host) sweep() {
	h.mu.Lock()
	doomed := h.doomed
	h.doomed = nil
	h.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		if len(alive(doomed)) == 0 {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	for _, p := range alive(doomed) {
		h.log("process %d outlived the agent; killing it", p)
		_ = syscall.Kill(p, syscall.SIGKILL)
	}
}

// alive keeps the pids the kernel still knows.
func alive(pids []int) []int {
	var out []int

	for _, p := range pids {
		if err := syscall.Kill(p, 0); err == nil || errors.Is(err, syscall.EPERM) {
			out = append(out, p)
		}
	}

	return out
}

// descendants lists every live process under pid from one ps snapshot; ps is
// the process table both macOS and Linux expose.
func descendants(pid int) []int {
	out, err := exec.Command("ps", "-axo", "pid=,ppid=").Output()
	if err != nil {
		return nil
	}

	children := make(map[int][]int)

	for _, line := range strings.Split(string(out), "\n") {
		var p, pp int
		if _, err := fmt.Sscan(line, &p, &pp); err != nil {
			continue
		}

		children[pp] = append(children[pp], p)
	}

	var tree []int

	queue := []int{pid}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]

		for _, c := range children[p] {
			tree = append(tree, c)
			queue = append(queue, c)
		}
	}

	return tree
}
