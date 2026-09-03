package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	fakeBin  string
	fleetBin string
)

// TestMain compiles the fake agent and a fleet binary (for the per-worker
// host subprocesses) into a temporary directory. Nothing touches the real
// home directory or a running daemon: every test gets a temporary home.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "fleet-testbins")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fakeBin = filepath.Join(tmp, "fakeagent")
	if out, err := exec.Command("go", "build", "-o", fakeBin, "testdata/fakeagent/main.go").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build fakeagent: %v\n%s\n", err, out)
		os.Exit(1)
	}

	fleetBin = filepath.Join(tmp, "fleet")
	if out, err := exec.Command("go", "build", "-o", fleetBin, "../../cmd/fleet").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build fleet: %v\n%s\n", err, out)
		os.Exit(1)
	}

	code := m.Run()

	_ = os.RemoveAll(tmp)
	os.Exit(code)
}

// newDaemonOn builds a Daemon on home that launches worker hosts from the
// test fleet binary instead of the test process itself.
func newDaemonOn(t *testing.T, home string) *Daemon {
	t.Helper()

	d, err := NewDaemon(home, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	d.hostArgv = []string{fleetBin, "host"}

	return d
}

func newTestDaemon(t *testing.T) (*Daemon, string) {
	t.Helper()

	// Unix socket paths are length-limited, so the home must stay short;
	// t.TempDir() under macOS $TMPDIR is too long for workers/<id>/host.sock.
	home, err := os.MkdirTemp("/tmp", "flt")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(home) })

	if err := os.MkdirAll(Root(home), 0o755); err != nil {
		t.Fatal(err)
	}

	agents, _ := json.Marshal(map[string]Agent{"fake": {Argv: []string{fakeBin}}})
	if err := os.WriteFile(filepath.Join(Root(home), "agents.json"), agents, 0o644); err != nil {
		t.Fatal(err)
	}

	return newDaemonOn(t, home), home
}

// startServe runs the daemon's HTTP socket and returns a stop function that
// shuts it down and waits for Serve to return.
func startServe(t *testing.T, d *Daemon) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- d.Serve(ctx) }()

	client := NewClient(d.home)
	waitFor(t, 5*time.Second, func() bool { return client.ping(ctx) == nil }, "daemon socket to answer")

	var once sync.Once

	return func() { once.Do(func() { cancel(); <-done }) }
}

func spawnFake(t *testing.T, d *Daemon, brief string, writes bool) string {
	t.Helper()

	id, err := d.Spawn(context.Background(), Spec{
		Agent: "fake", Model: "test/model", Thinking: "high",
		Cwd: t.TempDir(), Label: "integration test worker", Brief: brief, Writes: writes, Minutes: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

func waitFinal(t *testing.T, d *Daemon, id string, timeout time.Duration) Meta {
	t.Helper()

	var m Meta
	waitFor(t, timeout, func() bool {
		wk, err := d.worker(id)
		if err != nil {
			return false
		}

		m = wk.Meta()

		return m.State.Terminal()
	}, "worker "+id+" to finish")

	return m
}

// TestSpawnWaitResult covers one full turn: reply text, per-turn usage and
// the live usage_update figures.
func TestSpawnWaitResult(t *testing.T) {
	d, _ := newTestDaemon(t)

	id := spawnFake(t, d, "duration=500ms permdelay=50ms tag=one", true)

	m := waitFinal(t, d, id, 30*time.Second)
	if m.State != StateDone {
		t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
	}

	wk, err := d.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	text := wk.Result()
	for _, want := range []string{"alpha", "beta", "gamma perm=allowed", "tag=one"} {
		if !strings.Contains(text, want) {
			t.Errorf("reply missing %q:\n%s", want, text)
		}
	}

	if m.Usage.Input != 100 || m.Usage.Output != 50 || m.Usage.CachedRead != 10 {
		t.Errorf("per-turn usage %+v, want 100/50/10", m.Usage)
	}

	if m.ContextUsed != 176878 || m.ContextSize != 1048576 {
		t.Errorf("context %d/%d, want 176878/1048576", m.ContextUsed, m.ContextSize)
	}

	if m.CostUSD != 7.0859 {
		t.Errorf("cost %v, want 7.0859", m.CostUSD)
	}

	if ls := d.Ls(true, id); !strings.Contains(ls, "ctx 177k/1.0M $7.09") {
		t.Errorf("ls entry missing live usage:\n%s", ls)
	}
}

// TestDaemonRestartResumesTurn drops the daemon mid-turn and proves the next
// daemon reattaches to the surviving host: the worker stays RUNNING, finishes
// DONE with the full reply, and no journaled line is handled twice.
func TestDaemonRestartResumesTurn(t *testing.T) {
	d1, home := newTestDaemon(t)
	stop1 := startServe(t, d1)

	id := spawnFake(t, d1, "duration=20s permdelay=100ms tag=one", true)

	wk1, err := d1.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 15*time.Second, func() bool { return strings.Contains(wk1.Result(), "beta") }, "first chunks")

	ackBefore := wk1.Meta().AckSeq
	if ackBefore == 0 {
		t.Fatal("worker handled no journaled lines before the restart")
	}

	// The daemon dies: Serve stops and the Daemon value is dropped.
	stop1()
	d1 = nil

	d2 := newDaemonOn(t, home)

	wk2, err := d2.worker(id)
	if err != nil {
		t.Fatalf("worker not reattached: %v", err)
	}

	if m := wk2.Meta(); m.State != StateRunning {
		t.Fatalf("state %s after reattach, want RUNNING", m.State)
	}

	m := waitFinal(t, d2, id, 60*time.Second)
	if m.State != StateDone {
		t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
	}

	if m.AckSeq <= ackBefore {
		t.Errorf("stream position did not advance across the restart: %d -> %d", ackBefore, m.AckSeq)
	}

	text := wk2.Result()
	for _, frag := range []string{"alpha ", "beta ", "tag=one ", "gamma perm=allowed "} {
		if n := strings.Count(text, frag); n != 1 {
			t.Errorf("%q appears %d times in the reply, want exactly 1:\n%s", frag, n, text)
		}
	}

	if m.Usage.Input != 100 || m.Usage.Output != 50 {
		t.Errorf("per-turn usage %+v, want 100/50", m.Usage)
	}
}

// TestResultTurns reads earlier turns back after a follow-up, through the
// daemon's HTTP surface: turn=N, turn=all and the footer's turn list.
func TestResultTurns(t *testing.T) {
	d, home := newTestDaemon(t)
	stop := startServe(t, d)
	defer stop()

	id := spawnFake(t, d, "duration=300ms permdelay=50ms tag=one", true)

	if m := waitFinal(t, d, id, 30*time.Second); m.State != StateDone {
		t.Fatalf("turn 1: state %s (%s)", m.State, m.Detail)
	}

	wk, err := d.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	if status, err := wk.Say("duration=300ms permdelay=50ms tag=two"); err != nil {
		t.Fatalf("say: %v (%s)", err, status)
	}

	m := waitFinal(t, d, id, 30*time.Second)
	if m.State != StateDone || m.Turns != 2 {
		t.Fatalf("turn 2: state %s turns %d (%s)", m.State, m.Turns, m.Detail)
	}

	client := NewClient(home)
	ctx := context.Background()

	turn1, err := client.Result(ctx, id, "1")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(turn1, "tag=one") || strings.Contains(turn1, "tag=two") {
		t.Errorf("turn=1 returned the wrong turn:\n%s", turn1)
	}

	turn2, err := client.Result(ctx, id, "2")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(turn2, "tag=two") {
		t.Errorf("turn=2 missing turn 2 text:\n%s", turn2)
	}

	all, err := client.Result(ctx, id, "all")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"---- turn 1 (prompt: duration=300ms permdelay=50ms tag=one) ----",
		"---- turn 2 (prompt: duration=300ms permdelay=50ms tag=two) ----",
		"tag=one", "tag=two",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("turn=all missing %q:\n%s", want, all)
		}
	}

	for _, body := range []string{turn1, turn2} {
		if !strings.Contains(body, "turns: 1 DONE") || !strings.Contains(body, "2 DONE") || !strings.Contains(body, "(fleet_result turn=N for an earlier one)") {
			t.Errorf("result footer missing the turn list:\n%s", body)
		}
	}
}

// TestPermissionRejected covers the read-only refusal of an edit permission,
// both live and when the request reaches the journal during a restart gap and
// is replayed to the next daemon.
func TestPermissionRejected(t *testing.T) {
	t.Run("live", func(t *testing.T) {
		d, _ := newTestDaemon(t)

		id := spawnFake(t, d, "duration=300ms permdelay=50ms", false)

		m := waitFinal(t, d, id, 30*time.Second)
		if m.State != StateDone {
			t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
		}

		wk, _ := d.worker(id)
		if text := wk.Result(); !strings.Contains(text, "perm=rejected") {
			t.Errorf("read-only worker did not reject the edit:\n%s", text)
		}
	})

	t.Run("restart gap", func(t *testing.T) {
		d1, home := newTestDaemon(t)
		stop1 := startServe(t, d1)

		id := spawnFake(t, d1, "duration=4s permdelay=2s", false)

		wk1, err := d1.worker(id)
		if err != nil {
			t.Fatal(err)
		}

		waitFor(t, 10*time.Second, func() bool { return wk1.Meta().ContextUsed > 0 }, "usage update before the restart")

		stop1()
		d1 = nil

		// The permission request must reach the host's journal while no
		// daemon is connected, so the next daemon replays it.
		journal := filepath.Join(Root(home), "workers", id, "acp.jsonl")
		waitFor(t, 10*time.Second, func() bool {
			b, err := os.ReadFile(journal)

			return err == nil && strings.Contains(string(b), "request_permission")
		}, "permission request to reach the journal")

		d2 := newDaemonOn(t, home)

		m := waitFinal(t, d2, id, 30*time.Second)
		if m.State != StateDone {
			t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
		}

		wk2, _ := d2.worker(id)
		if text := wk2.Result(); !strings.Contains(text, "perm=rejected") {
			t.Errorf("replayed permission request was not rejected:\n%s", text)
		}
	})
}

// TestWaitEndpoint covers the wait endpoint's two answers: 202 with the
// entry while the worker runs, 200 with the entry once it is final.
func TestWaitEndpoint(t *testing.T) {
	d, home := newTestDaemon(t)
	stop := startServe(t, d)
	defer stop()

	id := spawnFake(t, d, "duration=3s permdelay=50ms", true)

	client := NewClient(home)
	ctx := context.Background()

	entry, final, err := client.WaitOnce(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}

	if final {
		t.Errorf("wait reported final for a running worker:\n%s", entry)
	}

	if !strings.Contains(entry, id) {
		t.Errorf("202 answer missing the ls entry:\n%s", entry)
	}

	entry, err = client.Wait(ctx, id, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(entry, "DONE") {
		t.Errorf("wait returned before the worker was DONE:\n%s", entry)
	}
}
