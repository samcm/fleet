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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/samcm/fleet/internal/acp"
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
	t.Cleanup(func() { killHosts(t, home) })

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
	waitFor(t, waitTimeout, func() bool { return client.ping(ctx) == nil }, "daemon socket to answer")

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

// waitTimeout bounds the condition waits. It is generous because a loaded CI
// runner under -race is several times slower than a developer machine, and the
// waits only bound how long a genuine failure takes to report.
const waitTimeout = 30 * time.Second

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

func TestSpawnAccountPool(t *testing.T) {
	const envKey = "OMP_AUTH_BROKER_ACCOUNT_POOL_FILE"
	t.Setenv(envKey, "")
	if err := os.Unsetenv(envKey); err != nil {
		t.Fatal(err)
	}

	d, home := newTestDaemon(t)
	const identity = "email:worker@example.com|org:test-org"
	agent := d.agents["fake"]
	agent.Env = make([]string, 2, 4)
	agent.Env[0] = "HOME=" + filepath.Join(Root(home), "barehome")
	agent.Env[1] = "PI_CODING_AGENT_DIR=" + filepath.Join(Root(home), "bareagent")
	d.agents["fake"] = agent
	const config = "accounts:\n  worker:\n    provider: test\n    identity: " + identity + "\n"
	path := filepath.Join(Root(home), "accounts.yaml")
	if err := os.WriteFile(path, []byte(config+"defaults:\n  test: worker\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	id := spawnFake(t, d, "duration=100ms permdelay=10ms accountpool=1", true)
	m := waitFinal(t, d, id, waitTimeout)
	if m.State != StateDone {
		t.Fatalf("pinned worker: %s (%s)", m.State, m.Detail)
	}

	wk, err := d.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	reply := wk.Result()
	if !strings.Contains(reply, "accountpool-present=true") {
		t.Fatalf("agent did not receive pool environment: %s", reply)
	}

	var poolPath string

	for _, want := range []string{"home=" + filepath.Join(Root(home), "barehome"), "agentdir=" + filepath.Join(Root(home), "bareagent")} {
		if !strings.Contains(reply, want+" ") {
			t.Errorf("agent environment missing %q: %s", want, reply)
		}
	}
	for _, token := range strings.Fields(reply) {
		if value, ok := strings.CutPrefix(token, "accountpool="); ok {
			poolPath = value
		}
	}

	if !filepath.IsAbs(poolPath) || poolPath != filepath.Join(Root(home), "workers", id, "accounts.json") {
		t.Fatalf("unexpected pool path %q", poolPath)
	}

	data, err := os.ReadFile(poolPath)
	if err != nil {
		t.Fatal(err)
	}

	var pool map[string][]string
	if err := json.Unmarshal(data, &pool); err != nil {
		t.Fatal(err)
	}

	if len(pool) != 1 || len(pool["test"]) != 1 || pool["test"][0] != identity {
		t.Fatalf("unexpected pool: %s", data)
	}

	info, err := os.Stat(poolPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode().Perm() != 0o600 {
		t.Fatalf("pool mode %o, want 600", info.Mode().Perm())
	}

	data, err = os.ReadFile(filepath.Join(Root(home), "workers", id, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}

	var persisted Meta
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}

	if persisted.Spec.Account != "worker" || persisted.AccountIdentity != identity {
		t.Fatalf("persisted account = %q, identity = %q", persisted.Spec.Account, persisted.AccountIdentity)
	}

	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	unpinnedID := spawnFake(t, d, "duration=100ms permdelay=10ms accountpool=1", true)
	unpinned := waitFinal(t, d, unpinnedID, waitTimeout)
	if unpinned.State != StateDone || unpinned.Spec.Account != "" || unpinned.AccountIdentity != "" {
		t.Fatalf("unpinned worker: %+v", unpinned)
	}

	unpinnedWorker, err := d.worker(unpinnedID)
	if err != nil {
		t.Fatal(err)
	}

	if reply := unpinnedWorker.Result(); !strings.Contains(reply, "accountpool= accountpool-present=false") {
		t.Fatalf("unpinned agent received pool environment: %s", reply)
	}

	for _, test := range []struct {
		id      string
		account string
	}{{id, "worker"}, {unpinnedID, "-"}} {
		lines := strings.Split(d.Ls(true, test.id), "\n")
		fields := strings.Fields(lines[1])
		if len(fields) < 8 || fields[6] != test.account {
			t.Errorf("ls account = %q, want %q", lines[1], test.account)
		}
	}

	t.Log(strings.Split(d.Ls(true, id), "\n")[0])
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

	waitFor(t, waitTimeout, func() bool { return strings.Contains(wk1.Result(), "beta") }, "first chunks")

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

		waitFor(t, waitTimeout, func() bool { return wk1.Meta().ContextUsed > 0 }, "usage update before the restart")

		stop1()
		d1 = nil

		// The permission request must reach the host's journal while no
		// daemon is connected, so the next daemon replays it.
		journal := filepath.Join(Root(home), "workers", id, "acp.jsonl")
		waitFor(t, waitTimeout, func() bool {
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

// killHosts ends every worker host under home so a test leaves no agent
// behind. Cleanups run last-registered first, so it runs before the home is
// removed.
func killHosts(t *testing.T, home string) {
	t.Helper()

	entries, _ := os.ReadDir(filepath.Join(Root(home), "workers"))
	for _, e := range entries {
		if _, err := acp.KillHost(filepath.Join(Root(home), "workers", e.Name()), waitTimeout); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}
}

func released(wk *Worker) bool {
	wk.mu.Lock()
	defer wk.mu.Unlock()

	return wk.released
}

func processGone(pid int) bool { return syscall.Kill(pid, 0) != nil }

// TestIdleWorkerReleased proves a finished worker's agent is ended once it
// has sat idle for idleAfter, and not before: the state, the reply and the
// ls entry stay, and say asks for a new worker.
func TestIdleWorkerReleased(t *testing.T) {
	d, _ := newTestDaemon(t)

	id := spawnFake(t, d, "duration=300ms permdelay=50ms tag=one", true)

	if m := waitFinal(t, d, id, 30*time.Second); m.State != StateDone {
		t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
	}

	wk, err := d.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	agent := wk.Meta().PID

	d.releaseIdleAt(time.Now().Add(idleAfter / 2))

	if released(wk) {
		t.Fatal("released before idleAfter")
	}

	d.releaseIdleAt(time.Now().Add(idleAfter + time.Minute))

	if !released(wk) {
		t.Fatal("not released after idleAfter")
	}

	waitFor(t, waitTimeout, func() bool { return processGone(agent) }, "agent to exit")

	m := wk.Meta()
	if m.State != StateDone || !strings.Contains(m.Detail, "released after") {
		t.Errorf("after release: state %s detail %q", m.State, m.Detail)
	}

	if _, err := wk.Say("continue"); err == nil || !strings.Contains(err.Error(), "released") {
		t.Errorf("say after release: %v, want a released error", err)
	}

	if text := wk.Result(); !strings.Contains(text, "tag=one") {
		t.Errorf("reply lost after release:\n%s", text)
	}

	if ls := d.Ls(true, id); !strings.Contains(ls, "DONE") || !strings.Contains(ls, "released after") {
		t.Errorf("ls entry after release:\n%s", ls)
	}
}

// TestStopKillsDetachedChild proves ending a worker also ends a process the
// agent started outside its process group.
func TestStopKillsDetachedChild(t *testing.T) {
	d, _ := newTestDaemon(t)

	id := spawnFake(t, d, "duration=20s permdelay=50ms detach=1", true)

	wk, err := d.worker(id)
	if err != nil {
		t.Fatal(err)
	}

	childPattern := regexp.MustCompile(`child=(\d+)`)

	var child int

	waitFor(t, waitTimeout, func() bool {
		m := childPattern.FindStringSubmatch(wk.Result())
		if m == nil {
			return false
		}

		child, _ = strconv.Atoi(m[1])

		return child > 0
	}, "detached child pid")

	if processGone(child) {
		t.Fatalf("child %d is not running", child)
	}

	wk.Stop()

	waitFor(t, waitTimeout, func() bool { return processGone(child) }, "detached child to be killed")
}

// TestResumeFailureKillsHost proves a daemon that gives up on a worker at
// startup also ends the host it will never talk to.
func TestResumeFailureKillsHost(t *testing.T) {
	d1, home := newTestDaemon(t)
	stop1 := startServe(t, d1)

	id := spawnFake(t, d1, "duration=300ms permdelay=50ms", true)

	m := waitFinal(t, d1, id, 30*time.Second)
	if m.State != StateDone {
		t.Fatalf("state %s (%s), want DONE", m.State, m.Detail)
	}

	stop1()

	// As if the previous daemon had died during the launch handshake.
	m.State = StateStarting

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(Root(home), "workers", id, "meta.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	d2 := newDaemonOn(t, home)

	d2.mu.Lock()
	hist, ok := d2.history[id]
	d2.mu.Unlock()

	if !ok || hist.State != StateFailed {
		t.Fatalf("worker %s: in history %v, state %s; want FAILED", id, ok, hist.State)
	}

	waitFor(t, waitTimeout, func() bool { return processGone(m.PID) }, "agent of the failed worker to exit")
}
