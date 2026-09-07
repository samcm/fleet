// Command fakeagent speaks the slice of the Agent Client Protocol that the
// fleet worker uses, so integration tests can run a real agent process
// without omp or a model provider. It understands initialize, session/new,
// session/set_config_option, session/cancel and session/prompt; a prompt
// emits message chunks and one usage_update, asks one edit permission
// mid-turn, sleeps for a duration named in the prompt text, and answers with
// end_turn and a usage object. detach=1 in the prompt starts a child in its
// own session and names its pid, standing in for the tool processes omp
// starts detached.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type rpcMsg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

var (
	outMu    sync.Mutex
	permCh   = make(chan rpcMsg, 1)
	cancelCh = make(chan struct{}, 1)
)

func send(v any) {
	b, _ := json.Marshal(v)

	outMu.Lock()
	_, _ = os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func result(id json.RawMessage, v any) {
	raw, _ := json.Marshal(v)
	send(rpcMsg{JSONRPC: "2.0", ID: id, Result: raw})
}

func notify(method string, params any) {
	raw, _ := json.Marshal(params)
	send(rpcMsg{JSONRPC: "2.0", Method: method, Params: raw})
}

func chunk(text string) {
	notify("session/update", map[string]any{"sessionId": "s1", "update": map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]string{"type": "text", "text": text},
	}})
}

// field extracts the value of the first space-separated key=value token.
func field(text, key string) string {
	for _, tok := range strings.Fields(text) {
		if s, ok := strings.CutPrefix(tok, key); ok {
			return s
		}
	}

	return ""
}

// durationField parses a key=value token as a Go duration, or as seconds when
// the value is a bare number.
func durationField(text, key string, fallback time.Duration) time.Duration {
	v := field(text, key)
	if v == "" {
		return fallback
	}

	if d, err := time.ParseDuration(v); err == nil {
		return d
	}

	if n, err := time.ParseDuration(v + "s"); err == nil {
		return n
	}

	return fallback
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)

	for scanner.Scan() {
		var msg rpcMsg
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		switch msg.Method {
		case "initialize":
			result(msg.ID, map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{}})
		case "session/new":
			result(msg.ID, map[string]any{"sessionId": "s1"})
		case "session/set_config_option":
			result(msg.ID, map[string]any{})
		case "session/prompt":
			go runPrompt(msg.ID, msg.Params)
		case "session/cancel":
			select {
			case cancelCh <- struct{}{}:
			default:
			}
		case "":
			// A response, which can only be to our permission request.
			if len(msg.ID) > 0 {
				select {
				case permCh <- msg:
				default:
				}
			}
		}
	}
}

func runPrompt(id json.RawMessage, params json.RawMessage) {
	var p struct {
		Prompt []struct {
			Text string `json:"text"`
		} `json:"prompt"`
	}

	_ = json.Unmarshal(params, &p)

	text := ""
	if len(p.Prompt) > 0 {
		text = p.Prompt[0].Text
	}

	dur := durationField(text, "duration=", time.Second)
	permDelay := durationField(text, "permdelay=", 200*time.Millisecond)

	chunk("alpha ")
	chunk("beta ")

	if tag := field(text, "tag="); tag != "" {
		chunk("tag=" + tag + " ")
	}

	if field(text, "detach=") != "" {
		child := exec.Command("sleep", "300")
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

		if err := child.Start(); err == nil {
			chunk(fmt.Sprintf("child=%d ", child.Process.Pid))
		}
	}

	notify("session/update", map[string]any{"sessionId": "s1", "update": map[string]any{
		"sessionUpdate": "usage_update",
		"used":          176878,
		"size":          1048576,
		"cost":          map[string]any{"amount": 7.0859, "currency": "USD"},
	}})

	time.Sleep(permDelay)

	params2, _ := json.Marshal(map[string]any{
		"sessionId": "s1",
		"toolCall":  map[string]string{"toolCallId": "tc1", "title": "edit main.go", "kind": "edit"},
		"options": []map[string]string{
			{"optionId": "yes", "kind": "allow_once"},
			{"optionId": "no", "kind": "reject_once"},
		},
	})
	send(rpcMsg{JSONRPC: "2.0", ID: json.RawMessage("1000"), Method: "session/request_permission", Params: params2})

	decision := "cancelled"

	select {
	case res := <-permCh:
		var r struct {
			Outcome struct {
				OptionID string `json:"optionId"`
			} `json:"outcome"`
		}

		if json.Unmarshal(res.Result, &r) == nil {
			switch r.Outcome.OptionID {
			case "yes":
				decision = "allowed"
			case "no":
				decision = "rejected"
			}
		}
	case <-cancelCh:
		result(id, map[string]any{"stopReason": "cancelled"})

		return
	case <-time.After(dur + 30*time.Second):
	}

	chunk("gamma perm=" + decision + " ")

	timer := time.NewTimer(dur)

	select {
	case <-cancelCh:
		timer.Stop()
		result(id, map[string]any{"stopReason": "cancelled"})

		return
	case <-timer.C:
	}

	result(id, map[string]any{
		"stopReason": "end_turn",
		"usage":      map[string]int64{"inputTokens": 100, "outputTokens": 50, "cachedReadTokens": 10},
	})
}
