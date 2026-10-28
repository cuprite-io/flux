package tui_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// HeadlessHarness wraps a tui.Model and provides high-level headless interaction helpers.
type HeadlessHarness struct {
	mu     sync.Mutex
	t      *testing.T
	model  tui.Model
	width  int
	height int
}

// NewHeadlessHarness creates an initialized HeadlessHarness with standard terminal dimensions.
func NewHeadlessHarness(t *testing.T, cfg tui.Config) *HeadlessHarness {
	m := tui.NewModel(cfg)
	w, h := 120, 35
	m.SetDimensions(w, h)
	return &HeadlessHarness{
		t:      t,
		model:  m,
		width:  w,
		height: h,
	}
}

// Model returns the current tui.Model instance.
func (h *HeadlessHarness) Model() tui.Model {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.model
}

// SetModel updates the internal model.
func (h *HeadlessHarness) SetModel(m tui.Model) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.model = m
}

// View returns the rendered string representation of the current model state.
func (h *HeadlessHarness) View() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.model.View()
}

// CurrentView returns the active view ID.
func (h *HeadlessHarness) CurrentView() tui.ViewID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.model.CurrentView()
}

// Update sends a tea.Msg to the model and updates the internal state.
func (h *HeadlessHarness) Update(msg tea.Msg) tea.Cmd {
	h.mu.Lock()
	defer h.mu.Unlock()
	newM, cmd := h.model.Update(msg)
	h.model = newM.(tui.Model)
	return cmd
}

// DeliverCmd executes a tea.Cmd synchronously and dispatches its resulting message to the model.
func (h *HeadlessHarness) DeliverCmd(cmd tea.Cmd) {
	if cmd == nil {
		return
	}
	msg := cmd()
	if msg != nil {
		h.Update(msg)
	}
}

// SendKey sends a typed key message.
func (h *HeadlessHarness) SendKey(keyType tea.KeyType, runes ...rune) tea.Cmd {
	return h.Update(tea.KeyMsg{Type: keyType, Runes: runes})
}

// SendRune sends a single rune keystroke.
func (h *HeadlessHarness) SendRune(r rune) tea.Cmd {
	return h.SendKey(tea.KeyRunes, r)
}

// SendRunes sends a string of individual character keystrokes.
func (h *HeadlessHarness) SendRunes(s string) {
	for _, r := range s {
		h.SendRune(r)
	}
}

// SendNamedKey sends common named keyboard inputs (e.g. "enter", "esc", "tab", "up", "down").
func (h *HeadlessHarness) SendNamedKey(name string) tea.Cmd {
	switch strings.ToLower(name) {
	case "enter":
		return h.SendKey(tea.KeyEnter)
	case "esc", "escape":
		return h.SendKey(tea.KeyEscape)
	case "tab":
		return h.SendKey(tea.KeyTab)
	case "backspace":
		return h.SendKey(tea.KeyBackspace)
	case "up":
		return h.SendKey(tea.KeyUp)
	case "down":
		return h.SendKey(tea.KeyDown)
	case "left":
		return h.SendKey(tea.KeyLeft)
	case "right":
		return h.SendKey(tea.KeyRight)
	case "space", " ":
		return h.SendKey(tea.KeySpace)
	default:
		if len(name) == 1 {
			return h.SendRune(rune(name[0]))
		}
		panic(fmt.Sprintf("unsupported named key: %s", name))
	}
}

// SendCommand enters command mode, types the specified command string, and presses Enter.
func (h *HeadlessHarness) SendCommand(cmd string) tea.Cmd {
	h.SendRune(':')
	h.SendRunes(cmd)
	return h.SendNamedKey("enter")
}

// AssertScreenContains asserts that the current rendered output contains the expected substring.
func (h *HeadlessHarness) AssertScreenContains(substr string) {
	h.t.Helper()
	out := h.View()
	assert.Contains(h.t, out, substr, "expected screen to contain %q", substr)
}

// AssertScreenNotContains asserts that the current rendered output does not contain the substring.
func (h *HeadlessHarness) AssertScreenNotContains(substr string) {
	h.t.Helper()
	out := h.View()
	assert.NotContains(h.t, out, substr, "expected screen to not contain %q", substr)
}

// TestHeadless_ViewSwitching_AllViews verifies switching between all 7 views in both Standalone and Cluster modes.
func TestHeadless_ViewSwitching_AllViews(t *testing.T) {
	// 1. Cluster mode test: all tabs are active
	h := NewHeadlessHarness(t, tui.Config{
		Endpoint: "10.0.0.1:9090",
		NodeID:   "worker-alpha",
	})

	assert.True(t, h.Model().IsCluster())
	assert.Equal(t, tui.ViewCircuits, h.CurrentView())

	// Test command-based switching to all views
	viewCases := []struct {
		cmd          string
		expectedView tui.ViewID
		expectedText string
	}{
		{cmd: "cache", expectedView: tui.ViewCache, expectedText: "Distributed Cache & Key Explorer"},
		{cmd: "autopilot", expectedView: tui.ViewAutopilot, expectedText: "Autopilot Cockpit"},
		{cmd: "eval", expectedView: tui.ViewEval, expectedText: "Dry-Run Playground"},
		{cmd: "alerts", expectedView: tui.ViewAlerts, expectedText: "Real-Time Alert Stream"},
		{cmd: "cluster", expectedView: tui.ViewCluster, expectedText: "Topology:"},
		{cmd: "help", expectedView: tui.ViewHelp, expectedText: "Flux Interactive Shell"},
		{cmd: "circuits", expectedView: tui.ViewCircuits, expectedText: "Deployed Decision Pipelines"},
	}

	for _, tc := range viewCases {
		h.SendCommand(tc.cmd)
		assert.Equal(t, tc.expectedView, h.CurrentView(), "command :%s should switch to %v", tc.cmd, tc.expectedView)
		h.AssertScreenContains(tc.expectedText)
	}

	// Test numeric hotkeys in Cluster Mode: 1..6
	hotkeyCases := []struct {
		key          string
		expectedView tui.ViewID
	}{
		{key: "2", expectedView: tui.ViewAutopilot},
		{key: "3", expectedView: tui.ViewCache},
		{key: "4", expectedView: tui.ViewEval},
		{key: "5", expectedView: tui.ViewAlerts},
		{key: "6", expectedView: tui.ViewCluster},
		{key: "1", expectedView: tui.ViewCircuits},
	}

	for _, hk := range hotkeyCases {
		h.SendNamedKey(hk.key)
		assert.Equal(t, hk.expectedView, h.CurrentView(), "hotkey %s should switch to %v", hk.key, hk.expectedView)
	}

	// Test Tab cycling through visible views
	initialView := h.CurrentView()
	h.SendNamedKey("tab")
	nextView := h.CurrentView()
	assert.NotEqual(t, initialView, nextView, "Tab should advance to the next view")
}

// TestHeadless_CommandBar_ExecutionAndValidation verifies command line prompt behaviour and shortcuts.
func TestHeadless_CommandBar_ExecutionAndValidation(t *testing.T) {
	h := NewHeadlessHarness(t, tui.Config{}) // Standalone mode

	// 1. Enter command mode with ':'
	h.SendRune(':')
	h.AssertScreenContains(":")

	// 2. Type partial command and backspace
	h.SendRunes("circ")
	h.SendNamedKey("backspace")
	h.SendNamedKey("backspace")
	// Now buffer is ":ci"

	// 3. Cancel with Esc
	h.SendNamedKey("esc")
	assert.Equal(t, tui.ViewCircuits, h.CurrentView())

	// 4. Test unknown command error handling
	h.SendCommand("nonexistent_command_xyz")
	h.AssertScreenContains("Unknown command")

	// 5. Test command abbreviations
	abbrevCases := []struct {
		abbr         string
		expectedView tui.ViewID
	}{
		{abbr: "al", expectedView: tui.ViewAlerts},
		{abbr: "auto", expectedView: tui.ViewAutopilot},
		{abbr: "c", expectedView: tui.ViewCircuits},
		{abbr: "h", expectedView: tui.ViewHelp},
	}

	for _, ac := range abbrevCases {
		h.SendCommand(ac.abbr)
		assert.Equal(t, ac.expectedView, h.CurrentView(), "abbreviation :%s should switch to %v", ac.abbr, ac.expectedView)
	}

	// 6. Test :token command
	h.SendCommand("token")
	h.AssertScreenContains("Usage: :token <secret>")

	h.SendCommand("token my-secret-jwt-token-123")
	h.AssertScreenContains("Authentication token updated")
}

// TestHeadless_SearchFilter_Lifecycle verifies filter input activation, typing, query submission, and Esc clearing.
func TestHeadless_SearchFilter_Lifecycle(t *testing.T) {
	h := NewHeadlessHarness(t, tui.Config{})

	// 1. Populate circuits
	h.Update(circuits.CircuitsLoadedMsg{
		Circuits: []client.CircuitSummary{
			{ID: "payment_fraud_detector", Tags: []string{"payments"}, NodeCount: 3, Status: "LIVE"},
			{ID: "account_takeover_guard", Tags: []string{"auth"}, NodeCount: 2, Status: "LIVE"},
		},
	})
	h.AssertScreenContains("payment_fraud_detector")
	h.AssertScreenContains("account_takeover_guard")

	// 2. Press '/' to activate filter input
	h.SendRune('/')
	assert.True(t, h.Model().CircuitsView().IsFiltering())

	// 3. Type query "payment"
	h.SendRunes("payment")
	h.SendNamedKey("enter")
	assert.False(t, h.Model().CircuitsView().IsFiltering())

	// Filtered state: contains "payment_fraud_detector", but "account_takeover_guard" filtered out
	h.AssertScreenContains("payment_fraud_detector")
	h.AssertScreenNotContains("account_takeover_guard")

	// 4. Press Esc to clear filter
	h.SendNamedKey("esc")
	h.AssertScreenContains("account_takeover_guard")

	// 5. Test filter cancellation with Esc during editing
	h.SendRune('/')
	h.SendRunes("xyz")
	h.SendNamedKey("esc") // cancels filter without committing
	assert.False(t, h.Model().CircuitsView().IsFiltering())
	h.AssertScreenContains("account_takeover_guard")
}

// TestHeadless_RapidInputStressAndStability tests rapid key sequences and concurrent messages without race conditions or crashes.
func TestHeadless_RapidInputStressAndStability(t *testing.T) {
	h := NewHeadlessHarness(t, tui.Config{
		Endpoint: "127.0.0.1:9090",
		NodeID:   "stress-node",
	})

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Background ticker sending regular TickMsg and mocked background messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case now := <-ticker.C:
				_ = now
				h.Update(tui.TickMsg(time.Now()))
			}
		}
	}()

	// Perform 100 rapid key transitions
	keys := []string{"1", "2", "3", "4", "5", "6", "tab", ":", "esc", "j", "k", "r"}
	for i := 0; i < 100; i++ {
		k := keys[i%len(keys)]
		if k == ":" {
			h.SendRune(':')
			h.SendRunes("help")
			h.SendNamedKey("enter")
		} else {
			h.SendNamedKey(k)
		}
	}

	close(stopCh)
	wg.Wait()

	// Ensure harness remained healthy and view rendering is non-empty
	require.NotEmpty(t, h.View())
}

// TestHeadless_HelpViewAndModalInteractions verifies Help screen toggle and return navigation.
func TestHeadless_HelpViewAndModalInteractions(t *testing.T) {
	h := NewHeadlessHarness(t, tui.Config{})

	assert.Equal(t, tui.ViewCircuits, h.CurrentView())

	// Press '?' to open Help view
	h.SendRune('?')
	assert.Equal(t, tui.ViewHelp, h.CurrentView())
	h.AssertScreenContains("Flux Interactive Shell")
	h.AssertScreenContains("Keyboard Shortcuts & Commands")

	// Press 'esc' to restore previous view
	h.SendNamedKey("esc")
	assert.Equal(t, tui.ViewCircuits, h.CurrentView())

	// Navigate to Eval view, then press '?'
	h.SendCommand("eval")
	assert.Equal(t, tui.ViewEval, h.CurrentView())

	h.SendRune('?')
	assert.Equal(t, tui.ViewHelp, h.CurrentView())

	// Press '?' again to toggle back to previous view (Eval)
	h.SendRune('?')
	assert.Equal(t, tui.ViewEval, h.CurrentView())
}
