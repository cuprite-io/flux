package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	"github.com/cuprite-io/flux/types"
)

// PaneFocus indicates which split pane currently has operator focus.
type PaneFocus int

const (
	PaneEditor PaneFocus = iota
	PaneTree
)

// Messages dispatched within the Eval view
type (
	// EvalCompletedMsg delivers the outcome of a dry-run evaluation.
	EvalCompletedMsg struct {
		Result *EvalResult
		Err    error
	}

	// ExemplarLoadedMsg delivers a pulled exemplar payload from Schema Tap.
	ExemplarLoadedMsg struct {
		Payload string
		Source  string
		Err     error
	}

	// CircuitsLoadedMsg delivers available circuits for switching.
	CircuitsLoadedMsg struct {
		Circuits []client.CircuitSummary
		Err      error
	}
)

// Model represents the interactive dry-run playground & branch evaluation REPL (:eval).
type Model struct {
	client client.Client
	width  int
	height int

	// Split panes
	editor   textarea.Model
	viewport viewport.Model
	focus    PaneFocus
	editMode bool

	// Circuit state
	circuits      []client.CircuitSummary
	activeIdx     int
	activeCircuit *types.Circuit

	// Results & telemetry
	lastResult   *EvalResult
	evaluating   bool
	jsonValid    bool
	jsonStatus   string
	statusMsg    string
	statusExpiry time.Time
}

// New creates and initializes an Eval playground model.
func New(cli client.Client) Model {
	ta := textarea.New()
	ta.Placeholder = "{\n  \"amount\": 12500,\n  \"currency\": \"USD\",\n  \"user_tier\": \"VIP\"\n}"
	ta.ShowLineNumbers = true
	ta.CharLimit = 65536
	ta.SetWidth(40)
	ta.SetHeight(15)

	initialPayload := "{\n  \"amount\": 12500.0,\n  \"currency\": \"USD\",\n  \"user_tier\": \"VIP\",\n  \"card_country\": \"US\",\n  \"ip_country\": \"UA\",\n  \"velocity_1h\": 4,\n  \"action\": \"checkout\",\n  \"risk_score\": 85\n}"
	ta.SetValue(initialPayload)

	vp := viewport.New(40, 15)
	vp.SetContent("  (press 'r' or 'Enter' to run dry-run evaluation)")

	m := Model{
		client:     cli,
		editor:     ta,
		viewport:   vp,
		focus:      PaneEditor,
		editMode:   false,
		circuits:   nil,
		activeIdx:  0,
		jsonValid:  true,
		jsonStatus: "✓ Valid JSON (8 keys)",
	}

	return m
}

// Init triggers initial data fetching: circuit inventory and initial exemplar.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink,
		m.loadCircuitsCmd(),
	)
}

// SetClient updates the active client bridge.
func (m *Model) SetClient(cli client.Client) tea.Cmd {
	m.client = cli
	return m.loadCircuitsCmd()
}

// SetDimensions handles terminal resize events, dividing screen space across split panes.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h

	paneWidth := (w - 3) / 2
	if paneWidth < 20 {
		paneWidth = 20
	}
	contentHeight := h - 4
	if contentHeight < 6 {
		contentHeight = 6
	}

	m.editor.SetWidth(paneWidth - 3)
	m.editor.SetHeight(contentHeight - 4)

	m.viewport.Width = w - paneWidth - 4
	m.viewport.Height = contentHeight - 2

	if m.lastResult != nil {
		m.viewport.SetContent(RenderEvaluationTree(m.lastResult, m.viewport.Width))
	}
}

// StatusMsg returns the latest transient status message if unexpired.
func (m Model) StatusMsg() string {
	if time.Now().Before(m.statusExpiry) {
		return m.statusMsg
	}
	return ""
}

// IsEditorFocused returns true if the operator is actively editing inside the JSON editor.
func (m Model) IsEditorFocused() bool {
	return m.focus == PaneEditor && m.editMode
}

// ActiveCircuitID returns the ID of the circuit currently loaded in the playground.
func (m Model) ActiveCircuitID() string {
	if m.activeCircuit != nil {
		return m.activeCircuit.ID
	}
	if len(m.circuits) > 0 && m.activeIdx < len(m.circuits) {
		return m.circuits[m.activeIdx].ID
	}
	return ""
}

// HandleHandoff loads a circuit handed off from the `:circuits` view and evaluates it immediately.
func (m *Model) HandleHandoff(msg circuits.DryRunHandoffMsg) tea.Cmd {
	if msg.Circuit != nil {
		m.activeCircuit = msg.Circuit
	} else if msg.CircuitID != "" {
		for i, c := range m.circuits {
			if c.ID == msg.CircuitID {
				m.activeIdx = i
				if c.Definition != nil {
					m.activeCircuit = c.Definition
				}
				break
			}
		}
	}

	// Pull exemplar for the handed-off circuit
	m.setStatus(fmt.Sprintf("Transferred circuit %q to Dry-Run Playground.", msg.CircuitID), false)
	return tea.Batch(
		m.loadExemplarCmd(),
		m.runEvalCmd(),
	)
}

func (m *Model) setStatus(msg string, isError bool) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(4 * time.Second)
}

func (m *Model) validateJSON() {
	raw := strings.TrimSpace(m.editor.Value())
	if raw == "" {
		m.jsonValid = true
		m.jsonStatus = "✓ Empty payload ({})"
		return
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		m.jsonValid = false
		m.jsonStatus = fmt.Sprintf("✗ Invalid JSON: %v", err)
		return
	}

	m.jsonValid = true
	m.jsonStatus = fmt.Sprintf("✓ Valid JSON (%d keys)", len(parsed))
}

func (m *Model) formatJSON() {
	raw := strings.TrimSpace(m.editor.Value())
	if raw == "" {
		return
	}

	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
		if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
			m.editor.SetValue(string(pretty))
			m.setStatus("Beautified JSON event payload.", false)
		}
	}
}

// Update handles interactive keyboard input and asynchronous evaluation messages.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case CircuitsLoadedMsg:
		if msg.Err == nil && len(msg.Circuits) > 0 {
			m.circuits = msg.Circuits
			if m.activeCircuit == nil {
				m.activeIdx = 0
				if msg.Circuits[0].Definition != nil {
					m.activeCircuit = msg.Circuits[0].Definition
				}
			}
			return m, m.runEvalCmd()
		}

	case ExemplarLoadedMsg:
		if msg.Err == nil && msg.Payload != "" {
			m.editor.SetValue(msg.Payload)
			m.validateJSON()
			m.setStatus(fmt.Sprintf("Loaded exemplar from %s.", msg.Source), false)
			return m, m.runEvalCmd()
		} else if msg.Err != nil {
			m.setStatus(fmt.Sprintf("Failed to load exemplar: %v", msg.Err), true)
		}

	case EvalCompletedMsg:
		m.evaluating = false
		if msg.Err != nil {
			m.setStatus(fmt.Sprintf("Eval error: %v", msg.Err), true)
		} else {
			m.lastResult = msg.Result
			m.viewport.SetContent(RenderEvaluationTree(msg.Result, m.viewport.Width))
			if msg.Result.Passed {
				m.setStatus(fmt.Sprintf("Dry-run evaluated in %dµs: PASSED", msg.Result.Duration.Microseconds()), false)
			} else {
				m.setStatus(fmt.Sprintf("Dry-run evaluated in %dµs: BRANCH PRUNED", msg.Result.Duration.Microseconds()), false)
			}
		}
		return m, nil

	case circuits.DryRunHandoffMsg:
		cmd := m.HandleHandoff(msg)
		return m, cmd

	case tea.KeyMsg:
		// Global Pane Switching via Tab
		if msg.String() == "tab" {
			if m.focus == PaneEditor {
				m.focus = PaneTree
				m.editMode = false
				m.editor.Blur()
			} else {
				m.focus = PaneEditor
				m.editMode = false
			}
			return m, nil
		}

		// Editor Pane Handling
		if m.focus == PaneEditor {
			if m.editMode {
				// Special hotkeys while actively typing in the editor
				switch msg.String() {
				case "esc":
					m.editMode = false
					m.editor.Blur()
					return m, nil

				case "ctrl+r", "ctrl+enter":
					m.validateJSON()
					return m, m.runEvalCmd()

				case "ctrl+e":
					return m, m.loadExemplarCmd()

				case "ctrl+f":
					m.formatJSON()
					m.validateJSON()
					return m, nil

				default:
					var cmd tea.Cmd
					m.editor, cmd = m.editor.Update(msg)
					m.validateJSON()
					return m, cmd
				}
			} else {
				// Navigation mode in Left Pane
				switch msg.String() {
				case "i", "enter":
					m.editMode = true
					m.editor.Focus()
					return m, textarea.Blink

				case "r":
					m.validateJSON()
					return m, m.runEvalCmd()

				case "e":
					return m, m.loadExemplarCmd()

				case "c":
					return m, m.cycleCircuitCmd()

				case "f":
					m.formatJSON()
					m.validateJSON()
					return m, nil
				}
			}
		}

		// Tree / Results Pane Handling
		if m.focus == PaneTree {
			switch msg.String() {
			case "r", "enter":
				m.validateJSON()
				return m, m.runEvalCmd()

			case "e":
				return m, m.loadExemplarCmd()

			case "c":
				return m, m.cycleCircuitCmd()

			case "i":
				m.focus = PaneEditor
				m.editMode = true
				m.editor.Focus()
				return m, textarea.Blink

			default:
				var cmd tea.Cmd
				m.viewport, cmd = m.viewport.Update(msg)
				return m, cmd
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) cycleCircuitCmd() tea.Cmd {
	if len(m.circuits) == 0 {
		m.setStatus("No deployed circuits available.", true)
		return nil
	}

	m.activeIdx = (m.activeIdx + 1) % len(m.circuits)
	sel := m.circuits[m.activeIdx]
	m.setStatus(fmt.Sprintf("Switched to circuit [%s] (%d/%d)", sel.ID, m.activeIdx+1, len(m.circuits)), false)

	if sel.Definition != nil {
		m.activeCircuit = sel.Definition
		return tea.Batch(
			m.loadExemplarCmd(),
			m.runEvalCmd(),
		)
	}

	// Fetch circuit definition if not populated in summary
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if m.client != nil {
			c, err := m.client.GetCircuit(ctx, sel.ID)
			if err == nil && c != nil && c.Definition != nil {
				return circuits.DryRunHandoffMsg{CircuitID: sel.ID, Circuit: c.Definition}
			}
		}
		return nil
	}
}

func (m Model) loadCircuitsCmd() tea.Cmd {
	return func() tea.Msg {
		if m.client == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		list, err := m.client.GetCircuits(ctx)
		return CircuitsLoadedMsg{Circuits: list, Err: err}
	}
}

func (m Model) loadExemplarCmd() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		payload, err := FetchOrGenerateExemplar(ctx, m.client, m.activeCircuit)
		return ExemplarLoadedMsg{Payload: payload, Source: "Schema Tap / Profiler", Err: err}
	}
}

func (m Model) runEvalCmd() tea.Cmd {
	circuit := m.activeCircuit
	payload := m.editor.Value()

	return func() tea.Msg {
		if circuit == nil {
			// If active circuit definition is not yet populated, synthesize demo circuit for sandbox
			circuit = synthesizeDemoCircuit()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		res, err := RunCircuitEval(ctx, circuit, payload)
		return EvalCompletedMsg{Result: res, Err: err}
	}
}

func synthesizeDemoCircuit() *types.Circuit {
	return &types.Circuit{
		ID:   "demo_playground_circuit",
		Tags: []string{"payments", "fraud"},
		Root: &types.Node{
			Name:      "root_guard",
			Condition: "payload.amount >= 1000.0",
			Steps: []*types.StepDefinition{
				{
					Type:   types.StepVolt,
					Script: "set('tier', payload.amount >= 10000.0 ? 'platinum' : 'gold')",
				},
				{
					Type:      types.StepSink,
					SinkName:  "slack_ops",
					Condition: "payload.amount >= 10000.0",
				},
			},
			Children: []*types.Node{
				{
					Name:      "cross_border_check",
					Condition: "payload.card_country != payload.ip_country",
					Steps: []*types.StepDefinition{
						{
							Type:   types.StepVolt,
							Script: "set('risk_score', 85)",
						},
						{
							Type:     types.StepSink,
							SinkName: "pagerduty_ops",
						},
					},
				},
				{
					Name:      "domestic_tier_check",
					Condition: "payload.card_country == payload.ip_country",
					Steps: []*types.StepDefinition{
						{
							Type:   types.StepVolt,
							Script: "set('risk_score', 15)",
						},
					},
				},
			},
		},
	}
}

// View renders the full split-screen playground.
func (m Model) View() string {
	if m.width == 0 {
		return "Initializing Dry-Run Playground..."
	}

	paneWidth := (m.width - 3) / 2
	if paneWidth < 20 {
		paneWidth = 20
	}
	rightWidth := m.width - paneWidth - 3

	// 1. Top Bar with Circuit indicator and keybinding hints
	circuitTitle := "demo_playground_circuit"
	circuitCount := 1
	circuitIdx := 1
	if len(m.circuits) > 0 {
		circuitCount = len(m.circuits)
		circuitIdx = m.activeIdx + 1
		circuitTitle = m.circuits[m.activeIdx].ID
	} else if m.activeCircuit != nil {
		circuitTitle = m.activeCircuit.ID
	}

	styleTopTitle := lipgloss.NewStyle().
		Foreground(colorCyan).
		Bold(true)

	styleCircuitBadge := lipgloss.NewStyle().
		Background(lipgloss.Color("#252538")).
		Foreground(colorWhite).
		Bold(true).
		Padding(0, 1)

	styleHelpText := lipgloss.NewStyle().
		Foreground(colorDim)

	topBar := fmt.Sprintf("%s  •  Circuit: %s %s\n%s",
		styleTopTitle.Render("🧪 Interactive Dry-Run Playground & REPL (:eval)"),
		styleCircuitBadge.Render(circuitTitle),
		styleHelpText.Render(fmt.Sprintf("[%d/%d - 'c' cycle]", circuitIdx, circuitCount)),
		styleHelpText.Render("[Tab] Switch Pane  [i] Edit  [e] Exemplar  [r/Enter] Run  [c] Circuit  [f] Format  [Esc] Normal Mode"),
	)

	// 2. Left Pane (JSON Event Payload Editor)
	editorBorderColor := colorGray
	editorTitleStyle := lipgloss.NewStyle().Foreground(colorDim).Bold(true)
	if m.focus == PaneEditor {
		editorBorderColor = colorCyan
		editorTitleStyle = lipgloss.NewStyle().Foreground(colorCyan).Bold(true)
	}

	modeIndicator := "[Normal Mode — press 'i' to edit]"
	if m.editMode {
		modeIndicator = lipgloss.NewStyle().Foreground(colorGreen).Bold(true).Render("[Insert Mode — 'Esc' exit, 'Ctrl+R' run]")
	}

	validationStyle := styleStatusPassed
	if !m.jsonValid {
		validationStyle = styleStatusFailed
	}

	leftHeader := fmt.Sprintf("%s  %s", editorTitleStyle.Render("📝 Event Payload (JSON)"), modeIndicator)
	leftFooter := validationStyle.Render(m.jsonStatus)

	leftContent := fmt.Sprintf("%s\n%s\n%s", leftHeader, m.editor.View(), leftFooter)

	leftPanel := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(editorBorderColor).
		Width(paneWidth).
		Height(m.height - 4).
		Padding(0, 1).
		Render(leftContent)

	// 3. Right Pane (Live Visual Evaluation Results)
	treeBorderColor := colorGray
	treeTitleStyle := lipgloss.NewStyle().Foreground(colorDim).Bold(true)
	if m.focus == PaneTree {
		treeBorderColor = colorCyan
		treeTitleStyle = lipgloss.NewStyle().Foreground(colorCyan).Bold(true)
	}

	rightHeader := treeTitleStyle.Render("🌲 Live Branch Evaluation & Simulated Sinks")
	rightContent := fmt.Sprintf("%s\n\n%s", rightHeader, m.viewport.View())

	rightPanel := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(treeBorderColor).
		Width(rightWidth).
		Height(m.height - 4).
		Padding(0, 1).
		Render(rightContent)

	// 4. Join split panes horizontally
	splitView := lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, " ", rightPanel)

	return fmt.Sprintf("%s\n%s", topBar, splitView)
}
