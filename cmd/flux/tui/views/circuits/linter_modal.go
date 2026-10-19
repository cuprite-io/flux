package circuits

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/internal/linter"
	"github.com/cuprite-io/flux/types"
)

var (
	styleModalBorder = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#00E5FF")).
				Background(lipgloss.Color("#161622")).
				Padding(1, 2)

	styleModalTitle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleLintPass = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676")).
			Bold(true)

	styleLintWarn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD600")).
			Bold(true)

	styleLintFail = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF1744")).
			Bold(true)

	styleSevError = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#D32F2F")).
			Bold(true).
			Padding(0, 1)

	styleSevWarn = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#000000")).
			Background(lipgloss.Color("#FFD600")).
			Bold(true).
			Padding(0, 1)

	styleSevInfo = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#000000")).
			Background(lipgloss.Color("#00E5FF")).
			Bold(true).
			Padding(0, 1)

	styleIssueRule = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleIssueNode = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#D183FF")).
			Bold(true)

	styleIssueMsg = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF"))

	styleIssueDetail = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#9E9EB8"))

	styleStatsBar = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#B0BEC5"))
)

// LinterModal manages the in-shell circuit linting diagnostics overlay.
type LinterModal struct {
	active    bool
	circuitID string
	result    *linter.Result
	viewport  viewport.Model
	width     int
	height    int
}

// NewLinterModal creates an initialized linter modal.
func NewLinterModal() LinterModal {
	vp := viewport.New(60, 15)
	return LinterModal{
		viewport: vp,
	}
}

// IsActive returns whether the linter modal is currently visible.
func (m LinterModal) IsActive() bool {
	return m.active
}

// Result returns the last diagnostic result.
func (m LinterModal) Result() *linter.Result {
	return m.result
}

// Open analyzes the provided circuit definition and opens the diagnostics overlay.
func (m *LinterModal) Open(circuit *types.Circuit, width, height int) {
	m.active = true
	m.width = width
	m.height = height

	if circuit == nil {
		m.circuitID = "unknown"
		m.result = &linter.Result{
			CircuitID: "unknown",
			Issues: []linter.Issue{
				{
					RuleID:   "LINT-000",
					Severity: linter.SeverityError,
					Message:  "circuit definition is nil or not loaded",
				},
			},
		}
	} else {
		m.circuitID = circuit.ID
		lnt, err := linter.New(linter.DefaultOptions())
		if err != nil {
			m.result = &linter.Result{
				CircuitID: circuit.ID,
				Issues: []linter.Issue{
					{
						RuleID:   "LINT-ERR",
						Severity: linter.SeverityError,
						Message:  fmt.Sprintf("failed to initialize linter compiler: %v", err),
					},
				},
			}
		} else {
			m.result = lnt.Lint(circuit)
		}
	}

	m.updateViewportContent()
}

// Close dismisses the linter modal.
func (m *LinterModal) Close() {
	m.active = false
}

// SetDimensions updates the modal layout dimensions.
func (m *LinterModal) SetDimensions(w, h int) {
	m.width = w
	m.height = h
	if m.active {
		m.updateViewportContent()
	}
}

func (m *LinterModal) updateViewportContent() {
	modalW := m.width - 8
	if modalW < 40 {
		modalW = 40
	}
	modalH := m.height - 10
	if modalH < 6 {
		modalH = 6
	}

	m.viewport.Width = modalW
	m.viewport.Height = modalH

	var sb strings.Builder

	if m.result == nil {
		sb.WriteString("No diagnostic results available.")
		m.viewport.SetContent(sb.String())
		return
	}

	// 1. Diagnostics Summary Header
	if !m.result.HasErrors() && !m.result.HasWarnings() {
		sb.WriteString(styleLintPass.Render("✓ PASS — Clean Circuit DAG"))
		sb.WriteString("\n")
		sb.WriteString(styleStatsBar.Render("All static analysis rules passed. No syntax errors, dead branches, or contradictory conditions detected."))
	} else if m.result.HasErrors() {
		sb.WriteString(styleLintFail.Render(fmt.Sprintf("✗ FAILED — %d Issue(s) Detected", len(m.result.Issues))))
		sb.WriteString("\n")
		sb.WriteString(styleStatsBar.Render("Critical errors must be resolved before deploying this circuit to production streaming pipelines."))
	} else {
		sb.WriteString(styleLintWarn.Render(fmt.Sprintf("⚠ WARNING — %d Potential Issue(s) Detected", len(m.result.Issues))))
		sb.WriteString("\n")
		sb.WriteString(styleStatsBar.Render("Circuit is executable, but static analysis detected non-fatal warnings or unused projections."))
	}
	sb.WriteString("\n\n")

	// 2. Metrics Bar
	stats := fmt.Sprintf("Circuit ID: %s  │  Nodes: %d  │  Steps: %d  │  Issues: %d",
		m.result.CircuitID, m.result.NodeCount, m.result.StepCount, len(m.result.Issues))
	sb.WriteString(styleStatsBar.Render(stats))
	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#3A3A55")).Render(strings.Repeat("─", modalW)))
	sb.WriteString("\n\n")

	// 3. Issue List
	if len(m.result.Issues) == 0 {
		sb.WriteString(styleLintPass.Render("• 0 diagnostics reported. Pipeline AST is sound."))
	} else {
		for i, iss := range m.result.Issues {
			var badge string
			switch iss.Severity {
			case linter.SeverityError:
				badge = styleSevError.Render("ERROR")
			case linter.SeverityWarning:
				badge = styleSevWarn.Render("WARN")
			default:
				badge = styleSevInfo.Render("INFO")
			}

			rule := styleIssueRule.Render(iss.RuleID)
			nodeCtx := ""
			if iss.Node != "" {
				nodeCtx = " " + styleIssueNode.Render(fmt.Sprintf("[node: %s]", iss.Node))
			}

			sb.WriteString(fmt.Sprintf("%s %s%s\n", badge, rule, nodeCtx))
			sb.WriteString("  ")
			sb.WriteString(styleIssueMsg.Render(iss.Message))
			sb.WriteString("\n")
			if iss.Detail != "" {
				sb.WriteString("  ")
				sb.WriteString(styleIssueDetail.Render("↳ " + iss.Detail))
				sb.WriteString("\n")
			}
			if i < len(m.result.Issues)-1 {
				sb.WriteString("\n")
			}
		}
	}

	m.viewport.SetContent(sb.String())
}

// Update handles keyboard navigation within the linter modal.
func (m *LinterModal) Update(msg tea.Msg) (bool, tea.Cmd) {
	if !m.active {
		return false, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q", "l":
			m.active = false
			return true, nil
		case "up", "k":
			m.viewport.LineUp(1)
			return true, nil
		case "down", "j":
			m.viewport.LineDown(1)
			return true, nil
		case "pgup", "u":
			m.viewport.HalfViewUp()
			return true, nil
		case "pgdown", "d", "ctrl+d":
			m.viewport.HalfViewDown()
			return true, nil
		}
		return true, nil
	}

	return false, nil
}

// View renders the modal overlay dialog.
func (m LinterModal) View() string {
	if !m.active {
		return ""
	}

	modalW := m.width - 8
	if modalW < 40 {
		modalW = 40
	}
	modalH := m.height - 6
	if modalH < 8 {
		modalH = 8
	}

	title := styleModalTitle.Render(fmt.Sprintf("🔍 Circuit Linter Static Analysis — %s", m.circuitID))
	footer := styleHintText.Render("Esc/q/l: close diagnostics modal  │  ↑/↓/j/k: scroll issues")

	content := title + "\n\n" + m.viewport.View() + "\n\n" + footer
	return styleModalBorder.Width(modalW).Height(modalH).Render(content)
}
