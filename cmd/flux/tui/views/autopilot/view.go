package autopilot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

var (
	styleTitle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleBadge = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7C4DFF")).
			Bold(true)

	stylePhaseActive = lipgloss.NewStyle().
				Background(lipgloss.Color("#00E5FF")).
				Foreground(lipgloss.Color("#000000")).
				Bold(true).
				Padding(0, 1)

	stylePhaseCompleted = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E676")).
				Bold(true)

	stylePhasePending = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#757575"))

	styleFilterPrompt = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleBorderFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#00E5FF")).
				Padding(0, 1)

	styleBorderNormal = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#3A3A55")).
				Padding(0, 1)

	styleSplitTitleActive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleSplitTitleInactive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#A0A0C0")).
				Bold(true)

	styleTabButtonActive = lipgloss.NewStyle().
				Background(lipgloss.Color("#7C4DFF")).
				Foreground(lipgloss.Color("#FFFFFF")).
				Bold(true).
				Padding(0, 1)

	styleTabButtonInactive = lipgloss.NewStyle().
				Background(lipgloss.Color("#252538")).
				Foreground(lipgloss.Color("#B0BEC5")).
				Padding(0, 1)

	styleKeyHelp = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#757575"))

	styleKey = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)
)

// AutopilotLoadedMsg delivers autopilot sessions fetched from the engine.
type AutopilotLoadedMsg struct {
	Sessions []client.AutopilotSession
	Err      error
}

// FetchAutopilotCmd queries the client for active and historical Autopilot sessions.
func FetchAutopilotCmd(c client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return AutopilotLoadedMsg{Err: errors.New("no client available")}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sessions, err := c.GetAutopilot(ctx)
		return AutopilotLoadedMsg{Sessions: sessions, Err: err}
	}
}

// Model represents the Live Autopilot Cockpit & AI Audit Log view (:autopilot).
type Model struct {
	client       client.Client
	sessions     []client.AutopilotSession
	filtered     []client.AutopilotSession
	selectedSess *client.AutopilotSession
	table        table.Model
	detailVP     viewport.Model

	// Layout & Focus
	focusedPanel int // 0: Left (Sessions Table & Gauge), 1: Right (Audit Log / Diff)
	panelTab     int // 0: AI Audit Log, 1: Circuit Diff
	diffMode     int // 0: Unified Diff, 1: Side-by-Side Diff

	// Filter
	filterInput textinput.Model
	isFiltering bool
	filterQuery string

	// Dimensions & Lifecycle
	width     int
	height    int
	loading   bool
	err       error
	statusMsg string
}

// New initializes the Autopilot Cockpit view component.
func New(c client.Client) Model {
	ti := textinput.New()
	ti.Prompt = "Filter Autopilot Sessions: "
	ti.PromptStyle = styleFilterPrompt
	ti.CharLimit = 32

	columns := []table.Column{
		{Title: "SESSION ID", Width: 16},
		{Title: "STATUS", Width: 14},
		{Title: "TAGS", Width: 16},
		{Title: "ACTIVE CIRCUIT", Width: 20},
		{Title: "USER GOAL / PROMPT", Width: 36},
	}

	tbl := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(10),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("#3A3A55")).
		BorderBottom(true).
		Bold(true).
		Foreground(lipgloss.Color("#00E5FF"))
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("#00E5FF")).
		Background(lipgloss.Color("#2A2A48")).
		Bold(true)
	tbl.SetStyles(s)

	detailVP := viewport.New(40, 15)

	return Model{
		client:       c,
		table:        tbl,
		detailVP:     detailVP,
		filterInput:  ti,
		focusedPanel: 0,
		panelTab:     0,
		diffMode:     0,
		loading:      true,
	}
}

// Init starts initial Autopilot sessions fetching.
func (m Model) Init() tea.Cmd {
	return FetchAutopilotCmd(m.client)
}

// SetClient updates the communication client bridge.
func (m *Model) SetClient(c client.Client) tea.Cmd {
	m.client = c
	m.loading = true
	return FetchAutopilotCmd(m.client)
}

// SetDimensions updates component dimensions.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h

	splitWidth := (w - 6) / 2
	if splitWidth < 30 {
		splitWidth = 30
	}
	contentHeight := h - 6
	if contentHeight < 6 {
		contentHeight = 6
	}

	// In left panel: Gauge takes top ~7 rows, table takes remainder
	tableHeight := contentHeight - 9
	if tableHeight < 5 {
		tableHeight = 5
	}

	m.table.SetWidth(splitWidth)
	m.table.SetHeight(tableHeight)
	m.detailVP.Width = splitWidth
	m.detailVP.Height = contentHeight - 4
	m.updateDetailView()
}

// Update handles Bubble Tea messages and user input.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case AutopilotLoadedMsg:
		m.loading = false
		m.err = msg.Err
		if msg.Err == nil {
			m.sessions = msg.Sessions
			m.applyFilter()
			m.statusMsg = fmt.Sprintf("Autopilot sessions updated: %d active.", len(m.sessions))
		} else {
			m.statusMsg = fmt.Sprintf("Autopilot fetch failed: %v", msg.Err)
		}
		return m, nil

	case tea.KeyMsg:
		if m.isFiltering {
			switch msg.Type {
			case tea.KeyEnter:
				m.isFiltering = false
				m.filterQuery = strings.TrimSpace(m.filterInput.Value())
				m.applyFilter()
				return m, nil
			case tea.KeyEsc:
				m.isFiltering = false
				m.filterInput.Reset()
				m.filterQuery = ""
				m.applyFilter()
				return m, nil
			default:
				var cmd tea.Cmd
				m.filterInput, cmd = m.filterInput.Update(msg)
				m.filterQuery = strings.TrimSpace(m.filterInput.Value())
				m.applyFilter()
				return m, cmd
			}
		}

		switch msg.String() {
		case "/":
			m.isFiltering = true
			m.filterInput.Focus()
			return m, textinput.Blink

		case "tab":
			m.focusedPanel = (m.focusedPanel + 1) % 2
			m.table.Focus()
			return m, nil

		case "v":
			// Toggle right panel tab (0: Audit Log <-> 1: Circuit Diff)
			if m.panelTab == 1 {
				m.panelTab = 0
			} else {
				m.panelTab = 1
			}
			m.updateDetailView()
			return m, nil

		case "a":
			// Jump directly to AI Audit Log
			m.panelTab = 0
			m.updateDetailView()
			return m, nil

		case "d":
			// Toggle diff format (Unified <-> Side-by-side)
			if m.panelTab == 1 {
				m.diffMode = (m.diffMode + 1) % 2
				m.updateDetailView()
			}
			return m, nil

		case "r":
			m.loading = true
			m.statusMsg = "Refreshing autopilot sessions..."
			return m, FetchAutopilotCmd(m.client)

		case "esc":
			if m.filterQuery != "" {
				m.filterQuery = ""
				m.filterInput.Reset()
				m.applyFilter()
				return m, nil
			}
			m.focusedPanel = 0
			return m, nil

		case "up", "k", "down", "j", "enter":
			if m.focusedPanel == 0 {
				var cmd tea.Cmd
				m.table, cmd = m.table.Update(msg)
				m.updateSelectedFromTable()
				return m, cmd
			}
			var cmd tea.Cmd
			m.detailVP, cmd = m.detailVP.Update(msg)
			return m, cmd

		case "pgup", "pgdown", "u", "ctrl+d":
			if m.focusedPanel == 1 {
				var cmd tea.Cmd
				m.detailVP, cmd = m.detailVP.Update(msg)
				return m, cmd
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) updateSelectedFromTable() {
	idx := m.table.Cursor()
	if idx >= 0 && idx < len(m.filtered) {
		sess := m.filtered[idx]
		m.selectedSess = &sess
	} else {
		m.selectedSess = nil
	}
	m.updateDetailView()
}

func (m *Model) applyFilter() {
	if m.filterQuery == "" {
		m.filtered = make([]client.AutopilotSession, len(m.sessions))
		copy(m.filtered, m.sessions)
	} else {
		q := strings.ToLower(m.filterQuery)
		m.filtered = nil
		for _, s := range m.sessions {
			if strings.Contains(strings.ToLower(s.SessionID), q) ||
				strings.Contains(strings.ToLower(s.Status), q) ||
				strings.Contains(strings.ToLower(s.Prompt), q) ||
				strings.Contains(strings.ToLower(strings.Join(s.Tags, ",")), q) {
				m.filtered = append(m.filtered, s)
			}
		}
	}

	rows := make([]table.Row, len(m.filtered))
	for i, s := range m.filtered {
		activeCirc := "(none)"
		if len(s.ActiveCircuits) > 0 {
			activeCirc = strings.Join(s.ActiveCircuits, ", ")
		}

		rows[i] = table.Row{
			s.SessionID,
			s.Status,
			strings.Join(s.Tags, ", "),
			activeCirc,
			s.Prompt,
		}
	}
	m.table.SetRows(rows)

	if len(m.filtered) > 0 {
		cursor := m.table.Cursor()
		if cursor < 0 || cursor >= len(m.filtered) {
			m.table.SetCursor(0)
			cursor = 0
		}
		sess := m.filtered[cursor]
		m.selectedSess = &sess
	} else {
		m.selectedSess = nil
	}
	m.updateDetailView()
}

func (m *Model) updateDetailView() {
	if m.selectedSess == nil {
		m.detailVP.SetContent(styleKeyHelp.Render("\n  No Autopilot session selected or matching filter."))
		return
	}

	if m.panelTab == 0 {
		// AI Audit Log View
		m.detailVP.SetContent(RenderAuditLog(m.selectedSess, m.detailVP.Width))
	} else {
		// Circuit Diff View
		var diffContent string
		if m.diffMode == 0 {
			diffContent = RenderUnifiedDiff(m.selectedSess.ActiveCircuit, m.selectedSess.CandidateCircuit, m.detailVP.Width)
		} else {
			diffContent = RenderSideBySideDiff(m.selectedSess.ActiveCircuit, m.selectedSess.CandidateCircuit, m.detailVP.Width)
		}
		m.detailVP.SetContent(diffContent)
	}
}

// View renders the Autopilot Cockpit screen.
func (m Model) View() string {
	if m.width <= 0 || m.height <= 0 {
		return "Initializing Autopilot Live Cockpit..."
	}

	splitWidth := (m.width - 6) / 2
	if splitWidth < 30 {
		splitWidth = 30
	}
	contentHeight := m.height - 6
	if contentHeight < 6 {
		contentHeight = 6
	}

	// 1. Overview & Top Banner
	overview := m.renderOverviewBanner()

	// 2. Left Panel: Session Gauge + Sessions Table
	leftBorder := styleBorderNormal
	if m.focusedPanel == 0 {
		leftBorder = styleBorderFocused
	}

	var leftContent strings.Builder
	leftTitle := styleSplitTitleActive.Render("Active Autopilot Sessions")
	if m.focusedPanel != 0 {
		leftTitle = styleSplitTitleInactive.Render("Active Autopilot Sessions")
	}
	leftContent.WriteString(leftTitle)
	leftContent.WriteString("\n\n")

	// Session Phase Gauge
	leftContent.WriteString(m.renderSessionGauge())
	leftContent.WriteString("\n\n")

	// Table
	leftContent.WriteString(m.table.View())

	leftBox := leftBorder.
		Width(splitWidth).
		Height(contentHeight).
		Render(leftContent.String())

	// 3. Right Panel: AI Audit Log or Visual Diff
	rightBorder := styleBorderNormal
	if m.focusedPanel == 1 {
		rightBorder = styleBorderFocused
	}

	tab0 := styleTabButtonInactive.Render("AI Audit Log (a)")
	tab1 := styleTabButtonInactive.Render("Circuit Diff (v)")
	if m.panelTab == 0 {
		tab0 = styleTabButtonActive.Render("AI Audit Log (a)")
	} else {
		tab1 = styleTabButtonActive.Render("Circuit Diff (v)")
	}

	rightTabs := lipgloss.JoinHorizontal(lipgloss.Top, tab0, "  ", tab1)
	if m.panelTab == 1 {
		modeHint := styleKeyHelp.Render("[Mode: Unified (press 'd' for side-by-side)]")
		if m.diffMode == 1 {
			modeHint = styleKeyHelp.Render("[Mode: Side-by-Side (press 'd' for unified)]")
		}
		rightTabs = lipgloss.JoinHorizontal(lipgloss.Center, rightTabs, "  ", modeHint)
	}

	rightContent := rightTabs + "\n\n" + m.detailVP.View()

	rightBox := rightBorder.
		Width(splitWidth).
		Height(contentHeight).
		Render(rightContent)

	mainSplit := lipgloss.JoinHorizontal(lipgloss.Top, leftBox, "  ", rightBox)

	// Filter bar if active
	if m.isFiltering {
		filterBar := styleBorderFocused.Width(m.width - 4).Render(m.filterInput.View())
		return lipgloss.JoinVertical(lipgloss.Top, overview, filterBar, mainSplit, m.renderFooter())
	}

	return lipgloss.JoinVertical(lipgloss.Top, overview, mainSplit, m.renderFooter())
}

func (m Model) renderOverviewBanner() string {
	activeCount := len(m.sessions)
	var liveCount, samplingCount, shadowCount int
	for _, s := range m.sessions {
		switch strings.ToUpper(s.Status) {
		case "LIVE":
			liveCount++
		case "SAMPLING":
			samplingCount++
		case "SHADOWING":
			shadowCount++
		}
	}

	stats := fmt.Sprintf("🤖 Autopilot Cockpit: %d Sessions │ Live: %d │ Shadow: %d │ Sampling: %d",
		activeCount, liveCount, shadowCount, samplingCount)
	return lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00E5FF")).
		Padding(0, 1).
		Render(stats)
}

func (m Model) renderSessionGauge() string {
	if m.selectedSess == nil {
		return styleKeyHelp.Render("  No active session selected.")
	}

	s := m.selectedSess
	status := strings.ToUpper(s.Status)

	var sb strings.Builder

	// 1. Multi-Stage Phase Indicator
	phase1 := stylePhasePending.Render("[1. SAMPLING ○]")
	phase2 := stylePhasePending.Render("[2. SYNTHESIZING ○]")
	phase3 := stylePhasePending.Render("[3. SHADOWING ○]")
	phase4 := stylePhasePending.Render("[4. LIVE ○]")

	arrow := styleKeyHelp.Render(" ──> ")

	switch status {
	case "SAMPLING":
		phase1 = stylePhaseActive.Render("[1. SAMPLING ●]")
	case "SYNTHESIZING":
		phase1 = stylePhaseCompleted.Render("[1. SAMPLING ✓]")
		phase2 = stylePhaseActive.Render("[2. SYNTHESIZING ●]")
	case "SHADOWING":
		phase1 = stylePhaseCompleted.Render("[1. SAMPLING ✓]")
		phase2 = stylePhaseCompleted.Render("[2. SYNTHESIZING ✓]")
		phase3 = stylePhaseActive.Render("[3. SHADOWING ●]")
	case "LIVE":
		phase1 = stylePhaseCompleted.Render("[1. SAMPLING ✓]")
		phase2 = stylePhaseCompleted.Render("[2. SYNTHESIZING ✓]")
		phase3 = stylePhaseCompleted.Render("[3. SHADOWING ✓]")
		phase4 = stylePhaseCompleted.Render("[4. LIVE ★]")
	case "REFINING":
		phase1 = stylePhaseCompleted.Render("[1. SAMPLING ✓]")
		phase2 = stylePhaseCompleted.Render("[2. SYNTHESIZING ✓]")
		phase3 = stylePhaseCompleted.Render("[3. SHADOWING ✓]")
		phase4 = stylePhaseActive.Render("[4. REFINING ⚡]")
	case "FAILED":
		phase4 = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744")).Bold(true).Render("[FAILED ✕]")
	}

	phaseFlow := lipgloss.JoinHorizontal(lipgloss.Center, phase1, arrow, phase2, arrow, phase3, arrow, phase4)
	sb.WriteString(phaseFlow)
	sb.WriteString("\n\n")

	// 2. Sampling Progress Bar
	if s.SampleThreshold > 0 {
		ratio := float64(s.SampledEvents) / float64(s.SampleThreshold)
		if ratio > 1 {
			ratio = 1
		}
		barWidth := 20
		filled := int(ratio * float64(barWidth))
		empty := barWidth - filled
		if empty < 0 {
			empty = 0
		}
		bar := lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Render(strings.Repeat("█", filled)) +
			lipgloss.NewStyle().Foreground(lipgloss.Color("#3A3A55")).Render(strings.Repeat("░", empty))
		sb.WriteString(fmt.Sprintf("  Sampling Progress: [%s] %d/%d events (%.0f%%)\n",
			bar, s.SampledEvents, s.SampleThreshold, ratio*100))
	} else {
		sb.WriteString("  Sampling Progress: [████████████████████] Complete (Profiling Active)\n")
	}

	// 3. Shadow Staging Verification Gauge
	if s.StagingResult != nil {
		sr := s.StagingResult
		rateBarWidth := 20
		rateRatio := sr.ObservedRate / 0.20 // normalize against typical 20% max envelope
		if rateRatio > 1 {
			rateRatio = 1
		} else if rateRatio < 0 {
			rateRatio = 0
		}
		filledRate := int(rateRatio * float64(rateBarWidth))
		emptyRate := rateBarWidth - filledRate
		if emptyRate < 0 {
			emptyRate = 0
		}

		rateColor := lipgloss.Color("#00E676")
		if sr.ObservedRate > 0.20 || sr.ErrorEvents > 0 {
			rateColor = lipgloss.Color("#FF1744")
		} else if sr.ObservedRate > 0.10 {
			rateColor = lipgloss.Color("#FFD600")
		}

		rateBar := lipgloss.NewStyle().Foreground(rateColor).Render(strings.Repeat("█", filledRate)) +
			lipgloss.NewStyle().Foreground(lipgloss.Color("#3A3A55")).Render(strings.Repeat("░", emptyRate))

		statusTag := stylePhaseCompleted.Render("PROMOTED ★")
		if !sr.Promoted {
			if sr.RejectionReason != "" {
				statusTag = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744")).Bold(true).Render("REJECTED ✕")
			} else {
				statusTag = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600")).Bold(true).Render("SHADOW EVALUATION")
			}
		}

		sb.WriteString(fmt.Sprintf("  Shadow Firing Rate: [%s] %.3f (events: %d, errors: %d) │ %s\n",
			rateBar, sr.ObservedRate, sr.TotalEvents, sr.ErrorEvents, statusTag))
	} else {
		sb.WriteString("  Shadow Verification: (standby / awaiting candidate synthesis)\n")
	}

	return sb.String()
}

func (m Model) renderFooter() string {
	return lipgloss.JoinHorizontal(lipgloss.Center,
		styleKeyHelp.Render("Press "),
		styleKey.Render("Tab"), styleKeyHelp.Render(" switch panel, "),
		styleKey.Render("v"), styleKeyHelp.Render(" toggle diff, "),
		styleKey.Render("a"), styleKeyHelp.Render(" audit log, "),
		styleKey.Render("d"), styleKeyHelp.Render(" diff mode, "),
		styleKey.Render("/"), styleKeyHelp.Render(" filter sessions, "),
		styleKey.Render("r"), styleKeyHelp.Render(" refresh, "),
		styleKey.Render("↑/↓/j/k"), styleKeyHelp.Render(" navigate"),
	)
}

// StatusMsg returns the active status message.
func (m Model) StatusMsg() string {
	return m.statusMsg
}

// IsFiltering returns true if filter input is currently focused.
func (m Model) IsFiltering() bool {
	return m.isFiltering
}

// Sessions returns all loaded autopilot sessions.
func (m Model) Sessions() []client.AutopilotSession {
	return m.sessions
}

// SelectedSession returns the currently selected autopilot session.
func (m Model) SelectedSession() *client.AutopilotSession {
	return m.selectedSess
}

// ActivePanel returns the currently focused panel (0: Table & Gauge, 1: Detail).
func (m Model) ActivePanel() int {
	return m.focusedPanel
}

// PanelTab returns the active right panel tab (0: Audit Log, 1: Circuit Diff).
func (m Model) PanelTab() int {
	return m.panelTab
}

// DiffMode returns the current diff mode (0: Unified, 1: Side-by-Side).
func (m Model) DiffMode() int {
	return m.diffMode
}
