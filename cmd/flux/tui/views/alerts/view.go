package alerts

import (
	"context"
	"encoding/json"
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

// AlertPaneFocus defines which split panel has active keyboard focus.
type AlertPaneFocus int

const (
	PaneTable AlertPaneFocus = iota
	PaneDetail
)

// Internal stream and data messages
type (
	// AlertHistoryLoadedMsg delivers historical alerts fetched from the engine/cache.
	AlertHistoryLoadedMsg struct {
		Alerts []client.AlertRecord
		Err    error
	}

	// AlertStreamStartedMsg is emitted once the SSE streaming connection is open.
	AlertStreamStartedMsg struct {
		eventCh <-chan client.AlertEvent
		errCh   <-chan error
		cancel  context.CancelFunc
	}

	// AlertStreamEventMsg delivers a live alert received over the SSE stream.
	AlertStreamEventMsg struct {
		Event   client.AlertEvent
		eventCh <-chan client.AlertEvent
		errCh   <-chan error
	}

	// AlertStreamErrorMsg notifies that the SSE stream encountered an error or was closed.
	AlertStreamErrorMsg struct {
		Err error
	}
)

var (
	styleTitle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleStreamLive = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676")).
			Bold(true)

	styleStreamPaused = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleBadgeUnreviewed = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleBadgeFalsePositive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FF1744")).
				Bold(true)

	styleBadgeValid = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676")).
			Bold(true)

	styleBadgeNoisy = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF9100")).
			Bold(true)

	styleBadgeMuted = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#757575")).
			Bold(true)

	styleMetaLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleMetaVal = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF"))

	styleDetailPanel = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#3A3A55")).
				Padding(0, 1)

	styleDetailPanelFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#00E5FF")).
				Padding(0, 1)
)

// Model represents the real-time alert stream and human-in-the-loop feedback console (:alerts).
type Model struct {
	client client.Client
	width  int
	height int

	alerts         []client.AlertRecord
	filteredAlerts []client.AlertRecord

	table          table.Model
	detailViewport viewport.Model
	feedbackModal  FeedbackModal

	focus           AlertPaneFocus
	filtering       bool
	filterInput     textinput.Model
	filterQuery     string
	streamingPaused bool
	streamActive    bool
	streamCancel    context.CancelFunc

	statusMsg    string
	statusExpiry time.Time
}

// New initializes the alerts stream and feedback view.
func New(cli client.Client) Model {
	columns := []table.Column{
		{Title: "TIME", Width: 10},
		{Title: "ALERT ID", Width: 14},
		{Title: "CIRCUIT", Width: 18},
		{Title: "NODE", Width: 16},
		{Title: "SINK", Width: 14},
		{Title: "FEEDBACK", Width: 20},
		{Title: "CONDITION", Width: 32},
	}

	t := table.New(
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
	t.SetStyles(s)

	vp := viewport.New(60, 10)
	vp.SetContent("  (select an alert to inspect triggering payload and feedback history)")

	fi := textinput.New()
	fi.Placeholder = "Filter by circuit, sink, alert ID, or payload..."
	fi.CharLimit = 64
	fi.Width = 40

	m := Model{
		client:         cli,
		table:          t,
		detailViewport: vp,
		feedbackModal:  NewFeedbackModal(),
		filterInput:    fi,
		focus:          PaneTable,
	}

	return m
}

// Init triggers initial historical alert loading and opens the real-time SSE stream.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.loadAlertHistoryCmd(),
		m.startAlertStreamCmd(),
	)
}

// SetClient updates the client bridge and restarts alert fetching.
func (m *Model) SetClient(cli client.Client) tea.Cmd {
	m.client = cli
	if m.streamCancel != nil {
		m.streamCancel()
		m.streamCancel = nil
	}
	m.streamActive = false
	return tea.Batch(
		m.loadAlertHistoryCmd(),
		m.startAlertStreamCmd(),
	)
}

// SetDimensions updates the view dimensions, resizing the table, detail pane, and modal.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h

	topBarH := 3
	contentH := h - topBarH
	if contentH < 8 {
		contentH = 8
	}

	// Split vertically: Top table (60%), Bottom payload inspector (40%)
	tableH := (contentH * 6) / 10
	if tableH < 5 {
		tableH = 5
	}
	detailH := contentH - tableH - 2
	if detailH < 4 {
		detailH = 4
	}

	m.table.SetWidth(w - 4)
	m.table.SetHeight(tableH)

	m.detailViewport.Width = w - 6
	m.detailViewport.Height = detailH

	m.feedbackModal.SetDimensions(w, h)
	m.refreshDetailContent()
}

// IsModalActive returns true if the Operator Feedback Modal is active.
func (m Model) IsModalActive() bool {
	return m.feedbackModal.IsActive()
}

// IsFiltering returns true if the search query filter input is focused.
func (m Model) IsFiltering() bool {
	return m.filtering
}

// StatusMsg returns the active status message if not expired.
func (m Model) StatusMsg() string {
	if time.Now().Before(m.statusExpiry) {
		return m.statusMsg
	}
	return ""
}

// Alerts returns the current full list of alerts.
func (m Model) Alerts() []client.AlertRecord {
	return m.alerts
}

// SelectedAlert returns the alert currently selected in the table, or nil.
func (m Model) SelectedAlert() *client.AlertRecord {
	if len(m.filteredAlerts) == 0 {
		return nil
	}
	idx := m.table.Cursor()
	if idx >= 0 && idx < len(m.filteredAlerts) {
		return &m.filteredAlerts[idx]
	}
	return nil
}

func (m *Model) setStatus(msg string, isError bool) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(4 * time.Second)
}

// Update processes Bubble Tea events, SSE streams, table navigation, and modal actions.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	// 1. If Feedback Modal is active, route all events to modal
	if m.feedbackModal.IsActive() {
		handled, cmd := m.feedbackModal.Update(msg, m.client)
		if handled {
			return m, cmd
		}
	}

	// 2. If Filter Input is active, route typing
	if m.filtering {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "enter":
				m.filtering = false
				m.filterQuery = strings.TrimSpace(m.filterInput.Value())
				m.filterInput.Blur()
				m.applyFilter()
				return m, nil
			case "esc":
				m.filtering = false
				m.filterInput.SetValue(m.filterQuery)
				m.filterInput.Blur()
				return m, nil
			}
		}
		var cmd tea.Cmd
		m.filterInput, cmd = m.filterInput.Update(msg)
		return m, cmd
	}

	// 3. Handle asynchronous stream & API messages
	switch msg := msg.(type) {
	case AlertHistoryLoadedMsg:
		if msg.Err == nil {
			m.alerts = msg.Alerts
			m.applyFilter()
			m.setStatus(fmt.Sprintf("Loaded %d alerts from history.", len(msg.Alerts)), false)
		} else {
			m.setStatus(fmt.Sprintf("Failed to load alerts: %v", msg.Err), true)
		}
		return m, nil

	case AlertStreamStartedMsg:
		m.streamActive = true
		m.streamCancel = msg.cancel
		return m, waitForNextAlert(msg.eventCh, msg.errCh)

	case AlertStreamEventMsg:
		// Convert AlertEvent to AlertRecord
		rec := client.AlertRecord{
			ID:        msg.Event.ID,
			CircuitID: msg.Event.CircuitID,
			NodeName:  msg.Event.NodeName,
			SinkName:  msg.Event.SinkName,
			Condition: msg.Event.Condition,
			Payload:   msg.Event.Payload,
		}

		// Prepend new alert to list
		m.alerts = append([]client.AlertRecord{rec}, m.alerts...)
		if len(m.alerts) > 500 {
			m.alerts = m.alerts[:500]
		}

		m.applyFilter()
		if !m.streamingPaused {
			m.refreshDetailContent()
		}

		return m, waitForNextAlert(msg.eventCh, msg.errCh)

	case AlertStreamErrorMsg:
		m.streamActive = false
		if msg.Err != nil {
			m.setStatus(fmt.Sprintf("Alert stream disconnected: %v", msg.Err), true)
		}
		return m, nil

	case FeedbackSubmittedMsg:
		m.feedbackModal.Close()
		if msg.Err != nil {
			m.setStatus(fmt.Sprintf("Failed to submit feedback: %v", msg.Err), true)
		} else {
			// Update local alert feedback badge
			for i := range m.alerts {
				if m.alerts[i].ID == msg.AlertID {
					m.alerts[i].Feedback = &client.FeedbackInfo{
						Classification: msg.FeedbackType,
						Reason:         msg.Notes,
						Author:         msg.Operator,
						UpdatedAt:      time.Now().UTC(),
					}
					break
				}
			}
			m.applyFilter()
			m.refreshDetailContent()

			tagBadge := msg.FeedbackType
			if msg.FeedbackType == string(FeedbackFalsePositive) {
				m.setStatus(fmt.Sprintf("✓ Recorded feedback 'false_positive' for alert %s (closed-loop refinement triggered)", msg.AlertID), false)
			} else {
				m.setStatus(fmt.Sprintf("✓ Recorded feedback %q for alert %s", tagBadge, msg.AlertID), false)
			}
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "tab":
			if m.focus == PaneTable {
				m.focus = PaneDetail
				m.table.Blur()
			} else {
				m.focus = PaneTable
				m.table.Focus()
			}
			return m, nil

		case "f":
			// Open Feedback Modal for selected alert
			sel := m.SelectedAlert()
			if sel != nil {
				m.feedbackModal.Open(*sel)
				return m, textinput.Blink
			} else {
				m.setStatus("No alert selected to provide feedback.", true)
			}
			return m, nil

		case "p", " ":
			// Toggle streaming pause / resume
			m.streamingPaused = !m.streamingPaused
			if m.streamingPaused {
				m.setStatus("Alert stream paused (press 'p' or Space to resume)", false)
			} else {
				m.setStatus("Alert stream live", false)
				m.refreshDetailContent()
			}
			return m, nil

		case "/":
			// Activate search filter
			m.filtering = true
			m.filterInput.Focus()
			return m, textinput.Blink

		case "esc":
			if m.filterQuery != "" {
				m.filterQuery = ""
				m.filterInput.Reset()
				m.applyFilter()
				m.setStatus("Cleared search filter.", false)
				return m, nil
			}

		case "r":
			// Refresh alert history
			m.setStatus("Refreshing alert stream...", false)
			return m, m.loadAlertHistoryCmd()
		}

		// Delegate navigation keys
		if m.focus == PaneTable {
			prevCursor := m.table.Cursor()
			var cmd tea.Cmd
			m.table, cmd = m.table.Update(msg)
			if m.table.Cursor() != prevCursor {
				m.refreshDetailContent()
			}
			return m, cmd
		}

		if m.focus == PaneDetail {
			var cmd tea.Cmd
			m.detailViewport, cmd = m.detailViewport.Update(msg)
			return m, cmd
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) applyFilter() {
	q := strings.ToLower(strings.TrimSpace(m.filterQuery))
	if q == "" {
		m.filteredAlerts = m.alerts
	} else {
		m.filteredAlerts = make([]client.AlertRecord, 0)
		for _, a := range m.alerts {
			match := strings.Contains(strings.ToLower(a.ID), q) ||
				strings.Contains(strings.ToLower(a.CircuitID), q) ||
				strings.Contains(strings.ToLower(a.NodeName), q) ||
				strings.Contains(strings.ToLower(a.SinkName), q) ||
				strings.Contains(strings.ToLower(a.Condition), q)
			if !match && a.Feedback != nil {
				match = strings.Contains(strings.ToLower(a.Feedback.Classification), q) ||
					strings.Contains(strings.ToLower(a.Feedback.Reason), q)
			}
			if match {
				m.filteredAlerts = append(m.filteredAlerts, a)
			}
		}
	}

	rows := make([]table.Row, len(m.filteredAlerts))
	for i, a := range m.filteredAlerts {
		timeStr := time.Now().Format("15:04:05")

		feedbackBadge := styleBadgeUnreviewed.Render("[UNREVIEWED]")
		if a.Feedback != nil {
			switch a.Feedback.Classification {
			case string(FeedbackFalsePositive):
				feedbackBadge = styleBadgeFalsePositive.Render("[FALSE POSITIVE ✗]")
			case string(FeedbackValid):
				feedbackBadge = styleBadgeValid.Render("[VALID ✓]")
			case string(FeedbackNoisy):
				feedbackBadge = styleBadgeNoisy.Render("[NOISY ~]")
			case string(FeedbackMuted):
				feedbackBadge = styleBadgeMuted.Render("[MUTED ⊘]")
			default:
				feedbackBadge = a.Feedback.Classification
			}
		}

		condStr := a.Condition
		if len(condStr) > 30 {
			condStr = condStr[:27] + "..."
		}

		rows[i] = table.Row{
			timeStr,
			a.ID,
			a.CircuitID,
			a.NodeName,
			a.SinkName,
			feedbackBadge,
			condStr,
		}
	}

	m.table.SetRows(rows)
}

func (m *Model) refreshDetailContent() {
	sel := m.SelectedAlert()
	if sel == nil {
		m.detailViewport.SetContent(styleMetaDim.Render("  (no alert selected)"))
		return
	}

	var sb strings.Builder

	// Header
	sb.WriteString(styleMetaLabel.Render("ALERT ID: "))
	sb.WriteString(styleMetaVal.Render(sel.ID))
	sb.WriteString("    ")
	sb.WriteString(styleMetaLabel.Render("CIRCUIT: "))
	sb.WriteString(styleMetaVal.Render(sel.CircuitID))
	sb.WriteString("    ")
	sb.WriteString(styleMetaLabel.Render("NODE: "))
	sb.WriteString(styleMetaVal.Render(sel.NodeName))
	sb.WriteString("    ")
	sb.WriteString(styleMetaLabel.Render("DEST SINK: "))
	sb.WriteString(styleMetaVal.Render(sel.SinkName))
	sb.WriteString("\n")

	if sel.Condition != "" {
		sb.WriteString(styleMetaLabel.Render("CONDITION: "))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFF59D")).Render(sel.Condition))
		sb.WriteString("\n")
	}

	// Feedback Status
	sb.WriteString(styleMetaLabel.Render("OPERATOR FEEDBACK: "))
	if sel.Feedback == nil {
		sb.WriteString(styleBadgeUnreviewed.Render("[UNREVIEWED]"))
		sb.WriteString(styleMetaDim.Render("  (press 'f' to tag false_positive or valid)"))
	} else {
		badge := fmt.Sprintf("[%s]", strings.ToUpper(sel.Feedback.Classification))
		if sel.Feedback.Classification == string(FeedbackFalsePositive) {
			badge = styleBadgeFalsePositive.Render("[FALSE POSITIVE ✗]")
		} else if sel.Feedback.Classification == string(FeedbackValid) {
			badge = styleBadgeValid.Render("[VALID ✓]")
		}
		sb.WriteString(badge)
		sb.WriteString(fmt.Sprintf("  •  Author: %s  •  Time: %s",
			styleMetaVal.Render(sel.Feedback.Author),
			styleMetaDim.Render(sel.Feedback.UpdatedAt.Format("15:04:05"))))
		if sel.Feedback.Reason != "" {
			sb.WriteString(fmt.Sprintf("\n  %s %s", styleMetaDim.Render("Notes:"), styleMetaVal.Render(sel.Feedback.Reason)))
		}
	}
	sb.WriteString("\n\n")

	// Triggering Payload Snapshot
	sb.WriteString(styleMetaLabel.Render("TRIGGERING EVENT PAYLOAD (SNAPSHOT):"))
	sb.WriteString("\n")

	if sel.Payload == nil || len(sel.Payload) == 0 {
		sb.WriteString(styleMetaDim.Render("  (empty payload)"))
	} else {
		payloadBytes, err := json.MarshalIndent(sel.Payload, "  ", "  ")
		if err == nil {
			sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Render(string(payloadBytes)))
		} else {
			sb.WriteString(fmt.Sprintf("  %v", sel.Payload))
		}
	}

	m.detailViewport.SetContent(sb.String())
}

func (m Model) loadAlertHistoryCmd() tea.Cmd {
	return func() tea.Msg {
		if m.client == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		records, err := m.client.GetAlertHistory(ctx, "", 100)
		return AlertHistoryLoadedMsg{Alerts: records, Err: err}
	}
}

func (m Model) startAlertStreamCmd() tea.Cmd {
	return func() tea.Msg {
		if m.client == nil {
			return nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		eventCh, errCh, err := m.client.StreamAlerts(ctx)
		if err != nil {
			cancel()
			return AlertStreamErrorMsg{Err: err}
		}
		return AlertStreamStartedMsg{
			eventCh: eventCh,
			errCh:   errCh,
			cancel:  cancel,
		}
	}
}

func waitForNextAlert(eventCh <-chan client.AlertEvent, errCh <-chan error) tea.Cmd {
	return func() tea.Msg {
		select {
		case evt, ok := <-eventCh:
			if !ok {
				return AlertStreamErrorMsg{Err: nil}
			}
			return AlertStreamEventMsg{
				Event:   evt,
				eventCh: eventCh,
				errCh:   errCh,
			}
		case err, ok := <-errCh:
			if !ok || err == nil {
				return AlertStreamErrorMsg{Err: nil}
			}
			return AlertStreamErrorMsg{Err: err}
		}
	}
}

// View renders the live alert streaming cockpit and detail inspector.
func (m Model) View() string {
	if m.width == 0 {
		return "Initializing Live Alert Stream..."
	}

	// 1. Top Header Bar
	streamBadge := styleStreamLive.Render("● STREAM LIVE")
	if m.streamingPaused {
		streamBadge = styleStreamPaused.Render("⏸ STREAM PAUSED")
	}

	unreviewed := 0
	falsePos := 0
	valid := 0
	for _, a := range m.alerts {
		if a.Feedback == nil {
			unreviewed++
		} else if a.Feedback.Classification == string(FeedbackFalsePositive) {
			falsePos++
		} else if a.Feedback.Classification == string(FeedbackValid) {
			valid++
		}
	}

	statsText := fmt.Sprintf("Total: %d  •  Unreviewed: %d  •  False Positives: %d  •  Valid: %d",
		len(m.alerts), unreviewed, falsePos, valid)

	headerTitle := fmt.Sprintf("%s  •  %s  │  %s",
		styleTitle.Render("📟 Real-Time Alert Stream & Human-in-the-Loop Feedback (:alerts)"),
		streamBadge,
		styleMetaDim.Render(statsText),
	)

	filterBar := ""
	if m.filtering {
		filterBar = fmt.Sprintf("Search Filter: %s", m.filterInput.View())
	} else if m.filterQuery != "" {
		filterBar = fmt.Sprintf("Filter: %q (press Esc to clear)", m.filterQuery)
	} else {
		filterBar = "[f] Operator Feedback  [p/Space] Pause/Resume Stream  [/] Filter  [r] Refresh  [Tab] Switch Pane"
	}
	helpFooter := styleMetaDim.Render(filterBar)

	// 2. Alert Table Panel
	tableBorder := lipgloss.Color("#3A3A55")
	if m.focus == PaneTable {
		tableBorder = lipgloss.Color("#00E5FF")
	}
	tableBox := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(tableBorder).
		Width(m.width - 4).
		Padding(0, 1).
		Render(m.table.View())

	// 3. Detail Inspector Panel
	detailStyle := styleDetailPanel
	if m.focus == PaneDetail {
		detailStyle = styleDetailPanelFocused
	}
	detailBox := detailStyle.
		Width(m.width - 4).
		Render(m.detailViewport.View())

	baseView := fmt.Sprintf("%s\n%s\n%s\n%s", headerTitle, helpFooter, tableBox, detailBox)

	// 4. If Feedback Modal is open, overlay modal dialog
	if m.feedbackModal.IsActive() {
		modalView := m.feedbackModal.View()
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, modalView,
			lipgloss.WithWhitespaceChars(" "),
			lipgloss.WithWhitespaceForeground(lipgloss.Color("#000000")))
	}

	return baseView
}
