package alerts

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

// FeedbackType identifies the operator classification tag for an alert.
type FeedbackType string

const (
	FeedbackFalsePositive FeedbackType = "false_positive"
	FeedbackValid         FeedbackType = "valid"
	FeedbackNoisy         FeedbackType = "noisy"
	FeedbackMuted         FeedbackType = "muted"
)

// FeedbackOption describes a selectable classification category.
type FeedbackOption struct {
	Type        FeedbackType
	Label       string
	Badge       string
	Description string
	Color       lipgloss.Color
}

var feedbackOptions = []FeedbackOption{
	{
		Type:        FeedbackFalsePositive,
		Label:       "false_positive",
		Badge:       "[FALSE POSITIVE ✗]",
		Description: "Flags false alarm; triggers closed-loop autonomous refinement",
		Color:       lipgloss.Color("#FF1744"), // Coral Red
	},
	{
		Type:        FeedbackValid,
		Label:       "valid",
		Badge:       "[VALID ✓]",
		Description: "Confirms legitimate incident / true positive",
		Color:       lipgloss.Color("#00E676"), // Emerald Green
	},
	{
		Type:        FeedbackNoisy,
		Label:       "noisy",
		Badge:       "[NOISY ~]",
		Description: "Valid condition but fires too frequently; triggers threshold tuning",
		Color:       lipgloss.Color("#FFD600"), // Neon Amber
	},
	{
		Type:        FeedbackMuted,
		Label:       "muted",
		Badge:       "[MUTED ⊘]",
		Description: "Temporarily suppress notifications for this condition",
		Color:       lipgloss.Color("#6E6E8E"), // Slate Gray
	},
}

var (
	styleModalBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#00E5FF")).
			Background(lipgloss.Color("#161622")).
			Padding(1, 2)

	styleModalHeader = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleAlertMeta = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true)

	styleMetaDim = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#A0A0C0"))

	styleOptionUnselected = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#A0A0C0")).
				Background(lipgloss.Color("#252538")).
				Padding(0, 1)

	styleFieldLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleModalFooter = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#A0A0C0"))
)

// FeedbackSubmittedMsg notifies the view that an operator feedback request was submitted.
type FeedbackSubmittedMsg struct {
	AlertID      string
	CircuitID    string
	FeedbackType string
	Notes        string
	Operator     string
	Err          error
}

// FeedbackModal manages the interactive dialog for classifying alerts and driving closed-loop adaptation.
type FeedbackModal struct {
	active          bool
	width           int
	height          int
	targetAlert     client.AlertRecord
	selectedTypeIdx int // 0..3
	notesInput      textinput.Model
	operatorInput   textinput.Model
	focusIndex      int // 0: type selector, 1: notes, 2: operator
	submitting      bool
	lastErr         error
}

// NewFeedbackModal creates an initialized feedback modal.
func NewFeedbackModal() FeedbackModal {
	ni := textinput.New()
	ni.Placeholder = "Optional note: reason for false positive or incident context..."
	ni.CharLimit = 256
	ni.Width = 60

	oi := textinput.New()
	oi.SetValue("operator")
	oi.Placeholder = "operator username"
	oi.CharLimit = 64
	oi.Width = 30

	return FeedbackModal{
		notesInput:    ni,
		operatorInput: oi,
		focusIndex:    0,
	}
}

// Open activates the modal for the given alert record.
func (m *FeedbackModal) Open(alert client.AlertRecord) {
	m.active = true
	m.targetAlert = alert
	m.selectedTypeIdx = 0 // default to false_positive
	m.focusIndex = 0
	m.submitting = false
	m.lastErr = nil
	m.notesInput.Reset()
	m.notesInput.Blur()
	m.operatorInput.SetValue("operator")
	m.operatorInput.Blur()
}

// Close deactivates the modal dialog.
func (m *FeedbackModal) Close() {
	m.active = false
	m.notesInput.Blur()
	m.operatorInput.Blur()
}

// IsActive returns true if the modal is currently open.
func (m FeedbackModal) IsActive() bool {
	return m.active
}

// SelectedType returns the currently chosen feedback classification.
func (m FeedbackModal) SelectedType() FeedbackType {
	if m.selectedTypeIdx >= 0 && m.selectedTypeIdx < len(feedbackOptions) {
		return feedbackOptions[m.selectedTypeIdx].Type
	}
	return FeedbackFalsePositive
}

// Notes returns the entered operator notes.
func (m FeedbackModal) Notes() string {
	return m.notesInput.Value()
}

// Operator returns the operator username.
func (m FeedbackModal) Operator() string {
	return m.operatorInput.Value()
}

// TargetAlert returns the alert being reviewed.
func (m FeedbackModal) TargetAlert() client.AlertRecord {
	return m.targetAlert
}

// SetDimensions updates the modal layout dimensions.
func (m *FeedbackModal) SetDimensions(w, h int) {
	m.width = w
	m.height = h
	inputWidth := w - 24
	if inputWidth < 30 {
		inputWidth = 30
	}
	if inputWidth > 80 {
		inputWidth = 80
	}
	m.notesInput.Width = inputWidth
	m.operatorInput.Width = inputWidth / 2
}

// SubmitCmd generates a Bubble Tea command to submit operator feedback to the engine.
func (m *FeedbackModal) SubmitCmd(cli client.Client) tea.Cmd {
	req := client.FeedbackRequest{
		CircuitID:    m.targetAlert.CircuitID,
		AlertID:      m.targetAlert.ID,
		FeedbackType: string(m.SelectedType()),
		Notes:        strings.TrimSpace(m.notesInput.Value()),
		Operator:     strings.TrimSpace(m.operatorInput.Value()),
	}

	m.submitting = true
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		if cli != nil {
			_, err = cli.SubmitFeedback(ctx, req)
		}
		return FeedbackSubmittedMsg{
			AlertID:      req.AlertID,
			CircuitID:    req.CircuitID,
			FeedbackType: req.FeedbackType,
			Notes:        req.Notes,
			Operator:     req.Operator,
			Err:          err,
		}
	}
}

// Update handles keyboard navigation and text input within the modal.
func (m *FeedbackModal) Update(msg tea.Msg, cli client.Client) (bool, tea.Cmd) {
	if !m.active {
		return false, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			m.Close()
			return true, nil

		case "enter":
			// If on a text input, submit directly
			cmd := m.SubmitCmd(cli)
			return true, cmd

		case "tab", "down":
			m.focusIndex = (m.focusIndex + 1) % 3
			m.updateFieldFocus()
			return true, nil

		case "shift+tab", "up":
			m.focusIndex = (m.focusIndex - 1 + 3) % 3
			m.updateFieldFocus()
			return true, nil

		case "1", "2", "3", "4":
			if m.focusIndex == 0 {
				idx := int(msg.Runes[0] - '1')
				if idx >= 0 && idx < len(feedbackOptions) {
					m.selectedTypeIdx = idx
				}
				return true, nil
			}

		case "left", "h":
			if m.focusIndex == 0 {
				m.selectedTypeIdx = (m.selectedTypeIdx - 1 + len(feedbackOptions)) % len(feedbackOptions)
				return true, nil
			}

		case "right", "l":
			if m.focusIndex == 0 {
				m.selectedTypeIdx = (m.selectedTypeIdx + 1) % len(feedbackOptions)
				return true, nil
			}
		}

		// Delegate text editing when focused
		if m.focusIndex == 1 {
			var cmd tea.Cmd
			m.notesInput, cmd = m.notesInput.Update(msg)
			return true, cmd
		}
		if m.focusIndex == 2 {
			var cmd tea.Cmd
			m.operatorInput, cmd = m.operatorInput.Update(msg)
			return true, cmd
		}
	}

	return false, nil
}

func (m *FeedbackModal) updateFieldFocus() {
	switch m.focusIndex {
	case 1:
		m.notesInput.Focus()
		m.operatorInput.Blur()
	case 2:
		m.notesInput.Blur()
		m.operatorInput.Focus()
	default:
		m.notesInput.Blur()
		m.operatorInput.Blur()
	}
}

// View renders the modal overlay dialog.
func (m FeedbackModal) View() string {
	if !m.active {
		return ""
	}

	modalW := m.width - 12
	if modalW < 50 {
		modalW = 50
	}
	if modalW > 96 {
		modalW = 96
	}

	var sb strings.Builder

	// Header
	sb.WriteString(styleModalHeader.Render("🏷️  Submit Operator Feedback on Alert"))
	sb.WriteString("\n\n")

	// Target Alert Information
	sb.WriteString(styleMetaDim.Render("Target Alert: "))
	sb.WriteString(styleAlertMeta.Render(m.targetAlert.ID))
	sb.WriteString(styleMetaDim.Render("  •  Circuit: "))
	sb.WriteString(styleAlertMeta.Render(m.targetAlert.CircuitID))
	sb.WriteString(styleMetaDim.Render("  •  Sink: "))
	sb.WriteString(styleAlertMeta.Render(m.targetAlert.SinkName))
	sb.WriteString("\n")

	if m.targetAlert.Condition != "" {
		sb.WriteString(styleMetaDim.Render("Trigger Condition: "))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFF59D")).Render(m.targetAlert.Condition))
		sb.WriteString("\n")
	}
	sb.WriteString("\n")

	// Classification Selector
	sb.WriteString(styleFieldLabel.Render("Select Classification Tag:"))
	sb.WriteString("\n")

	for i, opt := range feedbackOptions {
		isSelected := i == m.selectedTypeIdx
		cursor := "  "
		if isSelected {
			cursor = "▶ "
		}

		styleTag := styleOptionUnselected
		if isSelected {
			styleTag = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(opt.Color).
				Bold(true).
				Padding(0, 1)
		}

		badge := fmt.Sprintf("[%d] %s", i+1, opt.Label)
		sb.WriteString(cursor)
		sb.WriteString(styleTag.Render(badge))
		sb.WriteString("  ")
		sb.WriteString(styleMetaDim.Render(opt.Description))
		sb.WriteString("\n")
	}
	sb.WriteString("\n")

	// Operator Notes Input
	notesFocused := m.focusIndex == 1
	notesLabel := styleFieldLabel.Render("Operator Notes / Rationale:")
	if notesFocused {
		notesLabel = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676")).Bold(true).Render("Operator Notes / Rationale: (editing)")
	}
	sb.WriteString(notesLabel)
	sb.WriteString("\n")
	sb.WriteString(m.notesInput.View())
	sb.WriteString("\n\n")

	// Operator Username Input
	opFocused := m.focusIndex == 2
	opLabel := styleFieldLabel.Render("Operator Identifier:")
	if opFocused {
		opLabel = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676")).Bold(true).Render("Operator Identifier: (editing)")
	}
	sb.WriteString(opLabel)
	sb.WriteString("\n")
	sb.WriteString(m.operatorInput.View())
	sb.WriteString("\n\n")

	// Footer Help
	sb.WriteString(styleModalFooter.Render("Enter: Submit Feedback  │  Esc: Cancel  │  1-4: Select Tag  │  Tab/↑/↓: Switch Field"))

	return styleModalBox.Width(modalW).Render(sb.String())
}
