package circuits

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

var (
	styleViewTitle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleCountBadge = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7C4DFF")).
			Bold(true)

	styleFilterPrompt = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleFilterActive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E676")).
				Bold(true)

	styleSplitBorderFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#00E5FF")).
				Padding(0, 1)

	styleSplitBorderNormal = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#3A3A55")).
				Padding(0, 1)

	styleSplitTitleActive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleSplitTitleInactive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#A0A0C0")).
				Bold(true)

	styleEmptyBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#3A3A55")).
			Padding(1, 2)

	styleHintText = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#757575"))

	styleHintKey = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)
)

// CircuitsLoadedMsg delivers circuits fetched asynchronously from the engine.
type CircuitsLoadedMsg struct {
	Circuits []client.CircuitSummary
	Err      error
}

// CircuitDetailLoadedMsg delivers a single circuit definition.
type CircuitDetailLoadedMsg struct {
	Summary *client.CircuitSummary
	Err     error
}

// Model represents the interactive Circuits & DAG Inspector view.
type Model struct {
	client   client.Client
	circuits []client.CircuitSummary
	filtered []client.CircuitSummary
	table    table.Model

	// Detail Split-Screen State
	detailMode      bool
	focusedPanel    int // 0: Left (DAG Tree), 1: Right (Details Inspector)
	treeVP          viewport.Model
	detailsVP       viewport.Model
	selectedCircuit *client.CircuitSummary

	// Filter State
	filterInput textinput.Model
	isFiltering bool
	filterQuery string

	// Dimensions & Lifecycle
	width   int
	height  int
	loading bool
	err     error
}

// New creates an initialized Circuits view model.
func New(c client.Client) Model {
	ti := textinput.New()
	ti.Prompt = "Filter (/): "
	ti.PromptStyle = styleFilterPrompt
	ti.TextStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF"))
	ti.CharLimit = 64

	columns := []table.Column{
		{Title: "ID", Width: 26},
		{Title: "TAGS", Width: 22},
		{Title: "ROOT CONDITION", Width: 32},
		{Title: "NODES", Width: 7},
		{Title: "STATUS", Width: 8},
		{Title: "REV", Width: 5},
		{Title: "CREATED", Width: 18},
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

	treeVP := viewport.New(40, 15)
	detailsVP := viewport.New(40, 15)

	return Model{
		client:       c,
		table:        tbl,
		treeVP:       treeVP,
		detailsVP:    detailsVP,
		filterInput:  ti,
		focusedPanel: 0,
		loading:      true,
	}
}

// FetchCircuitsCmd creates a command to load all registered circuits from the client.
func FetchCircuitsCmd(c client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return CircuitsLoadedMsg{Err: errors.New("no client available")}
		}
		circuits, err := c.GetCircuits(context.Background())
		return CircuitsLoadedMsg{Circuits: circuits, Err: err}
	}
}

// FetchCircuitDetailCmd creates a command to load full circuit definition.
func FetchCircuitDetailCmd(c client.Client, id string) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return CircuitDetailLoadedMsg{Err: errors.New("no client available")}
		}
		summary, err := c.GetCircuit(context.Background(), id)
		return CircuitDetailLoadedMsg{Summary: summary, Err: err}
	}
}

// Init starts initial asynchronous data fetching.
func (m Model) Init() tea.Cmd {
	return FetchCircuitsCmd(m.client)
}

// SetClient updates the client bridge (e.g. after cluster connect/disconnect).
func (m *Model) SetClient(c client.Client) tea.Cmd {
	m.client = c
	m.loading = true
	m.err = nil
	return FetchCircuitsCmd(m.client)
}

// SetDimensions adjusts the viewport and table dimensions.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h

	// Adjust table height
	tblH := h - 4
	if tblH < 4 {
		tblH = 4
	}
	m.table.SetHeight(tblH)

	// Adjust split viewports
	panelH := h - 4
	if panelH < 4 {
		panelH = 4
	}

	leftW := (w - 4) * 45 / 100
	if leftW < 20 {
		leftW = 20
	}
	rightW := w - 4 - leftW
	if rightW < 20 {
		rightW = 20
	}

	m.treeVP.Width = leftW - 2
	m.treeVP.Height = panelH - 2
	m.detailsVP.Width = rightW - 2
	m.detailsVP.Height = panelH - 2

	if m.detailMode && m.selectedCircuit != nil {
		m.updateDetailViewports()
	}
}

// IsFiltering returns true if user is actively entering a search filter.
func (m Model) IsFiltering() bool {
	return m.isFiltering
}

// IsDetail returns true if detail split-screen inspector is open.
func (m Model) IsDetail() bool {
	return m.detailMode
}

// SelectedCircuit returns the currently highlighted/selected circuit.
func (m Model) SelectedCircuit() *client.CircuitSummary {
	if m.detailMode && m.selectedCircuit != nil {
		return m.selectedCircuit
	}
	if len(m.filtered) > 0 {
		idx := m.table.Cursor()
		if idx >= 0 && idx < len(m.filtered) {
			return &m.filtered[idx]
		}
	}
	return nil
}

// Circuits returns all loaded circuits.
func (m Model) Circuits() []client.CircuitSummary {
	return m.circuits
}

// Update handles messages and input for the Circuits view.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case CircuitsLoadedMsg:
		m.loading = false
		m.err = msg.Err
		if msg.Err == nil {
			m.circuits = msg.Circuits
			m.applyFilter()
		}
		return m, nil

	case CircuitDetailLoadedMsg:
		if msg.Err == nil && msg.Summary != nil {
			m.selectedCircuit = msg.Summary
			// Update in circuits array if matching
			for i := range m.circuits {
				if m.circuits[i].ID == msg.Summary.ID {
					m.circuits[i] = *msg.Summary
					break
				}
			}
			m.updateDetailViewports()
		}
		return m, nil

	case tea.KeyMsg:
		// 1. Filter Input Mode
		if m.isFiltering {
			switch msg.Type {
			case tea.KeyEnter:
				m.isFiltering = false
				m.filterInput.Blur()
				return m, nil
			case tea.KeyEsc:
				m.isFiltering = false
				m.filterInput.Reset()
				m.filterQuery = ""
				m.filterInput.Blur()
				m.applyFilter()
				return m, nil
			default:
				var cmd tea.Cmd
				m.filterInput, cmd = m.filterInput.Update(msg)
				m.filterQuery = m.filterInput.Value()
				m.applyFilter()
				return m, cmd
			}
		}

		// 2. Detail Split-Screen Inspector Mode
		if m.detailMode {
			switch msg.String() {
			case "esc", "q", "enter":
				m.detailMode = false
				return m, nil
			case "tab":
				m.focusedPanel = (m.focusedPanel + 1) % 2
				return m, nil
			case "up", "k":
				if m.focusedPanel == 0 {
					m.treeVP.LineUp(1)
				} else {
					m.detailsVP.LineUp(1)
				}
				return m, nil
			case "down", "j":
				if m.focusedPanel == 0 {
					m.treeVP.LineDown(1)
				} else {
					m.detailsVP.LineDown(1)
				}
				return m, nil
			case "pgup", "u":
				if m.focusedPanel == 0 {
					m.treeVP.HalfViewUp()
				} else {
					m.detailsVP.HalfViewUp()
				}
				return m, nil
			case "pgdown", "d":
				if m.focusedPanel == 0 {
					m.treeVP.HalfViewDown()
				} else {
					m.detailsVP.HalfViewDown()
				}
				return m, nil
			case "r":
				if m.selectedCircuit != nil {
					return m, FetchCircuitDetailCmd(m.client, m.selectedCircuit.ID)
				}
				return m, nil
			}
			return m, nil
		}

		// 3. Normal Table List Mode
		switch msg.String() {
		case "/":
			m.isFiltering = true
			m.filterInput.Focus()
			return m, textinput.Blink

		case "enter":
			if len(m.filtered) > 0 {
				idx := m.table.Cursor()
				if idx >= 0 && idx < len(m.filtered) {
					sel := m.filtered[idx]
					m.selectedCircuit = &sel
					m.detailMode = true
					m.focusedPanel = 0
					m.updateDetailViewports()

					// If definition is missing or empty, request full detail from client
					if sel.Definition == nil || sel.Definition.Root == nil {
						return m, FetchCircuitDetailCmd(m.client, sel.ID)
					}
					return m, nil
				}
			}

		case "esc":
			if m.filterQuery != "" {
				m.filterQuery = ""
				m.filterInput.Reset()
				m.applyFilter()
				return m, nil
			}

		case "r":
			m.loading = true
			return m, FetchCircuitsCmd(m.client)

		default:
			// Forward up/down/page navigation to table
			var cmd tea.Cmd
			m.table, cmd = m.table.Update(msg)
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) applyFilter() {
	q := strings.TrimSpace(strings.ToLower(m.filterQuery))
	if q == "" {
		m.filtered = make([]client.CircuitSummary, len(m.circuits))
		copy(m.filtered, m.circuits)
	} else {
		m.filtered = nil
		for _, c := range m.circuits {
			matchesID := strings.Contains(strings.ToLower(c.ID), q)
			matchesCond := strings.Contains(strings.ToLower(c.RootCondition), q)
			matchesTag := false
			for _, t := range c.Tags {
				if strings.Contains(strings.ToLower(t), q) {
					matchesTag = true
					break
				}
			}
			if matchesID || matchesCond || matchesTag {
				m.filtered = append(m.filtered, c)
			}
		}
	}

	var rows []table.Row
	for _, c := range m.filtered {
		tags := "-"
		if len(c.Tags) > 0 {
			tags = strings.Join(c.Tags, ", ")
		}

		cond := c.RootCondition
		if len(cond) > 30 {
			cond = cond[:27] + "..."
		}
		if cond == "" {
			cond = "-"
		}

		created := c.CreatedAt.Format("2006-01-02 15:04")
		if c.CreatedAt.IsZero() {
			created = "n/a"
		}

		rows = append(rows, table.Row{
			c.ID,
			tags,
			cond,
			fmt.Sprintf("%d", c.NodeCount),
			c.Status,
			fmt.Sprintf("r%d", c.Revision),
			created,
		})
	}

	m.table.SetRows(rows)
	if len(rows) > 0 && m.table.Cursor() >= len(rows) {
		m.table.SetCursor(len(rows) - 1)
	}
}

func (m *Model) updateDetailViewports() {
	if m.selectedCircuit == nil {
		return
	}
	leftW := (m.width - 4) * 45 / 100
	if leftW < 20 {
		leftW = 20
	}
	rightW := m.width - 4 - leftW
	if rightW < 20 {
		rightW = 20
	}

	treeContent := RenderDAGTree(m.selectedCircuit, leftW)
	detailContent := RenderCircuitDetails(m.selectedCircuit, rightW)

	m.treeVP.SetContent(treeContent)
	m.detailsVP.SetContent(detailContent)
}

// View renders the Circuits view component.
func (m Model) View() string {
	if m.detailMode {
		return m.renderDetailSplitScreen()
	}
	return m.renderTableView()
}

func (m Model) renderTableView() string {
	var sb strings.Builder

	// 1. Top Title & Search Bar
	title := styleViewTitle.Render("⚡ Deployed Decision Pipelines")
	countInfo := styleCountBadge.Render(fmt.Sprintf(" (%d registered)", len(m.circuits)))
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Center, title, countInfo))

	if m.isFiltering {
		sb.WriteString("  │  ")
		sb.WriteString(m.filterInput.View())
		sb.WriteString(styleHintText.Render(" (press Enter to apply, Esc to cancel)"))
	} else if m.filterQuery != "" {
		sb.WriteString("  │  ")
		sb.WriteString(styleFilterActive.Render(fmt.Sprintf("Filter: %q", m.filterQuery)))
		sb.WriteString(styleHintText.Render(" (matching " + fmt.Sprintf("%d of %d", len(m.filtered), len(m.circuits)) + " — press '/' to edit, Esc to clear)"))
	}
	sb.WriteString("\n\n")

	// 2. Loading / Error / Content
	if m.loading && len(m.circuits) == 0 {
		sb.WriteString(styleHintText.Render("  Connecting and fetching circuits from Flux engine..."))
		return sb.String()
	}

	if m.err != nil && len(m.circuits) == 0 {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744")).Render(fmt.Sprintf("  Error loading circuits: %v", m.err)))
		sb.WriteString("\n\n")
		sb.WriteString(styleHintText.Render("  Press 'r' to retry."))
		return sb.String()
	}

	if len(m.circuits) == 0 {
		sb.WriteString(m.renderEmptyState())
		return sb.String()
	}

	if len(m.filtered) == 0 {
		sb.WriteString(styleEmptyBox.Render(
			fmt.Sprintf("No circuits matching filter %q.\n\nPress Esc to reset search query.", m.filterQuery),
		))
		return sb.String()
	}

	// 3. Render Bubbles Table
	sb.WriteString(m.table.View())
	sb.WriteString("\n\n")

	// 4. Footer Key Hints
	hints := lipgloss.JoinHorizontal(lipgloss.Center,
		styleHintKey.Render("Enter"), styleHintText.Render(" inspect DAG  │  "),
		styleHintKey.Render("/"), styleHintText.Render(" filter  │  "),
		styleHintKey.Render("↑/↓/j/k"), styleHintText.Render(" navigate  │  "),
		styleHintKey.Render("r"), styleHintText.Render(" refresh"),
	)
	sb.WriteString(hints)

	return sb.String()
}

func (m Model) renderEmptyState() string {
	var sb strings.Builder
	sb.WriteString(styleViewTitle.Render("No circuits currently registered in Flux Engine.\n\n"))
	sb.WriteString(styleHintText.Render("• In Standalone Mode: Circuits will appear when registered or loaded.\n"))
	sb.WriteString(styleHintText.Render("• In Cluster Mode: Use ':connect <url>' to attach to an active Flux cluster.\n"))
	sb.WriteString(styleHintText.Render("• Press 'r' to force refresh."))
	return styleEmptyBox.Render(sb.String())
}

func (m Model) renderDetailSplitScreen() string {
	totalW := m.width
	if totalW < 40 {
		totalW = 80
	}
	panelH := m.height - 4
	if panelH < 6 {
		panelH = 10
	}

	leftW := (totalW - 4) * 45 / 100
	if leftW < 20 {
		leftW = 20
	}
	rightW := totalW - 4 - leftW
	if rightW < 20 {
		rightW = 20
	}

	// Left Box: DAG Tree
	treeTitle := styleSplitTitleInactive.Render(" [1] DAG Hierarchy & Tree ")
	leftBorderStyle := styleSplitBorderNormal
	if m.focusedPanel == 0 {
		treeTitle = styleSplitTitleActive.Render(" ▶ [1] DAG Hierarchy & Tree (Active) ")
		leftBorderStyle = styleSplitBorderFocused
	}
	leftContent := treeTitle + "\n\n" + m.treeVP.View()
	leftBox := leftBorderStyle.Width(leftW).Height(panelH).Render(leftContent)

	// Right Box: Specification & Grammar
	detailTitle := styleSplitTitleInactive.Render(" [2] Circuit Specification & Grammar ")
	rightBorderStyle := styleSplitBorderNormal
	if m.focusedPanel == 1 {
		detailTitle = styleSplitTitleActive.Render(" ▶ [2] Circuit Specification & Grammar (Active) ")
		rightBorderStyle = styleSplitBorderFocused
	}
	rightContent := detailTitle + "\n\n" + m.detailsVP.View()
	rightBox := rightBorderStyle.Width(rightW).Height(panelH).Render(rightContent)

	splitPanels := lipgloss.JoinHorizontal(lipgloss.Top, leftBox, " ", rightBox)

	// Bottom Hint
	focusedName := "DAG Tree"
	if m.focusedPanel == 1 {
		focusedName = "Details Inspector"
	}
	hints := lipgloss.JoinHorizontal(lipgloss.Center,
		styleHintKey.Render("Esc/q/Enter"), styleHintText.Render(" back to list  │  "),
		styleHintKey.Render("Tab"), styleHintText.Render(fmt.Sprintf(" switch focus (active: %s)  │  ", focusedName)),
		styleHintKey.Render("↑/↓/j/k"), styleHintText.Render(" scroll  │  "),
		styleHintKey.Render("r"), styleHintText.Render(" reload"),
	)

	return splitPanels + "\n" + hints
}
