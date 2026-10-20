package cache

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

// CacheKeysLoadedMsg delivers cache keys fetched from the engine.
type CacheKeysLoadedMsg struct {
	Keys []client.CacheKeyInfo
	Err  error
}

// CacheKeyDetailLoadedMsg delivers full key inspection details.
type CacheKeyDetailLoadedMsg struct {
	Detail *client.CacheKeyDetail
	Err    error
}

// Model represents the interactive Distributed Cache & State Browser view.
type Model struct {
	client   client.Client
	keys     []client.CacheKeyInfo
	filtered []client.CacheKeyInfo
	table    table.Model

	// Split Inspector State
	inspectorVP    viewport.Model
	selectedDetail *client.CacheKeyDetail
	loadingDetail  bool
	rawView        bool
	focusedPanel   int // 0: Left (Keys Table), 1: Right (Detail Inspector)

	// Filter State
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

// New creates an initialized Cache view model.
func New(c client.Client) Model {
	ti := textinput.New()
	ti.Prompt = "Filter (/): "
	ti.PromptStyle = styleFilterPrompt
	ti.TextStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF"))
	ti.CharLimit = 64

	columns := []table.Column{
		{Title: "KEY", Width: 32},
		{Title: "TYPE", Width: 10},
		{Title: "SIZE", Width: 10},
		{Title: "TTL", Width: 14},
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

	inspVP := viewport.New(40, 15)

	return Model{
		client:       c,
		table:        tbl,
		inspectorVP:  inspVP,
		filterInput:  ti,
		focusedPanel: 0,
		loading:      true,
	}
}

// FetchCacheKeysCmd queries the client for all active cache keys.
func FetchCacheKeysCmd(c client.Client, prefix string) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return CacheKeysLoadedMsg{Err: errors.New("no client available")}
		}
		keys, err := c.GetCacheKeys(context.Background(), prefix, 1000)
		return CacheKeysLoadedMsg{Keys: keys, Err: err}
	}
}

// FetchKeyDetailCmd queries full inspection data for a given cache key.
func FetchKeyDetailCmd(c client.Client, key string) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return CacheKeyDetailLoadedMsg{Err: errors.New("no client available")}
		}
		detail, err := c.InspectCacheKey(context.Background(), key)
		return CacheKeyDetailLoadedMsg{Detail: detail, Err: err}
	}
}

// Init starts initial data fetching.
func (m Model) Init() tea.Cmd {
	return FetchCacheKeysCmd(m.client, "")
}

// SetClient updates the client bridge (e.g. after cluster connect/disconnect).
func (m *Model) SetClient(c client.Client) tea.Cmd {
	m.client = c
	m.loading = true
	m.err = nil
	m.selectedDetail = nil
	return FetchCacheKeysCmd(m.client, "")
}

// SetDimensions adjusts component dimensions.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h

	panelH := h - 4
	if panelH < 6 {
		panelH = 6
	}

	leftW := (w - 4) * 45 / 100
	if leftW < 25 {
		leftW = 25
	}
	rightW := w - 4 - leftW
	if rightW < 25 {
		rightW = 25
	}

	m.table.SetWidth(leftW - 2)
	m.table.SetHeight(panelH - 3)

	m.inspectorVP.Width = rightW - 2
	m.inspectorVP.Height = panelH - 2

	m.updateInspectorContent()
}

// IsFiltering returns true if filter input is active.
func (m Model) IsFiltering() bool {
	return m.isFiltering
}

// SelectedKey returns the currently highlighted cache key.
func (m Model) SelectedKey() *client.CacheKeyInfo {
	if len(m.filtered) > 0 {
		idx := m.table.Cursor()
		if idx >= 0 && idx < len(m.filtered) {
			return &m.filtered[idx]
		}
	}
	return nil
}

// Keys returns all discovered keys.
func (m Model) Keys() []client.CacheKeyInfo {
	return m.keys
}

// SelectedDetail returns the actively inspected key detail.
func (m Model) SelectedDetail() *client.CacheKeyDetail {
	return m.selectedDetail
}

// StatusMsg returns the last status notification.
func (m Model) StatusMsg() string {
	return m.statusMsg
}

// FocusedPanel returns the currently focused panel (0: left table, 1: right inspector).
func (m Model) FocusedPanel() int {
	return m.focusedPanel
}

// IsRawView returns true if raw string view is enabled.
func (m Model) IsRawView() bool {
	return m.rawView
}

// Update handles incoming messages for the Cache view.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case CacheKeysLoadedMsg:
		m.loading = false
		m.err = msg.Err
		if msg.Err == nil {
			m.keys = msg.Keys
			m.applyFilter()
			// Automatically inspect the first key if none inspected yet
			if m.selectedDetail == nil && len(m.filtered) > 0 {
				return m, FetchKeyDetailCmd(m.client, m.filtered[0].Key)
			}
		} else {
			m.statusMsg = fmt.Sprintf("Error fetching cache keys: %v", msg.Err)
		}
		return m, nil

	case CacheKeyDetailLoadedMsg:
		m.loadingDetail = false
		if msg.Err == nil && msg.Detail != nil {
			m.selectedDetail = msg.Detail
			m.updateInspectorContent()
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

		// 2. Normal View Navigation
		switch msg.String() {
		case "/":
			m.isFiltering = true
			m.filterInput.Focus()
			return m, textinput.Blink

		case "tab":
			m.focusedPanel = (m.focusedPanel + 1) % 2
			if m.focusedPanel == 0 {
				m.table.Focus()
			} else {
				m.table.Blur()
			}
			return m, nil

		case "v":
			m.rawView = !m.rawView
			m.updateInspectorContent()
			if m.rawView {
				m.statusMsg = "Switched to RAW string view."
			} else {
				m.statusMsg = "Switched to FORMATTED inspector view."
			}
			return m, nil

		case "r":
			m.loading = true
			m.statusMsg = "Refreshing cache keys..."
			return m, FetchCacheKeysCmd(m.client, m.filterQuery)

		case "enter":
			sel := m.SelectedKey()
			if sel != nil {
				m.loadingDetail = true
				return m, FetchKeyDetailCmd(m.client, sel.Key)
			}
			return m, nil

		case "esc":
			if m.filterQuery != "" {
				m.filterQuery = ""
				m.filterInput.Reset()
				m.applyFilter()
				return m, nil
			}
			if m.focusedPanel == 1 {
				m.focusedPanel = 0
				m.table.Focus()
				return m, nil
			}

		case "up", "k":
			if m.focusedPanel == 0 {
				m.table.MoveUp(1)
				// Fetch detail for currently highlighted key
				if sel := m.SelectedKey(); sel != nil {
					return m, FetchKeyDetailCmd(m.client, sel.Key)
				}
				return m, nil
			}
			m.inspectorVP.LineUp(1)
			return m, nil

		case "down", "j":
			if m.focusedPanel == 0 {
				m.table.MoveDown(1)
				if sel := m.SelectedKey(); sel != nil {
					return m, FetchKeyDetailCmd(m.client, sel.Key)
				}
				return m, nil
			}
			m.inspectorVP.LineDown(1)
			return m, nil

		case "pgup", "u":
			if m.focusedPanel == 1 {
				m.inspectorVP.HalfViewUp()
			}
			return m, nil

		case "pgdown", "ctrl+d":
			if m.focusedPanel == 1 {
				m.inspectorVP.HalfViewDown()
			}
			return m, nil

		default:
			if m.focusedPanel == 0 {
				var cmd tea.Cmd
				m.table, cmd = m.table.Update(msg)
				cmds = append(cmds, cmd)
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *Model) applyFilter() {
	q := strings.TrimSpace(strings.ToLower(m.filterQuery))
	if q == "" {
		m.filtered = make([]client.CacheKeyInfo, len(m.keys))
		copy(m.filtered, m.keys)
	} else {
		m.filtered = nil
		for _, k := range m.keys {
			if strings.Contains(strings.ToLower(k.Key), q) || strings.Contains(strings.ToLower(k.Type), q) {
				m.filtered = append(m.filtered, k)
			}
		}
	}

	var rows []table.Row
	for _, k := range m.filtered {
		ttlStr := "perm"
		if k.TTL > 0 {
			ttlStr = formatTTL(k.TTL)
		}

		sizeStr := fmt.Sprintf("%d", k.Size)

		rows = append(rows, table.Row{
			k.Key,
			strings.ToUpper(k.Type),
			sizeStr,
			ttlStr,
		})
	}
	m.table.SetRows(rows)
}

func (m *Model) updateInspectorContent() {
	contentW := m.inspectorVP.Width
	if contentW < 30 {
		contentW = 30
	}
	content := RenderKeyInspector(m.selectedDetail, m.rawView, contentW)
	m.inspectorVP.SetContent(content)
}

// View renders the Cache Explorer view.
func (m Model) View() string {
	var sb strings.Builder

	// 1. Top Title & Search Bar
	title := styleViewTitle.Render("💾 Distributed Cache & Key Explorer")
	countInfo := styleCountBadge.Render(fmt.Sprintf(" (%d keys in backend)", len(m.keys)))
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Center, title, countInfo))

	if m.isFiltering {
		sb.WriteString("  │  ")
		sb.WriteString(m.filterInput.View())
		sb.WriteString(styleHintText.Render(" (press Enter to apply, Esc to cancel)"))
	} else if m.filterQuery != "" {
		sb.WriteString("  │  ")
		sb.WriteString(styleFilterActive.Render(fmt.Sprintf("Filter: %q", m.filterQuery)))
		sb.WriteString(styleHintText.Render(" (matching " + fmt.Sprintf("%d of %d", len(m.filtered), len(m.keys)) + " — press '/' to edit, Esc to clear)"))
	}
	sb.WriteString("\n\n")

	// 2. Loading / Error / Content
	if m.loading && len(m.keys) == 0 {
		sb.WriteString(styleHintText.Render("  Scanning CacheBackend keys from Flux engine..."))
		return sb.String()
	}

	if m.err != nil && len(m.keys) == 0 {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744")).Render(fmt.Sprintf("  Error loading cache keys: %v", m.err)))
		sb.WriteString("\n\n")
		sb.WriteString(styleHintText.Render("  Press 'r' to retry."))
		return sb.String()
	}

	if len(m.keys) == 0 {
		sb.WriteString(styleEmptyBox.Render("No keys currently registered in CacheBackend.\n\nPress 'r' to force refresh."))
		return sb.String()
	}

	// 3. Render Split Screen: Left (Keys Table) │ Right (Inspector)
	splitW := m.width
	if splitW < 40 {
		splitW = 80
	}
	panelH := m.height - 4
	if panelH < 6 {
		panelH = 10
	}

	leftW := (splitW - 4) * 45 / 100
	if leftW < 25 {
		leftW = 25
	}
	rightW := splitW - 4 - leftW
	if rightW < 25 {
		rightW = 25
	}

	// Left Box: Key Table
	leftTitle := styleSplitTitleInactive.Render(" [1] Keys & Namespaces ")
	leftBorderStyle := styleSplitBorderNormal
	if m.focusedPanel == 0 {
		leftTitle = styleSplitTitleActive.Render(" ▶ [1] Keys & Namespaces (Active) ")
		leftBorderStyle = styleSplitBorderFocused
	}
	leftContent := leftTitle + "\n\n" + m.table.View()
	leftBox := leftBorderStyle.Width(leftW).Height(panelH).Render(leftContent)

	// Right Box: Key Inspector
	rightTitle := styleSplitTitleInactive.Render(" [2] Key Data Inspector ")
	rightBorderStyle := styleSplitBorderNormal
	if m.focusedPanel == 1 {
		rightTitle = styleSplitTitleActive.Render(" ▶ [2] Key Data Inspector (Active) ")
		rightBorderStyle = styleSplitBorderFocused
	}
	rightContent := rightTitle + "\n\n" + m.inspectorVP.View()
	rightBox := rightBorderStyle.Width(rightW).Height(panelH).Render(rightContent)

	splitPanels := lipgloss.JoinHorizontal(lipgloss.Top, leftBox, " ", rightBox)
	sb.WriteString(splitPanels)
	sb.WriteString("\n")

	// 4. Footer Hints
	focusedName := "Keys List"
	if m.focusedPanel == 1 {
		focusedName = "Inspector"
	}

	rawLabel := "raw view"
	if m.rawView {
		rawLabel = "formatted view"
	}

	hints := lipgloss.JoinHorizontal(lipgloss.Center,
		styleHintKey.Render("Tab"), styleHintText.Render(fmt.Sprintf(" switch focus (%s)  │  ", focusedName)),
		styleHintKey.Render("Enter"), styleHintText.Render(" inspect  │  "),
		styleHintKey.Render("v"), styleHintText.Render(fmt.Sprintf(" toggle %s  │  ", rawLabel)),
		styleHintKey.Render("r"), styleHintText.Render(" refresh  │  "),
		styleHintKey.Render("/"), styleHintText.Render(" filter  │  "),
		styleHintKey.Render("↑/↓/j/k"), styleHintText.Render(" navigate/scroll"),
	)
	sb.WriteString(hints)

	return sb.String()
}
