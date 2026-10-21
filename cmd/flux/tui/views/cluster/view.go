package cluster

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

	styleLeaderRole = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD600")).
			Bold(true)

	styleFollowerRole = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#B0BEC5"))

	styleStatusHealthy = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E676")).
				Bold(true)

	styleStatusWarning = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleStatusExpired = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FF1744")).
				Bold(true)

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

// TopologyLoadedMsg delivers cluster topology fetched from the engine.
type TopologyLoadedMsg struct {
	Topology *client.ClusterTopology
	Err      error
}

// FetchTopologyCmd queries the client for cluster topology and node heartbeats.
func FetchTopologyCmd(c client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return TopologyLoadedMsg{Err: errors.New("no client available")}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		topo, err := c.GetClusterTopology(ctx)
		return TopologyLoadedMsg{Topology: topo, Err: err}
	}
}

// Model represents the Cluster Topology & Leader Election Inspector view (:cluster / :nodes).
type Model struct {
	client       client.Client
	tracker      *TopologyTracker
	nodes        []ClusterNode
	filtered     []ClusterNode
	table        table.Model
	detailVP     viewport.Model
	selectedNode *ClusterNode

	// Layout & Focus
	focusedPanel int // 0: Left (Nodes Table), 1: Right (Detail / Failover Log)
	panelTab     int // 0: Node Metadata Inspector, 1: Failover Event Log

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

// New initializes the Cluster Topology inspector component.
func New(c client.Client) Model {
	ti := textinput.New()
	ti.Prompt = "Filter Nodes: "
	ti.PromptStyle = styleFilterPrompt
	ti.CharLimit = 32

	columns := []table.Column{
		{Title: "NODE ID", Width: 20},
		{Title: "ROLE", Width: 12},
		{Title: "STATUS", Width: 12},
		{Title: "BIND ADDR", Width: 18},
		{Title: "STREAM PORT", Width: 12},
		{Title: "HEARTBEAT", Width: 12},
		{Title: "LEASE REMAIN", Width: 14},
		{Title: "UPTIME", Width: 12},
	}

	tbl := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(15),
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
		tracker:      NewTopologyTracker(50),
		table:        tbl,
		detailVP:     detailVP,
		filterInput:  ti,
		focusedPanel: 0,
		panelTab:     0,
		loading:      true,
	}
}

// Init starts initial cluster topology fetching.
func (m Model) Init() tea.Cmd {
	return FetchTopologyCmd(m.client)
}

// SetClient updates the communication client bridge.
func (m *Model) SetClient(c client.Client) tea.Cmd {
	m.client = c
	m.loading = true
	return FetchTopologyCmd(m.client)
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

	m.table.SetWidth(splitWidth)
	m.table.SetHeight(contentHeight - 2)
	m.detailVP.Width = splitWidth
	m.detailVP.Height = contentHeight - 4
	m.updateDetailView()
}

// Update handles Bubble Tea messages and user input.
func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case TopologyLoadedMsg:
		m.loading = false
		m.err = msg.Err
		if msg.Err == nil && msg.Topology != nil {
			selfID := ""
			if m.client != nil {
				if st, err := m.client.GetStatus(context.Background()); err == nil && st != nil {
					selfID = st.NodeID
				}
			}
			m.nodes = m.tracker.ProcessTopology(msg.Topology, selfID)
			m.applyFilter()
			m.statusMsg = fmt.Sprintf("Cluster topology updated: %d active nodes.", len(m.nodes))
		} else if msg.Err != nil {
			m.statusMsg = fmt.Sprintf("Topology fetch failed: %v", msg.Err)
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
			// Toggle right panel tab (0: Node Metadata <-> 1: Failover Log)
			m.panelTab = (m.panelTab + 1) % 2
			m.updateDetailView()
			return m, nil

		case "r":
			m.loading = true
			m.statusMsg = "Refreshing cluster topology..."
			return m, FetchTopologyCmd(m.client)

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
		node := m.filtered[idx]
		m.selectedNode = &node
	} else {
		m.selectedNode = nil
	}
	m.updateDetailView()
}

func (m *Model) applyFilter() {
	if m.filterQuery == "" {
		m.filtered = make([]ClusterNode, len(m.nodes))
		copy(m.filtered, m.nodes)
	} else {
		q := strings.ToLower(m.filterQuery)
		m.filtered = nil
		for _, n := range m.nodes {
			if strings.Contains(strings.ToLower(n.NodeID), q) ||
				strings.Contains(strings.ToLower(n.Role), q) ||
				strings.Contains(strings.ToLower(n.Status), q) ||
				strings.Contains(strings.ToLower(n.Addr), q) {
				m.filtered = append(m.filtered, n)
			}
		}
	}

	rows := make([]table.Row, len(m.filtered))
	for i, n := range m.filtered {
		roleLabel := n.Role
		if n.IsLeader {
			roleLabel = "LEADER ★"
		}

		statusLabel := n.Status
		switch n.Status {
		case StatusHealthy:
			statusLabel = "HEALTHY ●"
		case StatusWarning:
			statusLabel = "WARNING ⚠"
		case StatusExpired:
			statusLabel = "EXPIRED ✕"
		}

		addr := n.Addr
		if addr == "" {
			addr = "n/a"
		}

		streamPort := fmt.Sprintf("%d", n.StreamPort)
		if n.StreamPort <= 0 {
			streamPort = "default"
		}

		rows[i] = table.Row{
			n.NodeID,
			roleLabel,
			statusLabel,
			addr,
			streamPort,
			FormatDuration(n.HeartbeatAge),
			FormatLease(n.LeaseRemaining, n.Status),
			n.Uptime,
		}
	}
	m.table.SetRows(rows)
	m.updateSelectedFromTable()
}

func (m *Model) updateDetailView() {
	if m.panelTab == 1 {
		m.detailVP.SetContent(m.renderFailoverLog())
	} else {
		m.detailVP.SetContent(m.renderNodeMetadata())
	}
}

func (m Model) renderNodeMetadata() string {
	if m.selectedNode == nil {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("\n  No node selected. Use ↑/↓ to browse nodes.")
	}

	n := m.selectedNode
	var sb strings.Builder

	// Title
	title := fmt.Sprintf("Node: %s", n.NodeID)
	if n.IsSelf {
		title += " (local endpoint)"
	}
	sb.WriteString(styleTitle.Render(title))
	sb.WriteString("\n\n")

	// Role & Status Badges
	roleBadge := styleFollowerRole.Render("FOLLOWER")
	if n.IsLeader {
		roleBadge = styleLeaderRole.Render("LEADER ★ (Autopilot Synthesis Active)")
	}

	statusBadge := styleStatusHealthy.Render("HEALTHY ●")
	switch n.Status {
	case StatusWarning:
		statusBadge = styleStatusWarning.Render("WARNING ⚠ (Lease Expiring)")
	case StatusExpired:
		statusBadge = styleStatusExpired.Render("EXPIRED ✕ (Node Inactive)")
	}

	sb.WriteString(fmt.Sprintf("  Role:       %s\n", roleBadge))
	sb.WriteString(fmt.Sprintf("  Health:     %s\n", statusBadge))
	sb.WriteString(fmt.Sprintf("  Bind Addr:  %s\n", n.Addr))
	sb.WriteString(fmt.Sprintf("  Stream Port:%d\n", n.StreamPort))
	sb.WriteString(fmt.Sprintf("  Uptime:     %s\n", n.Uptime))
	sb.WriteString(fmt.Sprintf("  Heartbeat:  %s ago (last at %s)\n", FormatDuration(n.HeartbeatAge), n.UpdatedAt.Format("15:04:05 MST")))
	sb.WriteString(fmt.Sprintf("  Lease:      %s remaining (10s TTL window)\n\n", FormatLease(n.LeaseRemaining, n.Status)))

	// Lease countdown progress bar
	sb.WriteString(m.renderLeaseProgressBar(n.LeaseRemaining, 10*time.Second))
	sb.WriteString("\n\n")

	// Metadata attributes
	sb.WriteString(styleSplitTitleActive.Render("Node Attributes & Configuration:"))
	sb.WriteString("\n")
	if len(n.Metadata) == 0 {
		sb.WriteString("  (no additional metadata attributes advertised)\n")
	} else {
		for k, v := range n.Metadata {
			sb.WriteString(fmt.Sprintf("  • %s: %s\n", k, v))
		}
	}

	sb.WriteString("\n")
	sb.WriteString(styleSplitTitleInactive.Render("Consensus Engine Properties:"))
	sb.WriteString("\n")
	sb.WriteString("  • Algorithm: Deterministic Lexicographical Leader Selection\n")
	sb.WriteString("  • Pruning: Inactive heartbeats pruned after 10s TTL expiration\n")
	sb.WriteString("  • Failover: Automatic failover upon lease timeout\n")

	return sb.String()
}

func (m Model) renderLeaseProgressBar(remaining, ttl time.Duration) string {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	ratio := float64(remaining) / float64(ttl)
	if ratio < 0 {
		ratio = 0
	} else if ratio > 1 {
		ratio = 1
	}

	barWidth := 24
	filled := int(ratio * float64(barWidth))
	empty := barWidth - filled

	barStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676"))
	if ratio < 0.3 {
		barStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744"))
	} else if ratio < 0.6 {
		barStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600"))
	}

	bar := barStyle.Render(strings.Repeat("█", filled)) + lipgloss.NewStyle().Foreground(lipgloss.Color("#3A3A55")).Render(strings.Repeat("░", empty))
	return fmt.Sprintf("  Lease Gauge: [%s] %.0f%%", bar, ratio*100)
}

func (m Model) renderFailoverLog() string {
	events := m.tracker.Events()
	if len(events) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("\n  No failover or consensus events recorded yet.")
	}

	var sb strings.Builder
	sb.WriteString(styleTitle.Render("⚡ Consensus & Failover Audit Log:"))
	sb.WriteString("\n\n")

	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		tag := "[INFO]"
		tagStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true)
		switch e.Severity {
		case SeverityWarning:
			tag = "[WARN]"
			tagStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600")).Bold(true)
		case SeverityCritical:
			tag = "[CRIT]"
			tagStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF1744")).Bold(true)
		}

		timeStr := e.Timestamp.Format("15:04:05")
		sb.WriteString(fmt.Sprintf("  %s %s %s\n",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render(timeStr),
			tagStyle.Render(tag),
			e.Message,
		))
	}

	return sb.String()
}

// View renders the complete Cluster Topology view.
func (m Model) View() string {
	var sb strings.Builder

	// 1. Top Header Banner
	sb.WriteString(m.renderHeaderBanner())
	sb.WriteString("\n")

	// 2. Filter Bar (if active)
	if m.isFiltering || m.filterQuery != "" {
		sb.WriteString(m.filterInput.View())
		sb.WriteString("\n")
	}

	// 3. Split Screen Body
	splitWidth := (m.width - 6) / 2
	if splitWidth < 30 {
		splitWidth = 30
	}
	contentHeight := m.height - 6
	if contentHeight < 6 {
		contentHeight = 6
	}

	// Left: Nodes Table
	leftBorder := styleBorderNormal
	leftTitle := styleSplitTitleInactive.Render(fmt.Sprintf("Nodes Table (%d)", len(m.filtered)))
	if m.focusedPanel == 0 {
		leftBorder = styleBorderFocused
		leftTitle = styleSplitTitleActive.Render(fmt.Sprintf("Nodes Table (%d)", len(m.filtered)))
	}
	leftPanel := leftBorder.Width(splitWidth).Height(contentHeight).Render(
		lipgloss.JoinVertical(lipgloss.Left, leftTitle, "\n", m.table.View()),
	)

	// Right: Detail / Failover Log Panel
	rightBorder := styleBorderNormal
	tab0 := styleTabButtonInactive.Render("Node Metadata")
	tab1 := styleTabButtonInactive.Render("Failover Log")
	if m.panelTab == 0 {
		tab0 = styleTabButtonActive.Render("Node Metadata")
	} else {
		tab1 = styleTabButtonActive.Render("Failover Log")
	}

	tabsBar := lipgloss.JoinHorizontal(lipgloss.Top, tab0, " ", tab1)
	if m.focusedPanel == 1 {
		rightBorder = styleBorderFocused
	}

	rightPanel := rightBorder.Width(splitWidth).Height(contentHeight).Render(
		lipgloss.JoinVertical(lipgloss.Left, tabsBar, "\n", m.detailVP.View()),
	)

	// Join split panels horizontally
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, "  ", rightPanel))
	sb.WriteString("\n")

	// 4. View Footer
	sb.WriteString(m.renderFooter())

	return sb.String()
}

func (m Model) renderHeaderBanner() string {
	healthyCount := 0
	leaderID := "none"
	for _, n := range m.nodes {
		if n.Status == StatusHealthy {
			healthyCount++
		}
		if n.IsLeader {
			leaderID = n.NodeID
		}
	}

	overview := fmt.Sprintf("🌐 Topology: %d Active Nodes │ Leader: %s (Autopilot Active) │ Health: %d Healthy, %d Degraded",
		len(m.nodes), leaderID, healthyCount, len(m.nodes)-healthyCount)

	return lipgloss.NewStyle().
		Background(lipgloss.Color("#1E1E2E")).
		Foreground(lipgloss.Color("#00E5FF")).
		Bold(true).
		Padding(0, 1).
		Width(m.width - 4).
		Render(overview)
}

func (m Model) renderFooter() string {
	return lipgloss.JoinHorizontal(lipgloss.Center,
		styleKeyHelp.Render("Press "),
		styleKey.Render("Tab"), styleKeyHelp.Render(" switch panel, "),
		styleKey.Render("v"), styleKeyHelp.Render(" toggle metadata/failover log, "),
		styleKey.Render("/"), styleKeyHelp.Render(" filter nodes, "),
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

// Nodes returns the current list of discovered cluster nodes.
func (m Model) Nodes() []ClusterNode {
	return m.nodes
}

// Leader returns the active leader node if discovered.
func (m Model) Leader() *ClusterNode {
	for _, n := range m.nodes {
		if n.IsLeader {
			return &n
		}
	}
	return nil
}

// SelectedNode returns the currently selected node in the table.
func (m Model) SelectedNode() *ClusterNode {
	return m.selectedNode
}

// ActivePanel returns the currently focused panel (0: Table, 1: Detail).
func (m Model) ActivePanel() int {
	return m.focusedPanel
}

// PanelTab returns the active right panel tab (0: Metadata, 1: Failover Log).
func (m Model) PanelTab() int {
	return m.panelTab
}

// Tracker returns the underlying TopologyTracker.
func (m Model) Tracker() *TopologyTracker {
	return m.tracker
}
