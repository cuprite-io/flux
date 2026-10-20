package tui

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	cacheview "github.com/cuprite-io/flux/cmd/flux/tui/views/cache"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
)

// TickMsg triggers regular background UI refresh.
type TickMsg time.Time

// Model is the top-level Bubble Tea model for the Flux shell.
type Model struct {
	client       client.Client
	circuitsView circuits.Model
	cacheView    cacheview.Model
	keys         KeyMap
	currentView  ViewID
	previousView ViewID
	width        int
	height       int
	ready        bool

	// Adaptive Mode: Standalone vs Cluster
	isCluster bool
	endpoint  string
	token     string

	// Cluster State (Active in Cluster Mode)
	version     string
	nodeCount   int
	leaderID    string
	syncedKeys  int
	syncedBytes int64
	syncEnabled bool
	currentTime time.Time

	// Command Mode State (Vim-style :command)
	commandMode  bool
	commandInput textinput.Model

	// Status line message
	statusMsg     string
	statusIsError bool
	statusExpiry  time.Time
}

// NewModel initializes the Flux shell model with adaptive mode support.
func NewModel(cfg Config) Model {
	ti := textinput.New()
	ti.Prompt = ": "
	ti.PromptStyle = StyleCommandPrompt
	ti.TextStyle = lipgloss.NewStyle().Foreground(ColorLightText)
	ti.CharLimit = 64

	isCluster := cfg.Endpoint != ""
	endpoint := cfg.Endpoint
	if !isCluster {
		endpoint = "standalone (local memory)"
	}

	leaderID := cfg.NodeID
	if leaderID == "" {
		if isCluster {
			leaderID = "discovering..."
		} else {
			leaderID = "local-standalone"
		}
	}

	var cli client.Client
	if isCluster {
		c, err := client.NewHTTPClient(cfg.Endpoint, cfg.Token)
		if err == nil {
			cli = c
		}
	}
	if cli == nil {
		cli, _ = client.NewLocalClient()
	}

	return Model{
		client:       cli,
		circuitsView: circuits.New(cli),
		cacheView:    cacheview.New(cli),
		keys:         DefaultKeyMap(),
		currentView:  ViewCircuits,
		previousView: ViewCircuits,
		version:      flux.Version,
		isCluster:    isCluster,
		endpoint:     endpoint,
		token:        cfg.Token,
		nodeCount:    1,
		leaderID:     leaderID,
		currentTime:  time.Now(),
		commandInput: ti,
		statusMsg:    "Flux Shell active. Press '?' for help, ':' for commands.",
	}
}

// Client returns the active communication client bridge (HTTPClient or LocalClient).
func (m Model) Client() client.Client {
	return m.client
}

// CircuitsView returns the Circuits view component.
func (m Model) CircuitsView() circuits.Model {
	return m.circuitsView
}

// CacheView returns the Cache view component.
func (m Model) CacheView() cacheview.Model {
	return m.cacheView
}

// SetDimensions updates the window size.
func (m *Model) SetDimensions(w, h int) {
	m.width = w
	m.height = h
	m.ready = true
	contentHeight := h - 5
	if contentHeight < 6 {
		contentHeight = 6
	}
	m.circuitsView.SetDimensions(w-4, contentHeight-2)
	m.cacheView.SetDimensions(w-4, contentHeight-2)
}

// CurrentView returns the currently active view ID.
func (m Model) CurrentView() ViewID {
	return m.currentView
}

// IsCluster returns true if connected to a remote Flux cluster.
func (m Model) IsCluster() bool {
	return m.isCluster
}

// VisibleViews returns the list of views appropriate for the current operating mode.
func (m Model) VisibleViews() []ViewMeta {
	if !m.isCluster {
		return StandaloneViews
	}
	return ClusterViews
}

// SetView switches the active view.
func (m *Model) SetView(v ViewID) {
	if m.currentView != v {
		m.previousView = m.currentView
		m.currentView = v
	}
}

// SetStatus displays a status message.
func (m *Model) SetStatus(msg string, isError bool) {
	m.statusMsg = msg
	m.statusIsError = isError
	m.statusExpiry = time.Now().Add(5 * time.Second)
}

// WindowTitle returns the terminal title based on operating mode.
func (m Model) WindowTitle() string {
	if !m.isCluster {
		return "flux cli"
	}
	return fmt.Sprintf("flux cli - %s", m.endpoint)
}

// WindowTitleCmd emits a Bubble Tea command to rewrite the terminal window title.
func (m Model) WindowTitleCmd() tea.Cmd {
	return tea.SetWindowTitle(m.WindowTitle())
}

// TickCmd triggers a tick every second.
func TickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

// Init initializes the reactive model.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		TickCmd(),
		m.WindowTitleCmd(),
		m.circuitsView.Init(),
		m.cacheView.Init(),
	)
}

// Update handles incoming Elm messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetDimensions(msg.Width, msg.Height)

	case circuits.CircuitsLoadedMsg:
		var cmd tea.Cmd
		m.circuitsView, cmd = m.circuitsView.Update(msg)
		return m, cmd

	case circuits.CircuitDetailLoadedMsg:
		var cmd tea.Cmd
		m.circuitsView, cmd = m.circuitsView.Update(msg)
		return m, cmd

	case circuits.DryRunHandoffMsg:
		m.SetView(ViewEval)
		m.SetStatus(fmt.Sprintf("Transferred circuit %q to Dry-Run Playground.", msg.CircuitID), false)
		return m, nil

	case cacheview.CacheKeysLoadedMsg:
		var cmd tea.Cmd
		m.cacheView, cmd = m.cacheView.Update(msg)
		return m, cmd

	case cacheview.CacheKeyDetailLoadedMsg:
		var cmd tea.Cmd
		m.cacheView, cmd = m.cacheView.Update(msg)
		return m, cmd

	case TickMsg:
		m.currentTime = time.Time(msg)
		if !m.statusExpiry.IsZero() && time.Now().After(m.statusExpiry) {
			m.statusMsg = ""
			m.statusIsError = false
			m.statusExpiry = time.Time{}
		}
		cmds = append(cmds, TickCmd())

	case tea.KeyMsg:
		if m.commandMode {
			switch msg.Type {
			case tea.KeyEnter:
				raw := m.commandInput.Value()
				m.commandMode = false
				m.commandInput.Reset()
				m.commandInput.Blur()

				trimmed := strings.TrimSpace(strings.TrimPrefix(raw, ":"))
				if strings.HasPrefix(trimmed, "connect ") || trimmed == "connect" {
					args := strings.Fields(strings.TrimPrefix(trimmed, "connect"))
					if len(args) == 0 {
						m.SetStatus("Usage: :connect <endpoint_url> [optional_token]", true)
						return m, nil
					}
					addr := args[0]
					token := ""
					if len(args) > 1 {
						token = args[1]
					} else {
						token = m.token
					}

					probeRes, err := endpointProber(addr, token)
					if err != nil {
						m.SetStatus(fmt.Sprintf("Failed to connect to %s: %v", addr, err), true)
						return m, nil
					}

					m.endpoint = probeRes.URL
					if token != "" {
						m.token = token
					}
					if probeRes.LeaderID != "" {
						m.leaderID = probeRes.LeaderID
					}
					if probeRes.ClusterNodes > 0 {
						m.nodeCount = probeRes.ClusterNodes
					}
					m.isCluster = true
					if cli, err := client.NewHTTPClient(probeRes.URL, token); err == nil {
						if m.client != nil {
							_ = m.client.Close()
						}
						m.client = cli
						cmds = append(cmds, m.circuitsView.SetClient(m.client))
						cmds = append(cmds, m.cacheView.SetClient(m.client))
					}
					m.SetStatus(fmt.Sprintf("Connected to %s (Cluster Mode)", probeRes.URL), false)
					cmds = append(cmds, m.WindowTitleCmd())
					return m, tea.Batch(cmds...)
				}
				if strings.HasPrefix(trimmed, "token ") || trimmed == "token" {
					tok := strings.TrimSpace(strings.TrimPrefix(trimmed, "token"))
					if tok == "" {
						m.SetStatus("Usage: :token <secret>", true)
					} else {
						m.token = tok
						m.SetStatus("Authentication token updated.", false)
					}
					return m, nil
				}
				if trimmed == "disconnect" {
					m.isCluster = false
					m.endpoint = "standalone (local memory)"
					if m.client != nil {
						_ = m.client.Close()
					}
					m.client, _ = client.NewLocalClient()
					cmds = append(cmds, m.circuitsView.SetClient(m.client))
					cmds = append(cmds, m.cacheView.SetClient(m.client))
					m.SetView(ViewCircuits)
					m.SetStatus("Disconnected. Switched to Standalone Mode", false)
					cmds = append(cmds, m.WindowTitleCmd())
					return m, tea.Batch(cmds...)
				}

				if target, ok := ParseCommand(raw); ok {
					m.SetView(target)
					m.SetStatus(fmt.Sprintf("Switched to %s view", ViewByID(target).Title), false)
				} else if strings.TrimSpace(raw) == "q" || strings.TrimSpace(raw) == "quit" {
					return m, tea.Quit
				} else if raw != "" {
					m.SetStatus(fmt.Sprintf("Unknown command: %q (try :circuits, :eval, :autopilot, :alerts, :connect <addr>, :disconnect)", raw), true)
				}
				return m, nil

			case tea.KeyEsc:
				m.commandMode = false
				m.commandInput.Reset()
				m.commandInput.Blur()
				return m, nil

			default:
				var cmd tea.Cmd
				m.commandInput, cmd = m.commandInput.Update(msg)
				return m, cmd
			}
		}

		// Active View-Specific Key Handlers
		if m.currentView == ViewCircuits {
			if m.circuitsView.IsModalActive() {
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				if status := m.circuitsView.StatusMsg(); status != "" {
					m.SetStatus(status, false)
				}
				return m, cmd
			}
			if m.circuitsView.IsFiltering() {
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				return m, cmd
			}
			if m.circuitsView.IsDetail() {
				if key.Matches(msg, m.keys.Command) {
					m.commandMode = true
					m.commandInput.Focus()
					return m, textinput.Blink
				}
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				if status := m.circuitsView.StatusMsg(); status != "" {
					m.SetStatus(status, false)
				}
				return m, cmd
			}
			switch msg.String() {
			case "l", "r", "e", "d", "/":
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				if status := m.circuitsView.StatusMsg(); status != "" {
					m.SetStatus(status, false)
				}
				return m, cmd
			}
			if key.Matches(msg, m.keys.Enter) {
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				return m, cmd
			}
			if key.Matches(msg, m.keys.Up) || key.Matches(msg, m.keys.Down) {
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				return m, cmd
			}
			if key.Matches(msg, m.keys.Escape) {
				var cmd tea.Cmd
				m.circuitsView, cmd = m.circuitsView.Update(msg)
				return m, cmd
			}
		}

		if m.currentView == ViewCache {
			if m.cacheView.IsFiltering() {
				var cmd tea.Cmd
				m.cacheView, cmd = m.cacheView.Update(msg)
				return m, cmd
			}
			if key.Matches(msg, m.keys.Command) {
				m.commandMode = true
				m.commandInput.Focus()
				return m, textinput.Blink
			}
			switch msg.String() {
			case "/", "v", "r", "tab":
				var cmd tea.Cmd
				m.cacheView, cmd = m.cacheView.Update(msg)
				if status := m.cacheView.StatusMsg(); status != "" {
					m.SetStatus(status, false)
				}
				return m, cmd
			}
			if key.Matches(msg, m.keys.Enter) || key.Matches(msg, m.keys.Escape) ||
				key.Matches(msg, m.keys.Up) || key.Matches(msg, m.keys.Down) ||
				msg.String() == "k" || msg.String() == "j" ||
				msg.String() == "pgup" || msg.String() == "pgdown" ||
				msg.String() == "u" || msg.String() == "ctrl+d" {
				var cmd tea.Cmd
				m.cacheView, cmd = m.cacheView.Update(msg)
				if status := m.cacheView.StatusMsg(); status != "" {
					m.SetStatus(status, false)
				}
				return m, cmd
			}
		}

		// Normal Mode Key Handlers
		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit

		case key.Matches(msg, m.keys.Command):
			m.commandMode = true
			m.commandInput.Focus()
			return m, textinput.Blink

		case key.Matches(msg, m.keys.Help):
			if m.currentView == ViewHelp {
				m.SetView(m.previousView)
			} else {
				m.SetView(ViewHelp)
			}
			return m, nil

		case key.Matches(msg, m.keys.Escape):
			if m.currentView == ViewHelp {
				m.SetView(m.previousView)
			}

		case key.Matches(msg, m.keys.NextTab):
			views := m.VisibleViews()
			idx := 0
			for i, v := range views {
				if v.ID == m.currentView {
					idx = i
					break
				}
			}
			next := (idx + 1) % len(views)
			m.SetView(views[next].ID)
			return m, nil

		case key.Matches(msg, m.keys.PrevTab):
			views := m.VisibleViews()
			idx := 0
			for i, v := range views {
				if v.ID == m.currentView {
					idx = i
					break
				}
			}
			prev := (idx - 1 + len(views)) % len(views)
			m.SetView(views[prev].ID)
			return m, nil

		case key.Matches(msg, m.keys.Tab1):
			views := m.VisibleViews()
			if len(views) > 0 {
				m.SetView(views[0].ID)
			}
		case key.Matches(msg, m.keys.Tab2):
			views := m.VisibleViews()
			if len(views) > 1 {
				m.SetView(views[1].ID)
			}
		case key.Matches(msg, m.keys.Tab3):
			views := m.VisibleViews()
			if len(views) > 2 {
				m.SetView(views[2].ID)
			}
		case key.Matches(msg, m.keys.Tab4):
			views := m.VisibleViews()
			if len(views) > 3 {
				m.SetView(views[3].ID)
			}
		case key.Matches(msg, m.keys.Tab5):
			views := m.VisibleViews()
			if len(views) > 4 {
				m.SetView(views[4].ID)
			}
		case key.Matches(msg, m.keys.Tab6):
			views := m.VisibleViews()
			if len(views) > 5 {
				m.SetView(views[5].ID)
			}

		case key.Matches(msg, m.keys.Refresh):
			m.SetStatus("View refreshed.", false)

		case key.Matches(msg, m.keys.Sync):
			if !m.isCluster {
				m.SetStatus("Local sync is only active in Cluster Mode.", false)
			} else {
				m.syncEnabled = !m.syncEnabled
				if m.syncEnabled {
					m.SetStatus("Local snapshot sync ENABLED.", false)
				} else {
					m.SetStatus("Local snapshot sync DISABLED.", false)
				}
			}
		}
	}

	return m, tea.Batch(cmds...)
}

// View renders the entire shell screen.
func (m Model) View() string {
	if !m.ready {
		return "Initializing Flux Shell..."
	}

	var sb strings.Builder

	// 1. Header Bar
	sb.WriteString(m.renderHeader())
	sb.WriteString("\n")

	// 2. Tab Navigation Bar
	sb.WriteString(m.renderTabs())
	sb.WriteString("\n")

	// 3. Main Viewport Content
	contentHeight := m.height - 5 // Header(1) + Tabs(2) + Footer(2)
	if contentHeight < 6 {
		contentHeight = 6
	}
	sb.WriteString(m.renderContent(contentHeight))
	sb.WriteString("\n")

	// 4. Command / Footer Bar
	sb.WriteString(m.renderFooter())

	return sb.String()
}

func (m Model) renderHeader() string {
	brand := StyleBrand.Render(" ⚡ FLUX ") + StyleHeaderItemVal.Render("v"+m.version)
	clock := m.currentTime.Format("15:04:05 MST")

	var left string
	if !m.isCluster {
		left = lipgloss.JoinHorizontal(lipgloss.Center,
			brand,
			"  │  ", StyleHeaderItemVal.Render("Mode: STANDALONE (Local Memory)"),
		)
	} else {
		clusterInfo := fmt.Sprintf("Cluster: %d Nodes", m.nodeCount)
		leaderInfo := fmt.Sprintf("Leader: %s", m.leaderID)
		endpointInfo := fmt.Sprintf("Remote: %s", m.endpoint)

		syncInfo := "Local Sync: OFF"
		if m.syncEnabled {
			syncInfo = fmt.Sprintf("Local Sync: ON (%d keys)", m.syncedKeys)
		}

		left = lipgloss.JoinHorizontal(lipgloss.Center,
			brand,
			"  │  ", StyleHeaderItem.Render(clusterInfo),
			"  │  ", StyleHeaderLeader.Render(leaderInfo),
			"  │  ", StyleHeaderItem.Render(endpointInfo),
			"  │  ", StyleHeaderSynced.Render(syncInfo),
		)
	}

	gap := m.width - lipgloss.Width(left) - lipgloss.Width(clock) - 2
	if gap < 2 {
		gap = 2
	}

	bar := left + strings.Repeat(" ", gap) + StyleHeaderItem.Render(clock)
	return StyleHeader.Width(m.width).Render(bar)
}

func (m Model) renderTabs() string {
	var tabs []string
	views := m.VisibleViews()
	for _, v := range views {
		label := fmt.Sprintf(" %s:%s ", v.Key, v.Title)
		if v.ID == m.currentView {
			tabs = append(tabs, StyleTabActive.Render(label))
		} else {
			tabs = append(tabs, StyleTabInactive.Render(label))
		}
	}
	return StyleTabBar.Width(m.width).Render(lipgloss.JoinHorizontal(lipgloss.Top, tabs...))
}

func (m Model) renderFooter() string {
	if m.commandMode {
		return StyleCommandBar.Width(m.width).Render(m.commandInput.View())
	}

	left := ""
	if m.statusMsg != "" {
		if m.statusIsError {
			left = StyleStatusError.Render("✗ " + m.statusMsg)
		} else {
			left = StyleStatusMsg.Render("✓ " + m.statusMsg)
		}
	} else if m.currentView == ViewCircuits && m.circuitsView.IsModalActive() {
		left = StyleStatusMsg.Render("Modal active. Press Esc/q to dismiss or follow on-screen prompt.")
	} else if m.currentView == ViewCircuits && m.circuitsView.IsFiltering() {
		left = StyleStatusMsg.Render("Filter mode active. Type search query, Enter to accept, Esc to cancel.")
	} else if m.currentView == ViewCircuits && m.circuitsView.IsDetail() {
		left = StyleKeyHelp.Render("Press ") + StyleKey.Render("Esc/q/Enter") + StyleKeyHelp.Render(" back to list, ") +
			StyleKey.Render("Tab") + StyleKeyHelp.Render(" switch panel, ") +
			StyleKey.Render("↑/↓/j/k") + StyleKeyHelp.Render(" scroll, ") +
			StyleKey.Render(":") + StyleKeyHelp.Render(" commands")
	} else if m.currentView == ViewCache && m.cacheView.IsFiltering() {
		left = StyleStatusMsg.Render("Filter mode active. Type search query, Enter to accept, Esc to cancel.")
	} else if m.currentView == ViewCache {
		left = StyleKeyHelp.Render("Press ") + StyleKey.Render("Tab") + StyleKeyHelp.Render(" switch panel, ") +
			StyleKey.Render("Enter") + StyleKeyHelp.Render(" inspect, ") +
			StyleKey.Render("v") + StyleKeyHelp.Render(" raw/formatted, ") +
			StyleKey.Render("/") + StyleKeyHelp.Render(" filter, ") +
			StyleKey.Render("r") + StyleKeyHelp.Render(" refresh, ") +
			StyleKey.Render(":") + StyleKeyHelp.Render(" commands")
	} else {
		left = StyleKeyHelp.Render("Press ") + StyleKey.Render(":") + StyleKeyHelp.Render(" for commands, ") +
			StyleKey.Render("Tab") + StyleKeyHelp.Render(" switch view, ") +
			StyleKey.Render("s") + StyleKeyHelp.Render(" toggle sync, ") +
			StyleKey.Render("?") + StyleKeyHelp.Render(" help, ") +
			StyleKey.Render("q") + StyleKeyHelp.Render(" quit")
	}

	right := StyleKeyHelp.Render(fmt.Sprintf("[%s]", ViewByID(m.currentView).Title))
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(right) - 2
	if gap < 2 {
		gap = 2
	}

	bar := left + strings.Repeat(" ", gap) + right
	return StyleCommandBar.Width(m.width).Render(bar)
}

func (m Model) renderContent(height int) string {
	panelStyle := StylePanel.Width(m.width - 4).Height(height)

	switch m.currentView {
	case ViewCircuits:
		if m.circuitsView.IsDetail() || m.circuitsView.IsModalActive() {
			return StyleViewport.Render(m.circuitsView.View())
		}
		return panelStyle.Render(m.circuitsView.View())

	case ViewAutopilot:
		return panelStyle.Render(m.placeholderPanel(
			"🤖 Autopilot Live Cockpit (:autopilot)",
			"Autopilot session status: IDLE",
			[]string{
				"• Tracks sampling phase, candidate DAG synthesis, and reflection iterations.",
				"• Live shadow mode safety gauge (evaluating error coverage & firing rates).",
				"• Press 'v' to view candidate DAG diffs; 'a' to inspect raw LLM prompts & logs.",
			},
		))

	case ViewCache:
		return StyleViewport.Render(m.cacheView.View())

	case ViewEval:
		return panelStyle.Render(m.placeholderPanel(
			"🧪 Dry-Run Playground & REPL (:eval)",
			"Ready for offline simulation.",
			[]string{
				"• Interactive split-screen playground for testing JSON payloads against DAGs.",
				"• Press 'e' to load the latest real exemplar captured by Schema Tap.",
				"• Color-coded branch execution: Green=taken, Gray=pruned, Cyan=VoltScript mutations.",
			},
		))

	case ViewAlerts:
		return panelStyle.Render(m.placeholderPanel(
			"📟 Live Alert Stream & Feedback Monitor (:alerts)",
			"No alerts recorded yet.",
			[]string{
				"• Real-time streaming feed of all alerts dispatched across circuits and sinks.",
				"• Press 'f' to open Operator Feedback Modal: tag false_positive or true_positive.",
				"• Operator feedback immediately drives closed-loop autonomous refinement.",
			},
		))

	case ViewCluster:
		return panelStyle.Render(m.placeholderPanel(
			"🌐 Cluster Topology & Leader Election (:cluster)",
			fmt.Sprintf("Active nodes: %d | Current Leader: %s", m.nodeCount, m.leaderID),
			[]string{
				"• Real-time node membership heartbeat table and lease expiration countdowns.",
				"• Conflict-free leader election visualization based on lexicographical node ID.",
				"• Failover logs and heartbeat health monitors.",
			},
		))

	case ViewHelp:
		return panelStyle.Render(m.renderHelpView())

	default:
		return panelStyle.Render("Unknown view selected.")
	}
}

func (m Model) placeholderPanel(title, subtitle string, bullets []string) string {
	var sb strings.Builder
	sb.WriteString(StylePanelTitle.Render(title))
	sb.WriteString("\n")
	sb.WriteString(StyleHeaderItemVal.Render(subtitle))
	sb.WriteString("\n\n")

	for _, b := range bullets {
		sb.WriteString(StyleHeaderItem.Render(b))
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(StyleKeyHelp.Render("Press ':' then type view name (e.g. :circuits, :eval, :autopilot, :alerts) or use Tab/1-6 keys."))
	return sb.String()
}

func (m Model) renderHelpView() string {
	var sb strings.Builder
	sb.WriteString(StylePanelTitle.Render("📖 Flux Interactive Shell — Keyboard Shortcuts & Commands"))
	sb.WriteString("\n\n")

	items := [][]string{
		{"Navigation", ""},
		{"Tab", "Switch to next view"},
		{"Shift+Tab", "Switch to previous view"},
		{"1 .. 6", "Directly jump to view by index"},
		{":<command>", "Vim-style command launcher (:circuits, :eval, :autopilot, :alerts, :connect, :disconnect)"},
		{"?", "Toggle this help view"},
		{"q / Ctrl+C", "Quit Flux shell"},
		{"", ""},
		{"Cluster Actions", ""},
		{":connect <addr> [tok]", "Connect to remote Flux control plane (e.g. localhost:9090 [token])"},
		{":token <secret>", "Set or update authentication token for connection"},
		{":disconnect", "Disconnect from remote cluster and switch to Standalone mode"},
		{"s", "Toggle opt-in local snapshot synchronization (Cluster mode)"},
		{"r", "Force refresh active view data"},
		{"Enter", "Select / drill into item details"},
		{"Esc", "Close overlay / return to previous view"},
	}

	for _, it := range items {
		if it[1] == "" && it[0] != "" {
			sb.WriteString(lipgloss.NewStyle().Foreground(ColorWarning).Bold(true).Render(it[0]))
			sb.WriteString("\n")
		} else if it[0] != "" {
			keyCol := lipgloss.NewStyle().Foreground(ColorPrimary).Bold(true).Width(22).Render(it[0])
			descCol := StyleHeaderItem.Render(it[1])
			sb.WriteString(fmt.Sprintf("  %s %s\n", keyCol, descCol))
		} else {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// AdminProbeResult contains metadata discovered during connection handshake.
type AdminProbeResult struct {
	URL          string
	Service      string `json:"service"`
	Version      string `json:"version"`
	NodeID       string `json:"node_id"`
	LeaderID     string `json:"leader_id"`
	ClusterNodes int    `json:"cluster_nodes"`
}

var endpointProber = parseAndProbeEndpoint

func parseAndProbeEndpoint(rawAddr string, token string) (*AdminProbeResult, error) {
	raw := strings.TrimSpace(rawAddr)
	if raw == "" {
		return nil, fmt.Errorf("empty endpoint address")
	}

	// 1. Detect explicit schemes and validate
	var targetURL string
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("unsupported scheme %q (Flux Admin API requires http:// or https://)", u.Scheme)
		}
		targetURL = strings.TrimRight(raw, "/")
	} else {
		// No scheme provided.
		// If explicit port :443, infer https://, otherwise default to http://
		scheme := "http"
		if strings.HasSuffix(raw, ":443") {
			scheme = "https"
		}

		// If no port is specified and host is localhost or an IP, default to :9090
		// For domain names (e.g. flux.mycompany.com), standard port applies (80 or 443)
		host := raw
		if !strings.Contains(raw, ":") {
			if raw == "localhost" || net.ParseIP(raw) != nil {
				host = raw + ":9090"
			}
		}
		targetURL = fmt.Sprintf("%s://%s", scheme, host)
	}

	// 2. Perform HTTP probe with 1.5s timeout targeting /api/v1/status
	client := &http.Client{
		Timeout: 1500 * time.Millisecond,
	}

	req, err := http.NewRequest("GET", targetURL+"/api/v1/status", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create probe request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("endpoint unreachable: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

	// Check authentication rejection
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		if resp.Header.Get("X-Flux-Admin") == "true" || strings.Contains(string(bodyBytes), "flux") {
			return nil, fmt.Errorf("authentication required (token invalid or missing). Use :connect <addr> <token>")
		}
		return nil, fmt.Errorf("target server returned HTTP %d (not a Flux Admin endpoint)", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("target server is not a Flux Admin endpoint (HTTP %d from /api/v1/status)", resp.StatusCode)
	}

	// Verify Flux identity signature in body or headers
	var result AdminProbeResult
	result.URL = targetURL
	isJSON := strings.Contains(resp.Header.Get("Content-Type"), "application/json") || (len(bodyBytes) > 0 && bodyBytes[0] == '{')
	if isJSON {
		_ = json.Unmarshal(bodyBytes, &result)
	}

	hasHeaderSig := resp.Header.Get("X-Flux-Admin") == "true" || resp.Header.Get("X-Flux-Engine") != ""
	hasServiceSig := result.Service == "flux-admin" || strings.Contains(strings.ToLower(string(bodyBytes)), "flux-admin")

	if !hasHeaderSig && !hasServiceSig {
		return nil, fmt.Errorf("target server is not a Flux Admin control plane (missing Flux service signature)")
	}

	return &result, nil
}
