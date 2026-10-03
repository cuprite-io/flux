package tui

import (
	"strings"
)

// ViewID represents one of the major tabs/views in the Flux shell.
type ViewID int

const (
	ViewCircuits ViewID = iota
	ViewAutopilot
	ViewCache
	ViewEval
	ViewAlerts
	ViewCluster
	ViewHelp
)

// ViewMeta holds display metadata for a shell view.
type ViewMeta struct {
	ID       ViewID
	Title    string
	Command  string
	Shortcut string
	Key      string
}

// ClusterViews lists all available views when connected to a remote cluster.
var ClusterViews = []ViewMeta{
	{ID: ViewCircuits, Title: "Circuits", Command: "circuits", Shortcut: "c", Key: "1"},
	{ID: ViewAutopilot, Title: "Autopilot", Command: "autopilot", Shortcut: "auto", Key: "2"},
	{ID: ViewCache, Title: "Cache / State", Command: "cache", Shortcut: "state", Key: "3"},
	{ID: ViewEval, Title: "Dry-Run REPL", Command: "eval", Shortcut: "dryrun", Key: "4"},
	{ID: ViewAlerts, Title: "Live Alerts", Command: "alerts", Shortcut: "al", Key: "5"},
	{ID: ViewCluster, Title: "Cluster Nodes", Command: "cluster", Shortcut: "nodes", Key: "6"},
	{ID: ViewHelp, Title: "Help", Command: "help", Shortcut: "h", Key: "?"},
}

// StandaloneViews lists views available in local/standalone mode (no distributed cache/cluster tabs).
var StandaloneViews = []ViewMeta{
	{ID: ViewCircuits, Title: "Circuits", Command: "circuits", Shortcut: "c", Key: "1"},
	{ID: ViewEval, Title: "Dry-Run REPL", Command: "eval", Shortcut: "dryrun", Key: "2"},
	{ID: ViewAutopilot, Title: "Autopilot", Command: "autopilot", Shortcut: "auto", Key: "3"},
	{ID: ViewAlerts, Title: "Live Alerts", Command: "alerts", Shortcut: "al", Key: "4"},
	{ID: ViewHelp, Title: "Help", Command: "help", Shortcut: "h", Key: "?"},
}

// RegisteredViews is kept for backward compatibility and references ClusterViews.
var RegisteredViews = ClusterViews

// ViewByID returns metadata for the given view ID.
func ViewByID(id ViewID) ViewMeta {
	for _, v := range ClusterViews {
		if v.ID == id {
			return v
		}
	}
	return ClusterViews[0]
}

// ParseCommand parses a vim-style `:` command string into a target ViewID.
func ParseCommand(raw string) (ViewID, bool) {
	cmd := strings.TrimSpace(strings.TrimPrefix(raw, ":"))
	cmd = strings.ToLower(cmd)

	switch cmd {
	case "circuits", "circuit", "c", "1":
		return ViewCircuits, true
	case "autopilot", "auto", "a", "2":
		return ViewAutopilot, true
	case "cache", "state", "s", "3":
		return ViewCache, true
	case "eval", "dryrun", "dry-run", "e", "4":
		return ViewEval, true
	case "alerts", "alert", "al", "5":
		return ViewAlerts, true
	case "cluster", "nodes", "node", "n", "6":
		return ViewCluster, true
	case "help", "h", "?":
		return ViewHelp, true
	default:
		return ViewCircuits, false
	}
}
