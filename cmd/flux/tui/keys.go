package tui

import (
	"github.com/charmbracelet/bubbles/key"
)

// KeyMap defines the global keyboard navigation map.
type KeyMap struct {
	Up       key.Binding
	Down     key.Binding
	Left     key.Binding
	Right    key.Binding
	NextTab  key.Binding
	PrevTab  key.Binding
	Command  key.Binding
	Help     key.Binding
	Quit     key.Binding
	Escape   key.Binding
	Enter    key.Binding
	Refresh  key.Binding
	Sync     key.Binding
	Tab1     key.Binding
	Tab2     key.Binding
	Tab3     key.Binding
	Tab4     key.Binding
	Tab5     key.Binding
	Tab6     key.Binding
}

// DefaultKeyMap returns the default keybindings for the shell.
func DefaultKeyMap() KeyMap {
	return KeyMap{
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "move up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "move down"),
		),
		Left: key.NewBinding(
			key.WithKeys("left", "h"),
			key.WithHelp("←/h", "move left"),
		),
		Right: key.NewBinding(
			key.WithKeys("right", "l"),
			key.WithHelp("→/l", "move right"),
		),
		NextTab: key.NewBinding(
			key.WithKeys("tab", "]"),
			key.WithHelp("tab/]", "next view"),
		),
		PrevTab: key.NewBinding(
			key.WithKeys("shift+tab", "["),
			key.WithHelp("shift+tab/[", "previous view"),
		),
		Command: key.NewBinding(
			key.WithKeys(":"),
			key.WithHelp(":", "command launcher"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "toggle help"),
		),
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", "quit"),
		),
		Escape: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "cancel / close"),
		),
		Enter: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "select / execute"),
		),
		Refresh: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "refresh view"),
		),
		Sync: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "toggle local sync"),
		),
		Tab1: key.NewBinding(key.WithKeys("1"), key.WithHelp("1", "Circuits")),
		Tab2: key.NewBinding(key.WithKeys("2"), key.WithHelp("2", "Autopilot")),
		Tab3: key.NewBinding(key.WithKeys("3"), key.WithHelp("3", "Cache")),
		Tab4: key.NewBinding(key.WithKeys("4"), key.WithHelp("4", "Eval")),
		Tab5: key.NewBinding(key.WithKeys("5"), key.WithHelp("5", "Alerts")),
		Tab6: key.NewBinding(key.WithKeys("6"), key.WithHelp("6", "Cluster")),
	}
}
