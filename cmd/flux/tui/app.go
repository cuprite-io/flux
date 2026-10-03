package tui

import (
	"fmt"
	"io"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

// ANSI escape sequences for terminal title stack operations (ECMA-48 / XTerm standard).
// CSI 22 ; 0 t -> Push current window and icon title onto the title stack.
// CSI 23 ; 0 t -> Pop and restore window and icon title from the title stack.
const (
	SaveTitleSeq    = "\x1b[22;0t"
	RestoreTitleSeq = "\x1b[23;0t"
)

// SaveWindowTitle saves the terminal window and icon title onto the terminal title stack.
func SaveWindowTitle(w io.Writer) {
	if w != nil {
		_, _ = fmt.Fprint(w, SaveTitleSeq)
	}
}

// RestoreWindowTitle restores the previous terminal window and icon title from the terminal title stack.
func RestoreWindowTitle(w io.Writer) {
	if w != nil {
		_, _ = fmt.Fprint(w, RestoreTitleSeq)
	}
}

// Config configures the interactive shell session.
type Config struct {
	Endpoint string // Flux Admin control plane endpoint (e.g. "localhost:9090"). Omit for standalone mode.
	Token    string // Optional Bearer authentication token
	NodeID   string // Optional custom node identifier
}

// Run boots and executes the interactive Flux terminal shell.
func Run(cfg Config) error {
	// Save current window title before entering TUI
	SaveWindowTitle(os.Stdout)
	// Restore original window title upon exit
	defer RestoreWindowTitle(os.Stdout)

	m := NewModel(cfg)

	p := tea.NewProgram(
		m,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("flux shell runtime error: %w", err)
	}

	_ = finalModel
	return nil
}
