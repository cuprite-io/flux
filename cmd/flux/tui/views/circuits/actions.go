package circuits

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/types"
	"gopkg.in/yaml.v3"
)

// DryRunHandoffMsg requests top-level model to transfer circuit to the Dry-Run REPL.
type DryRunHandoffMsg struct {
	CircuitID string
	Circuit   *types.Circuit
}

// ActionModalType defines the kind of interactive action prompt active.
type ActionModalType int

const (
	ActionModalNone ActionModalType = iota
	ActionModalHotSwap
	ActionModalExport
)

// ActionState manages in-shell modals for Hot-Swap and Export prompts.
type ActionState struct {
	modalType ActionModalType
	input     textinput.Model
	target    *client.CircuitSummary
	statusMsg string
	isError   bool
	width     int
}

// NewActionState creates an action state manager.
func NewActionState() ActionState {
	ti := textinput.New()
	ti.PromptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true)
	ti.TextStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF"))
	ti.CharLimit = 256

	return ActionState{
		modalType: ActionModalNone,
		input:     ti,
	}
}

// IsActive returns true if an action input modal is open.
func (a ActionState) IsActive() bool {
	return a.modalType != ActionModalNone
}

// ModalType returns the currently active modal type.
func (a ActionState) ModalType() ActionModalType {
	return a.modalType
}

// OpenHotSwap opens the Hot-Swap file input prompt.
func (a *ActionState) OpenHotSwap(target *client.CircuitSummary, width int) tea.Cmd {
	a.modalType = ActionModalHotSwap
	a.target = target
	a.width = width
	a.statusMsg = ""
	a.isError = false
	a.input.Reset()
	a.input.Prompt = "Hot-Swap File (Enter to refresh, or enter .json/.yaml path): "
	a.input.Focus()
	return textinput.Blink
}

// OpenExport opens the Export file destination prompt.
func (a *ActionState) OpenExport(target *client.CircuitSummary, width int) tea.Cmd {
	a.modalType = ActionModalExport
	a.target = target
	a.width = width
	a.statusMsg = ""
	a.isError = false
	a.input.Reset()
	defaultPath := ""
	if target != nil {
		defaultPath = fmt.Sprintf("./%s.json", target.ID)
	}
	a.input.Prompt = "Export Destination (.json / .yaml): "
	a.input.SetValue(defaultPath)
	a.input.Focus()
	return textinput.Blink
}

// Close dismisses any active action modal.
func (a *ActionState) Close() {
	a.modalType = ActionModalNone
	a.target = nil
	a.input.Reset()
	a.input.Blur()
}

// LoadCircuitFromDisk reads and decodes a Circuit definition from a JSON or YAML file.
func LoadCircuitFromDisk(path string) (*types.Circuit, error) {
	cleanPath := filepath.Clean(strings.TrimSpace(path))
	if cleanPath == "" {
		return nil, errors.New("file path required")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", cleanPath, err)
	}

	var circuit types.Circuit
	ext := strings.ToLower(filepath.Ext(cleanPath))
	if ext == ".yaml" || ext == ".yml" {
		if err := yaml.Unmarshal(data, &circuit); err != nil {
			return nil, fmt.Errorf("failed to parse YAML circuit: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &circuit); err != nil {
			return nil, fmt.Errorf("failed to parse JSON circuit: %w", err)
		}
	}

	if strings.TrimSpace(circuit.ID) == "" {
		return nil, errors.New("circuit missing mandatory 'id' field")
	}
	if circuit.Root == nil {
		return nil, errors.New("circuit missing mandatory 'root' execution node")
	}

	return &circuit, nil
}

// ExportCircuit writes a circuit definition to a local file in JSON or YAML.
func ExportCircuit(target *client.CircuitSummary, destPath string, asYAML bool) error {
	if target == nil || target.Definition == nil {
		return errors.New("no circuit definition available to export")
	}

	cleanPath := filepath.Clean(strings.TrimSpace(destPath))
	if cleanPath == "" {
		return errors.New("destination file path required")
	}

	// Create directory if missing
	dir := filepath.Dir(cleanPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	var data []byte
	var err error

	ext := strings.ToLower(filepath.Ext(cleanPath))
	if asYAML || ext == ".yaml" || ext == ".yml" {
		data, err = yaml.Marshal(target.Definition)
		if err != nil {
			return fmt.Errorf("failed to encode YAML: %w", err)
		}
	} else {
		data, err = json.MarshalIndent(target.Definition, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to encode JSON: %w", err)
		}
	}

	if err := os.WriteFile(cleanPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write export file %s: %w", cleanPath, err)
	}

	return nil
}

// View renders the active action modal input bar.
func (a ActionState) View() string {
	if a.modalType == ActionModalNone {
		return ""
	}

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#00E5FF")).
		Background(lipgloss.Color("#1A1A28")).
		Padding(0, 1)

	var sb strings.Builder
	switch a.modalType {
	case ActionModalHotSwap:
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true).Render("🔄 Hot-Swap Circuit Pipeline"))
		sb.WriteString("\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#A0A0C0")).Render("Specify local file to atomically replace in-engine circuit with zero downtime:"))
		sb.WriteString("\n\n")
		sb.WriteString(a.input.View())
		sb.WriteString("\n\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("Enter: apply hot-swap (or refresh if blank)  │  Esc: cancel"))

	case ActionModalExport:
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true).Render("💾 Export Circuit Definition"))
		sb.WriteString("\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#A0A0C0")).Render("Save live compiled pipeline to disk as formatted JSON or YAML:"))
		sb.WriteString("\n\n")
		sb.WriteString(a.input.View())
		sb.WriteString("\n\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("Enter: save to disk  │  Esc: cancel"))
	}

	boxW := a.width - 6
	if boxW < 40 {
		boxW = 40
	}
	return borderStyle.Width(boxW).Render(sb.String())
}
