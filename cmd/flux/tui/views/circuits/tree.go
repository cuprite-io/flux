package circuits

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/types"
)

var (
	colorTreeCyan   = lipgloss.Color("#00E5FF")
	colorTreePurple = lipgloss.Color("#7C4DFF")
	colorTreeGreen  = lipgloss.Color("#00E676")
	colorTreeYellow = lipgloss.Color("#FFD600")
	colorTreeRed    = lipgloss.Color("#FF1744")
	colorTreeGray   = lipgloss.Color("#6E6E8E")
	colorTreeWhite  = lipgloss.Color("#FFFFFF")
	colorTreeDim    = lipgloss.Color("#A0A0C0")

	styleTreeRoot = lipgloss.NewStyle().
			Foreground(colorTreeCyan).
			Bold(true)

	styleTreeNodeBadge = lipgloss.NewStyle().
				Foreground(colorTreePurple).
				Bold(true)

	styleTreeNodeName = lipgloss.NewStyle().
				Foreground(colorTreeWhite).
				Bold(true)

	styleTreeLine = lipgloss.NewStyle().
			Foreground(colorTreeGray)

	styleTreeGuardBadge = lipgloss.NewStyle().
				Foreground(colorTreeYellow).
				Bold(true)

	styleTreeCondition = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFF59D"))

	styleTreeVolt = lipgloss.NewStyle().
			Foreground(colorTreeGreen).
			Bold(true)

	styleTreeSink = lipgloss.NewStyle().
			Foreground(colorTreePurple).
			Bold(true)

	styleTreeReturn = lipgloss.NewStyle().
			Foreground(colorTreeCyan).
			Bold(true)

	styleTreeAbort = lipgloss.NewStyle().
			Foreground(colorTreeRed).
			Bold(true)

	styleTreeVM = lipgloss.NewStyle().
			Foreground(colorTreeDim).
			Bold(true)

	styleTreeStepText = lipgloss.NewStyle().
				Foreground(colorTreeDim)
)

// RenderDAGTree formats the hierarchical DAG of a circuit into an ASCII/Unicode tree graph.
func RenderDAGTree(summary *client.CircuitSummary, width int) string {
	if summary == nil {
		return styleTreeStepText.Render("(no circuit selected)")
	}

	if summary.Definition == nil || summary.Definition.Root == nil {
		var sb strings.Builder
		sb.WriteString(styleTreeRoot.Render("● [Circuit: " + summary.ID + "]"))
		sb.WriteString("\n\n")
		if summary.RootCondition != "" {
			sb.WriteString(styleTreeGuardBadge.Render("  ? ROOT GUARD: "))
			sb.WriteString(styleTreeCondition.Render(summary.RootCondition))
			sb.WriteString("\n\n")
		}
		sb.WriteString(styleTreeStepText.Render("  (empty DAG or definition loading from engine)"))
		return sb.String()
	}

	var sb strings.Builder
	sb.WriteString(styleTreeRoot.Render("● [Circuit: " + summary.ID + "]"))
	if len(summary.Tags) > 0 {
		sb.WriteString(" ")
		sb.WriteString(styleTreeStepText.Render(fmt.Sprintf("[%s]", strings.Join(summary.Tags, ", "))))
	}
	sb.WriteString("\n")

	visited := make(map[*types.Node]bool)
	lines := renderNode(summary.Definition.Root, "", true, true, visited)
	for _, l := range lines {
		sb.WriteString(l)
		sb.WriteString("\n")
	}

	return sb.String()
}

func renderNode(node *types.Node, prefix string, isLast bool, isRoot bool, visited map[*types.Node]bool) []string {
	if node == nil {
		return nil
	}

	var lines []string

	if visited[node] {
		connector := "├── "
		if isLast {
			connector = "└── "
		}
		lines = append(lines, prefix+styleTreeLine.Render(connector)+styleTreeNodeBadge.Render("● [NODE] ")+styleTreeNodeName.Render(node.Name)+styleTreeAbort.Render(" (cycle detected)"))
		return lines
	}
	visited[node] = true
	defer func() {
		delete(visited, node)
	}()

	nodeName := node.Name
	if nodeName == "" {
		if node.ID != "" {
			nodeName = node.ID
		} else if isRoot {
			nodeName = "root"
		} else {
			nodeName = "anonymous"
		}
	}

	// 1. Render node header line
	if isRoot {
		lines = append(lines, styleTreeLine.Render("│"))
		lines = append(lines, styleTreeLine.Render("├── ")+styleTreeRoot.Render("● [ROOT: ")+styleTreeNodeName.Render(nodeName)+styleTreeRoot.Render("]"))
	} else {
		connector := "├── "
		if isLast {
			connector = "└── "
		}
		lines = append(lines, prefix+styleTreeLine.Render(connector)+styleTreeNodeBadge.Render("● [NODE: ")+styleTreeNodeName.Render(nodeName)+styleTreeNodeBadge.Render("]"))
	}

	// Prefix for child elements of this node
	var childPrefix string
	if isRoot {
		childPrefix = "│   "
	} else if isLast {
		childPrefix = prefix + "    "
	} else {
		childPrefix = prefix + "│   "
	}

	// Calculate total items (guard + steps + children) to format connectors correctly
	hasGuard := node.Condition != ""
	stepCount := len(node.Steps)
	childCount := len(node.Children)

	// 2. Render guard condition
	if hasGuard {
		isFinal := (stepCount == 0 && childCount == 0)
		connector := "├── "
		if isFinal {
			connector = "└── "
		}
		lines = append(lines, childPrefix+styleTreeLine.Render(connector)+styleTreeGuardBadge.Render("? GUARD: ")+styleTreeCondition.Render(node.Condition))
	}

	// 3. Render steps
	for i, s := range node.Steps {
		if s == nil {
			continue
		}
		isFinal := (i == stepCount-1 && childCount == 0)
		connector := "├── "
		if isFinal {
			connector = "└── "
		}

		stepLine := renderStep(s)
		lines = append(lines, childPrefix+styleTreeLine.Render(connector)+stepLine)
	}

	// 4. Render children branches recursively
	for i, child := range node.Children {
		isLastChild := (i == childCount-1)
		childLines := renderNode(child, childPrefix, isLastChild, false, visited)
		lines = append(lines, childLines...)
	}

	return lines
}

func renderStep(s *types.StepDefinition) string {
	var sb strings.Builder

	switch s.Type {
	case types.StepVolt:
		sb.WriteString(styleTreeVolt.Render("⚡ [VOLT] "))
		script := strings.TrimSpace(s.Script)
		if strings.Contains(script, "\n") {
			script = strings.ReplaceAll(script, "\n", " ")
		}
		if len(script) > 60 {
			script = script[:57] + "..."
		}
		sb.WriteString(styleTreeStepText.Render(script))

	case types.StepSink:
		sb.WriteString(styleTreeSink.Render("📡 [SINK] "))
		sb.WriteString(styleTreeNodeName.Render(s.SinkName))
		if s.SinkType != "" {
			sb.WriteString(styleTreeStepText.Render(" (" + s.SinkType + ")"))
		}
		if s.Payload != "" {
			p := s.Payload
			if len(p) > 30 {
				p = p[:27] + "..."
			}
			sb.WriteString(styleTreeStepText.Render(" payload:" + p))
		}

	case types.StepReturn:
		sb.WriteString(styleTreeReturn.Render("↩ [RETURN] "))
		if len(s.ReturnMap) > 0 {
			var keys []string
			for k := range s.ReturnMap {
				keys = append(keys, k)
			}
			sb.WriteString(styleTreeStepText.Render(fmt.Sprintf("{%s}", strings.Join(keys, ", "))))
		} else {
			sb.WriteString(styleTreeStepText.Render("output projection"))
		}

	case types.StepAbort:
		sb.WriteString(styleTreeAbort.Render("⛔ [ABORT]"))
		if s.Condition != "" {
			sb.WriteString(styleTreeStepText.Render(" if " + s.Condition))
		}

	case types.StepVM:
		sb.WriteString(styleTreeVM.Render("⚙ [VM] precompiled bytecode"))

	default:
		sb.WriteString(styleTreeStepText.Render("[STEP] unknown"))
	}

	if s.Condition != "" && s.Type != types.StepAbort {
		sb.WriteString(" ")
		sb.WriteString(styleTreeGuardBadge.Render("? cond: "))
		sb.WriteString(styleTreeCondition.Render(s.Condition))
	}

	return sb.String()
}
