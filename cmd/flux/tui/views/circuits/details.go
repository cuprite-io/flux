package circuits

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/types"
)

var (
	styleDetailTitle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleDetailMetaLabel = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#A0A0C0")).
				Width(14)

	styleDetailMetaVal = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Bold(true)

	styleDetailStatusLive = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E676")).
				Bold(true)

	styleDetailStatusShadow = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleDetailTag = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Background(lipgloss.Color("#1E2A38")).
			Padding(0, 1)

	styleDetailSection = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#7C4DFF")).
				Bold(true)

	styleDetailNodeBox = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#3A3A55")).
				Padding(0, 1).
				Margin(0, 0, 1, 0)

	styleDetailStepNum = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleDetailCodeBlock = lipgloss.NewStyle().
				Background(lipgloss.Color("#161622")).
				Foreground(lipgloss.Color("#F0F0FF")).
				Padding(0, 1).
				Margin(0, 0, 1, 0)
)

// RenderCircuitDetails formats a comprehensive specification report of a circuit.
func RenderCircuitDetails(summary *client.CircuitSummary, width int) string {
	if summary == nil {
		return styleTreeStepText.Render("(no circuit selected)")
	}

	var sb strings.Builder

	// 1. Title Banner
	sb.WriteString(styleDetailTitle.Render("📋 CIRCUIT SPECIFICATION: " + summary.ID))
	sb.WriteString("\n\n")

	// 2. Metadata Grid
	sb.WriteString(renderMetadataGrid(summary))
	sb.WriteString("\n\n")

	// 3. Root Guard Condition
	if summary.RootCondition != "" {
		sb.WriteString(styleDetailSection.Render("⚡ ROOT GUARD EXPRESSION (Volt / CEL):"))
		sb.WriteString("\n")
		highlightedRoot := HighlightCode(summary.RootCondition)
		sb.WriteString(styleDetailCodeBlock.Render("  " + highlightedRoot))
		sb.WriteString("\n\n")
	}

	// 4. Execution Step Grammar Breakdown
	if summary.Definition != nil && summary.Definition.Root != nil {
		sb.WriteString(styleDetailSection.Render("🔄 EXECUTION PIPELINE STEPS:"))
		sb.WriteString("\n")

		visited := make(map[*types.Node]bool)
		stepBlocks := renderNodeStepGrammar(summary.Definition.Root, 1, visited)
		for _, b := range stepBlocks {
			sb.WriteString(b)
			sb.WriteString("\n")
		}
	} else {
		sb.WriteString(styleDetailSection.Render("🔄 EXECUTION PIPELINE:"))
		sb.WriteString("\n")
		sb.WriteString(styleTreeStepText.Render("  (Detailed node grammar unavailable or empty)"))
		sb.WriteString("\n")
	}

	return sb.String()
}

func renderMetadataGrid(s *client.CircuitSummary) string {
	var sb strings.Builder

	// Row 1: ID & Status
	statusStyle := styleDetailStatusLive
	if strings.ToUpper(s.Status) == "SHADOW" {
		statusStyle = styleDetailStatusShadow
	}
	sb.WriteString(styleDetailMetaLabel.Render("Circuit ID:"))
	sb.WriteString(styleDetailMetaVal.Render(s.ID))
	sb.WriteString("\n")
	sb.WriteString(styleDetailMetaLabel.Render("Status:"))
	sb.WriteString(statusStyle.Render(s.Status))
	sb.WriteString("\n")
	sb.WriteString(styleDetailMetaLabel.Render("Revision:"))
	sb.WriteString(styleDetailMetaVal.Render(fmt.Sprintf("r%d", s.Revision)))
	sb.WriteString("\n")

	// Row 2: Stream Tags
	if len(s.Tags) > 0 {
		var tagPills []string
		for _, t := range s.Tags {
			tagPills = append(tagPills, styleDetailTag.Render(t))
		}
		sb.WriteString(styleDetailMetaLabel.Render("Stream Tags:"))
		sb.WriteString(strings.Join(tagPills, " "))
		sb.WriteString("\n")
	} else {
		sb.WriteString(styleDetailMetaLabel.Render("Stream Tags:"))
		sb.WriteString(styleTreeStepText.Render("none"))
		sb.WriteString("\n")
	}

	// Row 3: Timestamps & Topology
	sb.WriteString(styleDetailMetaLabel.Render("Created:"))
	sb.WriteString(styleDetailMetaVal.Render(s.CreatedAt.Format("2006-01-02 15:04:05 MST")))
	sb.WriteString("\n")
	sb.WriteString(styleDetailMetaLabel.Render("Total Nodes:"))
	sb.WriteString(styleDetailMetaVal.Render(fmt.Sprintf("%d nodes", s.NodeCount)))
	sb.WriteString("\n")

	// Calculate total steps
	totalSteps := 0
	if s.Definition != nil && s.Definition.Root != nil {
		visited := make(map[*types.Node]bool)
		totalSteps = countTotalSteps(s.Definition.Root, visited)
	}
	sb.WriteString(styleDetailMetaLabel.Render("Total Steps:"))
	sb.WriteString(styleDetailMetaVal.Render(fmt.Sprintf("%d steps", totalSteps)))

	return sb.String()
}

func countTotalSteps(n *types.Node, visited map[*types.Node]bool) int {
	if n == nil || visited[n] {
		return 0
	}
	visited[n] = true
	defer func() { delete(visited, n) }()

	count := len(n.Steps)
	for _, c := range n.Children {
		count += countTotalSteps(c, visited)
	}
	return count
}

func renderNodeStepGrammar(node *types.Node, depth int, visited map[*types.Node]bool) []string {
	if node == nil {
		return nil
	}
	if visited[node] {
		return []string{styleTreeAbort.Render(fmt.Sprintf("  [Cycle Detected on Node %s]", node.Name))}
	}
	visited[node] = true
	defer func() { delete(visited, node) }()

	var blocks []string
	var sb strings.Builder

	nodeName := node.Name
	if nodeName == "" {
		if node.ID != "" {
			nodeName = node.ID
		} else {
			nodeName = "unnamed"
		}
	}

	indent := strings.Repeat("  ", depth-1)
	sb.WriteString(indent)
	sb.WriteString(styleTreeNodeBadge.Render("● Node: "))
	sb.WriteString(styleTreeNodeName.Render(nodeName))
	if node.ID != "" && node.ID != nodeName {
		sb.WriteString(styleTreeStepText.Render(fmt.Sprintf(" (id: %s)", node.ID)))
	}
	sb.WriteString("\n")

	if node.Condition != "" {
		sb.WriteString(indent)
		sb.WriteString("  ")
		sb.WriteString(styleTreeGuardBadge.Render("Guard Condition: "))
		sb.WriteString(HighlightCode(node.Condition))
		sb.WriteString("\n")
	}

	if len(node.Steps) == 0 {
		sb.WriteString(indent)
		sb.WriteString("  ")
		sb.WriteString(styleTreeStepText.Render("(no execution steps in this node)"))
		sb.WriteString("\n")
	} else {
		for i, s := range node.Steps {
			stepNum := fmt.Sprintf("Step %d", i+1)
			sb.WriteString(indent)
			sb.WriteString("  ")
			sb.WriteString(styleDetailStepNum.Render(stepNum))
			sb.WriteString(": ")

			switch s.Type {
			case types.StepVolt:
				sb.WriteString(styleTreeVolt.Render("[VOLT EXPRESSION]"))
				sb.WriteString("\n")
				sb.WriteString(indent)
				sb.WriteString("    ")
				sb.WriteString(HighlightCode(s.Script))
				sb.WriteString("\n")

			case types.StepSink:
				sb.WriteString(styleTreeSink.Render("[EXTERNAL SINK]"))
				sb.WriteString("\n")
				sb.WriteString(indent)
				sb.WriteString("    Target: ")
				sb.WriteString(styleTreeNodeName.Render(s.SinkName))
				if s.SinkType != "" {
					sb.WriteString(styleTreeStepText.Render(" (Driver: " + s.SinkType + ")"))
				}
				sb.WriteString("\n")
				if s.Payload != "" {
					sb.WriteString(indent)
					sb.WriteString("    Payload: ")
					sb.WriteString(HighlightCode(s.Payload))
					sb.WriteString("\n")
				}

			case types.StepReturn:
				sb.WriteString(styleTreeReturn.Render("[RETURN PROJECTION]"))
				sb.WriteString("\n")
				if len(s.ReturnMap) > 0 {
					data, _ := json.Marshal(s.ReturnMap)
					sb.WriteString(indent)
					sb.WriteString("    Output: ")
					sb.WriteString(HighlightCode(string(data)))
					sb.WriteString("\n")
				} else {
					sb.WriteString(indent)
					sb.WriteString("    Output: ")
					sb.WriteString(styleTreeStepText.Render("identity state projection"))
					sb.WriteString("\n")
				}

			case types.StepAbort:
				sb.WriteString(styleTreeAbort.Render("[ABORT / HALT]"))
				sb.WriteString("\n")
				if s.Condition != "" {
					sb.WriteString(indent)
					sb.WriteString("    Condition: ")
					sb.WriteString(HighlightCode(s.Condition))
					sb.WriteString("\n")
				}

			case types.StepVM:
				sb.WriteString(styleTreeVM.Render("[VM BYTECODE]"))
				sb.WriteString("\n")
				sb.WriteString(indent)
				sb.WriteString("    Native bytecode execution\n")

			default:
				sb.WriteString(styleTreeStepText.Render("[UNKNOWN STEP]"))
				sb.WriteString("\n")
			}

			if s.Condition != "" && s.Type != types.StepAbort {
				sb.WriteString(indent)
				sb.WriteString("    ")
				sb.WriteString(styleTreeGuardBadge.Render("Step Guard: "))
				sb.WriteString(HighlightCode(s.Condition))
				sb.WriteString("\n")
			}
		}
	}

	blocks = append(blocks, sb.String())

	// Recurse child nodes
	for _, child := range node.Children {
		childBlocks := renderNodeStepGrammar(child, depth+1, visited)
		blocks = append(blocks, childBlocks...)
	}

	return blocks
}

// Token highlighting styles
var (
	styleKw  = lipgloss.NewStyle().Foreground(lipgloss.Color("#D183FF")).Bold(true) // Purple / Magenta
	styleId  = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true) // Cyan
	styleStr = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676"))            // Emerald Green
	styleNum = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600"))            // Yellow / Amber
	styleOp  = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFA726")).Bold(true) // Orange
)

var (
	reVoltString = regexp.MustCompile(`"([^"\\]|\\.)*"`)
	reVoltNumber = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	reVoltOp     = regexp.MustCompile(`(&&|\|\||==|!=|>=|<=|>|<|\+|-|\*|/|!)`)
	reVoltKw     = regexp.MustCompile(`\b(if|else|return|let|true|false|null|nil|var|in)\b`)
	reVoltRoot   = regexp.MustCompile(`\b(payload|state|context|meta)\b`)
)

// HighlightCode applies syntax color highlighting to VoltScript and CEL expressions.
func HighlightCode(code string) string {
	if code == "" {
		return ""
	}

	// We format strings, keywords, operators, and roots
	res := code

	// 1. Strings
	res = reVoltString.ReplaceAllStringFunc(res, func(m string) string {
		return styleStr.Render(m)
	})

	// 2. Roots (payload, state, context)
	res = reVoltRoot.ReplaceAllStringFunc(res, func(m string) string {
		return styleId.Render(m)
	})

	// 3. Keywords
	res = reVoltKw.ReplaceAllStringFunc(res, func(m string) string {
		return styleKw.Render(m)
	})

	// 4. Numbers
	res = reVoltNumber.ReplaceAllStringFunc(res, func(m string) string {
		return styleNum.Render(m)
	})

	// 5. Operators
	res = reVoltOp.ReplaceAllStringFunc(res, func(m string) string {
		return styleOp.Render(m)
	})

	return res
}
