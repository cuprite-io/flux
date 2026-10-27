package eval

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/types"
)

var (
	colorGreen  = lipgloss.Color("#00E676") // Conditions true / Taken branches
	colorGray   = lipgloss.Color("#6E6E8E") // Pruned / Dead branches
	colorCyan   = lipgloss.Color("#00E5FF") // VoltScript variable mutations
	colorYellow = lipgloss.Color("#FFD600") // Triggered sinks / Sinks active
	colorRed    = lipgloss.Color("#FF1744") // Errors / Fails
	colorWhite  = lipgloss.Color("#FFFFFF") // Primary text
	colorDim    = lipgloss.Color("#A0A0C0") // Secondary labels

	styleStatusPassed = lipgloss.NewStyle().
				Foreground(colorGreen).
				Bold(true)

	styleStatusPruned = lipgloss.NewStyle().
				Foreground(colorGray)

	styleStatusFailed = lipgloss.NewStyle().
				Foreground(colorRed).
				Bold(true)

	styleBadgeTaken = lipgloss.NewStyle().
			Background(lipgloss.Color("#003822")).
			Foreground(colorGreen).
			Bold(true).
			Padding(0, 1)

	styleBadgePruned = lipgloss.NewStyle().
				Background(lipgloss.Color("#222233")).
				Foreground(colorGray).
				Padding(0, 1)

	styleBadgeFailed = lipgloss.NewStyle().
				Background(lipgloss.Color("#3B000B")).
				Foreground(colorRed).
				Bold(true).
				Padding(0, 1)

	styleNodeTaken = lipgloss.NewStyle().
			Foreground(colorWhite).
			Bold(true)

	styleNodePruned = lipgloss.NewStyle().
			Foreground(colorGray)

	styleTreeLine = lipgloss.NewStyle().
			Foreground(colorGray)

	styleGuardBadge = lipgloss.NewStyle().
			Foreground(colorYellow).
			Bold(true)

	styleGuardCondition = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFF59D"))

	styleVoltStep = lipgloss.NewStyle().
			Foreground(colorCyan).
			Bold(true)

	styleVoltMutation = lipgloss.NewStyle().
				Foreground(colorCyan)

	styleSinkDispatched = lipgloss.NewStyle().
				Foreground(colorYellow).
				Bold(true)

	styleSinkSuppressed = lipgloss.NewStyle().
				Foreground(colorGray)

	styleInspectorBox = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorYellow).
				Padding(0, 1)

	styleInspectorTitle = lipgloss.NewStyle().
				Foreground(colorYellow).
				Bold(true)
)

// RenderEvaluationTree formats the evaluation trace into an interactive color-coded Unicode tree graph.
func RenderEvaluationTree(res *EvalResult, width int) string {
	if res == nil {
		return styleStatusPruned.Render("  (no evaluation run yet — press 'r' or 'Enter' to execute)")
	}

	var sb strings.Builder

	// 1. Metric Summary Banner
	statusBadge := styleStatusPassed.Render("✓ PASSED")
	if !res.Passed {
		statusBadge = styleStatusFailed.Render("✗ FAILED")
	}

	durStr := fmt.Sprintf("%dµs", res.Duration.Microseconds())
	if res.Duration >= time.Millisecond {
		durStr = fmt.Sprintf("%.2fms", float64(res.Duration.Microseconds())/1000.0)
	}

	sb.WriteString(fmt.Sprintf("%s  •  Circuit: %s  •  Time: %s  •  Nodes: %s taken, %s pruned  •  Sinks: %s\n",
		statusBadge,
		styleNodeTaken.Render(res.CircuitID),
		styleVoltStep.Render(durStr),
		styleStatusPassed.Render(fmt.Sprintf("%d", res.PassedNodes)),
		styleStatusPruned.Render(fmt.Sprintf("%d", res.PrunedNodes)),
		styleSinkDispatched.Render(fmt.Sprintf("%d dispatched", len(res.SimulatedSinks))),
	))

	if len(res.Errors) > 0 {
		sb.WriteString(styleStatusFailed.Render("  Errors: " + strings.Join(res.Errors, "; ")))
		sb.WriteString("\n")
	}

	sb.WriteString(styleTreeLine.Render(strings.Repeat("─", min(width-4, 80))))
	sb.WriteString("\n\n")

	// 2. DAG Branch Execution Flow
	if res.RootTrace != nil {
		renderNodeTrace(&sb, res.RootTrace, "", true, true)
	} else {
		sb.WriteString(styleStatusPruned.Render("  (no DAG nodes evaluated)\n"))
	}

	// 3. Mutated State Scratchpad
	if len(res.MutatedState) > 0 {
		sb.WriteString("\n")
		sb.WriteString(styleVoltStep.Render("⚡ Mutated State Scratchpad (VoltScript assignments):"))
		sb.WriteString("\n")
		for k, v := range res.MutatedState {
			vBytes, err := json.Marshal(v)
			vStr := string(vBytes)
			if err != nil {
				vStr = fmt.Sprintf("%v", v)
			}
			sb.WriteString(fmt.Sprintf("   ↳ %s = %s\n", styleVoltStep.Render(k), styleVoltMutation.Render(vStr)))
		}
	}

	// 4. Simulated Outbound Sink Inspector
	sb.WriteString("\n")
	sb.WriteString(RenderSimulatedSinks(res.SimulatedSinks, width))

	return sb.String()
}

func renderNodeTrace(sb *strings.Builder, trace *NodeTrace, prefix string, isLast bool, isRoot bool) {
	if trace == nil {
		return
	}

	connector := "├── "
	if isLast {
		connector = "└── "
	}
	if isRoot {
		connector = "● "
	}

	isTaken := trace.Status == NodeStatusPassed
	isFailed := trace.Status == NodeStatusFailed

	// Node Header Line
	badge := styleBadgeTaken.Render("✓ TAKEN")
	nodeNameStyle := styleNodeTaken
	if trace.Status == NodeStatusPruned {
		badge = styleBadgePruned.Render("✗ PRUNED")
		nodeNameStyle = styleNodePruned
	} else if isFailed {
		badge = styleBadgeFailed.Render("! FAILED")
		nodeNameStyle = styleStatusFailed
	}

	linePrefix := prefix
	if !isRoot {
		linePrefix += styleTreeLine.Render(connector)
	}

	sb.WriteString(linePrefix)
	sb.WriteString(nodeNameStyle.Render(fmt.Sprintf("[%s]", trace.Name)))
	sb.WriteString(" ")
	sb.WriteString(badge)
	sb.WriteString("\n")

	// Child indent prefix for steps and child nodes
	childPrefix := prefix
	if !isRoot {
		if isLast {
			childPrefix += "    "
		} else {
			childPrefix += styleTreeLine.Render("│   ")
		}
	} else {
		childPrefix += "  "
	}

	// Guard Condition Line
	if trace.Condition != "" {
		sb.WriteString(childPrefix)
		if isTaken {
			sb.WriteString(styleGuardBadge.Render("? GUARD: "))
			sb.WriteString(styleGuardCondition.Render(trace.Condition))
			sb.WriteString(styleStatusPassed.Render(" → true"))
		} else if isFailed {
			sb.WriteString(styleStatusFailed.Render("? GUARD (FAILED): "))
			sb.WriteString(styleGuardCondition.Render(trace.Condition))
			if trace.Error != "" {
				sb.WriteString(styleStatusFailed.Render(" (" + trace.Error + ")"))
			}
		} else {
			sb.WriteString(styleStatusPruned.Render("? GUARD: " + trace.Condition + " → false (branch dead)"))
		}
		sb.WriteString("\n")
	}

	// Sequential Steps (only detailed if taken or failed)
	for _, step := range trace.Steps {
		sb.WriteString(childPrefix)
		switch step.Type {
		case types.StepVolt:
			sb.WriteString(styleVoltStep.Render("⚡ VoltScript: "))
			sb.WriteString(styleVoltMutation.Render(step.Script))
			sb.WriteString("\n")
			for k, v := range step.Mutations {
				vBytes, _ := json.Marshal(v)
				sb.WriteString(childPrefix)
				sb.WriteString(fmt.Sprintf("   ↳ %s = %s\n",
					styleVoltStep.Render(k),
					styleVoltMutation.Render(string(vBytes))))
			}

		case types.StepSink:
			if step.Passed {
				sb.WriteString(styleSinkDispatched.Render("🚨 SINK DISPATCH: "))
				sb.WriteString(styleNodeTaken.Render(step.SinkName))
				if step.Condition != "" {
					sb.WriteString(styleGuardCondition.Render(fmt.Sprintf(" [cond: %s]", step.Condition)))
				}
				sb.WriteString(styleBadgeTaken.Render("SIMULATED"))
			} else {
				sb.WriteString(styleSinkSuppressed.Render("⚪ SINK (SUPPRESSED): "))
				sb.WriteString(styleStatusPruned.Render(step.SinkName))
				if step.Condition != "" {
					sb.WriteString(styleStatusPruned.Render(fmt.Sprintf(" [cond: %s → false]", step.Condition)))
				}
			}
			sb.WriteString("\n")
		}
	}

	if trace.Status == NodeStatusPruned && len(trace.Steps) > 0 {
		sb.WriteString(childPrefix)
		sb.WriteString(styleStatusPruned.Render(fmt.Sprintf("  (suppressed %d steps on dead branch)\n", len(trace.Steps))))
	}

	// Recurse Children
	numChildren := len(trace.Children)
	for i, child := range trace.Children {
		lastChild := i == numChildren-1
		renderNodeTrace(sb, child, childPrefix, lastChild, false)
	}
}

// RenderSimulatedSinks renders the Simulated Outbound Sink Inspector panel.
func RenderSimulatedSinks(sinks []SimulatedSinkDispatch, width int) string {
	var sb strings.Builder

	title := "📡 Simulated Outbound Sink Inspector (Zero External Side-Effects)"
	sb.WriteString(styleInspectorTitle.Render(title))
	sb.WriteString("\n")

	if len(sinks) == 0 {
		sb.WriteString(styleStatusPruned.Render("  (no outbound sink dispatches triggered; all sink conditions false or branches pruned)\n"))
		return sb.String()
	}

	for i, s := range sinks {
		sb.WriteString(fmt.Sprintf("  %s %s  •  Node: %s  •  Time: %s\n",
			styleSinkDispatched.Render(fmt.Sprintf("[%d] SINK:", i+1)),
			styleNodeTaken.Render(s.SinkName),
			styleVoltStep.Render(s.NodeName),
			styleStatusPruned.Render(s.Timestamp.Format("15:04:05.000")),
		))

		if s.Condition != "" {
			sb.WriteString(fmt.Sprintf("      Condition: %s\n", styleGuardCondition.Render(s.Condition)))
		}

		payloadBytes, err := json.MarshalIndent(s.Payload, "      ", "  ")
		if err == nil {
			sb.WriteString("      Outbound Payload:\n")
			sb.WriteString(styleVoltMutation.Render(string(payloadBytes)))
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}
