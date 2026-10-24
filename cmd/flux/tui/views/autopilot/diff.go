package autopilot

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/types"
)

var (
	styleDiffAdded = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676")).
			Bold(true)

	styleDiffRemoved = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FF1744")).
				Bold(true)

	styleDiffContext = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#90A4AE"))

	styleDiffHeader = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleDiffSubheader = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD600")).
				Bold(true)

	styleDiffBadgeAdded = lipgloss.NewStyle().
				Background(lipgloss.Color("#00E676")).
				Foreground(lipgloss.Color("#000000")).
				Bold(true).
				Padding(0, 1)

	styleDiffBadgeRemoved = lipgloss.NewStyle().
				Background(lipgloss.Color("#FF1744")).
				Foreground(lipgloss.Color("#FFFFFF")).
				Bold(true).
				Padding(0, 1)

	styleDiffBadgeNeutral = lipgloss.NewStyle().
				Background(lipgloss.Color("#3A3A55")).
				Foreground(lipgloss.Color("#FFFFFF")).
				Bold(true).
				Padding(0, 1)
)

// DiffOp represents the operation of a diff line.
type DiffOp int

const (
	DiffOpContext DiffOp = iota
	DiffOpAdd
	DiffOpRemove
)

// DiffLine represents a single line in a circuit diff.
type DiffLine struct {
	Op       DiffOp
	LeftNum  int // line number in original/active
	RightNum int // line number in candidate
	Text     string
}

// DiffResult holds the computed diff lines and statistics.
type DiffResult struct {
	Additions int
	Deletions int
	Lines     []DiffLine
	HasDiff   bool
}

// ComputeCircuitDiff computes a line-by-line diff between the active circuit and candidate circuit.
func ComputeCircuitDiff(active, candidate *types.Circuit) DiffResult {
	if active == nil && candidate == nil {
		return DiffResult{HasDiff: false}
	}

	var activeLines, candidateLines []string
	if active != nil {
		if b, err := json.MarshalIndent(active, "", "  "); err == nil {
			activeLines = strings.Split(string(b), "\n")
		}
	}
	if candidate != nil {
		if b, err := json.MarshalIndent(candidate, "", "  "); err == nil {
			candidateLines = strings.Split(string(b), "\n")
		}
	}

	return computeLCSDiff(activeLines, candidateLines)
}

// computeLCSDiff performs standard Longest Common Subsequence line diffing.
func computeLCSDiff(a, b []string) DiffResult {
	m, n := len(a), len(b)
	if m == 0 && n == 0 {
		return DiffResult{HasDiff: false}
	}

	// DP table for LCS lengths
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			if a[i] == b[j] {
				dp[i+1][j+1] = dp[i][j] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i+1][j+1] = dp[i+1][j]
			} else {
				dp[i+1][j+1] = dp[i][j+1]
			}
		}
	}

	// Backtrack to build diff
	var rawLines []DiffLine
	i, j := m, n
	for i > 0 || j > 0 {
		if i > 0 && j > 0 && a[i-1] == b[j-1] {
			rawLines = append(rawLines, DiffLine{
				Op:       DiffOpContext,
				LeftNum:  i,
				RightNum: j,
				Text:     a[i-1],
			})
			i--
			j--
		} else if j > 0 && (i == 0 || dp[i][j-1] >= dp[i-1][j]) {
			rawLines = append(rawLines, DiffLine{
				Op:       DiffOpAdd,
				LeftNum:  0,
				RightNum: j,
				Text:     b[j-1],
			})
			j--
		} else if i > 0 && (j == 0 || dp[i][j-1] < dp[i-1][j]) {
			rawLines = append(rawLines, DiffLine{
				Op:       DiffOpRemove,
				LeftNum:  i,
				RightNum: 0,
				Text:     a[i-1],
			})
			i--
		}
	}

	// Reverse since we backtracked from end
	lines := make([]DiffLine, len(rawLines))
	additions := 0
	deletions := 0
	for idx, rl := range rawLines {
		revIdx := len(rawLines) - 1 - idx
		lines[revIdx] = rl
		switch rl.Op {
		case DiffOpAdd:
			additions++
		case DiffOpRemove:
			deletions++
		}
	}

	return DiffResult{
		Additions: additions,
		Deletions: deletions,
		Lines:     lines,
		HasDiff:   additions > 0 || deletions > 0,
	}
}

// RenderUnifiedDiff renders a unified diff view suitable for terminal display.
func RenderUnifiedDiff(active, candidate *types.Circuit, width int) string {
	if active == nil && candidate == nil {
		return styleDiffContext.Render("  No active or candidate circuit available for diff.")
	}

	diff := ComputeCircuitDiff(active, candidate)
	var sb strings.Builder

	// Header banner
	activeID := "none (cold-start synthesis)"
	if active != nil {
		activeID = active.ID
	}
	candID := "none"
	if candidate != nil {
		candID = candidate.ID
	}

	header := fmt.Sprintf("Circuit Diff: Live [%s] vs Candidate [%s]", activeID, candID)
	sb.WriteString(styleDiffHeader.Render(header))
	sb.WriteString("\n")

	stats := lipgloss.JoinHorizontal(lipgloss.Center,
		styleDiffBadgeAdded.Render(fmt.Sprintf("+%d additions", diff.Additions)),
		" ",
		styleDiffBadgeRemoved.Render(fmt.Sprintf("-%d deletions", diff.Deletions)),
		" ",
		styleDiffBadgeNeutral.Render(fmt.Sprintf("%d total lines", len(diff.Lines))),
	)
	sb.WriteString(stats)
	sb.WriteString("\n\n")

	if !diff.HasDiff {
		sb.WriteString(styleDiffAdded.Render("  ✓ Candidate matches active live circuit exactly (0 differences)."))
		sb.WriteString("\n")
		return sb.String()
	}

	for _, line := range diff.Lines {
		switch line.Op {
		case DiffOpAdd:
			prefix := fmt.Sprintf("    + %4d │ ", line.RightNum)
			sb.WriteString(styleDiffAdded.Render(prefix + line.Text))
		case DiffOpRemove:
			prefix := fmt.Sprintf("-%4d      │ ", line.LeftNum)
			sb.WriteString(styleDiffRemoved.Render(prefix + line.Text))
		case DiffOpContext:
			prefix := fmt.Sprintf(" %4d %4d │ ", line.LeftNum, line.RightNum)
			sb.WriteString(styleDiffContext.Render(prefix + line.Text))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// RenderSideBySideDiff renders a 2-column side-by-side terminal comparison.
func RenderSideBySideDiff(active, candidate *types.Circuit, width int) string {
	if active == nil && candidate == nil {
		return styleDiffContext.Render("  No active or candidate circuit available for diff.")
	}

	diff := ComputeCircuitDiff(active, candidate)
	var sb strings.Builder

	halfWidth := (width - 7) / 2
	if halfWidth < 25 {
		halfWidth = 25
	}

	// Column Headers
	leftTitle := "Active Live Circuit"
	if active != nil {
		leftTitle = fmt.Sprintf("Active: %s (rev %d)", active.ID, active.Version)
	}
	rightTitle := "Synthesized Candidate"
	if candidate != nil {
		rightTitle = fmt.Sprintf("Candidate: %s (rev %d)", candidate.ID, candidate.Version)
	}

	leftHeader := lipgloss.NewStyle().
		Width(halfWidth).
		Foreground(lipgloss.Color("#FF8A80")).
		Bold(true).
		Render(truncateString(leftTitle, halfWidth))

	rightHeader := lipgloss.NewStyle().
		Width(halfWidth).
		Foreground(lipgloss.Color("#B9F6CA")).
		Bold(true).
		Render(truncateString(rightTitle, halfWidth))

	sb.WriteString(leftHeader)
	sb.WriteString("  │  ")
	sb.WriteString(rightHeader)
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", halfWidth))
	sb.WriteString("──┼──")
	sb.WriteString(strings.Repeat("─", halfWidth))
	sb.WriteString("\n")

	for _, line := range diff.Lines {
		var leftText, rightText string
		switch line.Op {
		case DiffOpContext:
			leftText = styleDiffContext.Render(truncateString(fmt.Sprintf("%3d │ %s", line.LeftNum, line.Text), halfWidth))
			rightText = styleDiffContext.Render(truncateString(fmt.Sprintf("%3d │ %s", line.RightNum, line.Text), halfWidth))
		case DiffOpRemove:
			leftText = styleDiffRemoved.Render(truncateString(fmt.Sprintf("%3d - %s", line.LeftNum, line.Text), halfWidth))
			rightText = styleDiffContext.Render(strings.Repeat(" ", halfWidth))
		case DiffOpAdd:
			leftText = styleDiffContext.Render(strings.Repeat(" ", halfWidth))
			rightText = styleDiffAdded.Render(truncateString(fmt.Sprintf("%3d + %s", line.RightNum, line.Text), halfWidth))
		}

		sb.WriteString(padRight(leftText, halfWidth))
		sb.WriteString("  │  ")
		sb.WriteString(padRight(rightText, halfWidth))
		sb.WriteString("\n")
	}

	return sb.String()
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func padRight(s string, width int) string {
	visualWidth := lipgloss.Width(s)
	if visualWidth >= width {
		return s
	}
	return s + strings.Repeat(" ", width-visualWidth)
}
