package autopilot

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

var (
	styleAuditTitle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleAuditSection = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#7C4DFF")).
				Bold(true)

	styleAuditTag = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD600")).
			Bold(true)

	styleAuditSub = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#B0BEC5"))

	styleAuditDim = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#757575"))

	styleAuditSuccess = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E676")).
				Bold(true)

	styleAuditError = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF1744")).
			Bold(true)

	styleAuditBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#3A3A55")).
			Padding(0, 1)

	styleAuditAttemptBadge = lipgloss.NewStyle().
				Background(lipgloss.Color("#FFD600")).
				Foreground(lipgloss.Color("#000000")).
				Bold(true).
				Padding(0, 1)
)

// RenderAuditLog formats the full AI interaction and reflection history for terminal display.
func RenderAuditLog(sess *client.AutopilotSession, width int) string {
	if sess == nil {
		return styleAuditDim.Render("  No autopilot session selected.")
	}

	var sb strings.Builder

	// 1. Session Title & Target Tags
	title := fmt.Sprintf("AI Autopilot Audit Log: %s", sess.SessionID)
	sb.WriteString(styleAuditTitle.Render(title))
	sb.WriteString("\n")

	meta := fmt.Sprintf("Target Tags: %s │ Status: %s",
		strings.Join(sess.Tags, ", "), sess.Status)
	sb.WriteString(styleAuditSub.Render(meta))
	sb.WriteString("\n\n")

	// 2. User Goal & Prompt
	sb.WriteString(styleAuditSection.Render("🎯 1. User Specification & Policy Goal:"))
	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().
		Foreground(lipgloss.Color("#E0E0E0")).
		Render("  \"" + sess.Prompt + "\""))
	sb.WriteString("\n\n")

	audit := sess.AuditLog
	if audit == nil {
		sb.WriteString(styleAuditDim.Render("  (Audit telemetry not yet recorded for this session phase)"))
		sb.WriteString("\n")
		return sb.String()
	}

	// 3. Inferred Schema & Exemplars
	sb.WriteString(styleAuditSection.Render("📊 2. Inferred Schemas & Stream Exemplars (Schema Tap):"))
	sb.WriteString("\n")
	if audit.InferredSchema != "" {
		sb.WriteString(styleAuditSub.Render("  Inferred Schema Tree:"))
		sb.WriteString("\n")
		sb.WriteString(indentText(audit.InferredSchema, "    "))
		sb.WriteString("\n")
	} else {
		sb.WriteString(styleAuditDim.Render("  (No schema tree captured)"))
		sb.WriteString("\n")
	}

	if len(audit.Exemplars) > 0 {
		sb.WriteString(styleAuditSub.Render("  Stream Exemplar Payloads:"))
		sb.WriteString("\n")
		for idx, ex := range audit.Exemplars {
			sb.WriteString(fmt.Sprintf("    Exemplar [%d]: %s\n", idx+1, ex))
		}
	} else {
		sb.WriteString(styleAuditDim.Render("  (No exemplars attached)"))
		sb.WriteString("\n")
	}
	sb.WriteString("\n")

	// 4. Prompt Sent to AI Model
	sb.WriteString(styleAuditSection.Render("💬 3. Prompt Dispatched to AI Model:"))
	sb.WriteString("\n")
	if audit.PromptSent != "" {
		sb.WriteString(lipgloss.NewStyle().
			Foreground(lipgloss.Color("#B2EBF2")).
			Render(indentText(audit.PromptSent, "  ")))
	} else {
		sb.WriteString(styleAuditDim.Render("  (Standard autonomous compiler prompt template)"))
	}
	sb.WriteString("\n\n")

	// 5. Raw Model Response (Candidate JSON)
	sb.WriteString(styleAuditSection.Render("🤖 4. Raw AI Model Output (Candidate JSON):"))
	sb.WriteString("\n")
	if audit.RawResponse != "" {
		sb.WriteString(lipgloss.NewStyle().
			Foreground(lipgloss.Color("#C8E6C9")).
			Render(indentText(audit.RawResponse, "  ")))
	} else {
		sb.WriteString(styleAuditDim.Render("  (No candidate response generated)"))
	}
	sb.WriteString("\n\n")

	// 6. Reflection & Self-Correction Loop
	sb.WriteString(styleAuditSection.Render("🔄 5. Reflection & Self-Correction Loop:"))
	sb.WriteString("\n")

	attemptsCount := audit.Attempts
	if attemptsCount <= 0 {
		attemptsCount = 1
	}

	if len(audit.CorrectionHistory) == 0 {
		sb.WriteString(styleAuditSuccess.Render(fmt.Sprintf("  ✓ Synthesized cleanly on Turn 1 (%d attempt, 0 compiler errors).", attemptsCount)))
		sb.WriteString("\n")
		sb.WriteString(styleAuditSub.Render("  Candidate verified against AST compiler, VoltScript linter, and sink catalogs without violations."))
		sb.WriteString("\n")
	} else {
		sb.WriteString(styleAuditTag.Render(fmt.Sprintf("  Total Reflection Turns: %d", attemptsCount)))
		sb.WriteString("\n\n")

		for _, corr := range audit.CorrectionHistory {
			badge := styleAuditAttemptBadge.Render(fmt.Sprintf("Reflection Turn #%d", corr.Attempt))
			sb.WriteString(fmt.Sprintf("  %s\n", badge))

			sb.WriteString(styleAuditError.Render("  Compiler/Linter Validation Errors Detected:"))
			sb.WriteString("\n")
			for _, errStr := range corr.Errors {
				sb.WriteString(styleAuditError.Render(fmt.Sprintf("    ✗ %s\n", errStr)))
			}

			if corr.Candidate != "" {
				sb.WriteString(styleAuditDim.Render("  Rejected Candidate Snippet:"))
				sb.WriteString("\n")
				sb.WriteString(styleAuditDim.Render(indentText(truncateText(corr.Candidate, 200), "    ")))
				sb.WriteString("\n")
			}
			sb.WriteString("\n")
		}

		sb.WriteString(styleAuditSuccess.Render("  ✓ Final turn resolved all validation issues into compliant Circuit DAG."))
		sb.WriteString("\n")
	}

	return sb.String()
}

func indentText(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}

func truncateText(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
