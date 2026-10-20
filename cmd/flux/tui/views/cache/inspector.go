package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

var (
	styleInspKeyTitle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00E5FF")).
				Bold(true)

	styleInspMetaLabel = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#757575"))

	styleInspMetaVal = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Bold(true)

	styleInspTypeString = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#000000")).
				Background(lipgloss.Color("#00E5FF")).
				Bold(true).
				Padding(0, 1)

	styleInspTypeMap = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(lipgloss.Color("#7C4DFF")).
				Bold(true).
				Padding(0, 1)

	styleInspTypeSet = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#000000")).
				Background(lipgloss.Color("#00E676")).
				Bold(true).
				Padding(0, 1)

	styleInspTypeWindow = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#000000")).
				Background(lipgloss.Color("#FFD600")).
				Bold(true).
				Padding(0, 1)

	styleInspDivider = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#3A3A55"))

	styleJSONKey = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleJSONString = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF"))

	styleJSONNumber = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#D183FF")).
			Bold(true)

	styleJSONBool = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676")).
			Bold(true)

	styleJSONNull = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#757575")).
			Italic(true)
)

// RenderKeyInspector formats and highlights the inspected key's data structure and payload.
func RenderKeyInspector(detail *client.CacheKeyDetail, rawMode bool, width int) string {
	if detail == nil {
		var sb strings.Builder
		sb.WriteString(styleInspKeyTitle.Render("🔍 Cache Key Inspector"))
		sb.WriteString("\n\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render(
			"No key currently inspected.\n\n" +
				"• Press Enter on any key to fetch and inspect its data structure.\n" +
				"• Press 'v' to toggle between formatted tree and raw string views.\n" +
				"• Press '/' to filter keys by prefix or pattern.",
		))
		return sb.String()
	}

	var sb strings.Builder

	// 1. Header & Type Badge
	var typeBadge string
	switch strings.ToLower(detail.Type) {
	case "map":
		typeBadge = styleInspTypeMap.Render("MAP / HASH")
	case "set":
		typeBadge = styleInspTypeSet.Render("SET")
	case "window":
		typeBadge = styleInspTypeWindow.Render("SLIDING WINDOW")
	default:
		typeBadge = styleInspTypeString.Render("STRING")
	}

	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Center,
		typeBadge, "  ", styleInspKeyTitle.Render(detail.Key),
	))
	sb.WriteString("\n\n")

	// 2. Metadata Grid
	ttlStr := "Permanent (No TTL)"
	if detail.TTL > 0 {
		ttlStr = formatTTL(detail.TTL)
	}

	sizeStr := fmt.Sprintf("%d bytes", detail.Size)
	switch detail.Type {
	case "map", "set":
		sizeStr = fmt.Sprintf("%d items", detail.Size)
	case "window":
		sizeStr = fmt.Sprintf("%d events in window", detail.Size)
	}

	metaLine := fmt.Sprintf("%s %s  │  %s %s  │  %s %s",
		styleInspMetaLabel.Render("Type:"), styleInspMetaVal.Render(strings.ToUpper(detail.Type)),
		styleInspMetaLabel.Render("Size:"), styleInspMetaVal.Render(sizeStr),
		styleInspMetaLabel.Render("TTL:"), styleInspMetaVal.Render(ttlStr),
	)
	sb.WriteString(metaLine)
	sb.WriteString("\n")

	dividerW := width - 4
	if dividerW < 30 {
		dividerW = 30
	}
	sb.WriteString(styleInspDivider.Render(strings.Repeat("─", dividerW)))
	sb.WriteString("\n\n")

	// 3. Raw Mode vs Formatted Mode
	if rawMode {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600")).Bold(true).Render("[RAW VIEW]"))
		sb.WriteString("\n\n")
		switch v := detail.Value.(type) {
		case string:
			sb.WriteString(v)
		default:
			b, _ := json.Marshal(v)
			sb.WriteString(string(b))
		}
		return sb.String()
	}

	// 4. Formatted Value Rendering based on type
	switch detail.Type {
	case "map":
		sb.WriteString(renderMapValue(detail.Value, dividerW))
	case "set":
		sb.WriteString(renderSetValue(detail.Value))
	case "window":
		sb.WriteString(renderWindowValue(detail.Value))
	default:
		sb.WriteString(renderStringOrJSONValue(detail.Value))
	}

	return sb.String()
}

func renderStringOrJSONValue(val any) string {
	strVal, ok := val.(string)
	if !ok {
		b, err := json.MarshalIndent(val, "", "  ")
		if err == nil {
			return HighlightJSON(string(b))
		}
		return fmt.Sprintf("%v", val)
	}

	// Check if strVal is valid JSON
	var parsed any
	trimmed := strings.TrimSpace(strVal)
	if (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]")) {
		if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
			pretty, errIndent := json.MarshalIndent(parsed, "", "  ")
			if errIndent == nil {
				return HighlightJSON(string(pretty))
			}
		}
	}

	return strVal
}

func renderMapValue(val any, width int) string {
	var sb strings.Builder

	var mp map[string]string
	switch m := val.(type) {
	case map[string]string:
		mp = m
	case map[string]any:
		mp = make(map[string]string, len(m))
		for k, v := range m {
			mp[k] = fmt.Sprintf("%v", v)
		}
	default:
		return fmt.Sprintf("%v", val)
	}

	if len(mp) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("(empty map)")
	}

	fields := make([]string, 0, len(mp))
	for f := range mp {
		fields = append(fields, f)
	}
	sort.Strings(fields)

	for _, f := range fields {
		v := mp[f]
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true).Render("◆ " + f))
		sb.WriteString("\n")

		// Check if value is JSON
		trimmed := strings.TrimSpace(v)
		if (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
			(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]")) {
			var parsed any
			if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
				if pretty, err := json.MarshalIndent(parsed, "  ", "  "); err == nil {
					sb.WriteString("  ")
					sb.WriteString(HighlightJSON(string(pretty)))
					sb.WriteString("\n\n")
					continue
				}
			}
		}

		sb.WriteString("  ")
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Render(v))
		sb.WriteString("\n\n")
	}

	return sb.String()
}

func renderSetValue(val any) string {
	var sb strings.Builder

	var items []string
	switch s := val.(type) {
	case []string:
		items = s
	case []any:
		for _, item := range s {
			items = append(items, fmt.Sprintf("%v", item))
		}
	default:
		return fmt.Sprintf("%v", val)
	}

	if len(items) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("(empty set)")
	}

	for _, item := range items {
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676")).Bold(true).Render("  • "))
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Render(item))
		sb.WriteString("\n")
	}

	return sb.String()
}

func renderWindowValue(val any) string {
	var sb strings.Builder
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD600")).Bold(true).Render("📊 Sliding Window Rate Counter"))
	sb.WriteString("\n\n")
	sb.WriteString(fmt.Sprintf("Current aggregate count: %s\n",
		lipgloss.NewStyle().Foreground(lipgloss.Color("#00E5FF")).Bold(true).Render(fmt.Sprintf("%v", val)),
	))
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render(
		"Sliding window counters record event frequency with in-place rolling time expiry.",
	))
	return sb.String()
}

// HighlightJSON syntax highlights pretty-printed JSON text for the terminal.
func HighlightJSON(jsonStr string) string {
	var buf bytes.Buffer
	lines := strings.Split(jsonStr, "\n")

	for i, line := range lines {
		// Key: "key": value
		colonIdx := strings.Index(line, ": ")
		if colonIdx != -1 && strings.Contains(line[:colonIdx], "\"") {
			valPart := line[colonIdx+2:]

			// Color the key
			keyPart := line[:colonIdx]
			coloredKey := styleJSONKey.Render(keyPart)
			buf.WriteString(coloredKey)
			buf.WriteString(": ")

			// Color the value
			coloredVal := highlightJSONValue(valPart)
			buf.WriteString(coloredVal)
		} else {
			buf.WriteString(highlightJSONValue(line))
		}

		if i < len(lines)-1 {
			buf.WriteString("\n")
		}
	}

	return buf.String()
}

func highlightJSONValue(s string) string {
	trimmed := strings.TrimSpace(strings.TrimRight(s, ","))
	if trimmed == "true" || trimmed == "false" {
		return styleJSONBool.Render(s)
	}
	if trimmed == "null" {
		return styleJSONNull.Render(s)
	}
	if strings.HasPrefix(trimmed, "\"") {
		return styleJSONString.Render(s)
	}
	if len(trimmed) > 0 && (trimmed[0] >= '0' && trimmed[0] <= '9' || trimmed[0] == '-') {
		return styleJSONNumber.Render(s)
	}
	return s
}

func formatTTL(d time.Duration) string {
	if d <= 0 {
		return "expired"
	}
	secs := int(d.Seconds())
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	mins := secs / 60
	remSecs := secs % 60
	if mins < 60 {
		return fmt.Sprintf("%dm%ds", mins, remSecs)
	}
	hours := mins / 60
	remMins := mins % 60
	if hours < 24 {
		return fmt.Sprintf("%dh%dm", hours, remMins)
	}
	days := hours / 24
	remHours := hours % 24
	return fmt.Sprintf("%dd%dh", days, remHours)
}
