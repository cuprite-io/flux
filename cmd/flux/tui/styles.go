package tui

import (
	"github.com/charmbracelet/lipgloss"
)

// Theme palette inspired by modern terminal dashboards.
var (
	ColorPrimary   = lipgloss.Color("#00E5FF") // Neon Cyan
	ColorSecondary = lipgloss.Color("#7C4DFF") // Vibrant Purple
	ColorSuccess   = lipgloss.Color("#00E676") // Bright Emerald
	ColorWarning   = lipgloss.Color("#FFD600") // Neon Amber
	ColorDanger    = lipgloss.Color("#FF1744") // Coral Red
	ColorMuted     = lipgloss.Color("#757575") // Slate Gray
	ColorDarkBg    = lipgloss.Color("#121212") // Dark Background
	ColorPanelBg   = lipgloss.Color("#1E1E1E") // Panel Background
	ColorLightText = lipgloss.Color("#FFFFFF") // Bright White
	ColorDimText   = lipgloss.Color("#B0BEC5") // Soft Gray Text
)

// Global Layout & Header Styles
var (
	StyleHeader = lipgloss.NewStyle().
			Background(lipgloss.Color("#181824")).
			Foreground(ColorLightText).
			Bold(true).
			Padding(0, 1)

	StyleBrand = lipgloss.NewStyle().
			Foreground(ColorPrimary).
			Bold(true)

	StyleHeaderItem = lipgloss.NewStyle().
			Foreground(ColorDimText)

	StyleHeaderItemVal = lipgloss.NewStyle().
				Foreground(ColorLightText).
				Bold(true)

	StyleHeaderLeader = lipgloss.NewStyle().
				Foreground(ColorWarning).
				Bold(true)

	StyleHeaderSynced = lipgloss.NewStyle().
				Foreground(ColorSuccess).
				Bold(true)

	// Navigation Tabs
	StyleTabActive = lipgloss.NewStyle().
			Background(ColorSecondary).
			Foreground(ColorLightText).
			Bold(true).
			Padding(0, 1)

	StyleTabInactive = lipgloss.NewStyle().
				Background(lipgloss.Color("#252538")).
				Foreground(ColorDimText).
				Padding(0, 1)

	StyleTabBar = lipgloss.NewStyle().
			Background(lipgloss.Color("#1A1A28")).
			Margin(0, 0, 1, 0)

	// View Container & Panels
	StyleViewport = lipgloss.NewStyle().
			Padding(0, 1)

	StylePanel = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#3A3A55")).
			Padding(1)

	StylePanelFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(ColorPrimary).
				Padding(1)

	StylePanelTitle = lipgloss.NewStyle().
			Foreground(ColorPrimary).
			Bold(true)

	// Table & List Items
	StyleTableHeader = lipgloss.NewStyle().
				Foreground(ColorPrimary).
				Bold(true).
				Border(lipgloss.NormalBorder(), false, false, true, false).
				BorderForeground(lipgloss.Color("#3A3A55"))

	StyleTableRow = lipgloss.NewStyle().
			Foreground(ColorLightText)

	StyleTableRowSelected = lipgloss.NewStyle().
				Background(lipgloss.Color("#2A2A48")).
				Foreground(ColorPrimary).
				Bold(true)

	// Status & Command Bar
	StyleCommandBar = lipgloss.NewStyle().
			Background(lipgloss.Color("#181824")).
			Foreground(ColorLightText).
			Padding(0, 1)

	StyleCommandPrompt = lipgloss.NewStyle().
				Foreground(ColorPrimary).
				Bold(true)

	StyleStatusMsg = lipgloss.NewStyle().
			Foreground(ColorSuccess).
			Bold(true)

	StyleStatusError = lipgloss.NewStyle().
				Foreground(ColorDanger).
				Bold(true)

	StyleKeyHelp = lipgloss.NewStyle().
			Foreground(ColorMuted)

	StyleKey = lipgloss.NewStyle().
			Foreground(ColorPrimary).
			Bold(true)
)
