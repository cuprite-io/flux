package cache

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

var (
	styleNSHeader = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E5FF")).
			Bold(true)

	styleNSTreeBranch = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#7C4DFF"))

	styleNSName = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true)

	styleNSBadge = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD600")).
			Bold(true)

	styleNSType = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00E676"))
)

// NamespaceNode represents a node in the hierarchical namespace tree.
type NamespaceNode struct {
	Name     string
	Path     string
	Count    int
	Children map[string]*NamespaceNode
	KeyCount int
}

// BuildNamespaceTree organizes flat cache keys into a hierarchical tree based on ':' delimiters.
func BuildNamespaceTree(keys []client.CacheKeyInfo) *NamespaceNode {
	root := &NamespaceNode{
		Name:     "root",
		Path:     "",
		Children: make(map[string]*NamespaceNode),
	}

	for _, k := range keys {
		parts := strings.Split(k.Key, ":")
		if len(parts) == 0 {
			continue
		}

		current := root
		current.Count++

		// Navigate / create namespace segments up to the second-to-last or last part
		for i := 0; i < len(parts)-1; i++ {
			part := parts[i]
			if part == "" {
				continue
			}

			child, exists := current.Children[part]
			if !exists {
				path := part
				if current.Path != "" {
					path = current.Path + ":" + part
				}
				child = &NamespaceNode{
					Name:     part,
					Path:     path,
					Children: make(map[string]*NamespaceNode),
				}
				current.Children[part] = child
			}
			child.Count++
			current = child
		}
		current.KeyCount++
	}

	return root
}

// RenderNamespaceTree produces an ASCII/Unicode hierarchy summarizing all active namespaces.
func RenderNamespaceTree(root *NamespaceNode, width int) string {
	if root == nil || len(root.Children) == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#757575")).Render("No cache namespaces discovered.")
	}

	var sb strings.Builder
	sb.WriteString(styleNSHeader.Render("🗂 Cache Namespace Hierarchy"))
	sb.WriteString("\n\n")

	renderNodeRecursive(&sb, root, "", true)
	return sb.String()
}

func renderNodeRecursive(sb *strings.Builder, node *NamespaceNode, prefix string, isRoot bool) {
	if !isRoot {
		badge := fmt.Sprintf("(%d keys)", node.Count)
		sb.WriteString(prefix)
		sb.WriteString(styleNSName.Render("[" + node.Name + "]"))
		sb.WriteString(" ")
		sb.WriteString(styleNSBadge.Render(badge))
		sb.WriteString("\n")
	}

	// Sort child names for deterministic order
	childNames := make([]string, 0, len(node.Children))
	for name := range node.Children {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	for i, name := range childNames {
		child := node.Children[name]
		isLast := i == len(childNames)-1

		var nextPrefix string
		if isRoot {
			nextPrefix = "  "
		} else if isLast {
			sb.WriteString(prefix)
			sb.WriteString(styleNSTreeBranch.Render("  └─ "))
			nextPrefix = prefix + "     "
		} else {
			sb.WriteString(prefix)
			sb.WriteString(styleNSTreeBranch.Render("  ├─ "))
			nextPrefix = prefix + styleNSTreeBranch.Render("  │  ")
		}

		renderChildLine(sb, child, nextPrefix)
	}
}

func renderChildLine(sb *strings.Builder, node *NamespaceNode, prefix string) {
	badge := fmt.Sprintf("(%d keys)", node.Count)
	sb.WriteString(styleNSName.Render("[" + node.Name + "]"))
	sb.WriteString(" ")
	sb.WriteString(styleNSBadge.Render(badge))
	sb.WriteString("\n")

	// Sort subchildren
	subNames := make([]string, 0, len(node.Children))
	for name := range node.Children {
		subNames = append(subNames, name)
	}
	sort.Strings(subNames)

	for i, name := range subNames {
		subChild := node.Children[name]
		isLast := i == len(subNames)-1

		branch := "├─ "
		if isLast {
			branch = "└─ "
		}
		sb.WriteString(prefix)
		sb.WriteString(styleNSTreeBranch.Render(branch))
		nextPrefix := prefix + styleNSTreeBranch.Render("│  ")
		if isLast {
			nextPrefix = prefix + "   "
		}
		renderChildLine(sb, subChild, nextPrefix)
	}
}
