package compiler

import (
	"strings"
)

// SetStatement represents an extracted set('key', expression) operation.
type SetStatement struct {
	Key  string
	Expr string
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func findNextSetCall(script string, fromIdx int) int {
	inQuote := false
	var quoteChar byte
	escaped := false

	for i := fromIdx; i+4 <= len(script); i++ {
		c := script[i]
		if inQuote {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == quoteChar {
				inQuote = false
			}
			continue
		}

		if c == '\'' || c == '"' {
			inQuote = true
			quoteChar = c
			escaped = false
			continue
		}

		if script[i:i+4] == "set(" {
			if i == 0 || !isIdentChar(script[i-1]) {
				return i
			}
		}
	}
	return -1
}

func isTrivialRemainder(s string) bool {
	cleaned := strings.ReplaceAll(s, "true", "")
	cleaned = strings.ReplaceAll(cleaned, "TRUE", "")
	cleaned = strings.ReplaceAll(cleaned, "(", "")
	cleaned = strings.ReplaceAll(cleaned, ")", "")
	cleaned = strings.ReplaceAll(cleaned, "&", "")
	cleaned = strings.TrimSpace(cleaned)
	return cleaned == ""
}

// ExtractSetStatementsAndRemainder parses VoltScript and separates set(...) statements from the trailing condition expression.
func ExtractSetStatementsAndRemainder(script string) ([]SetStatement, string) {
	var stmts []SetStatement
	idx := 0
	var sb strings.Builder
	lastEnd := 0

	for {
		callStart := findNextSetCall(script, idx)
		if callStart == -1 {
			break
		}
		start := callStart + 4

		// Find comma
		commaPos := -1
		quoteChar := byte(0)
		inQuote := false
		escaped := false

		for i := start; i < len(script); i++ {
			c := script[i]
			if inQuote {
				if escaped {
					escaped = false
					continue
				}
				if c == '\\' {
					escaped = true
					continue
				}
				if c == quoteChar {
					inQuote = false
				}
				continue
			}
			if c == '\'' || c == '"' {
				inQuote = true
				quoteChar = c
				escaped = false
				continue
			}
			if c == ',' {
				commaPos = i
				break
			}
		}

		if commaPos == -1 {
			idx = start
			continue
		}

		keyPart := strings.TrimSpace(script[start:commaPos])
		keyPart = strings.Trim(keyPart, "'\"")

		// Find matching closing paren
		parenCount := 1
		exprEnd := -1
		inQuote = false
		escaped = false
		for i := commaPos + 1; i < len(script); i++ {
			c := script[i]
			if inQuote {
				if escaped {
					escaped = false
					continue
				}
				if c == '\\' {
					escaped = true
					continue
				}
				if c == quoteChar {
					inQuote = false
				}
				continue
			}
			if c == '\'' || c == '"' {
				inQuote = true
				quoteChar = c
				escaped = false
				continue
			}
			if c == '(' {
				parenCount++
			} else if c == ')' {
				parenCount--
				if parenCount == 0 {
					exprEnd = i
					break
				}
			}
		}

		if exprEnd != -1 {
			exprPart := strings.TrimSpace(script[commaPos+1 : exprEnd])
			stmts = append(stmts, SetStatement{Key: keyPart, Expr: exprPart})

			// Append slice before set(...) call
			sb.WriteString(script[lastEnd:callStart])
			// Replace set(...) call with true in remainder
			sb.WriteString("true")

			lastEnd = exprEnd + 1
			idx = exprEnd + 1
		} else {
			idx = commaPos + 1
		}
	}

	if len(stmts) == 0 {
		return nil, script
	}

	sb.WriteString(script[lastEnd:])
	remainder := strings.TrimSpace(sb.String())
	if isTrivialRemainder(remainder) {
		remainder = ""
	}
	return stmts, remainder
}
