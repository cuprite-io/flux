package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/linter"
	"github.com/cuprite-io/flux/types"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "flux",
	Short: "Flux — High-performance, zero-allocation Circuit execution engine in Go",
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the Flux engine version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Flux Engine v%s\n", flux.Version)
	},
}

var payloadFlag string

var evalCmd = &cobra.Command{
	Use:   "eval <circuit.json>",
	Short: "Evaluate a Circuit file offline against a JSON payload",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		circuitPath := args[0]
		circuit, err := flux.LoadCircuitFile(circuitPath)
		if err != nil {
			return fmt.Errorf("failed to load circuit: %w", err)
		}

		var payload any
		if payloadFlag != "" {
			if payloadFlag[0] == '{' || payloadFlag[0] == '[' {
				if err := json.Unmarshal([]byte(payloadFlag), &payload); err != nil {
					return fmt.Errorf("failed to parse payload JSON: %w", err)
				}
			} else {
				// Read payload from file
				data, err := os.ReadFile(payloadFlag)
				if err != nil {
					return fmt.Errorf("failed to read payload file: %w", err)
				}
				if err := json.Unmarshal(data, &payload); err != nil {
					return fmt.Errorf("failed to parse payload file JSON: %w", err)
				}
			}
		} else {
			payload = map[string]any{}
		}

		eng, err := flux.New()
		if err != nil {
			return err
		}
		defer eng.Close()

		_ = eng.Registry().Put(context.Background(), circuit)
		res, err := eng.Spark(context.Background(), payload, circuit.Tags...)
		if err != nil {
			return fmt.Errorf("evaluation failed: %w", err)
		}

		outJSON, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(outJSON))
		return nil
	},
}

func validateNodeExpressions(comp *compiler.Compiler, n *types.Node) error {
	if n == nil {
		return nil
	}
	if n.Condition != "" {
		if err := comp.Validate(n.Condition); err != nil {
			return fmt.Errorf("node %q condition syntax error: %w", n.Name, err)
		}
	}
	for _, s := range n.Steps {
		if s.Type == types.StepVolt && s.Script != "" {
			setStmts, remainder := compiler.ExtractSetStatementsAndRemainder(s.Script)
			for _, stmt := range setStmts {
				if err := comp.Validate(stmt.Expr); err != nil {
					return fmt.Errorf("node %q set(%q) syntax error: %w", n.Name, stmt.Key, err)
				}
			}
			if remainder != "" {
				if err := comp.Validate(remainder); err != nil {
					return fmt.Errorf("node %q script remainder syntax error: %w", n.Name, err)
				}
			}
		}
		if s.Condition != "" {
			if err := comp.Validate(s.Condition); err != nil {
				return fmt.Errorf("node %q step condition syntax error: %w", n.Name, err)
			}
		}
	}
	for _, child := range n.Children {
		if err := validateNodeExpressions(comp, child); err != nil {
			return err
		}
	}
	return nil
}

var validateCmd = &cobra.Command{
	Use:   "validate <files...>",
	Short: "Validate Circuit or Item JSON/YAML files for schema and syntax errors",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		comp, _ := compiler.NewCompiler(nil)
		for _, file := range args {
			c, err := flux.LoadCircuitFile(file)
			if err == nil && c != nil {
				if comp != nil {
					if valErr := validateNodeExpressions(comp, c.Root); valErr != nil {
						fmt.Printf("✗ %s: Invalid Circuit Volt Syntax (%v)\n", file, valErr)
						continue
					}
				}
				fmt.Printf("✓ %s: Valid Circuit (ID: %s, Nodes: %d)\n", file, c.ID, countNodes(c.Root))
				continue
			}

			it, errItem := flux.LoadItemFile(file)
			if errItem == nil && it != nil {
				if it.Circuit != nil && comp != nil {
					if valErr := validateNodeExpressions(comp, it.Circuit.Root); valErr != nil {
						fmt.Printf("✗ %s: Invalid Item Embedded Circuit Volt Syntax (%v)\n", file, valErr)
						continue
					}
				}
				fmt.Printf("✓ %s: Valid Candidate Item (ID: %s, Tags: %v)\n", file, it.ID, it.Tags)
				continue
			}

			fmt.Printf("✗ %s: Invalid (%v)\n", file, err)
		}
		return nil
	},
}

var (
	lintStrictFlag bool
	lintJSONFlag   bool
)

var lintCmd = &cobra.Command{
	Use:   "lint <files...>",
	Short: "Perform deep static analysis and rule checking on Circuit DAG files",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		lnt, err := linter.New(linter.Options{
			MaxTreeDepth: 10,
			MaxStepCount: 50,
			CheckSinks:   true,
			Strict:       lintStrictFlag,
		})
		if err != nil {
			return fmt.Errorf("failed to initialize linter: %w", err)
		}

		var allResults []*linter.Result
		totalErrors := 0
		totalWarnings := 0

		for _, filePath := range args {
			c, err := flux.LoadCircuitFile(filePath)
			if err != nil {
				res := &linter.Result{
					FilePath: filePath,
					Issues: []linter.Issue{
						{
							RuleID:   "LINT-000",
							Severity: linter.SeverityError,
							Message:  fmt.Sprintf("failed to parse circuit file: %v", err),
						},
					},
				}
				allResults = append(allResults, res)
				totalErrors++
				continue
			}

			res := lnt.Lint(c)
			res.FilePath = filePath
			allResults = append(allResults, res)

			for _, iss := range res.Issues {
				if iss.Severity == linter.SeverityError {
					totalErrors++
				} else if iss.Severity == linter.SeverityWarning {
					totalWarnings++
				}
			}
		}

		if lintJSONFlag {
			out, _ := json.MarshalIndent(allResults, "", "  ")
			fmt.Println(string(out))
		} else {
			for _, res := range allResults {
				if len(res.Issues) == 0 {
					fmt.Printf("✓ %s (Circuit: %s, %d nodes, %d steps) - No issues detected\n",
						res.FilePath, res.CircuitID, res.NodeCount, res.StepCount)
				} else {
					statusIcon := "⚠"
					if res.HasErrors() {
						statusIcon = "✗"
					}
					fmt.Printf("%s %s (Circuit: %s, %d nodes, %d steps):\n",
						statusIcon, res.FilePath, res.CircuitID, res.NodeCount, res.StepCount)
					for _, iss := range res.Issues {
						fmt.Printf("   %s\n", iss.String())
					}
				}
			}

			fmt.Println("------------------------------------------------------------")
			fmt.Printf("Files Checked: %d | Errors: %d | Warnings: %d\n", len(args), totalErrors, totalWarnings)
		}

		if totalErrors > 0 || (lintStrictFlag && totalWarnings > 0) {
			os.Exit(1)
		}
		return nil
	},
}

var inspectCmd = &cobra.Command{
	Use:   "inspect <circuit.json>",
	Short: "Inspect and render the visual tree structure of a Circuit file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		circuit, err := flux.LoadCircuitFile(args[0])
		if err != nil {
			return err
		}

		fmt.Printf("Circuit ID: %s (Version: %d)\n", circuit.ID, circuit.Version)
		fmt.Printf("Tags: %v\n", circuit.Tags)
		fmt.Println("Tree Structure:")
		printNodeTree(circuit.Root, "", true)
		return nil
	},
}

func countNodes(n *types.Node) int {
	if n == nil {
		return 0
	}
	count := 1
	for _, c := range n.Children {
		count += countNodes(c)
	}
	return count
}

func printNodeTree(n *types.Node, prefix string, isLast bool) {
	if n == nil {
		return
	}

	connector := "├── "
	if isLast {
		connector = "└── "
	}

	condStr := ""
	if n.Condition != "" {
		condStr = fmt.Sprintf(" [Guard: %s]", n.Condition)
	}
	fmt.Printf("%s%sNode: %s (%d steps)%s\n", prefix, connector, n.Name, len(n.Steps), condStr)

	childPrefix := prefix
	if isLast {
		childPrefix += "    "
	} else {
		childPrefix += "│   "
	}

	for i, c := range n.Children {
		printNodeTree(c, childPrefix, i == len(n.Children)-1)
	}
}

func init() {
	evalCmd.Flags().StringVarP(&payloadFlag, "payload", "p", "", "JSON payload string or path to payload file")
	lintCmd.Flags().BoolVar(&lintStrictFlag, "strict", false, "Fail with non-zero exit code on warnings")
	lintCmd.Flags().BoolVar(&lintJSONFlag, "json", false, "Output results in structured JSON format")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(evalCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(lintCmd)
	rootCmd.AddCommand(inspectCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
