package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/compiler"
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
		if _, err := comp.Compile(n.Condition); err != nil {
			return fmt.Errorf("node %q condition syntax error: %w", n.Name, err)
		}
	}
	for _, s := range n.Steps {
		if s.Type == types.StepVolt && s.Script != "" {
			if _, err := comp.Compile(s.Script); err != nil {
				return fmt.Errorf("node %q volt script syntax error: %w", n.Name, err)
			}
		}
		if s.Condition != "" {
			if _, err := comp.Compile(s.Condition); err != nil {
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
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(evalCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(inspectCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
