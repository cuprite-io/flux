package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/cmd/flux/tui"
	"github.com/mattn/go-isatty"
)

func main() {
	var (
		endpointFlag = flag.String("endpoint", "", "Flux Engine Admin control plane endpoint (e.g. localhost:9090). Omit for standalone mode.")
		tokenFlag    = flag.String("token", "", "Optional Bearer authentication token for Admin control plane")
		nodeIDFlag   = flag.String("node-id", "", "Custom node identifier for cluster identification")
		versionFlag  = flag.Bool("version", false, "Print Flux engine version and exit")
		helpFlag     = flag.Bool("help", false, "Print help information and exit")
	)
	flag.StringVar(endpointFlag, "e", "", "Flux Engine Admin control plane endpoint (shorthand)")
	flag.BoolVar(versionFlag, "v", false, "Print Flux engine version and exit (shorthand)")
	flag.BoolVar(helpFlag, "h", false, "Print help information and exit (shorthand)")

	flag.Usage = func() {
		fmt.Printf(`⚡ FLUX — Autonomous Streaming Data Plane & Interactive Console (v%s)

USAGE:
  flux [options]

DESCRIPTION:
  Flux launches directly into an interactive terminal console (TUI) for real-time
  observability into streaming pipelines, Autopilot AI reflection cycles, dry-run
  evaluations, distributed cache state, and live alert feeds.

MODES:
  • Standalone Mode: Run 'flux' with no flags to test, lint, and dry-run circuits offline.
  • Cluster Mode:    Run 'flux -e <addr>' to connect to a running Flux engine control plane.

OPTIONS:
  -e, --endpoint <addr>      Flux Admin API endpoint (e.g. "localhost:9090", "10.0.1.45:9090")
      --token <secret>       Bearer authentication token for the Admin control plane
      --node-id <id>         Override node ID in cluster topology
  -v, --version              Print Flux engine version and exit
  -h, --help                 Show this help message

INTERACTIVE SHELL SHORTCUTS:
  1 .. 6        Switch views (adaptive to Standalone vs Cluster mode)
  :<command>    Vim-style command launcher (:circuits, :eval, :autopilot, :alerts, :connect <addr>, :disconnect)
  Tab           Cycle to next view
  Shift+Tab     Cycle to previous view
  s             Toggle opt-in local snapshot synchronization (Cluster mode)
  r             Refresh active view data
  ?             Toggle help screen
  q             Quit Flux shell
`, flux.Version)
	}

	flag.Parse()

	if *versionFlag {
		fmt.Printf("Flux Engine v%s\n", flux.Version)
		os.Exit(0)
	}

	if *helpFlag {
		flag.Usage()
		os.Exit(0)
	}

	// Check if running in an interactive terminal
	isTerminalOut := isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
	isTerminalIn := isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd())
	if !isTerminalOut || !isTerminalIn {
		fmt.Fprintf(os.Stderr, "Notice: Flux is an interactive streaming terminal console.\n"+
			"stdout or stdin is not a TTY. Run 'flux --help' for available flags.\n")
		os.Exit(1)
	}

	cfg := tui.Config{
		Endpoint: *endpointFlag,
		Token:    *tokenFlag,
		NodeID:   *nodeIDFlag,
	}

	if err := tui.Run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
