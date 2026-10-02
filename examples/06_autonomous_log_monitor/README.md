# Example 6: End-to-End Autonomous Log Monitoring & Closed-Loop Refinement

This production example demonstrates **Flux Autopilot** executing a completely autonomous stream monitoring and continuous refinement lifecycle.

From a simple natural-language policy, Flux autonomously infers stream schemas, synthesizes and validates executable Circuit DAGs with AI reflection guardrails, tests candidate rules in risk-free shadow mode on live traffic, promotes verified rules with zero downtime, and refines them in a closed feedback loop.

---

## 🏗️ Autonomous Lifecycle Architecture

```
                                  1. Natural Language Prompt
                                              │
                                              ▼
                                ┌───────────────────────────┐
                                │   Flux Engine Autopilot   │
                                └─────────────┬─────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      │                                               │
                      ▼                                               ▼
         ┌──────────────────────────┐                    ┌──────────────────────────┐
         │ 2. Cold-Start Profiling  │                    │ 3. LLM Synthesis & AST   │
         │  (Infers Schema Trees    │ ─────────────────▶ │    Reflection Loop       │
         │   & Exemplars via assay) │                    │ (Validates CEL & Sinks)  │
         └──────────────────────────┘                    └────────────┬─────────────┘
                                                                      │
                                                                      ▼
         ┌──────────────────────────┐                    ┌──────────────────────────┐
         │ 5. Zero-Downtime Live    │                    │ 4. Shadow Staging Phase  │
         │    Execution & Sinks     │ ◀───────────────── │ (Verifies Firing Rate &  │
         │ (Slack, PagerDuty, etc.) │   Promoted if Safe │  Rejects Alert Storms)   │
         └────────────┬─────────────┘                    └──────────────────────────┘
                      │
                      ▼
         ┌──────────────────────────┐
         │ 6. Closed-Loop Refinement│
         │ (Adapts to False Positive│
         │  Feedback & Hot-Swaps)   │
         └──────────────────────────┘
```

---

## ⚡ The 6 Autonomous Phases Demonstrated

1. **Subsystem Auto-Provisioning & Engine Wiring**:
   - Boots a distributed cache node ([Capacitor](https://github.com/cuprite-io/capacitor) or in-memory backend).
   - Registers production notification destinations: `slack_ops` (Warnings) and `pagerduty_critical` (High Severity).
   - Launches an autonomous session with `eng.Autopilot(ctx, prompt, provider, tags, opts...)`.

2. **Cold-Start Schema & Exemplar Sampling (`StatusSampling`)**:
   - Ingests streaming web server and database logs.
   - Infers payload schemas, types, and value profiles automatically without manual schema definitions.

3. **Compiler-Verified Circuit DAG Synthesis (`StatusSynthesizing`)**:
   - Generates executable Volt/CEL Circuit DAGs using the configured `AIProvider` (OpenAI, Gemini, Ollama, or built-in simulator).
   - Runs static analysis and AST validation: if syntax or sink errors occur, the autonomous reflection loop feeds compiler diagnostics back to the LLM to self-repair.

4. **Risk-Free Shadow Verification & Safety Gate (`StatusShadowing`)**:
   - Deploys the candidate rule in shadow execution mode (`shadow:<circuit_id>`).
   - Evaluates the rule against incoming streaming traffic: conditions and Volt transformations evaluate, but external sink dispatches are suppressed.
   - Enforces the safety envelope: rejects alert storms ($> 35\%$) or silent rules on errors ($0\%$).
   - Automatically promotes verified rules within safe firing bounds ($1\% - 20\%$) to `Live` in `Registry` with zero downtime.

5. **Live Production Execution & Multi-Channel Alerting (`StatusLive`)**:
   - Routes live production traffic through the promoted circuit.
   - Emits operational warnings to Slack and escalates fatal crashes directly to PagerDuty.

6. **Closed-Loop Operator Feedback & Dynamic Hot-Swapping (`StatusRefining`)**:
   - An operator reviews historical alerts and flags transient 503 deployment restarts as `false_positive`.
   - The autonomous refiner automatically synthesizes an updated Circuit DAG incorporating the feedback context, updates the root guard condition, and atomically hot-swaps the circuit with zero downtime and zero dropped events.

---

## 🚀 Running the Example

### 1. Out-of-the-Box Run (Embedded Autonomous Simulator)
Run directly from the repository root with zero external dependencies or API keys:
```bash
go run ./examples/06_autonomous_log_monitor/main.go
```

### 2. Run with Live Cloud or Local LLMs
Flux Autopilot natively supports any OpenAI-compatible endpoint:

- **OpenAI (GPT-4o)**:
  ```bash
  OPENAI_API_KEY="sk-..." go run ./examples/06_autonomous_log_monitor/main.go
  ```

- **Google Gemini**:
  ```bash
  GEMINI_API_KEY="AIza..." go run ./examples/06_autonomous_log_monitor/main.go
  ```

- **Local Ollama (e.g. Qwen 2.5 Coder, Llama 3.3, DeepSeek)**:
  ```bash
  OLLAMA_HOST="http://localhost:11434" go run ./examples/06_autonomous_log_monitor/main.go
  ```
