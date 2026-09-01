# Example 2: Dynamic Personalized Offer Qualification

This example demonstrates using **Flux** with **Capacitor** to score and qualify candidate items via `Conduct`.

### Key Features:
* **Silent Omission**: Ineligible offers whose qualification criteria fail are silently omitted from the return payload.
* **Dynamic Output Projection**: Qualified items return personalized discount percentages and badges computed by `StepReturn` without altering catalog base items.

### Running the Example:

```bash
cd examples/02_personalized_offers
go run main.go
```
