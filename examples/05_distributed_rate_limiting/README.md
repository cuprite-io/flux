# Example 5: Distributed Multi-Tenant API Rate Limiting

This example demonstrates using **Flux** with **Capacitor**'s sliding window counters (`IncrementSlidingWindow`) to enforce low-latency, zero-allocation rate limits on high-concurrency API gateways.

### Running the Example:

```bash
cd examples/05_distributed_rate_limiting
go run main.go
```
