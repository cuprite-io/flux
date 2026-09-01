# Example 1: Real-Time Fraud & Impossible Travel Anomaly Engine

This example combines **historical baseline tracking** (all-time highest transaction, novel device detection) and **geographical velocity physics** ("impossible travel") by pairing **Flux** with **Capacitor**.

---

### 🌐 Key Features Demonstrated:
* **Historical Baseline Breakouts**: Capacitor tracks user's `all_time_max_amount` and `known_devices` (CRDT Set). Flux detects when an incoming transaction exceeds $3\times$ the historical all-time peak from an unrecognized device.
* **Impossible Travel Detection**: Computes geographical displacement using `geo.distance_km(lat1, lon1, lat2, lon2)` and transit velocity. Flags physically impossible transit speeds ($>900\text{ km/h}$).
* **Multi-Sink Pipeline**: Dispatches alerts to an incident response webhook and triggers SMS/Push step-up biometric verification while halting execution via `StepAbort`.

---

### 🚀 Running the Example:

```bash
cd examples/01_fraud_detection
go run main.go
```
