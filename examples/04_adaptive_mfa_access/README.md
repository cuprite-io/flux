# Example 4: Adaptive Multi-Factor Authentication (MFA) & Zero-Trust Access Control

This production example demonstrates how **Flux** paired with **Capacitor** executes real-time Zero-Trust adaptive security evaluations. It showcases real-time sign-in risk scoring via `Spark`, state hydration in Capacitor, and dynamic policy qualification / challenge resolution via `Conduct`.

---

## 🏗️ Architecture & Dual-Paradigm Flow

```
                           Incoming Sign-In Attempt (Spark)
                  (Device, IP, ASN Type, Client Headers, User ID)
                                         │
                                         ▼
                  ┌─────────────────────────────────────────────┐
                  │           auth_event_ingress Node           │
                  │  - Email PII Masking: mask.email()          │
                  │  - Failed Login Velocity: window.count(5m)  │
                  │  - Global IP Burst Rate: window.count(1m)   │
                  │  - ASN & Headless Browser Anomaly Scoring   │
                  │  - Dynamic Risk Score [0-100]               │
                  └──────────────────────┬──────────────────────┘
                                         │
         ┌───────────────────────────────┼───────────────────────────────┐
         │                               │                               │
         ▼                               ▼                               ▼
┌──────────────────────┐        ┌──────────────────────┐        ┌──────────────────────┐
│   Critical Threat    │        │    Elevated Risk     │        │  Low-Risk Baseline   │
│ (Risk >= 80 or       │        │ (40 <= Risk < 80)    │        │ (Risk < 40)          │
│  Tor + Bot Burst)    │        │                      │        │                      │
└──────────┬───────────┘        └──────────┬───────────┘        └──────────┬───────────┘
         │                               │                               │
         ▼                               ▼                               ▼
SOC Incident Webhook              Step-Up Challenge             Seamless Access
+ Hardware FIDO2 Only           (TOTP / Authenticator)       (FastPath / Biometric)
         │
         │ (Sync to Capacitor `entity:<user_id>`)
         ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                         Candidate MFA Policy Resolution (Conduct)                    │
│                    Evaluates candidate policies in `auth:mfa_methods`:               │
│                                                                                      │
│   * policy_seamless_fastpath     -> Qualifies only if Risk < 40 & Trusted Device     │
│   * policy_biometric_platform    -> Qualifies if Trusted Device & Risk < 80          │
│   * policy_totp_authenticator    -> Qualifies if 30 <= Risk < 80                     │
│   * policy_fido2_hardware_key    -> Mandatory for Risk >= 40 (Shortened session)     │
│   * policy_sms_otp_deprecated    -> Restricted only to ultra-low risk (< 25)         │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Real-World Scenarios Demonstrated

1. **Scenario 1: Trusted Everyday Employee Sign-In (Corporate Residential Fiber)**
   - Known managed laptop, residential ISP ASN, normal single-request traffic, 0 failed logins.
   - **Risk Score**: 15 (LOW).
   - **Conduct Outcome**: Seamless FastPath bypass granted with 12-hour session lifetime and platform biometric option.
2. **Scenario 2: Remote Employee on Commercial VPN & Unmanaged Mobile Device (1 Password Typo)**
   - Unmanaged smartphone, commercial VPN egress ASN, 1 recent password typo.
   - **Risk Score**: 55 (ELEVATED).
   - **Conduct Outcome**: FastPath is silently pruned. Mandatory step-up challenge required: TOTP Authenticator app or FIDO2 hardware key.
3. **Scenario 3: Automated Credential Stuffing & Bot Attack (Tor Exit Node + Headless Browser + Burst IP Rate)**
   - Unrecognized device, Tor exit node ASN, client header anomaly (`python-requests` signature without standard browser headers), 12 auth attempts/min from this IP, and 3 brute-force failures on this account.
   - **Risk Score**: 150 (CRITICAL).
   - **Conduct Outcome**: Dispatches immediate alert to Security Operations Center (SOC) incident sink. All software MFA methods are revoked; only physical FIDO2 WebAuthn touch with PIN and 15-minute emergency session is permitted.

---

## 🚀 Running the Example

Execute from the repository root:
```bash
go run ./examples/04_adaptive_mfa_access/main.go
```

Or run directly within the directory:
```bash
cd examples/04_adaptive_mfa_access
go run main.go
```

---

## 📊 Sample Output & Micro-Benchmark

```
================================================================================
   🛡️ FLUX + CAPACITOR: ADAPTIVE MFA & ZERO-TRUST RISK-BASED ACCESS CONTROL
================================================================================
✔ Deployed Circuit: adaptive_auth_risk_pipeline (Tags: [stream:auth stream:access_control])
✔ Indexed 5 candidate MFA authentication policies into 'auth:mfa_methods'

================================================================================
▶ [STEP 1/3] Scenario 1: Trusted Everyday Employee Sign-In (Corporate Residential Fiber)
================================================================================
  ⚡ [Spark Risk Engine] Evaluated in 10.9ms
     Account: a****@enterprise.corp | Risk: LOW (Score: 15/100)
     Network ASN: RESIDENTIAL | IP Velocity (1m): 2 req/min | Failed Passwords (5m): 1
     Engine Action: SEAMLESS_ACCESS_GRANTED

  🎯 [Conduct Zero-Trust Policy Resolution] Evaluated in 1.9ms
     Candidate Methods Evaluated: 5 | Qualified Policies: 3
     ✨ [Option 1] policy_sms_otp_deprecated    | Policy Decision: map[action:ENTER_SMS_CODE allowed:true max_session_duration_minutes:60 method:SMS_OTP warning:SMS_IS_VULNERABLE_TO_SIM_SWAPPING]
     ✨ [Option 2] policy_biometric_platform    | Policy Decision: map[action:SCAN_BIOMETRIC allowed:true max_session_duration_minutes:480 method:PLATFORM_BIOMETRICS reason:SEAMLESS_KNOWN_DEVICE_BIOMETRIC]
     ✨ [Option 3] policy_seamless_fastpath     | Policy Decision: map[action:BYPASS_MFA allowed:true max_session_duration_minutes:720 method:SEAMLESS_FASTPATH reason:TRUSTED_DEVICE_AND_LOW_RISK_PROFILE]

================================================================================
▶ [STEP 2/3] Scenario 2: Remote Employee on Commercial VPN & Unmanaged Mobile Device (1 Password Typo)
================================================================================
  ⚡ [Spark Risk Engine] Evaluated in 730µs
     Account: b****@enterprise.corp | Risk: ELEVATED (Score: 55/100)
     Network ASN: COMMERCIAL_VPN | IP Velocity (1m): 3 req/min | Failed Passwords (5m): 2
     Engine Action: STEP_UP_BIOMETRIC_OR_TOTP_REQUIRED

  🎯 [Conduct Zero-Trust Policy Resolution] Evaluated in 1.5ms
     Candidate Methods Evaluated: 5 | Qualified Policies: 2
     ✨ [Option 1] policy_totp_authenticator_app | Policy Decision: map[action:ENTER_6_DIGIT_CODE allowed:true max_session_duration_minutes:240 method:TOTP_AUTHENTICATOR reason:STANDARD_STEP_UP_CHALLENGE]
     ✨ [Option 2] policy_fido2_hardware_key    | Policy Decision: map[action:PROMPT_HARDWARE_TOUCH allowed:true max_session_duration_minutes:60 method:FIDO2_HARDWARE_KEY reason:HIGH_ASSURANCE_REQUIRED_FOR_ELEVATED_RISK requires_pin:true]

================================================================================
▶ [STEP 3/3] Scenario 3: Automated Credential Stuffing & Bot Attack (Tor Exit Node + Headless Browser + Burst IP Rate)
================================================================================
      🚨 [SOC INCIDENT SINK] Critical anomaly reported! Security Operations Center dispatched.
  ⚡ [Spark Risk Engine] Evaluated in 813µs
     Account: c****@enterprise.corp | Risk: CRITICAL (Score: 150/100)
     Network ASN: TOR_EXIT | IP Velocity (1m): 13 req/min | Failed Passwords (5m): 4
     Engine Action: LOCKOUT_OR_FIDO2_HARDWARE_TOKEN_REQUIRED

  🎯 [Conduct Zero-Trust Policy Resolution] Evaluated in 614µs
     Candidate Methods Evaluated: 5 | Qualified Policies: 1
     ✨ [Option 1] policy_fido2_hardware_key    | Policy Decision: map[action:PROMPT_HARDWARE_TOUCH allowed:true max_session_duration_minutes:15 method:FIDO2_HARDWARE_KEY reason:HIGH_ASSURANCE_REQUIRED_FOR_ELEVATED_RISK requires_pin:true]

================================================================================
   📊 HIGH-THROUGHPUT ZERO-TRUST SECURITY BENCHMARK (5,000 OPS)
================================================================================
--- ⚡ Spark Risk Assessment Throughput & Latency ---
Throughput:            1,868.88 evals/sec
Mean Spark Latency:    534.70 µs
P50 (Median) Latency:  524.19 µs
P90 Latency:           759.00 µs
P99 Latency:           1.02 ms

--- 🎯 Conduct Zero-Trust Policy Resolution Throughput & Latency ---
Throughput:            1,843.64 queries/sec
Mean Conduct Latency:  541.97 µs
P50 (Median) Latency:  528.31 µs
P90 Latency:           685.26 µs
P99 Latency:           905.88 µs
================================================================================
```
