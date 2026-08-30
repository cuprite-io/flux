# Security Policy

## Supported Versions

We currently support and provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| v0.1.x  | :white_check_mark: |

## Reporting a Vulnerability

We take the security and integrity of Flux seriously. If you believe you have found a security vulnerability, please do not open a public issue. Instead, report it via the following process:

1. **Email us**: Send a detailed report to [security@cuprite.io](mailto:security@cuprite.io).
2. **Details**: Include a description of the vulnerability, a proof of concept, and the potential impact.
3. **Response**: You will receive an acknowledgment within 48 hours.
4. **Disclosure**: We follow coordinated disclosure. We will work with you to fix the issue before making it public.

## Security & Sandboxing Invariants

Flux provides built-in safety mechanisms:
- **Volt Sandbox**: VoltScript expressions are evaluated in a memory-safe, non-Turing-complete environment preventing infinite loops, memory leaks, and unauthorized host OS access.
- **Binary Integrity**: All compiled Circuit bytecode artifacts (`FBWF`) stored in `Capacitor` include an ECMA CRC64 checksum to prevent execution of corrupt or tampered instructions.
- **Node-Level Panic Isolation**: Runtime anomalies in user-provided functions are contained at node boundaries without crashing the host process.
