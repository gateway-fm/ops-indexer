# Security Policy

## Supported Versions

This project is preparing an initial open-source release. Until the first public
release is tagged, security support applies to the current `main` branch and any
release candidate branches explicitly announced by maintainers.

## Reporting a Vulnerability

Please do not open public issues for suspected vulnerabilities.

Report security issues privately to:

- security@gateway.fm

Include:

- Affected component or endpoint.
- Reproduction steps or proof of concept.
- Impact and affected deployment assumptions.
- Suggested mitigation, if known.

Maintainers should acknowledge reports within 3 business days and provide an
initial triage response within 10 business days.

## Public Disclosure

Security issues should remain private until a fix or mitigation is available,
unless maintainers and the reporter agree on a different disclosure timeline.

## Scope

In scope:

- chain-indexer service code, protocol definitions, generated Go stubs, and
  deployment materials in this repository.
- Source release materials such as scripts, docs, examples, and generated code.

Out of scope:

- Third-party services operated outside this repository.
- Public test networks and local-only demo environments, except where they
  expose a vulnerability in production guidance or code.
