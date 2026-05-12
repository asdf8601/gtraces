## v0.2.3 (2026-05-12)

### Fix

- **search**: push span filter server-side and match nested spans (#4)

## v0.2.2 (2026-04-17)

### Fix

- rename workflow to publish.yml to match PyPI trusted publisher config

## v0.2.1 (2026-04-17)

### Fix

- clarify PyPI as recommended install method in README

## v0.2.0 (2026-04-17)

### BREAKING CHANGE

- CLI entry point renamed from `gct` to `gtraces`. Update
installs with `uv tool install gtraces`.

### Feat

- rename CLI from gct to gtraces and add PyPI release automation
- add conditional compare command for B|A latency analysis
- add programmatic library API for import without gcloud CLI
- add stats command, server-side filters, and search enhancements
- gtraces CLI for GCP Cloud Trace API v1

### Fix

- improve compare reliability under rate limits
- harden compare command and add retry with backoff on 429
- address library API review findings

### Refactor

- fix naming inconsistencies across all commands
