# gct

CLI for querying and analyzing traces from the GCP Cloud Trace API v1.

## Install

```bash
# From GitHub
uv tool install git+https://github.com/asdf8601/gct

# From source
uv sync
```

Requires `gcloud` CLI for authentication (`gcloud auth print-access-token`).

Set your GCP project:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
```

Or pass `--project` on each invocation.

## Commands

```
gct list       # recent traces
gct services   # services and endpoints seen
gct spans      # distinct span names
gct get ID     # span tree
gct analyze ID # timeline with bottleneck detection
gct search     # filter by service, labels, latency, span name
gct outliers   # find slow traces with per-span breakdown
```

## Common flags

| Flag | Description |
|------|-------------|
| `--start` | Time window start (`30m`, `3h`, `2d`, or RFC3339) |
| `--end` | Time window end (default: now) |
| `--limit` | Max traces to fetch |
| `--service` | Filter by `service.name` (repeatable) |
| `--label` | Filter by label `key=value` (repeatable) |
| `--min-latency` | Min latency (`500ms`, `1s`) |
| `--max-latency` | Max latency (`500ms`, `1s`) |
| `--json` | Raw JSON output (global, before command) |
| `--project` | GCP project ID |

## Examples

```bash
# List traces from the last hour
gct list --start 1h

# Services in the last 3 hours
gct services --start 3h

# Search with latency range
gct search --service my-service --min-latency 300ms --max-latency 500ms

# Find p95 outliers and compare with another service
gct outliers --service my-service --compare other-service

# Analyze a specific trace
gct analyze <trace-id>

# JSON output
gct --json search --service my-service --start 1h
```
