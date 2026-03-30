#!/usr/bin/env python3
"""gct - CLI for GCP Cloud Trace API v1.

Also importable as a library:

    import gtraces
    gtraces.set_token("my-token")          # or monkey-patch get_token
    traces = gtraces.trace_list("my-project", start="1h")
"""

import functools
import json
import os
import random
import re
import subprocess
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import click

__all__ = [
    "trace_list",
    "trace_services",
    "trace_spans",
    "trace_get",
    "trace_search",
    "trace_outliers",
    "trace_stats",
    "trace_compare",
    "get_token",
    "set_token",
    "fetch_traces",
    "filter_traces",
    "ApiError",
]

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
API = "https://cloudtrace.googleapis.com/v1/projects"

INTERESTING_LABELS = {
    "bids",
    "deadline_ms",
    "actual_deadline_ms",
    "parent_deadline_ms",
    "auctionType",
    "done_by",
    "done_at_ms",
    "ctx_done_at_ms",
    "responses_before_deadline",
    "responses_before_done",
    "drained_count",
    "auction",
    "http.response.status_code",
    "http.ttfb_ms",
    "otel.status_code",
    "otel.status_description",
    "service.name",
    "cloud.region",
    "k8s.cluster.name",
    "placement",
    "publisher_country",
    "abtest",
}

# ── Auth & HTTP ──────────────────────────────────────────────────────────────

_token = None


class ApiError(Exception):
    """Raised on API failures with actionable hints.

    Inherits from Exception (not click.ClickException) so library consumers
    don't need click as a dependency for exception handling.
    """


def get_token():
    """Get access token via gcloud (cached for process lifetime)."""
    global _token
    if _token:
        return _token
    try:
        r = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True,
            text=True,
            check=True,
        )
        _token = r.stdout.strip()
        return _token
    except FileNotFoundError:
        raise ApiError(
            "gcloud not found. Install: https://cloud.google.com/sdk/docs/install"
        )
    except subprocess.CalledProcessError:
        raise ApiError("Auth failed. Run: gcloud auth login")


def set_token(tok):
    """Set the auth token directly, bypassing gcloud CLI.

    Convenience for library consumers who obtain tokens externally
    (e.g. via google-auth ADC).
    """
    global _token
    _token = tok


_MAX_RETRIES = 3
_RETRY_BASE_SEC = 1.0


def api_get(project, path, params=None):
    """GET from Cloud Trace API v1 with retry on 429. Returns parsed JSON."""
    url = f"{API}/{project}{path}"
    if params:
        url += "?" + urlencode(params)
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        req = Request(url, headers={"Authorization": f"Bearer {get_token()}"})
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 429 and attempt < _MAX_RETRIES:
                delay = _RETRY_BASE_SEC * (2**attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
                last_exc = e
                continue
            msgs = {
                401: "Auth expired. Run: gcloud auth login",
                403: f"Permission denied for project '{project}'",
                404: "Not found",
                429: (
                    f"Rate limited after {_MAX_RETRIES} retries. "
                    "Reduce --limit or try again later"
                ),
            }
            raise ApiError(f"{msgs.get(e.code, f'HTTP {e.code}')}\n{body}")
    raise ApiError(f"Request failed after {_MAX_RETRIES} retries: {last_exc}")


def fetch_traces(project, params, max_results=None):
    """Fetch traces with automatic pagination."""
    traces = []
    while True:
        data = api_get(project, "/traces", params)
        traces.extend(data.get("traces", []))
        if max_results and len(traces) >= max_results:
            return traces[:max_results]
        token = data.get("nextPageToken")
        if not token:
            break
        params = {**params, "pageToken": token}
    return traces


# ── Helpers ──────────────────────────────────────────────────────────────────


def _ts(s):
    """RFC3339 string to datetime."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _dur(span):
    """Span duration in milliseconds."""
    return (_ts(span["endTime"]) - _ts(span["startTime"])).total_seconds() * 1000


def _root(spans):
    """Find the root span (no parentSpanId)."""
    for s in spans:
        if not s.get("parentSpanId"):
            return s
    return spans[0] if spans else None


def _fmt_ms(ms):
    """Format milliseconds for display."""
    if ms >= 1000:
        return f"{ms / 1000:.2f}s"
    return f"{ms:.1f}ms"


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(s):
    """Parse relative (1h, 30m, 2d) or RFC3339 to RFC3339 string."""
    m = re.match(r"^(\d+)([mhd])$", s)
    if m:
        n, u = int(m.group(1)), m.group(2)
        delta = {
            "m": timedelta(minutes=n),
            "h": timedelta(hours=n),
            "d": timedelta(days=n),
        }[u]
        return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")
    return s


def _parse_latency(s):
    """Parse '500ms' or '1.5s' to (api_filter_str, ms_float). Returns (None, None) if empty."""
    if not s:
        return None, None
    m = re.match(r"^(\d+(?:\.\d+)?)(ms|s)$", s)
    if not m:
        raise ValueError(f"Bad latency: {s} (use e.g. 500ms, 1s)")
    val, unit = float(m.group(1)), m.group(2)
    ms = val if unit == "ms" else val * 1000
    return f"{m.group(1)}{m.group(2)}", ms


def _parse_labels(label_tuple):
    """Parse ('key=value', ...) tuple into a dict."""
    d = {}
    for label in label_tuple:
        if "=" not in label:
            raise click.BadParameter(f"Expected key=value, got: {label}")
        k, v = label.split("=", 1)
        d[k] = v
    return d or None


def _parse_group_by(group_by):
    """Parse comma-separated grouping keys."""
    if not group_by:
        return []
    return [k.strip() for k in group_by.split(",") if k.strip()]


def _build_params(
    start, end, limit, view="ROOTSPAN", min_latency=None, services=None, labels=None
):
    """Build common API query params."""
    params = {
        "pageSize": min(limit, 100),
        "startTime": _parse_time(start),
        "endTime": end or _now(),
        "view": view,
    }
    parts = []
    if min_latency:
        filt, ms = _parse_latency(min_latency)
        if ms and ms > 0:
            parts.append(f"latency:{filt}")
    if services:
        for svc in services:
            parts.append(f"service.name:{svc}")
    if labels:
        for k, v in labels.items():
            parts.append(f"{k}:{v}")
    if parts:
        params["filter"] = " ".join(parts)
    return params


def _matches_trace(
    trace,
    *,
    span_name=None,
    services=None,
    labels=None,
    min_ms=None,
    max_ms=None,
):
    """Check whether trace matches root/service/label/latency filters."""
    if isinstance(services, str):
        services = (services,)
    spans = trace.get("spans", [])
    r = _root(spans)
    if not r:
        return False
    if span_name and span_name not in (r.get("name") or ""):
        return False
    dur = _dur(r)
    if min_ms is not None and dur < min_ms:
        return False
    if max_ms is not None and dur > max_ms:
        return False
    svc_set = set(services) if services else None
    if svc_set and not any(
        s.get("labels", {}).get("service.name") in svc_set for s in spans
    ):
        return False
    rl = r.get("labels", {})
    if labels and not all(rl.get(k) == v for k, v in labels.items()):
        return False
    return True


def filter_traces(
    traces, *, span_name=None, services=None, labels=None, min_ms=None, max_ms=None
):
    """Filter traces by root span name, service(s), labels, and duration range.

    Unified filter — replaces the former match_traces + _filter_by_labels.
    """
    return [
        t
        for t in traces
        if _matches_trace(
            t,
            span_name=span_name,
            services=services,
            labels=labels,
            min_ms=min_ms,
            max_ms=max_ms,
        )
    ]


def _to_durations(traces):
    """Return sorted (ms, trace) list from traces with root spans."""
    out = []
    for t in traces:
        r = _root(t.get("spans", []))
        if r:
            out.append((_dur(r), t))
    out.sort(key=lambda x: x[0])
    return out


def _resolve_threshold(threshold, pvals):
    """Resolve a threshold string to milliseconds."""
    if threshold in pvals:
        return pvals[threshold]
    _, ms = _parse_latency(threshold)
    if ms is None:
        raise ValueError(
            f"Bad threshold: {threshold} (use p50/p90/p95/p99 or e.g. 500ms)"
        )
    return ms


def _pcts(values):
    """Compute p50/p90/p95/p99 from a sorted list of numbers."""
    n = len(values)
    if n == 0:
        return {"p50": 0, "p90": 0, "p95": 0, "p99": 0}
    targets = {"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99}
    return {k: values[min(int(v * n), n - 1)] for k, v in targets.items()}


def _cli_validate(fn):
    """Decorator: convert library exceptions to Click exceptions in CLI commands."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError as e:
            raise click.BadParameter(str(e))
        except ApiError as e:
            raise click.ClickException(str(e))

    return wrapper


# ── Timeseries helpers ───────────────────────────────────────────────────────

_SPARK = "▁▂▃▄▅▆▇█"


def _sparkline(values):
    """Render a list of numbers as a Unicode sparkline."""
    if not values:
        return ""
    mn, mx = min(values), max(values)
    if mn == mx:
        return _SPARK[3] * len(values)
    rng = mx - mn
    return "".join(_SPARK[min(int((v - mn) / rng * 7), 7)] for v in values)


def _parse_bucket(s):
    """Parse bucket size (5m, 10m, 1h) to timedelta."""
    m = re.match(r"^(\d+)([mhd])$", s)
    if not m:
        raise ValueError(f"Bad bucket: {s} (use e.g. 5m, 1h)")
    n, u = int(m.group(1)), m.group(2)
    return {"m": timedelta(minutes=n), "h": timedelta(hours=n), "d": timedelta(days=n)}[
        u
    ]


def _make_buckets(data, bucket_delta):
    """Split [(datetime, ms)] into aligned time buckets.

    Returns [(label, sorted_durs)] sorted by time, including empty buckets.
    """
    if not data:
        return []
    bsec = bucket_delta.total_seconds()
    min_ts = min(ts for ts, _ in data)
    max_ts = max(ts for ts, _ in data)

    # Align to bucket boundary
    start_epoch = (min_ts.timestamp() // bsec) * bsec
    max_idx = int((max_ts.timestamp() - start_epoch) // bsec)

    bins = defaultdict(list)
    for ts, dur in data:
        idx = int((ts.timestamp() - start_epoch) // bsec)
        bins[idx].append(dur)

    result = []
    for idx in range(max_idx + 1):
        t = datetime.fromtimestamp(start_epoch + idx * bsec, tz=timezone.utc)
        t_end = t + bucket_delta
        label = f"{t.strftime('%H:%M')}-{t_end.strftime('%H:%M')}"
        durs = sorted(bins.get(idx, []))
        result.append((label, durs))
    return result


def _trend(data, n_buckets=12):
    """Compute p50 sparkline with range indicator from [(datetime, ms)]."""
    if len(data) < 2:
        return ""
    min_ts = min(ts for ts, _ in data)
    max_ts = max(ts for ts, _ in data)
    span_secs = (max_ts - min_ts).total_seconds()
    if span_secs <= 0:
        return ""
    bucket_secs = span_secs / n_buckets
    buckets = [[] for _ in range(n_buckets)]
    for ts, dur in data:
        idx = min(int((ts - min_ts).total_seconds() / bucket_secs), n_buckets - 1)
        buckets[idx].append(dur)
    p50s = []
    for b in buckets:
        if b:
            b.sort()
            p50s.append(b[len(b) // 2])
        else:
            p50s.append(0)
    spark = _sparkline(p50s)
    real = [v for v in p50s if v > 0]
    if real:
        return f"{spark} {_fmt_ms(min(real))}-{_fmt_ms(max(real))}"
    return spark


# ── Rendering ────────────────────────────────────────────────────────────────


def _parse_fields(field_str):
    """Parse comma-separated field spec (e.g. 'spans,label:cloud.region')."""
    if not field_str:
        return []
    fields = []
    for f in field_str.split(","):
        f = f.strip()
        if f.startswith("label:"):
            key = f[6:]
            fields.append({"type": "label", "key": key, "header": key})
        elif f == "spans":
            fields.append({"type": "spans", "header": "SPANS"})
        else:
            raise click.BadParameter(
                f"Unknown field: {f}  (use 'spans' or 'label:KEY')"
            )
    return fields


def _extract_field(t, root, field):
    """Extract a field value from a trace."""
    if field["type"] == "label":
        return (root or {}).get("labels", {}).get(field["key"], "")
    if field["type"] == "spans":
        return str(len(t.get("spans", [])))
    return ""


def render_list(traces, show_labels=False, fields=None):
    """Render traces as a compact table with dynamic column widths."""
    if not traces:
        click.echo("No traces found.")
        return

    SEP = "  "

    # Pre-compute all cell values per row
    headers = ["TRACE ID", "ROOT SPAN", "DURATION", "TIME"]
    extra_headers = [f["header"] for f in fields] if fields else []
    all_headers = headers + extra_headers

    rows = []
    for t in traces:
        r = _root(t.get("spans", []))
        tid = t.get("traceId", "?")
        name = r.get("name", "?") if r else "?"
        dur = _fmt_ms(_dur(r)) if r else "?"
        time = r.get("startTime", "?")[:19] if r else "?"
        extra = [_extract_field(t, r, f) for f in fields] if fields else []
        lbl_str = ""
        if show_labels and r:
            lbl = r.get("labels", {})
            interesting = {k: v for k, v in lbl.items() if k in INTERESTING_LABELS}
            if interesting:
                lbl_str = " ".join(f"{k}={v}" for k, v in interesting.items())
        rows.append(([tid, name, dur, time] + extra, lbl_str))

    # Compute column widths from header + data
    col_widths = [len(h) for h in all_headers]
    for cells, _ in rows:
        for i, val in enumerate(cells):
            col_widths[i] = max(col_widths[i], len(val))

    # Right-align DURATION column (index 2)
    right_align = {2}

    def fmt_row(cells):
        parts = []
        for i, val in enumerate(cells):
            w = col_widths[i]
            parts.append(f"{val:>{w}}" if i in right_align else f"{val:<{w}}")
        return SEP.join(parts)

    click.echo(fmt_row(all_headers) + (SEP + "LABELS" if show_labels else ""))
    click.echo("\u2500" * sum(col_widths + [len(SEP) * (len(col_widths) - 1)]))

    for cells, lbl_str in rows:
        line = fmt_row(cells)
        if lbl_str:
            line += SEP + lbl_str
        click.echo(line)


def _bar(offset_ms, dur_ms, total_ms, width):
    """Render a positioned horizontal bar using box-drawing characters."""
    if total_ms <= 0 or width <= 0:
        return ""
    start = offset_ms / total_ms * width
    length = dur_ms / total_ms * width
    si = int(start)
    lead = " " * si
    full = int(length)
    half = (length - full) >= 0.5
    bar = "\u2501" * full + ("\u2578" if half else "")  # ━ and ╸
    if not bar and dur_ms > 0:
        bar = "\u2578"  # ╸
    return f"{lead}{bar}"


def render_tree(trace, bars=False, name_width=35):
    """Render trace as a span tree, optionally with waterfall timing bars."""
    spans = trace.get("spans", [])
    if not spans:
        click.echo("No spans.")
        return

    r = _root(spans)
    total = _dur(r) if r else 0
    root_start = _ts(r["startTime"]) if r else _ts(spans[0]["startTime"])
    click.echo(
        f"Trace {trace.get('traceId', '?')} | {_fmt_ms(total)} | {len(spans)} spans\n"
    )

    children = {}
    for s in spans:
        pid = s.get("parentSpanId")
        children.setdefault(pid, []).append(s)

    name_col = name_width
    dur_col = 10
    bar_width = 0
    if bars:
        try:
            term_width = os.get_terminal_size().columns
        except (AttributeError, ValueError, OSError):
            term_width = 120
        bar_width = max(20, term_width - name_col - dur_col - 4)

    lines = []

    def walk(span, prefix="", last=True):
        d = _dur(span)
        name = span.get("name", "?")
        if span is r:
            tree_str = name
        else:
            conn = "\u2514\u2500 " if last else "\u251c\u2500 "
            tree_str = f"{prefix}{conn}{name}"
        dur_str = _fmt_ms(d)
        if bars:
            offset = (_ts(span["startTime"]) - root_start).total_seconds() * 1000
            if len(tree_str) > name_col:
                tree_str = tree_str[: name_col - 2] + ".."
            lines.append((tree_str, dur_str, _bar(offset, d, total, bar_width)))
        else:
            lines.append((tree_str, dur_str))

        ext = "   " if last else "\u2502  "
        kids = sorted(
            children.get(span.get("spanId"), []),
            key=lambda x: x["startTime"],
        )
        for i, kid in enumerate(kids):
            walk(kid, prefix + ext, i == len(kids) - 1)

    if r:
        walk(r)
    if bars:
        for tree_str, dur_str, bar_str in lines:
            click.echo(f"{tree_str:<{name_col}}  {dur_str:>{dur_col - 2}}  {bar_str}")
    else:
        for tree_str, dur_str in lines:
            click.echo(f"{tree_str}  {dur_str}")


def render_timeline(trace):
    """Render chronological timeline with bottleneck summary."""
    spans = trace.get("spans", [])
    if not spans:
        click.echo("No spans.")
        return

    r = _root(spans)
    root_start = _ts(r["startTime"]) if r else _ts(spans[0]["startTime"])
    total = _dur(r) if r else 1

    # Find labels that are identical on every span — show once in header
    all_labels = [s.get("labels", {}) for s in spans]
    common = {}
    if all_labels:
        shared_keys = set(all_labels[0].keys())
        for lbl in all_labels[1:]:
            shared_keys &= set(lbl.keys())
        for k in shared_keys:
            vals = {lbl[k] for lbl in all_labels}
            if len(vals) == 1:
                common[k] = next(iter(vals))

    header = " ".join(
        f"{k}={v}" for k, v in sorted(common.items()) if k in INTERESTING_LABELS
    )
    click.echo(
        f"Trace {trace.get('traceId', '?')} | {_fmt_ms(total)} | {len(spans)} spans"
    )
    if header:
        click.echo(f"  {header}")
    click.echo()
    click.echo(f"{'OFFSET':>10}  {'SPAN':<55} {'DURATION':>10} {'%':>5}  LABELS")
    click.echo("\u2500" * 110)

    by_id = {s.get("spanId"): s for s in spans}

    def depth(span):
        d, pid = 0, span.get("parentSpanId")
        while pid and pid in by_id:
            d += 1
            pid = by_id[pid].get("parentSpanId")
        return d

    ranked = []
    for s in sorted(spans, key=lambda x: x["startTime"]):
        d = _dur(s)
        offset = (_ts(s["startTime"]) - root_start).total_seconds() * 1000
        dep = depth(s)
        pct = d / total * 100 if total > 0 else 0

        labels = s.get("labels", {})
        unique = {
            k: v
            for k, v in labels.items()
            if k in INTERESTING_LABELS and (k not in common or common[k] != v)
        }
        lbl = " ".join(f"{k}={v}" for k, v in unique.items())

        indent = "  " * dep
        name = f"{indent}{s.get('name', '?')}"
        slow = " *" if d > 100 else ""
        click.echo(
            f"+{_fmt_ms(offset):>9}  {name:<55} {_fmt_ms(d) + slow:>12} {pct:>4.0f}%  {lbl}"
        )
        ranked.append((s.get("name", "?"), d, pct))

    ranked.sort(key=lambda x: -x[1])
    click.echo("\nSlowest spans:")
    for name, d, pct in ranked[:5]:
        click.echo(f"  {name:<55} {_fmt_ms(d):>10}  ({pct:.0f}%)")


def _render_comparison(comparison, primary_pvals, services):
    """Render cross-service comparison text from structured data."""
    compare_svc = comparison["service"]
    primary_label = ", ".join(services) if services else "primary"
    dist = comparison.get("distribution")

    if not dist:
        click.echo(f"  No traces found for {compare_svc}.")
        return

    cmp_pvals = dist["percentiles"]
    cmp_n = dist["count"]

    click.echo(f"  {compare_svc} latency ({cmp_n} traces):\n")
    click.echo(f"  {'PCTL':<6} {primary_label:<30} {compare_svc}")
    click.echo(f"  {'─' * 70}")
    for label in ("p50", "p90", "p95", "p99"):
        click.echo(
            f"  {label:<6} {_fmt_ms(primary_pvals[label]):<30} "
            f"{_fmt_ms(cmp_pvals[label])}"
        )

    click.echo("\n  During outlier windows:")
    click.echo(
        f"  {'#':<3} {'TIME':<26} "
        f"{primary_label + ' latency':<25} {compare_svc + ' latency'}"
    )
    click.echo(f"  {'─' * 80}")

    for i, w in enumerate(comparison.get("windows", []), 1):
        if "avgMs" in w:
            cmp_str = (
                f"avg {_fmt_ms(w['avgMs'])}, max {_fmt_ms(w['maxMs'])} "
                f"({w['count']} traces)"
            )
        else:
            cmp_str = "(no traces)"
        click.echo(f"  {i:<3} {w['time']:<26} {_fmt_ms(w['primaryMs']):<25} {cmp_str}")


def _fmt_opt_ms(ms):
    """Format optional milliseconds value."""
    return _fmt_ms(ms) if ms is not None else "-"


def render_compare(result):
    """Render conditional B|A comparison in text mode."""
    a = result["conditionA"]
    b = result["sampleB"]
    group_by = result["meta"].get("groupBy", [])

    click.echo("Conditional comparison B | A (descriptive, non-causal)")
    click.echo(
        f"A sample: {a['tracesMatched']} traces from {a['tracesFetched']} fetched "
        f"(missing root: {a['skippedNoRoot']})"
    )
    click.echo(
        f"B windows: {b['windowsSucceeded']}/{b['windowsTotal']} succeeded, "
        f"{b['windowsFailed']} failed"
    )
    click.echo(
        f"B traces: seen={b['tracesSeen']} deduped={b['tracesDeduped']} "
        f"(missing root: {b['skippedNoRoot']})"
    )
    click.echo()

    dist = b["distribution"]
    if dist["count"] == 0:
        click.echo("No B traces matched in conditioned windows.")
    else:
        p = dist["percentiles"]
        click.echo(
            f"B latency  avg {_fmt_opt_ms(dist['avgMs'])}  "
            f"p50 {_fmt_opt_ms(p['p50'])}  p90 {_fmt_opt_ms(p['p90'])}  "
            f"p95 {_fmt_opt_ms(p['p95'])}  p99 {_fmt_opt_ms(p['p99'])}"
        )

    groups = result.get("groups", [])
    if groups:
        click.echo()
        click.echo(
            f"{'GROUP':<42} {'COUNT':>6} {'avg':>10} {'p50':>10} {'p90':>10} {'p95':>10} {'p99':>10}"
        )
        click.echo("─" * 108)
        for g in groups:
            key = g.get("key", {})
            label = (
                " ".join(f"{k}={key.get(k, '') or '(none)'}" for k in group_by)
                or "(all)"
            )
            p = g["percentiles"]
            click.echo(
                f"{label:<42} {g['count']:>6} {_fmt_opt_ms(g['avgMs']):>10} "
                f"{_fmt_opt_ms(p['p50']):>10} {_fmt_opt_ms(p['p90']):>10} "
                f"{_fmt_opt_ms(p['p95']):>10} {_fmt_opt_ms(p['p99']):>10}"
            )

    if result.get("warnings"):
        click.echo()
        click.echo("Warnings:")
        for w in result["warnings"]:
            click.echo(f"  - [{w['code']}] {w['message']}")


# ── Programmatic API ─────────────────────────────────────────────────────────


def trace_list(
    project,
    *,
    start="1h",
    end=None,
    limit=20,
    services=(),
    labels=None,
    min_latency=None,
    max_latency=None,
):
    """Fetch and filter recent traces. Returns list of trace dicts."""
    _, max_ms = _parse_latency(max_latency)
    params = _build_params(
        start, end, limit, min_latency=min_latency, services=services, labels=labels
    )
    traces = fetch_traces(project, params, max_results=limit)
    if services or max_ms is not None:
        traces = filter_traces(traces, services=services, max_ms=max_ms)
    return traces


def trace_get(project, trace_id):
    """Fetch a single trace by ID. Returns trace dict."""
    return api_get(project, f"/traces/{trace_id}")


def trace_services(project, *, start="3h", end=None, limit=200):
    """Collect service and endpoint counts.

    Returns {services: dict, endpoints: dict, trace_count: int}.
    """
    params = _build_params(start, end, limit)
    traces = fetch_traces(project, params, max_results=limit)
    if not traces:
        return {"services": {}, "endpoints": {}, "trace_count": 0}
    svc_counts = Counter()
    ep_counts = Counter()
    for t in traces:
        for s in t.get("spans", []):
            svc = s.get("labels", {}).get("service.name")
            if svc:
                svc_counts[svc] += 1
            if not s.get("parentSpanId"):
                ep_counts[s.get("name", "?")] += 1
    return {
        "services": dict(svc_counts),
        "endpoints": dict(ep_counts),
        "trace_count": len(traces),
    }


def trace_spans(
    project,
    *,
    start="1h",
    end=None,
    limit=20,
    services=(),
    min_latency=None,
    max_latency=None,
):
    """Collect distinct span name counts.

    Returns {spans: dict[str, int], trace_count: int}.
    """
    _, max_ms = _parse_latency(max_latency)
    params = _build_params(
        start, end, limit, view="COMPLETE", min_latency=min_latency, services=services
    )
    traces = fetch_traces(project, params, max_results=limit)
    traces = filter_traces(traces, services=services, max_ms=max_ms)
    span_counts = Counter()
    for t in traces:
        for s in t.get("spans", []):
            span_counts[s.get("name", "?")] += 1
    return {"spans": dict(span_counts.most_common()), "trace_count": len(traces)}


def trace_search(
    project,
    *,
    start="1h",
    end=None,
    limit=50,
    span_name=None,
    labels=None,
    min_latency=None,
    max_latency=None,
    services=(),
    parent_span_id=None,
    order_asc=None,
    order_desc=None,
):
    """Search traces with client-side filtering.

    Without parent_span_id: returns list of trace dicts.
    With parent_span_id: returns list of {traceId, spans} dicts containing
    only the spans matching that parent.
    """
    if order_asc and order_desc:
        raise ValueError("Cannot use both order_asc and order_desc")
    _, min_ms = _parse_latency(min_latency)
    _, max_ms = _parse_latency(max_latency)
    view = "COMPLETE" if parent_span_id else "ROOTSPAN"
    params = _build_params(
        start,
        end,
        limit,
        view=view,
        min_latency=min_latency,
        services=services,
        labels=labels,
    )
    traces = fetch_traces(project, params, max_results=limit)

    if not parent_span_id:
        filtered = filter_traces(
            traces,
            span_name=span_name,
            services=services or None,
            min_ms=min_ms,
            max_ms=max_ms,
        )
        if order_asc or order_desc:
            reverse = order_desc is not None
            filtered.sort(
                key=lambda t: _dur(r) if (r := _root(t.get("spans", []))) else 0,
                reverse=reverse,
            )
        return filtered

    matches = []
    for t in traces:
        matched_spans = [
            s
            for s in t.get("spans", [])
            if s.get("parentSpanId") == parent_span_id
            and (not span_name or s.get("name") == span_name)
        ]
        if matched_spans:
            matches.append({"traceId": t["traceId"], "spans": matched_spans})
    return matches


def trace_outliers(
    project,
    *,
    start="1h",
    end=None,
    limit=50,
    services=(),
    labels=None,
    min_latency=None,
    max_latency=None,
    threshold="p95",
    top=5,
    compare_svc=None,
):
    """Find outlier traces with per-span breakdown. Always returns a dict.

    Keys: distribution, count, threshold, thresholdMs, outliers.
    Optional: comparison (when compare_svc is given).
    """
    _, max_ms = _parse_latency(max_latency)
    params = _build_params(
        start,
        end,
        limit,
        view="COMPLETE",
        min_latency=min_latency,
        services=services,
        labels=labels,
    )
    all_traces = fetch_traces(project, params, max_results=limit)
    filtered = filter_traces(all_traces, max_ms=max_ms)
    durations = _to_durations(filtered)

    if not durations:
        return {
            "distribution": {"p50": 0, "p90": 0, "p95": 0, "p99": 0},
            "count": 0,
            "threshold": threshold,
            "thresholdMs": 0,
            "outliers": [],
        }

    pvals = _pcts([ms for ms, _ in durations])
    n = len(durations)
    thresh_ms = _resolve_threshold(threshold, pvals)
    outlier_list = [(ms, t) for ms, t in durations if ms >= thresh_ms]
    outlier_list.sort(key=lambda x: -x[0])
    outlier_list = outlier_list[:top]

    json_out = []
    for total_ms, t in outlier_list:
        tid = t.get("traceId")
        span_self = _span_breakdown(t.get("spans", []))
        total_self = sum(ms for _, ms in span_self) or 1
        json_out.append(
            {
                "traceId": tid,
                "totalMs": round(total_ms, 1),
                "totalSelfMs": round(total_self, 1),
                "spans": [
                    {
                        "name": name,
                        "selfMs": round(ms, 1),
                        "pct": round(ms / total_self * 100),
                    }
                    for name, ms in span_self[:8]
                ],
            }
        )

    result = {
        "distribution": {k: round(v, 1) for k, v in pvals.items()},
        "count": n,
        "threshold": threshold,
        "thresholdMs": round(thresh_ms, 1),
        "outliers": json_out,
    }

    if compare_svc:
        result["comparison"] = _compare_services(
            project,
            outlier_list,
            all_traces,
            compare_svc,
        )

    return result


def trace_stats(
    project,
    *,
    start="1h",
    end=None,
    limit=100,
    span_pattern=None,
    group_by=None,
    services=(),
    labels=None,
    min_latency=None,
    max_latency=None,
    bucket=None,
    sparkline=False,
):
    """Compute latency stats, optionally grouped by span labels.

    Parameters:
      group_by   — list of label keys to group by, or None
      labels     — dict of label filters, or None
      bucket     — bucket size string (e.g. '5m', '1h'), or None
      sparkline  — include trend data per group

    Returns:
      {} — when no traces found.
      {"totalSpans": 0, "totalTraces": int} — when traces found but no spans matched.
      Full dict with totalSpans, totalTraces, percentiles, groups, etc. on success.
    """
    _, max_ms = _parse_latency(max_latency)
    group_keys = list(group_by) if group_by else []
    bucket_delta = _parse_bucket(bucket) if bucket else None

    params = _build_params(
        start,
        end,
        limit,
        view="COMPLETE",
        min_latency=min_latency,
        services=services,
        labels=labels,
    )
    all_traces = fetch_traces(project, params, max_results=limit)
    filtered = filter_traces(all_traces, max_ms=max_ms)

    if not filtered:
        return {}

    groups = defaultdict(list)
    total_spans = 0

    for t in filtered:
        for s in t.get("spans", []):
            if span_pattern and span_pattern not in s.get("name", ""):
                continue
            total_spans += 1
            span_ts = _ts(s["startTime"])
            dur = _dur(s)
            if group_keys:
                lbl = s.get("labels", {})
                key = tuple(lbl.get(k, "") for k in group_keys)
                groups[key].append((span_ts, dur))
            else:
                groups[()].append((span_ts, dur))

    if not total_spans:
        return {"totalSpans": 0, "totalTraces": len(filtered)}

    sorted_groups = sorted(groups.items(), key=lambda x: -len(x[1]))

    json_groups = []
    for key, data in sorted_groups:
        durs = sorted(d for _, d in data)
        entry = {
            "count": len(durs),
            "avgMs": round(sum(durs) / len(durs), 1),
            "percentiles": {k: round(v, 1) for k, v in _pcts(durs).items()},
        }
        if group_keys:
            entry["key"] = dict(zip(group_keys, key))
        if bucket_delta:
            bkts = _make_buckets(data, bucket_delta)
            entry["buckets"] = [
                {
                    "time": lbl,
                    "count": len(bd),
                    "avgMs": round(sum(bd) / len(bd), 1) if bd else None,
                    "percentiles": (
                        {k: round(v, 1) for k, v in _pcts(bd).items()} if bd else None
                    ),
                }
                for lbl, bd in bkts
            ]
        if sparkline:
            trend_vals = _trend(data)
            if trend_vals:
                entry["trend"] = trend_vals
        json_groups.append(entry)

    out = {"totalSpans": total_spans, "totalTraces": len(filtered)}
    if span_pattern:
        out["span"] = span_pattern
    if group_keys:
        out["groupBy"] = group_keys
        out["groups"] = json_groups
    elif len(json_groups) == 1:
        out.update(json_groups[0])
    if bucket:
        out["bucket"] = bucket
    return out


def _pct_or_none(values):
    """Compute rounded percentiles or nulls for empty lists."""
    if not values:
        return {"p50": None, "p90": None, "p95": None, "p99": None}
    return {k: round(v, 1) for k, v in _pcts(sorted(values)).items()}


def _avg_or_none(values):
    """Compute rounded average or null for empty lists."""
    if not values:
        return None
    return round(sum(values) / len(values), 1)


def trace_compare(
    project,
    *,
    start="1h",
    end=None,
    limit=50,
    a_services=(),
    a_labels=None,
    a_span_name=None,
    a_min_latency=None,
    a_max_latency=None,
    b_service=None,
    b_labels=None,
    b_span_name=None,
    window_sec=30,
    group_by=None,
):
    """Describe B latency conditioned on traces where A matches filters.

    This is descriptive co-occurrence analysis in time windows, not causal
    inference across traces.
    """
    if not b_service:
        raise ValueError("--b-service is required")
    if window_sec <= 0:
        raise ValueError("--window-sec must be > 0")

    resolved_end = end or _now()
    group_keys = list(group_by or [])
    _, a_min_ms = _parse_latency(a_min_latency)
    _, a_max_ms = _parse_latency(a_max_latency)

    a_params = _build_params(
        start,
        resolved_end,
        limit,
        view="ROOTSPAN",
        min_latency=a_min_latency,
        services=a_services,
        labels=a_labels,
    )
    a_traces = fetch_traces(project, a_params, max_results=limit)
    a_roots = []
    skipped_a_no_root = 0
    for t in a_traces:
        r = _root(t.get("spans", []))
        if not r:
            skipped_a_no_root += 1
            continue
        if not _matches_trace(
            t,
            span_name=a_span_name,
            services=a_services or None,
            labels=a_labels,
            min_ms=a_min_ms,
            max_ms=a_max_ms,
        ):
            continue
        a_roots.append(r)

    warnings = []
    if not a_roots:
        warnings.append(
            {
                "code": "A_EMPTY_SAMPLE",
                "message": "No A traces matched the given condition",
            }
        )

    # Build per-root windows, then merge overlapping ones to reduce API calls
    raw_windows = []
    for r in a_roots:
        t_start = _ts(r["startTime"])
        raw_windows.append(
            (
                t_start - timedelta(seconds=window_sec),
                t_start + timedelta(seconds=window_sec),
            )
        )
    raw_windows.sort()

    windows = []
    for ws, we in raw_windows:
        if windows and ws <= windows[-1][1]:
            # Overlapping — extend the previous window
            prev_s, prev_e = windows[-1]
            windows[-1] = (prev_s, max(prev_e, we))
        else:
            windows.append((ws, we))

    # Format merged windows as RFC3339 strings
    windows = [
        (ws.strftime("%Y-%m-%dT%H:%M:%SZ"), we.strftime("%Y-%m-%dT%H:%M:%SZ"))
        for ws, we in windows
    ]

    b_seen = 0
    b_roots_by_trace = {}
    b_skipped_no_root = 0
    windows_succeeded = 0
    windows_failed = 0

    def _fetch_window(win):
        win_start, win_end = win
        params = _build_params(
            win_start,
            win_end,
            100,
            view="ROOTSPAN",
            services=(b_service,),
            labels=b_labels,
        )
        try:
            traces = fetch_traces(project, params, max_results=100)
            return {"ok": True, "traces": traces, "start": win_start, "end": win_end}
        except (ApiError, OSError, ValueError) as e:
            return {
                "ok": False,
                "error": str(e),
                "start": win_start,
                "end": win_end,
            }

    if windows:
        with ThreadPoolExecutor(max_workers=min(len(windows), 8)) as pool:
            futures = {pool.submit(_fetch_window, w): w for w in windows}
            for fut in as_completed(futures):
                data = fut.result()
                if not data["ok"]:
                    windows_failed += 1
                    warnings.append(
                        {
                            "code": "B_WINDOW_FETCH_FAILED",
                            "message": (
                                f"window {data['start']}..{data['end']} failed: "
                                f"{data['error'].splitlines()[0]}"
                            ),
                        }
                    )
                    continue

                windows_succeeded += 1
                traces = data["traces"]
                for t in traces:
                    r = _root(t.get("spans", []))
                    if not r:
                        b_skipped_no_root += 1
                        continue
                    if not _matches_trace(
                        t,
                        span_name=b_span_name,
                        services=(b_service,),
                        labels=b_labels,
                    ):
                        continue
                    b_seen += 1
                    tid = t.get("traceId")
                    if tid and tid not in b_roots_by_trace:
                        b_roots_by_trace[tid] = r

    if windows and windows_succeeded == 0:
        warnings.append(
            {
                "code": "B_ALL_WINDOWS_FAILED",
                "message": "All B window fetches failed; result is empty",
            }
        )

    b_durations = sorted(_dur(r) for r in b_roots_by_trace.values())
    groups = defaultdict(list)
    if group_keys:
        for r in b_roots_by_trace.values():
            rl = r.get("labels", {})
            key = tuple(rl.get(k, "") for k in group_keys)
            groups[key].append(_dur(r))

    json_groups = []
    for key, durs in groups.items():
        durs = sorted(durs)
        json_groups.append(
            {
                "key": dict(zip(group_keys, key)),
                "count": len(durs),
                "avgMs": _avg_or_none(durs),
                "percentiles": _pct_or_none(durs),
            }
        )

    json_groups.sort(
        key=lambda g: (
            -g["count"],
            json.dumps(g.get("key", {}), sort_keys=True, separators=(",", ":")),
        )
    )

    dist = {
        "count": len(b_durations),
        "avgMs": _avg_or_none(b_durations),
        "percentiles": _pct_or_none(b_durations),
    }

    return {
        "meta": {
            "schemaVersion": "compare.v1",
            "project": project,
            "start": _parse_time(start),
            "end": resolved_end,
            "limit": limit,
            "windowSec": window_sec,
            "groupBy": group_keys,
        },
        "conditionA": {
            "filters": {
                "services": list(a_services),
                "labels": a_labels or {},
                "spanName": a_span_name,
                "minLatency": a_min_latency,
                "maxLatency": a_max_latency,
            },
            "tracesFetched": len(a_traces),
            "tracesMatched": len(a_roots),
            "skippedNoRoot": skipped_a_no_root,
        },
        "sampleB": {
            "filters": {
                "service": b_service,
                "labels": b_labels or {},
                "spanName": b_span_name,
            },
            "windowsTotal": len(windows),
            "windowsSucceeded": windows_succeeded,
            "windowsFailed": windows_failed,
            "tracesSeen": b_seen,
            "tracesDeduped": len(b_roots_by_trace),
            "skippedNoRoot": b_skipped_no_root,
            "distribution": dist,
        },
        "groups": json_groups,
        "warnings": warnings,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────


@click.group()
@click.option(
    "--project",
    envvar="GOOGLE_CLOUD_PROJECT",
    required=True,
    help="GCP project ID (or set GOOGLE_CLOUD_PROJECT)",
)
@click.option("--json", "as_json", is_flag=True, help="Raw JSON output")
@click.pass_context
def cli(ctx, project, as_json):
    """gct - query and analyze GCP Cloud Traces."""
    ctx.ensure_object(dict)
    ctx.obj["project"] = project
    ctx.obj["json"] = as_json
    # Pre-warm auth token before any concurrency
    try:
        get_token()
    except ApiError as e:
        raise click.ClickException(str(e))


@cli.command("list")
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=20, show_default=True, type=int, help="Max traces to fetch"
)
@click.option(
    "--service", "services", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option("--label", "labels", multiple=True, help="Label key=value (repeatable)")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.pass_context
@_cli_validate
def list_cmd(ctx, start, end, limit, services, labels, min_latency, max_latency):
    """List recent traces."""
    label_dict = _parse_labels(labels)
    traces = trace_list(
        ctx.obj["project"],
        start=start,
        end=end,
        limit=limit,
        services=services,
        labels=label_dict,
        min_latency=min_latency,
        max_latency=max_latency,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(traces, indent=2))
    else:
        render_list(traces)


@cli.command()
@click.option(
    "--start",
    default="3h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=200, show_default=True, type=int, help="Max traces to fetch"
)
@click.pass_context
@_cli_validate
def services(ctx, start, end, limit):
    """List services and endpoints seen in recent traces."""
    result = trace_services(ctx.obj["project"], start=start, end=end, limit=limit)
    if not result["services"] and not result["endpoints"]:
        click.echo("No traces found.")
        return

    if ctx.obj["json"]:
        out = {k: v for k, v in result.items() if k != "trace_count"}
        click.echo(json.dumps(out, indent=2))
        return

    svc_counts = Counter(result["services"])
    ep_counts = Counter(result["endpoints"])
    click.echo(f"{'SERVICE':<45} TRACES")
    click.echo("\u2500" * 55)
    for svc, n in svc_counts.most_common():
        click.echo(f"  {svc:<45} {n}")
    click.echo()
    click.echo(f"{'ENDPOINT':<45} TRACES")
    click.echo("\u2500" * 55)
    for ep, n in ep_counts.most_common():
        click.echo(f"  {ep:<45} {n}")
    click.echo(f"\nScanned {result['trace_count']} traces from last {start}.")


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=20, show_default=True, type=int, help="Max traces to fetch"
)
@click.option(
    "--service", "services", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.pass_context
@_cli_validate
def spans(ctx, start, end, limit, services, min_latency, max_latency):
    """List distinct span names from sampled traces."""
    result = trace_spans(
        ctx.obj["project"],
        start=start,
        end=end,
        limit=limit,
        services=services,
        min_latency=min_latency,
        max_latency=max_latency,
    )
    if not result["spans"]:
        click.echo("No traces found.")
        return

    if ctx.obj["json"]:
        click.echo(json.dumps(result["spans"], indent=2))
        return

    span_counts = Counter(result["spans"])
    click.echo(f"{'SPAN NAME':<60} COUNT")
    click.echo("\u2500" * 70)
    for name, n in span_counts.most_common():
        click.echo(f"  {name:<60} {n}")
    click.echo(f"\nSampled {result['trace_count']} traces.")


@cli.command()
@click.argument("trace_id")
@click.option("--bars", is_flag=True, help="Show waterfall timing bars")
@click.option(
    "--name-width",
    default=35,
    show_default=True,
    type=int,
    help="Span name column width",
)
@click.pass_context
@_cli_validate
def get(ctx, trace_id, bars, name_width):
    """Show trace as a span tree."""
    trace = trace_get(ctx.obj["project"], trace_id)
    if ctx.obj["json"]:
        click.echo(json.dumps(trace, indent=2))
    else:
        render_tree(trace, bars=bars, name_width=name_width)


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=50, show_default=True, type=int, help="Max traces to fetch"
)
@click.option("--span-name", default=None, help="Root span name (substring match)")
@click.option("--label", "labels", multiple=True, help="Label key=value (repeatable)")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.option(
    "--service", "services", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option(
    "--parent-span-id",
    default=None,
    help="Find spans with this parentSpanId (fetches full traces)",
)
@click.option("--show-labels", is_flag=True, help="Show interesting labels in output")
@click.option(
    "--extra-fields",
    "field_str",
    default=None,
    help="Extra columns (e.g. spans,label:cloud.region,label:placement)",
)
@click.option(
    "--order-asc", default=None, type=click.Choice(["duration"]), help="Sort ascending"
)
@click.option(
    "--order-desc",
    default=None,
    type=click.Choice(["duration"]),
    help="Sort descending",
)
@click.pass_context
@_cli_validate
def search(
    ctx,
    start,
    end,
    limit,
    span_name,
    labels,
    min_latency,
    max_latency,
    services,
    parent_span_id,
    show_labels,
    field_str,
    order_asc,
    order_desc,
):
    """Search traces with client-side filtering.

    When --parent-span-id is used, full trace details are fetched to match
    inner spans (not just root spans). Useful for cross-service correlation.

    \b
    Examples:
      gct search --span-name "POST /v1/rtb" --min-latency 500ms
      gct search --min-latency 300ms --max-latency 500ms --service my-service
      gct search --service my-service --parent-span-id 123456
    """
    p = ctx.obj["project"]
    label_dict = _parse_labels(labels)
    fields = _parse_fields(field_str)

    result = trace_search(
        p,
        start=start,
        end=end,
        limit=limit,
        span_name=span_name,
        labels=label_dict,
        min_latency=min_latency,
        max_latency=max_latency,
        services=services,
        parent_span_id=parent_span_id,
        order_asc=order_asc,
        order_desc=order_desc,
    )

    if not parent_span_id:
        if ctx.obj["json"]:
            click.echo(json.dumps(result, indent=2))
        else:
            render_list(result, show_labels=show_labels, fields=fields)
            click.echo(f"\n{len(result)} traces matched.")
        return

    if not result:
        click.echo("No spans matched.")
        return

    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
        return

    total_spans = sum(len(m["spans"]) for m in result)
    click.echo(
        f"Found {total_spans} span(s) in {len(result)} trace(s) "
        f"with parentSpanId={parent_span_id}\n"
    )
    click.echo(
        f"{'TRACE ID':<36}  {'SPAN NAME':<40}  "
        f"{'DURATION':>10}  {'SPAN ID':<20}  LABELS"
    )
    click.echo("\u2500" * 120)
    for m in result:
        tid = m.get("traceId", "?")
        for s in m["spans"]:
            name = s.get("name", "?")[:40]
            dur = _fmt_ms(_dur(s))
            sid = s.get("spanId", "?")
            lbl_d = s.get("labels", {})
            interesting = {k: v for k, v in lbl_d.items() if k in INTERESTING_LABELS}
            lbl = " ".join(f"{k}={v}" for k, v in interesting.items())
            click.echo(f"{tid:<36}  {name:<40}  {dur:>10}  {sid:<20}  {lbl}")


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=50, show_default=True, type=int, help="Max A traces to fetch"
)
@click.option(
    "--a-service",
    "a_services",
    multiple=True,
    help="A filter: service.name (repeatable)",
)
@click.option("--a-label", "a_labels", multiple=True, help="A filter: label key=value")
@click.option(
    "--a-span-name",
    default=None,
    help="A filter: root span name (substring match)",
)
@click.option(
    "--a-min-latency",
    default=None,
    help="A filter: min root latency (500ms, 1s)",
)
@click.option(
    "--a-max-latency",
    default=None,
    help="A filter: max root latency (500ms, 1s)",
)
@click.option(
    "--b-service",
    required=True,
    help="B target service.name (required)",
)
@click.option("--b-label", "b_labels", multiple=True, help="B filter: label key=value")
@click.option(
    "--b-span-name",
    default=None,
    help="B filter: root span name (substring match)",
)
@click.option(
    "--window-sec",
    default=30,
    show_default=True,
    type=int,
    help="Conditioning window around each A trace (seconds)",
)
@click.option(
    "--group-by",
    default=None,
    help="Comma-separated B root label keys to group by",
)
@click.pass_context
@_cli_validate
def compare(
    ctx,
    start,
    end,
    limit,
    a_services,
    a_labels,
    a_span_name,
    a_min_latency,
    a_max_latency,
    b_service,
    b_labels,
    b_span_name,
    window_sec,
    group_by,
):
    """Describe B latency conditioned on A traces (descriptive, non-causal).

    \b
    Examples:
      gct compare --a-service config-service --b-service ssp-service-go
      gct compare --a-service config-service --a-min-latency 600ms --b-service ssp-service-go
      gct --json compare --a-service config-service --b-service ssp-service-go --group-by cloud.region
    """
    result = trace_compare(
        ctx.obj["project"],
        start=start,
        end=end,
        limit=limit,
        a_services=a_services,
        a_labels=_parse_labels(a_labels),
        a_span_name=a_span_name,
        a_min_latency=a_min_latency,
        a_max_latency=a_max_latency,
        b_service=b_service,
        b_labels=_parse_labels(b_labels),
        b_span_name=b_span_name,
        window_sec=window_sec,
        group_by=_parse_group_by(group_by),
    )

    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        render_compare(result)


@cli.command()
@click.argument("trace_id")
@click.pass_context
@_cli_validate
def analyze(ctx, trace_id):
    """Timeline analysis with bottleneck detection."""
    trace = trace_get(ctx.obj["project"], trace_id)
    if ctx.obj["json"]:
        click.echo(json.dumps(trace, indent=2))
    else:
        render_timeline(trace)


# ── Outliers helpers ─────────────────────────────────────────────────────────


def _span_breakdown(all_spans):
    """Compute per-span exclusive (self) time. Returns sorted [(name, ms)]."""
    children_dur = defaultdict(float)
    for s in all_spans:
        pid = s.get("parentSpanId")
        if pid:
            children_dur[pid] += _dur(s)

    span_self = []
    for s in all_spans:
        sid = s.get("spanId")
        self_time = max(0, _dur(s) - children_dur.get(sid, 0))
        if self_time > 0:
            span_self.append((s.get("name", "?"), self_time))

    span_self.sort(key=lambda x: -x[1])
    return span_self


def _compare_services(project, outlier_list, all_traces, compare_svc):
    """Cross-service latency comparison during outlier windows.

    Returns comparison data dict with distribution and per-window metrics.
    """
    cmp_durations = _to_durations(filter_traces(all_traces, services=compare_svc))

    comparison = {"service": compare_svc, "distribution": None, "windows": []}

    if not cmp_durations:
        return comparison

    cmp_pvals = _pcts([ms for ms, _ in cmp_durations])
    cmp_n = len(cmp_durations)
    comparison["distribution"] = {
        "count": cmp_n,
        "percentiles": {k: round(v, 1) for k, v in cmp_pvals.items()},
    }

    # Build window params for each outlier
    windows = []
    for total_ms, t in outlier_list:
        r = _root(t.get("spans", []))
        if not r:
            continue
        t_start = _ts(r["startTime"])
        win_start = (t_start - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        win_end = (t_start + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        windows.append(
            (
                total_ms,
                r,
                {
                    "pageSize": 20,
                    "startTime": win_start,
                    "endTime": win_end,
                    "view": "ROOTSPAN",
                },
            )
        )

    # Fetch all windows in parallel
    def _fetch_window(win_params):
        return fetch_traces(project, win_params, max_results=20)

    with ThreadPoolExecutor(max_workers=min(len(windows), 8)) as pool:
        futures = {
            pool.submit(_fetch_window, wp): idx
            for idx, (_, _, wp) in enumerate(windows)
        }
        win_results = [[] for _ in windows]
        for fut in as_completed(futures):
            win_results[futures[fut]] = fut.result()

    for _, ((total_ms, r, _), win_traces) in enumerate(zip(windows, win_results), 1):
        win_durs = _to_durations(filter_traces(win_traces or [], services=compare_svc))

        window_data = {"time": r["startTime"][:19], "primaryMs": round(total_ms, 1)}
        if win_durs:
            cmp_vals = [ms for ms, _ in win_durs]
            avg = sum(cmp_vals) / len(cmp_vals)
            mx = max(cmp_vals)
            window_data["avgMs"] = round(avg, 1)
            window_data["maxMs"] = round(mx, 1)
            window_data["count"] = len(cmp_vals)

        comparison["windows"].append(window_data)

    return comparison


# ── Outliers command ─────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=50, show_default=True, type=int, help="Max traces to fetch"
)
@click.option(
    "--service", "services", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option("--label", "labels", multiple=True, help="Label key=value (repeatable)")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.option(
    "--threshold",
    default="p95",
    show_default=True,
    help="Outlier threshold (p50, p90, p95, p99, or raw like 500ms)",
)
@click.option("--top", default=5, show_default=True, type=int, help="Outliers to show")
@click.option(
    "--compare",
    "compare_svc",
    default=None,
    help="Compare with another service in the same time window",
)
@click.pass_context
@_cli_validate
def outliers(
    ctx,
    start,
    end,
    limit,
    services,
    labels,
    min_latency,
    max_latency,
    threshold,
    top,
    compare_svc,
):
    """Find outlier traces and show per-span time breakdown.

    Use --compare to correlate with another service at the same timestamps.

    \b
    Examples:
      gct outliers --service my-service
      gct outliers --service my-service --label k8s.cluster.name=us-east1-a
      gct outliers --service my-service --compare other-service
    """
    p = ctx.obj["project"]
    as_json = ctx.obj["json"]
    label_dict = _parse_labels(labels)

    result = trace_outliers(
        p,
        start=start,
        end=end,
        limit=limit,
        services=services,
        labels=label_dict,
        min_latency=min_latency,
        max_latency=max_latency,
        threshold=threshold,
        top=top,
        compare_svc=compare_svc,
    )

    if as_json:
        click.echo(json.dumps(result, indent=2))
        return

    # No traces
    if result["count"] == 0:
        click.echo("No traces found.")
        return

    # Distribution
    pvals = result["distribution"]
    click.echo(f"Latency distribution ({result['count']} traces):\n")
    for label, ms in pvals.items():
        click.echo(f"  {label}  {_fmt_ms(ms)}")
    click.echo()

    if not result["outliers"]:
        click.echo(
            f"No outliers above {result['threshold']} "
            f"({_fmt_ms(result['thresholdMs'])})."
        )
        return

    # Outlier table
    click.echo(
        f"Outliers above {result['threshold']} "
        f"({_fmt_ms(result['thresholdMs'])}): "
        f"{len(result['outliers'])} shown\n"
    )
    click.echo(f"{'#':<3} {'TRACE ID':<36} {'TOTAL':>10}  TOP SPANS (self time)")
    click.echo("\u2500" * 110)

    for i, o in enumerate(result["outliers"], 1):
        total_ms = o["totalMs"]
        tid = o["traceId"]
        total_self = o["totalSelfMs"] or 1
        top_spans = o["spans"][:5]
        first = top_spans[0]
        first_pct = first["selfMs"] / total_self * 100
        click.echo(
            f"{i:<3} {tid:<36} {_fmt_ms(total_ms):>10}  "
            f"{first['name']} {_fmt_ms(first['selfMs'])} ({first_pct:.0f}%)"
        )
        for s in top_spans[1:]:
            pct = s["selfMs"] / total_self * 100
            click.echo(f"{'':>52}{s['name']} {_fmt_ms(s['selfMs'])} ({pct:.0f}%)")

    # Comparison
    if result.get("comparison"):
        click.echo(f"\n{'=' * 110}")
        click.echo(f"Comparing with: {result['comparison']['service']}\n")
        _render_comparison(result["comparison"], pvals, services)


# ── Stats command ────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=100, show_default=True, type=int, help="Max traces to fetch"
)
@click.option(
    "--span-name",
    "span_pattern",
    default=None,
    help="Span name filter (substring match)",
)
@click.option(
    "--group-by",
    "group_by",
    default=None,
    help="Comma-separated label keys to group by",
)
@click.option(
    "--service", "services", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option("--label", "labels", multiple=True, help="Label key=value (repeatable)")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.option(
    "--bucket", "bucket_str", default=None, help="Time bucket size (e.g. 5m, 10m, 1h)"
)
@click.option(
    "--sparkline", "sparkline", is_flag=True, help="Show p50 trend sparkline per group"
)
@click.pass_context
@_cli_validate
def stats(
    ctx,
    start,
    end,
    limit,
    span_pattern,
    group_by,
    services,
    labels,
    min_latency,
    max_latency,
    bucket_str,
    sparkline,
):
    """Latency stats, optionally grouped by span labels.

    Collects matching spans from fetched traces and computes percentile
    distributions.  Use --group-by to break down by one or more label keys.

    \b
    Examples:
      gct stats --span-name "POST /v1/rtb" --group-by cloud.region
      gct stats --span-name HTTP --group-by cloud.region,service.name
      gct stats --service my-service --group-by abtest
      gct stats --service my-service --bucket 5m
      gct stats --service my-service --group-by abtest --sparkline
    """
    p = ctx.obj["project"]
    as_json = ctx.obj["json"]
    label_dict = _parse_labels(labels)
    group_keys = [k.strip() for k in group_by.split(",")] if group_by else []

    result = trace_stats(
        p,
        start=start,
        end=end,
        limit=limit,
        span_pattern=span_pattern,
        group_by=group_keys or None,
        services=services,
        labels=label_dict,
        min_latency=min_latency,
        max_latency=max_latency,
        bucket=bucket_str,
        sparkline=sparkline,
    )

    if not result:
        click.echo("No traces found.")
        return

    if result.get("totalSpans", 0) == 0:
        click.echo("No spans matched.")
        return

    if as_json:
        click.echo(json.dumps(result, indent=2))
        return

    # ── Text output ──────────────────────────────────────────────────────
    total_spans = result["totalSpans"]
    total_traces = result["totalTraces"]
    desc = f"Latency stats ({total_spans} spans across {total_traces} traces)"
    if span_pattern:
        desc += f'  span ~ "{span_pattern}"'
    click.echo(desc)
    click.echo()

    groups = result.get("groups", [result] if "percentiles" in result else [])

    # ── Bucket mode ──────────────────────────────────────────────────────
    if bucket_str:
        for gi, g in enumerate(groups):
            if group_keys:
                key = g.get("key", {})
                label = " ".join(
                    f"{k}={key.get(k, '') or '(none)'}" for k in group_keys
                )
                click.echo(f"\u2500\u2500 {label} ({g['count']} spans) \u2500\u2500")
                click.echo()

            click.echo(
                f"{'TIME':<13}  {'COUNT':>6}  "
                f"{'avg':>10}  {'p50':>10}  {'p90':>10}  {'p95':>10}  {'p99':>10}"
            )
            click.echo("\u2500" * 81)

            for b in g.get("buckets", []):
                if b["count"] > 0:
                    pv = b["percentiles"]
                    click.echo(
                        f"{b['time']:<13}  {b['count']:>6}  "
                        f"{_fmt_ms(b['avgMs']):>10}  {_fmt_ms(pv['p50']):>10}  "
                        f"{_fmt_ms(pv['p90']):>10}  {_fmt_ms(pv['p95']):>10}  "
                        f"{_fmt_ms(pv['p99']):>10}"
                    )
                else:
                    click.echo(
                        f"{b['time']:<13}  {'0':>6}  "
                        f"{'-':>10}  {'-':>10}  {'-':>10}  {'-':>10}  {'-':>10}"
                    )

            if gi < len(groups) - 1:
                click.echo()
        click.echo()
        return

    # ── Summary mode (with optional sparkline) ───────────────────────────
    if group_keys:
        rows = []
        for g in groups:
            key = g.get("key", {})
            label = " ".join(f"{k}={key.get(k, '') or '(none)'}" for k in group_keys)
            rows.append((label, g))

        max_lbl = max(len(r[0]) for r in rows)
        max_lbl = max(max_lbl, 5)

        hdr = (
            f"{'GROUP':<{max_lbl}}  {'COUNT':>6}  "
            f"{'avg':>10}  {'p50':>10}  {'p90':>10}  {'p95':>10}  {'p99':>10}"
        )
        if sparkline:
            hdr += "  TREND"
        click.echo(hdr)
        click.echo("\u2500" * (max_lbl + 68 + (30 if sparkline else 0)))

        for label, g in rows:
            pv = g["percentiles"]
            line = (
                f"{label:<{max_lbl}}  {g['count']:>6}  "
                f"{_fmt_ms(g['avgMs']):>10}  {_fmt_ms(pv['p50']):>10}  "
                f"{_fmt_ms(pv['p90']):>10}  {_fmt_ms(pv['p95']):>10}  "
                f"{_fmt_ms(pv['p99']):>10}"
            )
            if g.get("trend"):
                line += f"  {g['trend']}"
            click.echo(line)
    else:
        g = groups[0] if groups else result
        pv = g["percentiles"]
        click.echo(f"  avg  {_fmt_ms(g['avgMs'])}")
        for label, ms in pv.items():
            click.echo(f"  {label}  {_fmt_ms(ms)}")
        if sparkline and g.get("trend"):
            click.echo(f"\n  trend  {g['trend']}")

    click.echo()


if __name__ == "__main__":
    cli()
