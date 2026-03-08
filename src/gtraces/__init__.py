#!/usr/bin/env python3
"""gct - CLI for GCP Cloud Trace API v1."""

import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import click

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


class ApiError(click.ClickException):
    """Raised on API failures; safe to propagate from worker threads."""

    def __init__(self, message):
        super().__init__(message)


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


def api_get(project, path, params=None):
    """GET from Cloud Trace API v1. Returns parsed JSON."""
    url = f"{API}/{project}{path}"
    if params:
        url += "?" + urlencode(params)
    req = Request(url, headers={"Authorization": f"Bearer {get_token()}"})
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        msgs = {
            401: "Auth expired. Run: gcloud auth login",
            403: f"Permission denied for project '{project}'",
            404: "Not found",
            429: "Rate limited. Try again shortly",
        }
        raise ApiError(f"{msgs.get(e.code, f'HTTP {e.code}')}\n{body}")


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


def parse_time(s):
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


def parse_latency(s):
    """Parse '500ms' or '1.5s' to (api_filter_str, ms_float). Returns (None, None) if empty."""
    if not s:
        return None, None
    m = re.match(r"^(\d+(?:\.\d+)?)(ms|s)$", s)
    if not m:
        raise click.BadParameter(f"Bad latency: {s} (use e.g. 500ms, 1s)")
    val, unit = float(m.group(1)), m.group(2)
    ms = val if unit == "ms" else val * 1000
    return f"{m.group(1)}{m.group(2)}", ms


def _parse_labels(label_tuple):
    """Parse ('key=value', ...) tuple into a dict."""
    d = {}
    for l in label_tuple:
        if "=" not in l:
            raise click.BadParameter(f"Expected key=value, got: {l}")
        k, v = l.split("=", 1)
        d[k] = v
    return d or None


def _build_params(start, end, limit, view="ROOTSPAN", min_latency=None):
    """Build common API query params."""
    params = {
        "pageSize": min(limit, 100),
        "startTime": parse_time(start),
        "endTime": end or _now(),
        "view": view,
    }
    if min_latency:
        filt, _ = parse_latency(min_latency)
        params["filter"] = f"latency:{filt}"
    return params


def filter_traces(
    traces, *, span_name=None, services=None, labels=None, min_ms=None, max_ms=None
):
    """Filter traces by root span name, service(s), labels, and duration range.

    Unified filter — replaces the former match_traces + _filter_by_labels.
    """
    if isinstance(services, str):
        services = (services,)
    svc_set = set(services) if services else None
    out = []
    for t in traces:
        r = _root(t.get("spans", []))
        if not r:
            continue
        if span_name and r.get("name") != span_name:
            continue
        dur = _dur(r)
        if min_ms is not None and dur < min_ms:
            continue
        if max_ms is not None and dur > max_ms:
            continue
        rl = r.get("labels", {})
        if svc_set and rl.get("service.name") not in svc_set:
            continue
        if labels and not all(rl.get(k) == v for k, v in labels.items()):
            continue
        out.append(t)
    return out


def _to_durations(traces):
    """Return sorted (ms, trace) list from traces with root spans."""
    out = []
    for t in traces:
        r = _root(t.get("spans", []))
        if r:
            out.append((_dur(r), t))
    out.sort(key=lambda x: x[0])
    return out


def _percentiles(durations):
    """Compute p50/p90/p95/p99 from a sorted list of (ms, trace) tuples."""
    n = len(durations)
    pcts = {"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99}
    return {k: durations[min(int(v * n), n - 1)][0] for k, v in pcts.items()}


def _resolve_threshold(threshold, pvals):
    """Resolve a threshold string to milliseconds."""
    if threshold in pvals:
        return pvals[threshold]
    _, ms = parse_latency(threshold)
    if ms is None:
        raise click.BadParameter(
            f"Bad threshold: {threshold} (use p50/p90/p95/p99 or e.g. 500ms)"
        )
    return ms


# ── Rendering ────────────────────────────────────────────────────────────────


def render_list(traces):
    """Render traces as a compact table."""
    if not traces:
        click.echo("No traces found.")
        return
    click.echo(f"{'TRACE ID':<36}  {'ROOT SPAN':<40}  {'DURATION':>10}  TIME")
    click.echo("\u2500" * 100)
    for t in traces:
        r = _root(t.get("spans", []))
        tid = t.get("traceId", "?")
        name = (r.get("name", "?") if r else "?")[:40]
        d = _fmt_ms(_dur(r)) if r else "?"
        time = r.get("startTime", "?")[:19] if r else "?"
        click.echo(f"{tid:<36}  {name:<40}  {d:>10}  {time}")


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
    get_token()


@cli.command("list")
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option("--limit", default=20, show_default=True, type=int, help="Max traces")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.pass_context
def list_cmd(ctx, start, end, limit, min_latency, max_latency):
    """List recent traces."""
    p = ctx.obj["project"]
    _, max_ms = parse_latency(max_latency)
    params = _build_params(start, end, limit, min_latency=min_latency)
    traces = fetch_traces(p, params, max_results=limit)
    if max_ms is not None:
        traces = filter_traces(traces, max_ms=max_ms)
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
    "--limit", default=200, show_default=True, type=int, help="Traces to scan"
)
@click.pass_context
def services(ctx, start, end, limit):
    """List services and endpoints seen in recent traces."""
    p = ctx.obj["project"]
    params = _build_params(start, end, limit)
    traces = fetch_traces(p, params, max_results=limit)
    if not traces:
        click.echo("No traces found.")
        return

    svc_counts = Counter()
    ep_counts = Counter()
    for t in traces:
        for s in t.get("spans", []):
            svc = s.get("labels", {}).get("service.name")
            if svc:
                svc_counts[svc] += 1
            if not s.get("parentSpanId"):
                ep_counts[s.get("name", "?")] += 1

    if ctx.obj["json"]:
        click.echo(
            json.dumps(
                {"services": dict(svc_counts), "endpoints": dict(ep_counts)}, indent=2
            )
        )
        return

    click.echo(f"{'SERVICE':<45} TRACES")
    click.echo("\u2500" * 55)
    for svc, n in svc_counts.most_common():
        click.echo(f"  {svc:<45} {n}")
    click.echo()
    click.echo(f"{'ENDPOINT':<45} TRACES")
    click.echo("\u2500" * 55)
    for ep, n in ep_counts.most_common():
        click.echo(f"  {ep:<45} {n}")
    click.echo(f"\nScanned {len(traces)} traces from last {start}.")


@cli.command()
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (1h, 30m, 2d, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--limit", default=20, show_default=True, type=int, help="Traces to fetch in full"
)
@click.option(
    "--service", "service", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.pass_context
def spans(ctx, start, end, limit, service, min_latency, max_latency):
    """List distinct span names from sampled traces."""
    p = ctx.obj["project"]
    _, max_ms = parse_latency(max_latency)
    params = _build_params(start, end, limit, view="COMPLETE", min_latency=min_latency)
    traces = fetch_traces(p, params, max_results=limit)
    if max_ms is not None:
        traces = filter_traces(traces, max_ms=max_ms)

    # Filter by service across ALL spans (not just root), because this command
    # is about discovering span names within a service, not root-span ownership.
    if service:
        svc_set = set(service)
        traces = [
            t
            for t in traces
            if any(
                s.get("labels", {}).get("service.name") in svc_set
                for s in t.get("spans", [])
            )
        ]

    if not traces:
        click.echo("No traces found.")
        return

    span_counts = Counter()
    for t in traces:
        for s in t.get("spans", []):
            span_counts[s.get("name", "?")] += 1

    if ctx.obj["json"]:
        click.echo(json.dumps(dict(span_counts.most_common()), indent=2))
        return

    click.echo(f"{'SPAN NAME':<60} COUNT")
    click.echo("\u2500" * 70)
    for name, n in span_counts.most_common():
        click.echo(f"  {name:<60} {n}")
    click.echo(f"\nSampled {len(traces)} traces.")


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
def get(ctx, trace_id, bars, name_width):
    """Show trace as a span tree."""
    trace = api_get(ctx.obj["project"], f"/traces/{trace_id}")
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
    "--limit", default=50, show_default=True, type=int, help="Max traces to scan"
)
@click.option("--span-name", default=None, help="Root span name (exact match)")
@click.option("--label", "labels", multiple=True, help="Label key=value (repeatable)")
@click.option("--min-latency", default=None, help="Min latency (500ms, 1s)")
@click.option("--max-latency", default=None, help="Max latency (500ms, 1s)")
@click.option(
    "--service", "service", multiple=True, help="Filter by service.name (repeatable)"
)
@click.option(
    "--parent-span-id",
    default=None,
    help="Find spans with this parentSpanId (fetches full traces)",
)
@click.pass_context
def search(
    ctx,
    start,
    end,
    limit,
    span_name,
    labels,
    min_latency,
    max_latency,
    service,
    parent_span_id,
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
    _, min_ms = parse_latency(min_latency)
    _, max_ms = parse_latency(max_latency)
    view = "COMPLETE" if parent_span_id else "ROOTSPAN"
    params = _build_params(start, end, limit, view=view, min_latency=min_latency)
    traces = fetch_traces(p, params, max_results=limit)

    # Pre-filter by service/labels on root span
    if service or label_dict:
        traces = filter_traces(traces, services=service, labels=label_dict)

    if not parent_span_id:
        # Fast path: root-span-only filtering
        filtered = filter_traces(
            traces,
            span_name=span_name,
            labels=label_dict,
            min_ms=min_ms,
            max_ms=max_ms,
        )
        if ctx.obj["json"]:
            click.echo(json.dumps(filtered, indent=2))
        else:
            render_list(filtered)
            if traces:
                click.echo(f"\n{len(filtered)}/{len(traces)} traces matched.")
        return

    # All spans already fetched via COMPLETE view — filter in-memory
    matches = []
    for t in traces:
        matched_spans = [
            s
            for s in t.get("spans", [])
            if s.get("parentSpanId") == parent_span_id
            and (not span_name or s.get("name") == span_name)
        ]
        if matched_spans:
            matches.append({"trace": t, "matched_spans": matched_spans})

    if not matches:
        click.echo("No spans matched.")
        return

    if ctx.obj["json"]:
        click.echo(
            json.dumps(
                [
                    {"traceId": m["trace"]["traceId"], "spans": m["matched_spans"]}
                    for m in matches
                ],
                indent=2,
            )
        )
        return

    total_spans = sum(len(m["matched_spans"]) for m in matches)
    click.echo(
        f"Found {total_spans} span(s) in {len(matches)} trace(s) "
        f"with parentSpanId={parent_span_id}\n"
    )
    click.echo(
        f"{'TRACE ID':<36}  {'SPAN NAME':<40}  "
        f"{'DURATION':>10}  {'SPAN ID':<20}  LABELS"
    )
    click.echo("\u2500" * 120)
    for m in matches:
        tid = m["trace"].get("traceId", "?")
        for s in m["matched_spans"]:
            name = s.get("name", "?")[:40]
            dur = _fmt_ms(_dur(s))
            sid = s.get("spanId", "?")
            lbl_d = s.get("labels", {})
            interesting = {k: v for k, v in lbl_d.items() if k in INTERESTING_LABELS}
            lbl = " ".join(f"{k}={v}" for k, v in interesting.items())
            click.echo(f"{tid:<36}  {name:<40}  {dur:>10}  {sid:<20}  {lbl}")


@cli.command()
@click.argument("trace_id")
@click.pass_context
def analyze(ctx, trace_id):
    """Timeline analysis with bottleneck detection."""
    trace = api_get(ctx.obj["project"], f"/traces/{trace_id}")
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


def _compare_services(
    project,
    outlier_list,
    all_traces,
    compare_svc,
    primary_pvals,
    primary_label,
    as_json,
):
    """Cross-service latency comparison during outlier windows.

    Returns comparison data dict (for JSON mode) or None (after printing text).
    """
    cmp_durations = _to_durations(filter_traces(all_traces, services=compare_svc))

    comparison = {"service": compare_svc, "distribution": None, "windows": []}

    if not cmp_durations:
        if not as_json:
            click.echo(f"  No traces found for {compare_svc}.")
        return comparison

    cmp_pvals = _percentiles(cmp_durations)
    cmp_n = len(cmp_durations)
    comparison["distribution"] = {
        "count": cmp_n,
        "percentiles": {k: round(v, 1) for k, v in cmp_pvals.items()},
    }

    if not as_json:
        click.echo(f"  {compare_svc} latency ({cmp_n} traces):\n")
        click.echo(f"  {'PCTL':<6} {primary_label:<30} {compare_svc}")
        click.echo(f"  {'─' * 70}")
        for label in ("p50", "p90", "p95", "p99"):
            click.echo(
                f"  {label:<6} {_fmt_ms(primary_pvals[label]):<30} "
                f"{_fmt_ms(cmp_pvals[label])}"
            )
        click.echo(f"\n  During outlier windows:")
        click.echo(
            f"  {'#':<3} {'TIME':<26} "
            f"{primary_label + ' latency':<25} {compare_svc + ' latency'}"
        )
        click.echo(f"  {'─' * 80}")

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

    for i, ((total_ms, r, _), win_traces) in enumerate(zip(windows, win_results), 1):
        win_durs = _to_durations(filter_traces(win_traces or [], services=compare_svc))

        window_data = {"time": r["startTime"][:19], "primaryMs": round(total_ms, 1)}
        if win_durs:
            cmp_vals = [ms for ms, _ in win_durs]
            avg = sum(cmp_vals) / len(cmp_vals)
            mx = max(cmp_vals)
            window_data["avgMs"] = round(avg, 1)
            window_data["maxMs"] = round(mx, 1)
            window_data["count"] = len(cmp_vals)
            cmp_str = f"avg {_fmt_ms(avg)}, max {_fmt_ms(mx)} ({len(cmp_vals)} traces)"
        else:
            cmp_str = "(no traces)"

        comparison["windows"].append(window_data)
        if not as_json:
            click.echo(
                f"  {i:<3} {r['startTime'][:19]:<26} {_fmt_ms(total_ms):<25} {cmp_str}"
            )

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
@click.option("--limit", default=50, show_default=True, type=int, help="Traces to scan")
@click.option(
    "--service", "service", multiple=True, help="Filter by service.name (repeatable)"
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
def outliers(
    ctx,
    start,
    end,
    limit,
    service,
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
    _, max_ms = parse_latency(max_latency)
    params = _build_params(start, end, limit, view="COMPLETE", min_latency=min_latency)

    all_traces = fetch_traces(p, params, max_results=limit)
    filtered = filter_traces(
        all_traces, services=service, labels=label_dict, max_ms=max_ms
    )
    durations = _to_durations(filtered)

    if not durations:
        click.echo("No traces found.")
        return

    pvals = _percentiles(durations)
    n = len(durations)

    if not as_json:
        click.echo(f"Latency distribution ({n} traces):\n")
        for label, ms in pvals.items():
            click.echo(f"  {label}  {_fmt_ms(ms)}")
        click.echo()

    thresh_ms = _resolve_threshold(threshold, pvals)
    outlier_list = [(ms, t) for ms, t in durations if ms >= thresh_ms]
    outlier_list.sort(key=lambda x: -x[0])
    outlier_list = outlier_list[:top]

    if not outlier_list:
        click.echo(f"No outliers above {threshold} ({_fmt_ms(thresh_ms)}).")
        return

    if not as_json:
        click.echo(
            f"Outliers above {threshold} ({_fmt_ms(thresh_ms)}): "
            f"{len(outlier_list)} shown\n"
        )
        click.echo(f"{'#':<3} {'TRACE ID':<36} {'TOTAL':>10}  TOP SPANS (self time)")
        click.echo("\u2500" * 110)

    # Per-outlier span breakdown
    json_out = []
    for i, (total_ms, t) in enumerate(outlier_list, 1):
        tid = t.get("traceId")
        span_self = _span_breakdown(t.get("spans", []))
        total_self = sum(ms for _, ms in span_self) or 1

        if as_json:
            json_out.append(
                {
                    "traceId": tid,
                    "totalMs": round(total_ms, 1),
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
        else:
            top_spans = span_self[:5]
            first_name, first_ms = top_spans[0]
            first_pct = first_ms / total_self * 100
            click.echo(
                f"{i:<3} {tid:<36} {_fmt_ms(total_ms):>10}  "
                f"{first_name} {_fmt_ms(first_ms)} ({first_pct:.0f}%)"
            )
            for name, ms in top_spans[1:]:
                pct = ms / total_self * 100
                click.echo(f"{'':>52}{name} {_fmt_ms(ms)} ({pct:.0f}%)")

    # Cross-service comparison
    comparison = None
    if compare_svc:
        if not as_json:
            click.echo(f"\n{'=' * 110}")
            click.echo(f"Comparing with: {compare_svc}\n")

        svc_label = ", ".join(service) if service else "primary"
        comparison = _compare_services(
            p,
            outlier_list,
            all_traces,
            compare_svc,
            pvals,
            svc_label,
            as_json,
        )

    if as_json:
        out = {
            "distribution": {k: round(v, 1) for k, v in pvals.items()},
            "count": n,
            "threshold": threshold,
            "thresholdMs": round(thresh_ms, 1),
            "outliers": json_out,
        }
        if comparison:
            out["comparison"] = comparison
        click.echo(json.dumps(out, indent=2))


if __name__ == "__main__":
    cli()
