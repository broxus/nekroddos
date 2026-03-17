#!/usr/bin/env python3

from __future__ import annotations

import argparse
import html
import io
import math
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
    print(
        "Missing Python dependency for report generation. "
        "Run this tool from `nix develop` or install pandas, numpy, and matplotlib.",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


METRICS = ("mean", "p50", "p95", "p99", "max")
PALETTE = ["#0f766e", "#2563eb", "#dc2626", "#9333ea", "#ea580c", "#0891b2"]
ROLLING_MEDIAN_WINDOW = 51
HISTOGRAM_BINS = 64


@dataclass(frozen=True)
class Summary:
    path: Path
    label: str
    raw_latencies_ns: np.ndarray
    raw_latencies_ms: np.ndarray
    sorted_latencies_ns: np.ndarray
    sorted_latencies_ms: np.ndarray
    count: int
    min_ns: int
    mean_ns: float
    p50_ns: float
    p95_ns: float
    p99_ns: float
    max_ns: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare latency CSV files with a required latency_ns column and emit "
            "a self-contained HTML report."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="CSV files and/or directories containing CSV files",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Baseline CSV file used for delta calculations",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("latency-comparison.html"),
        help="Output HTML report path",
    )
    parser.add_argument(
        "--title",
        default="Latency Comparison Report",
        help="Report title",
    )
    return parser


def resolve_inputs(items: list[str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[Path] = set()

    for item in items:
        path = Path(item).expanduser().resolve()
        if path.is_dir():
            matches = sorted(candidate.resolve() for candidate in path.glob("*.csv"))
            if not matches:
                raise SystemExit(f"no CSV files found in directory: {path}")
            for match in matches:
                if match not in seen:
                    seen.add(match)
                    resolved.append(match)
            continue

        if not path.is_file():
            raise SystemExit(f"input does not exist: {path}")
        if path.suffix.lower() != ".csv":
            raise SystemExit(f"input is not a CSV file: {path}")
        if path not in seen:
            seen.add(path)
            resolved.append(path)

    if len(resolved) < 2:
        raise SystemExit("comparison requires at least two CSV files")
    return resolved


def clean_label(path: Path) -> str:
    stem = path.stem
    stem = stem.removeprefix("latency-report.")
    stem = stem.removeprefix("latency_report_")
    stem = re.sub(r"\b([a-f0-9]{40})\b", lambda match: match.group(1)[:8], stem)
    stem = stem.replace("params28changed", "params28")
    stem = stem.replace("_", " ").replace("-", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or path.name


def chart_label(label: str, width: int = 14) -> str:
    wrapped = textwrap.wrap(label, width=width, break_long_words=False)
    if not wrapped:
        return label
    if len(wrapped) > 2:
        wrapped = wrapped[:2]
        wrapped[-1] = textwrap.shorten(wrapped[-1], width=width, placeholder="...")
    return "\n".join(wrapped)


def read_summary(path: Path) -> Summary:
    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - surfaced directly to CLI
        raise SystemExit(f"failed to read CSV {path}: {exc}") from exc

    if "latency_ns" not in frame.columns:
        raise SystemExit(f"CSV is missing required latency_ns column: {path}")

    series = frame["latency_ns"]
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        bad_row = int(np.flatnonzero(numeric.isna().to_numpy())[0]) + 2
        bad_value = series.iloc[bad_row - 2]
        raise SystemExit(f"non-numeric latency_ns value in {path} at CSV row {bad_row}: {bad_value!r}")

    if ((numeric % 1) != 0).any():
        bad_row = int(np.flatnonzero(((numeric % 1) != 0).to_numpy())[0]) + 2
        raise SystemExit(f"latency_ns must contain integers only in {path} at CSV row {bad_row}")

    if (numeric < 0).any():
        bad_row = int(np.flatnonzero((numeric < 0).to_numpy())[0]) + 2
        bad_value = int(numeric.iloc[bad_row - 2])
        raise SystemExit(f"latency_ns must be >= 0 in {path} at CSV row {bad_row}: {bad_value}")

    raw_latencies_ns = numeric.astype("int64").to_numpy()
    if raw_latencies_ns.size == 0:
        raise SystemExit(f"CSV contains no latency rows: {path}")

    sorted_latencies_ns = np.sort(raw_latencies_ns.copy())
    raw_latencies_ms = raw_latencies_ns / 1_000_000.0
    sorted_latencies_ms = sorted_latencies_ns / 1_000_000.0

    return Summary(
        path=path,
        label=clean_label(path),
        raw_latencies_ns=raw_latencies_ns,
        raw_latencies_ms=raw_latencies_ms,
        sorted_latencies_ns=sorted_latencies_ns,
        sorted_latencies_ms=sorted_latencies_ms,
        count=raw_latencies_ns.size,
        min_ns=int(sorted_latencies_ns[0]),
        mean_ns=float(raw_latencies_ns.mean()),
        p50_ns=float(np.percentile(sorted_latencies_ns, 50)),
        p95_ns=float(np.percentile(sorted_latencies_ns, 95)),
        p99_ns=float(np.percentile(sorted_latencies_ns, 99)),
        max_ns=int(sorted_latencies_ns[-1]),
    )


def metric_value(item: Summary, metric: str) -> float:
    return float(getattr(item, f"{metric}_ns"))


def ms(ns_value: float) -> float:
    return ns_value / 1_000_000.0


def format_ms(ns_value: float) -> str:
    return f"{ms(ns_value):,.1f} ms"


def delta_percent(current: float, baseline: float) -> float:
    if baseline == 0:
        return math.nan
    return ((current - baseline) / baseline) * 100.0


def improvement_copy(delta: float) -> str:
    if math.isnan(delta):
        return "n/a"
    if math.isclose(delta, 0.0, abs_tol=0.05):
        return "equal to baseline"
    if delta < 0:
        return f"{abs(delta):.1f}% lower"
    return f"{abs(delta):.1f}% higher"


def sort_runs(items: list[Summary], baseline: Path | None) -> list[Summary]:
    if baseline is None:
        return sorted(items, key=lambda item: (item.p95_ns, item.p99_ns, item.mean_ns))

    baseline = baseline.resolve()
    anchor = next(item for item in items if item.path == baseline)
    rest = sorted(
        (item for item in items if item.path != baseline),
        key=lambda item: (item.p95_ns, item.p99_ns, item.mean_ns),
    )
    return [anchor, *rest]


def save_svg(fig: plt.Figure) -> str:
    buf = io.StringIO()
    fig.savefig(buf, format="svg", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    svg = buf.getvalue()
    return re.sub(r"^<\?xml.*?\?>", "", svg, count=1).strip()


def build_ecdf_chart(items: list[Summary], colors: dict[Path, str], baseline: Path | None) -> str:
    fig, ax = plt.subplots(figsize=(10.8, 5.4))
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.8, bottom=0.22, left=0.09, right=0.98)
    ax.set_facecolor("#fffdf8")

    for item in items:
        y = np.arange(1, item.count + 1) / item.count
        width = 2.8 if baseline and item.path == baseline else 2.1
        ax.step(
            item.sorted_latencies_ms,
            y,
            where="post",
            color=colors[item.path],
            linewidth=width,
            label=chart_label(item.label, width=18),
            alpha=0.95,
        )

    fig.text(0.09, 0.94, "Distribution Overview", fontsize=16, color="#1f2937", weight="bold")
    fig.text(0.09, 0.912, "ECDF of latency values.", fontsize=10, color="#5b6472")
    ax.set_xlabel("Latency (ms)", color="#364152")
    ax.set_ylabel("Cumulative share", color="#364152")
    ax.grid(True, axis="both", color="#dfd7ca", linewidth=0.8, alpha=0.7)
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncols=min(3, len(items)))
    for spine in ax.spines.values():
        spine.set_visible(False)

    return save_svg(fig)


def build_percentile_chart(items: list[Summary], colors: dict[Path, str]) -> str:
    fig, ax = plt.subplots(figsize=(10.8, 5.4))
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.8, bottom=0.28, left=0.09, right=0.98)
    ax.set_facecolor("#fffdf8")

    metrics = ("p50", "p95", "p99")
    labels = [chart_label(item.label) for item in items]
    positions = np.arange(len(labels))
    width = 0.22

    for offset, metric in zip((-width, 0.0, width), metrics):
        values = [ms(metric_value(item, metric)) for item in items]
        ax.bar(
            positions + offset,
            values,
            width=width,
            label=metric.upper(),
            color={
                "p50": "#0f766e",
                "p95": "#c2410c",
                "p99": "#7c2d12",
            }[metric],
            alpha=0.92,
        )

    fig.text(0.09, 0.94, "Percentile Ladder", fontsize=16, color="#1f2937", weight="bold")
    fig.text(0.09, 0.912, "P50, P95, and P99 by run.", fontsize=10, color="#5b6472")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=18, ha="right", fontsize=10)
    ax.set_ylabel("Latency (ms)", color="#364152")
    ax.grid(True, axis="y", color="#dfd7ca", linewidth=0.8, alpha=0.7)
    ax.legend(frameon=False, ncols=3, loc="upper center", bbox_to_anchor=(0.5, -0.16))
    for spine in ax.spines.values():
        spine.set_visible(False)

    return save_svg(fig)


def build_points_chart(items: list[Summary], colors: dict[Path, str], baseline: Summary | None) -> str:
    if baseline is None:
        return build_sequence_chart(items, colors)

    fig, ax = plt.subplots(figsize=(10.8, 4.8))
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.84, bottom=0.16, left=0.11, right=0.92)
    ax.set_facecolor("#fffdf8")

    baseline_count = baseline.count
    baseline_rolling = (
        pd.Series(baseline.raw_latencies_ms)
        .rolling(window=ROLLING_MEDIAN_WINDOW, center=True, min_periods=1)
        .median()
        .to_numpy()
    )

    max_points = baseline_count
    ax.axhline(0.0, color="#6b7280", linewidth=1.3, alpha=0.95, zorder=1)

    for item in items:
        if item.path == baseline.path:
            continue

        count = min(item.count, baseline_count)
        x = np.arange(count)
        rolling = (
            pd.Series(item.raw_latencies_ms)
            .rolling(window=ROLLING_MEDIAN_WINDOW, center=True, min_periods=1)
            .median()
            .to_numpy()
        )
        delta = rolling[:count] - baseline_rolling[:count]
        ax.plot(
            x,
            delta,
            linewidth=2.0,
            color=colors[item.path],
            alpha=0.96,
            zorder=2,
        )
        label_x = max(count - 1, 0)
        label_y = float(delta[count - 1]) if count else 0.0
        ax.text(
            label_x + max(10, count * 0.008),
            label_y,
            chart_label(item.label, width=24),
            color=colors[item.path],
            fontsize=10,
            fontweight="bold",
            va="center",
        )
        max_points = max(max_points, count)

    fig.text(0.11, 0.94, "Rolling Median vs Baseline", fontsize=16, color="#1f2937", weight="bold")
    fig.text(
        0.11,
        0.912,
        f"Centered rolling median delta, window {ROLLING_MEDIAN_WINDOW} samples. Negative is lower than baseline.",
        fontsize=10,
        color="#5b6472",
    )
    ax.text(
        0.99,
        0.96,
        f"baseline: {baseline.label}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        color="#6b7280",
        bbox={
            "boxstyle": "round,pad=0.22,rounding_size=0.5",
            "facecolor": "#fcfaf6",
            "edgecolor": "#dfd7ca",
            "linewidth": 0.8,
            "alpha": 0.92,
        },
    )
    ax.set_xlabel("Zero-based sample index", color="#364152")
    ax.set_ylabel("Delta (ms)", color="#364152")
    ax.grid(True, axis="y", color="#dfd7ca", linewidth=0.8, alpha=0.7)
    ax.set_xlim(0, max_points - 1 + max(56, max_points * 0.12))
    for spine in ax.spines.values():
        spine.set_visible(False)

    return save_svg(fig)


def build_histogram_chart(
    baseline: Summary,
    challenger: Summary,
    colors: dict[Path, str],
    mode: str,
) -> str:
    fig, ax = plt.subplots(figsize=(10.8, 4.8))
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.84, bottom=0.18, left=0.11, right=0.98)
    ax.set_facecolor("#fffdf8")

    min_latency = min(float(baseline.raw_latencies_ms.min()), float(challenger.raw_latencies_ms.min()))
    max_latency = max(float(baseline.raw_latencies_ms.max()), float(challenger.raw_latencies_ms.max()))
    if math.isclose(min_latency, max_latency):
        max_latency = min_latency + 1.0
    bins = np.linspace(min_latency, max_latency, HISTOGRAM_BINS + 1)

    series: list[tuple[Summary, str]]
    if mode == "overlay":
        series = [(baseline, colors[baseline.path]), (challenger, colors[challenger.path])]
    elif mode == "baseline":
        series = [(baseline, colors[baseline.path])]
    elif mode == "challenger":
        series = [(challenger, colors[challenger.path])]
    else:  # pragma: no cover - internal contract
        raise ValueError(f"unknown histogram mode: {mode}")

    for item, color in series:
        ax.hist(
            item.raw_latencies_ms,
            bins=bins,
            density=True,
            color=color,
            alpha=0.34 if mode == "overlay" else 0.55,
            edgecolor=color,
            linewidth=1.1,
            label=item.label,
        )

    subtitle = {
        "overlay": "Overlayed density histogram in milliseconds.",
        "baseline": f"Baseline only: {baseline.label}.",
        "challenger": f"Challenger only: {challenger.label}.",
    }[mode]
    fig.text(0.11, 0.94, "Latency Histogram", fontsize=16, color="#1f2937", weight="bold")
    fig.text(0.11, 0.912, subtitle, fontsize=10, color="#5b6472")
    ax.set_xlabel("Latency (ms)", color="#364152")
    ax.set_ylabel("Density", color="#364152")
    ax.grid(True, axis="y", color="#dfd7ca", linewidth=0.8, alpha=0.7)
    if mode == "overlay":
        ax.legend(frameon=False, loc="upper right")
    for spine in ax.spines.values():
        spine.set_visible(False)

    return save_svg(fig)


def build_histogram_panel(items: list[Summary], baseline: Summary | None, colors: dict[Path, str]) -> str:
    if baseline is None or len(items) != 2:
        return ""

    challenger = next(item for item in items if item.path != baseline.path)
    views = {
        "overlay": build_histogram_chart(baseline, challenger, colors, "overlay"),
        "baseline": build_histogram_chart(baseline, challenger, colors, "baseline"),
        "challenger": build_histogram_chart(baseline, challenger, colors, "challenger"),
    }

    chart_html = "".join(
        (
            f"<div class='histogram-view{' is-active' if key == 'overlay' else ''}' "
            f"data-histogram-view='{key}'>{svg}</div>"
        )
        for key, svg in views.items()
    )

    return (
        "<section class='panel chart-panel histogram-panel' data-histogram-panel>"
        "<div class='histogram-toolbar' role='tablist' aria-label='Histogram view'>"
        "<button type='button' class='histogram-toggle is-active' data-histogram-target='overlay'>Overlay</button>"
        f"<button type='button' class='histogram-toggle' data-histogram-target='baseline'>{html.escape(baseline.label)}</button>"
        f"<button type='button' class='histogram-toggle' data-histogram-target='challenger'>{html.escape(challenger.label)}</button>"
        "</div>"
        "<p class='histogram-note'>Histogram view of baseline vs challenger. Buttons swap instantly between overlay and solo views.</p>"
        f"{chart_html}"
        "</section>"
    )


def build_sequence_chart(items: list[Summary], colors: dict[Path, str]) -> str:
    height = max(4.6, 2.2 * len(items) + 0.8)
    fig, axes = plt.subplots(
        len(items),
        1,
        figsize=(10.8, height),
        sharex=True,
        sharey=True,
    )
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.9, bottom=0.1, left=0.11, right=0.98, hspace=0.12)
    if not isinstance(axes, np.ndarray):
        axes = np.array([axes])

    for ax, item in zip(axes, items):
        ax.set_facecolor("#fffdf8")
        x = np.arange(item.count)
        ax.plot(
            x,
            item.raw_latencies_ms,
            linewidth=1.0,
            color=colors[item.path],
            alpha=0.88,
        )
        ax.text(
            0.012,
            0.92,
            chart_label(item.label, width=28),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            color="#1f2937",
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.28,rounding_size=0.6",
                "facecolor": "#fcfaf6",
                "edgecolor": "#dfd7ca",
                "linewidth": 0.8,
                "alpha": 0.92,
            },
        )
        ax.grid(True, axis="y", color="#dfd7ca", linewidth=0.8, alpha=0.7)
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.text(0.11, 0.955, "Sequence by Sample Index", fontsize=16, color="#1f2937", weight="bold")
    fig.text(0.11, 0.925, "Latency by CSV row index.", fontsize=10, color="#5b6472")
    fig.text(0.035, 0.5, "Latency (ms)", rotation=90, va="center", ha="center", color="#364152")
    axes[-1].set_xlabel("Sample index", color="#364152")

    return save_svg(fig)


def table_metric_cell(item: Summary, baseline: Summary | None, metric: str) -> str:
    value = metric_value(item, metric)
    value_html = html.escape(format_ms(value))

    if baseline is None:
        return f"<td><strong>{value_html}</strong></td>"

    if item.path == baseline.path:
        return f"<td><strong>{value_html}</strong></td>"

    delta = delta_percent(value, metric_value(baseline, metric))
    if math.isnan(delta):
        badge = "<div class='delta-badge delta-neutral'>n/a</div>"
    else:
        direction = "lower" if delta < 0 else "higher" if delta > 0 else "equal"
        klass = "delta-good" if delta < 0 else "delta-bad" if delta > 0 else "delta-neutral"
        badge = f"<div class='delta-badge {klass}'>{abs(delta):.1f}% {direction}</div>"

    return f"<td><strong>{value_html}</strong>{badge}</td>"


def build_delta_heatmap(items: list[Summary], baseline: Summary, colors: dict[Path, str]) -> str:
    challengers = [item for item in items if item.path != baseline.path]
    if not challengers:
        fig, ax = plt.subplots(figsize=(10.5, 2.8))
        fig.patch.set_facecolor("#f4efe7")
        ax.set_facecolor("#fffdf8")
        ax.text(
            0.5,
            0.5,
            "No challenger runs beyond the baseline.",
            ha="center",
            va="center",
            fontsize=16,
            color="#364152",
        )
        ax.axis("off")
        return save_svg(fig)

    data = np.array(
        [
            [delta_percent(metric_value(item, metric), metric_value(baseline, metric)) for metric in METRICS]
            for item in challengers
        ]
    )

    fig, ax = plt.subplots(figsize=(10.5, 3.8 + 0.35 * len(challengers)))
    fig.patch.set_facecolor("#f4efe7")
    fig.subplots_adjust(top=0.78, bottom=0.16, left=0.16, right=0.96)
    ax.set_facecolor("#fffdf8")

    finite = data[np.isfinite(data)]
    limit = max(5.0, float(np.abs(finite).max())) if finite.size else 5.0
    masked = np.ma.masked_invalid(data)
    cmap = plt.get_cmap("RdYlGn_r").copy()
    cmap.set_bad(color="#ebe4da")
    image = ax.imshow(masked, cmap=cmap, vmin=-limit, vmax=limit, aspect="auto")

    fig.text(0.16, 0.935, "Delta vs Baseline", fontsize=16, color="#1f2937", weight="bold")
    fig.text(
        0.16,
        0.907,
        f"Negative values are lower than {baseline.label}; positive values are higher.",
        fontsize=10,
        color="#5b6472",
    )
    ax.set_xticks(np.arange(len(METRICS)))
    ax.set_xticklabels([metric.upper() for metric in METRICS])
    ax.set_yticks(np.arange(len(challengers)))
    ax.set_yticklabels([chart_label(item.label, width=18) for item in challengers], fontsize=10)

    for row, item in enumerate(challengers):
        for col, metric in enumerate(METRICS):
            value = data[row, col]
            ax.text(
                col,
                row,
                "n/a" if math.isnan(value) else f"{value:+.1f}%",
                ha="center",
                va="center",
                fontsize=10,
                color="#111827",
                fontweight="bold" if metric in {"p95", "p99"} else "normal",
            )

    cbar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
    cbar.ax.set_ylabel("% vs baseline", rotation=270, labelpad=20, color="#364152")
    cbar.outline.set_visible(False)

    for spine in ax.spines.values():
        spine.set_visible(False)

    return save_svg(fig)


def metric_box(label: str, value: str, badge: str = "") -> str:
    badge_html = f"<em class='metric-chip'>{html.escape(badge)}</em>" if badge else ""
    return (
        "<div class='mini-metric'>"
        f"<span>{html.escape(label)}</span>"
        f"{badge_html}"
        f"<strong>{html.escape(value)}</strong>"
        "</div>"
    )


def make_run_cards(items: list[Summary], baseline: Summary | None) -> str:
    cards: list[str] = []
    tail_winner = min(items, key=lambda item: (item.p95_ns, item.p99_ns, item.mean_ns))
    min_winner = min(items, key=lambda item: (item.min_ns, item.mean_ns))
    mean_winner = min(items, key=lambda item: (item.mean_ns, item.p95_ns))

    for item in items:
        metrics = [
            ("Mean", format_ms(item.mean_ns), "lowest mean" if item.path == mean_winner.path else ""),
            ("P50", format_ms(item.p50_ns), ""),
            ("P95", format_ms(item.p95_ns), "lowest tail" if item.path == tail_winner.path else ""),
            ("P99", format_ms(item.p99_ns), "lowest tail" if item.path == tail_winner.path else ""),
            ("Max", format_ms(item.max_ns), ""),
        ]

        delta_html = ""
        if baseline and item.path != baseline.path:
            delta = delta_percent(item.p95_ns, baseline.p95_ns)
            delta_label = "n/a" if math.isnan(delta) else f"{delta:+.1f}%"
            delta_html = (
                "<div class='card-callout'>"
                f"<span class='callout-kicker'>Against baseline P95</span>"
                "<div class='callout-row'>"
                f"<strong>{html.escape(improvement_copy(delta))}</strong>"
                f"<span>{delta_label}</span>"
                "</div>"
                "</div>"
            )
        elif baseline and item.path == baseline.path:
            delta_html = (
                "<div class='card-callout baseline-tag'>"
                "<span class='callout-kicker'>Reference run</span>"
                "<div class='callout-row'>"
                "<strong>Baseline</strong>"
                "<span>used for deltas</span>"
                "</div>"
                "</div>"
            )

        metric_html = "".join(metric_box(label, value, badge) for label, value, badge in metrics)

        winner_chip = ""
        if item.path == tail_winner.path:
            winner_chip = "<span class='winner-chip'>lowest tail</span>"

        cards.append(
            "<article class='run-card'>"
            "<header>"
            "<div class='card-head'>"
            f"<h3>{html.escape(item.label)}</h3>"
            f"{winner_chip}"
            "</div>"
            f"<p>{html.escape(item.path.name)}</p>"
            "</header>"
            f"{delta_html}"
            "<div class='mini-grid'>"
            f"<div class='mini-metric'><span>Rows</span><strong>{item.count:,}</strong></div>"
            f"{metric_box('Min', format_ms(item.min_ns), 'lowest min' if item.path == min_winner.path else '')}"
            f"{metric_html}"
            "</div>"
            "</article>"
        )

    return "".join(cards)


def make_table(items: list[Summary], baseline: Summary | None) -> str:
    headers = "".join(f"<th>{metric.upper()}</th>" for metric in METRICS)
    delta_header = "<th>P95 vs baseline</th>" if baseline else ""
    rows: list[str] = []

    for item in items:
        delta_cell = ""
        if baseline and item.path != baseline.path:
            delta = delta_percent(item.p95_ns, baseline.p95_ns)
            delta_text = improvement_copy(delta)
            if not math.isnan(delta):
                delta_text = f"{delta_text} ({delta:+.1f}%)"
            delta_cell = f"<td class='delta-cell' data-label='P95 vs baseline'>{html.escape(delta_text)}</td>"
        elif baseline:
            delta_cell = "<td class='delta-cell' data-label='P95 vs baseline'>Baseline</td>"

        rows.append(
            "<tr>"
            f"<td data-label='Run'><strong>{html.escape(item.label)}</strong><div class='path-note'>{html.escape(item.path.name)}</div></td>"
            f"<td data-label='Rows'>{item.count:,}</td>"
            f"<td data-label='Mean'>{html.escape(format_ms(item.mean_ns))}</td>"
            f"<td data-label='P50'>{html.escape(format_ms(item.p50_ns))}</td>"
            f"<td data-label='P95'>{html.escape(format_ms(item.p95_ns))}</td>"
            f"<td data-label='P99'>{html.escape(format_ms(item.p99_ns))}</td>"
            f"<td data-label='Max'>{html.escape(format_ms(item.max_ns))}</td>"
            f"{delta_cell}"
            "</tr>"
        )

    return (
        "<table>"
        "<thead><tr><th>Run</th><th>Rows</th>"
        f"{headers}{delta_header}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def top_findings(items: list[Summary], baseline: Summary | None) -> list[str]:
    if baseline is None:
        winner = min(items, key=lambda item: (item.p95_ns, item.p99_ns))
        return [
            f"Lowest tail latency: {winner.label} (P95 {format_ms(winner.p95_ns)}, P99 {format_ms(winner.p99_ns)}).",
            "Mode: peer comparison.",
        ]

    winner = min(items, key=lambda item: (item.p95_ns, item.p99_ns))
    best_p95 = delta_percent(winner.p95_ns, baseline.p95_ns)
    best_p99 = delta_percent(winner.p99_ns, baseline.p99_ns)
    mean_winner = min(items, key=lambda item: item.mean_ns)
    first_line = (
        f"Lowest tail latency: baseline {baseline.label}."
        if winner.path == baseline.path
        else f"Lowest tail latency: {winner.label} (P95 {improvement_copy(best_p95)}, P99 {improvement_copy(best_p99)} vs baseline)."
    )
    return [
        first_line,
        f"Lowest mean latency: {mean_winner.label} ({format_ms(mean_winner.mean_ns)}).",
        "Units shown: milliseconds.",
    ]


def build_html_report(items: list[Summary], baseline: Summary | None, title: str, output: Path) -> str:
    colors = {item.path: PALETTE[index % len(PALETTE)] for index, item in enumerate(items)}
    table_rows = "".join(
        "<tr>"
        f"<td>{html.escape(item.label)}</td>"
        f"{table_metric_cell(item, baseline, 'min')}"
        f"{table_metric_cell(item, baseline, 'p50')}"
        f"{table_metric_cell(item, baseline, 'mean')}"
        f"{table_metric_cell(item, baseline, 'p95')}"
        f"{table_metric_cell(item, baseline, 'p99')}"
        f"{table_metric_cell(item, baseline, 'max')}"
        "</tr>"
        for item in items
    )
    table = (
        "<table>"
        "<thead><tr><th>Run</th><th>Min</th><th>P50</th><th>Mean</th><th>P95</th><th>P99</th><th>Max</th></tr></thead>"
        f"<tbody>{table_rows}</tbody>"
        "</table>"
    )
    ecdf_svg = build_ecdf_chart(items, colors, baseline.path if baseline else None)
    points_svg = build_points_chart(items, colors, baseline)
    histogram_panel = build_histogram_panel(items, baseline, colors)

    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(title)}</title>
    <style>
      :root {{
        --paper: #f4efe7;
        --panel: #fcfaf6;
        --panel-strong: #fffdf8;
        --ink: #1f2937;
        --muted: #5b6472;
        --line: rgba(96, 82, 64, 0.16);
        --accent: #1d4ed8;
        --shadow: 0 8px 22px rgba(76, 58, 40, 0.08);
      }}

      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
        color: var(--ink);
        background: var(--paper);
      }}

      .wrap {{
        width: min(1120px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 24px 0 40px;
      }}

      .panel {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 20px;
        box-shadow: var(--shadow);
        padding: 16px 16px 14px;
      }}

      h1, h2 {{
        margin: 0;
        font-weight: 700;
        letter-spacing: -0.02em;
      }}

      h1 {{
        font-size: 2rem;
        line-height: 1;
        margin-bottom: 16px;
      }}

      .grid {{
        display: grid;
        grid-template-columns: 1fr;
        gap: 14px;
      }}

      .chart-panel svg {{
        width: 100%;
        height: auto;
        display: block;
      }}

      .histogram-toolbar {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin: 2px 0 12px;
      }}

      .histogram-toggle {{
        border: 1px solid var(--line);
        background: #f6f0e7;
        color: var(--muted);
        border-radius: 999px;
        padding: 8px 12px;
        font: inherit;
        font-size: 0.92rem;
        font-weight: 600;
        cursor: pointer;
      }}

      .histogram-toggle:hover {{
        background: #efe7dc;
      }}

      .histogram-toggle.is-active {{
        background: var(--accent);
        border-color: var(--accent);
        color: #fffdf8;
      }}

      .histogram-note {{
        margin: 0 0 12px;
        color: var(--muted);
        font-size: 0.95rem;
      }}

      .histogram-view {{
        display: none;
      }}

      .histogram-view.is-active {{
        display: block;
      }}

      table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.95rem;
      }}

      th,
      td {{
        padding: 14px 12px;
        text-align: left;
        border-bottom: 1px solid var(--line);
        vertical-align: top;
      }}

      th {{
        color: var(--muted);
        font-size: 0.76rem;
        letter-spacing: 0.09em;
        text-transform: uppercase;
      }}

      td strong {{
        display: block;
      }}

      .delta-badge {{
        display: inline-flex;
        margin-top: 6px;
        padding: 4px 8px;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }}

      .delta-good {{
        background: #e8f7ef;
        border: 1px solid #b9e3c8;
        color: #166534;
      }}

      .delta-bad {{
        background: #fdecec;
        border: 1px solid #f5c2c2;
        color: #b42318;
      }}

      .delta-neutral {{
        background: #f2f4f7;
        border: 1px solid #d5d9e0;
        color: #475467;
      }}

      @media (max-width: 980px) {{
        .wrap {{
          width: min(100vw - 28px, 980px);
        }}

        table {{
          font-size: 0.88rem;
        }}
      }}

      @media (max-width: 640px) {{
        .wrap {{
          width: min(100vw - 20px, 980px);
        }}

        .panel {{
          overflow-x: auto;
        }}

        .histogram-toolbar {{
          flex-direction: column;
        }}
      }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <h1>{html.escape(title)}</h1>

      <section class="grid">
        <section class="panel">
          {table}
        </section>
        <section class="panel chart-panel">{points_svg}</section>
        {histogram_panel}
        <section class="panel chart-panel">{ecdf_svg}</section>
      </section>
    </main>
    <script>
      for (const panel of document.querySelectorAll("[data-histogram-panel]")) {{
        const buttons = panel.querySelectorAll("[data-histogram-target]");
        const views = panel.querySelectorAll("[data-histogram-view]");
        for (const button of buttons) {{
          button.addEventListener("click", () => {{
            const target = button.dataset.histogramTarget;
            for (const candidate of buttons) {{
              candidate.classList.toggle("is-active", candidate === button);
            }}
            for (const view of views) {{
              view.classList.toggle("is-active", view.dataset.histogramView === target);
            }}
          }});
        }}
      }}
    </script>
  </body>
</html>
"""


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    inputs = resolve_inputs(args.inputs)
    baseline_path = args.baseline.expanduser().resolve() if args.baseline else None
    if baseline_path and baseline_path not in inputs:
        raise SystemExit(f"--baseline must be one of the resolved CSV inputs: {baseline_path}")

    items = [read_summary(path) for path in inputs]
    items = sort_runs(items, baseline_path)
    baseline = next((item for item in items if item.path == baseline_path), None)

    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_html_report(items, baseline, args.title, output), encoding="utf-8")

    print(f"wrote {output}")


if __name__ == "__main__":
    main()
