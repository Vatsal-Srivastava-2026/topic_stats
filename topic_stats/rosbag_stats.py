#!/usr/bin/env python3
"""Analyze ROS 2 bag timing, throughput, and recorder/source latency."""

import argparse
from array import array
import csv
from dataclasses import dataclass, field
import math
from pathlib import Path
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import rosbag2_py
from rclpy.serialization import deserialize_message
from rclpy.utilities import remove_ros_args
from rosidl_runtime_py.utilities import get_message


DEFAULT_WINDOWS_S = [2.0, 1.0, 0.5, 0.25, 0.1]
plt = None


class ProgressDisplay:
    """Small dependency-free progress bar for interactive and redirected stdout."""

    def __init__(self, label: str, total: int, unit: str):
        self.label = label
        self.total = max(1, int(total))
        self.unit = unit
        self.started_at = time.monotonic()
        self.last_render_at = 0.0
        self.last_bucket = -1
        self.last_width = 0
        self.is_tty = sys.stdout.isatty()

    @staticmethod
    def _format_duration(seconds: float) -> str:
        if not math.isfinite(seconds) or seconds < 0:
            return "--:--"
        total_seconds = int(round(seconds))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def update(
        self,
        value: int,
        status: str,
        *,
        force: bool = False,
    ) -> None:
        now = time.monotonic()
        completed = max(0, int(value))
        fraction = min(1.0, completed / self.total)
        bucket = int(fraction * 20)

        if self.is_tty:
            if not force and completed < self.total and now - self.last_render_at < 0.1:
                return
        elif not force and completed < self.total and bucket <= self.last_bucket:
            return

        elapsed = max(now - self.started_at, 1e-9)
        rate = completed / elapsed
        remaining = max(0, self.total - completed)
        eta = remaining / rate if rate > 0.0 else math.inf
        bar_width = 28
        filled = int(round(fraction * bar_width))
        bar = "#" * filled + "-" * (bar_width - filled)
        clean_status = " ".join(str(status).splitlines())
        line = (
            f"{self.label} [{bar}] {100.0 * fraction:6.2f}% "
            f"{completed:,}/{self.total:,} | {rate:,.1f} {self.unit}/s | "
            f"ETA {self._format_duration(eta)} | {clean_status}"
        )

        if self.is_tty:
            padding = " " * max(0, self.last_width - len(line))
            print(f"\r{line}{padding}", end="", flush=True)
            self.last_width = len(line)
        else:
            print(line, flush=True)

        self.last_render_at = now
        self.last_bucket = bucket

    def finish(self, value: int, status: str) -> None:
        self.update(value, status, force=True)
        if self.is_tty:
            print()


def load_plotting() -> None:
    """Import Matplotlib only when plots are actually requested."""
    global plt
    if plt is not None:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as pyplot

    plt = pyplot


@dataclass(frozen=True)
class TopicInfo:
    index: int
    name: str
    type_name: str
    message_count: int


@dataclass
class TopicSamples:
    info: TopicInfo
    arrival_ns: array = field(default_factory=lambda: array("q"))
    # Aligned with arrival_ns. Zero means no usable msg.header.stamp.
    header_ns: array = field(default_factory=lambda: array("q"))
    messages_without_header: int = 0
    zero_header_stamps: int = 0
    deserialization_errors: int = 0
    message_type_error: Optional[str] = None

    def arrival_array(self) -> np.ndarray:
        return np.asarray(self.arrival_ns, dtype=np.int64)

    def header_array(self) -> np.ndarray:
        values = np.asarray(self.header_ns, dtype=np.int64)
        return values[values != 0]

    def paired_arrays(self) -> Tuple[np.ndarray, np.ndarray]:
        arrivals = self.arrival_array()
        headers = np.asarray(self.header_ns, dtype=np.int64)
        mask = headers != 0
        return arrivals[mask], headers[mask]


@dataclass(frozen=True)
class NumericStats:
    count: int
    mean: float
    std: float
    minimum: float
    maximum: float
    p50: float
    p95: float
    p99: float


@dataclass(frozen=True)
class GapStats:
    values: NumericStats
    negative_count: int
    zero_count: int


@dataclass(frozen=True)
class BurstStats:
    window_s: float
    n_bins: int
    mean_count: float
    std_count: float
    min_count: int
    max_count: int
    mean_rate_hz: float
    peak_rate_hz: float
    peak_to_mean: float
    coefficient_of_variation: float
    zero_bin_count: int
    zero_bin_fraction: float


def safe_slug(topic: str) -> str:
    slug = topic.strip("/").replace("/", "_")
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", slug).strip("_.-")
    return slug or "topic"


def normalize_bag_dir(value: Path) -> Path:
    path = value.expanduser().resolve()
    if path.is_file():
        if path.name == "metadata.yaml" or path.suffix in {".mcap", ".db3"}:
            path = path.parent
    if not path.is_dir():
        raise ValueError(f"Rosbag directory does not exist: {path}")
    if not (path / "metadata.yaml").is_file():
        raise ValueError(
            f"Expected {path / 'metadata.yaml'}; pass the rosbag directory "
            "that contains metadata.yaml and the bag data file(s)."
        )
    return path


def read_metadata(bag_dir: Path):
    try:
        return rosbag2_py.Info().read_metadata(str(bag_dir), "")
    except Exception as exc:
        raise RuntimeError(f"Could not read rosbag metadata: {exc}") from exc


def inventory_from_metadata(metadata) -> List[TopicInfo]:
    rows = []
    nonempty = [
        item
        for item in metadata.topics_with_message_count
        if int(item.message_count) > 0
    ]
    nonempty.sort(key=lambda item: item.topic_metadata.name)
    for index, item in enumerate(nonempty, start=1):
        rows.append(
            TopicInfo(
                index=index,
                name=str(item.topic_metadata.name),
                type_name=str(item.topic_metadata.type),
                message_count=int(item.message_count),
            )
        )
    return rows


def print_inventory(inventory: Sequence[TopicInfo]) -> None:
    print("\nTopics with nonzero messages")
    print("=" * 92)
    print(f"{'index':>5}  {'messages':>12}  {'topic':<44}  type")
    print("-" * 92)
    for item in inventory:
        print(
            f"{item.index:5d}  {item.message_count:12d}  "
            f"{item.name:<44}  {item.type_name}"
        )
    print("=" * 92)


def _expand_selection_token(token: str, inventory: Sequence[TopicInfo]) -> Iterable[int]:
    token = token.strip()
    if not token:
        return []
    if token.lower() == "all":
        return range(1, len(inventory) + 1)
    if token.isdigit():
        return [int(token)]
    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", token)
    if match:
        start, end = map(int, match.groups())
        step = 1 if end >= start else -1
        return range(start, end + step, step)
    by_name = {item.name: item.index for item in inventory}
    if token in by_name:
        return [by_name[token]]
    raise ValueError(f"Unknown topic index, range, or name: {token!r}")


def parse_selection(text: str, inventory: Sequence[TopicInfo]) -> List[TopicInfo]:
    tokens = [part for part in re.split(r"[\s,]+", text.strip()) if part]
    if not tokens:
        raise ValueError("No topics selected")
    selected_indices: List[int] = []
    for token in tokens:
        selected_indices.extend(_expand_selection_token(token, inventory))
    invalid = sorted({i for i in selected_indices if i < 1 or i > len(inventory)})
    if invalid:
        raise ValueError(f"Topic indices out of range: {invalid}")
    wanted = set(selected_indices)
    return [item for item in inventory if item.index in wanted]


def select_topics(
    inventory: Sequence[TopicInfo], requested: Optional[Sequence[str]]
) -> List[TopicInfo]:
    if requested:
        return parse_selection(" ".join(requested), inventory)
    while True:
        try:
            answer = input(
                "Select topic indices (for example: 1 3-5), topic names, or 'all': "
            )
        except EOFError as exc:
            raise RuntimeError(
                "Interactive input is unavailable. Use --select all or "
                "--select 1 3-5."
            ) from exc
        try:
            return parse_selection(answer, inventory)
        except ValueError as exc:
            print(f"Invalid selection: {exc}", file=sys.stderr)


def make_reader(bag_dir: Path, storage_id: str, topics: Sequence[str]):
    reader = rosbag2_py.SequentialReader()
    storage_options = rosbag2_py.StorageOptions(
        uri=str(bag_dir), storage_id=storage_id
    )
    converter_options = rosbag2_py.ConverterOptions("", "")
    reader.open(storage_options, converter_options)
    bag_filter = rosbag2_py.StorageFilter(topics=list(topics))
    reader.set_filter(bag_filter)
    return reader


def extract_header_stamp_ns(message) -> Tuple[Optional[int], str]:
    header = getattr(message, "header", None)
    stamp = getattr(header, "stamp", None) if header is not None else None
    if stamp is None or not hasattr(stamp, "sec") or not hasattr(stamp, "nanosec"):
        return None, "missing"
    value = int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)
    if value == 0:
        return None, "zero"
    return value, "ok"


def read_selected_topics(
    bag_dir: Path,
    storage_id: str,
    selected: Sequence[TopicInfo],
) -> Dict[str, TopicSamples]:
    print("Status: loading message types for selected topics ...", flush=True)
    samples = {item.name: TopicSamples(info=item) for item in selected}
    message_classes = {}
    for item in selected:
        try:
            message_classes[item.name] = get_message(item.type_name)
        except Exception as exc:
            samples[item.name].message_type_error = str(exc)
            message_classes[item.name] = None

    print(f"Status: opening {storage_id} rosbag for sequential reading ...", flush=True)
    reader = make_reader(bag_dir, storage_id, [item.name for item in selected])
    expected = sum(item.message_count for item in selected)
    processed = 0
    progress = ProgressDisplay("Reading messages", expected, "msg")
    progress.update(0, "reader opened; waiting for first selected message", force=True)
    while reader.has_next():
        topic, serialized, timestamp_ns = reader.read_next()
        topic_samples = samples.get(topic)
        if topic_samples is None:
            continue
        topic_samples.arrival_ns.append(int(timestamp_ns))
        msg_cls = message_classes[topic]
        if msg_cls is None:
            topic_samples.header_ns.append(0)
        else:
            try:
                message = deserialize_message(serialized, msg_cls)
                header_ns, status = extract_header_stamp_ns(message)
                topic_samples.header_ns.append(header_ns or 0)
                if status == "missing":
                    topic_samples.messages_without_header += 1
                elif status == "zero":
                    topic_samples.zero_header_stamps += 1
            except Exception:
                topic_samples.header_ns.append(0)
                topic_samples.deserialization_errors += 1
        processed += 1
        if processed % 100 == 0 or processed == expected:
            progress.update(processed, f"reading {topic}")
    completion_status = f"complete; last topic: {topic}" if processed else "complete; no messages read"
    progress.finish(processed, completion_status)
    print(f"Read {processed:,} selected messages from the bag.")
    if processed != expected:
        print(
            f"Warning: bag metadata expected {expected:,} selected messages, but "
            f"the reader returned {processed:,}.",
            file=sys.stderr,
        )
    return samples


def numeric_stats(values: np.ndarray) -> Optional[NumericStats]:
    values = np.asarray(values, dtype=np.float64)
    if values.size == 0:
        return None
    return NumericStats(
        count=int(values.size),
        mean=float(np.mean(values)),
        std=float(np.std(values)),
        minimum=float(np.min(values)),
        maximum=float(np.max(values)),
        p50=float(np.percentile(values, 50)),
        p95=float(np.percentile(values, 95)),
        p99=float(np.percentile(values, 99)),
    )


def gap_values_s(timestamps_ns: np.ndarray) -> np.ndarray:
    values = np.asarray(timestamps_ns, dtype=np.int64)
    if values.size < 2:
        return np.asarray([], dtype=np.float64)
    return np.diff(values).astype(np.float64) * 1e-9


def gap_stats(timestamps_ns: np.ndarray) -> Optional[GapStats]:
    gaps = gap_values_s(timestamps_ns)
    values = numeric_stats(gaps)
    if values is None:
        return None
    return GapStats(
        values=values,
        negative_count=int(np.count_nonzero(gaps < 0.0)),
        zero_count=int(np.count_nonzero(gaps == 0.0)),
    )


def bin_counts(
    timestamps_ns: np.ndarray, window_s: float, max_bins: int
) -> Tuple[np.ndarray, np.ndarray]:
    timestamps = np.asarray(timestamps_ns, dtype=np.int64)
    if timestamps.size == 0:
        return np.asarray([], dtype=np.float64), np.asarray([], dtype=np.int64)
    window_ns = max(1, int(round(window_s * 1e9)))
    origin = int(np.min(timestamps))
    indices = (timestamps - origin) // window_ns
    n_bins = int(np.max(indices)) + 1
    if n_bins > max_bins:
        raise ValueError(
            f"requires {n_bins:,} bins, exceeding --max-bins={max_bins:,}"
        )
    counts = np.bincount(indices.astype(np.int64), minlength=n_bins)
    centers_s = (np.arange(n_bins, dtype=np.float64) + 0.5) * window_s
    return centers_s, counts


def burst_stats(counts: np.ndarray, window_s: float) -> Optional[BurstStats]:
    counts = np.asarray(counts, dtype=np.int64)
    if counts.size == 0:
        return None
    mean_count = float(np.mean(counts))
    std_count = float(np.std(counts))
    min_count = int(np.min(counts))
    max_count = int(np.max(counts))
    zero_count = int(np.count_nonzero(counts == 0))
    return BurstStats(
        window_s=window_s,
        n_bins=int(counts.size),
        mean_count=mean_count,
        std_count=std_count,
        min_count=min_count,
        max_count=max_count,
        mean_rate_hz=mean_count / window_s,
        peak_rate_hz=max_count / window_s,
        peak_to_mean=(max_count / mean_count) if mean_count else math.inf,
        coefficient_of_variation=(std_count / mean_count) if mean_count else math.inf,
        zero_bin_count=zero_count,
        zero_bin_fraction=zero_count / counts.size,
    )


def downsample_xy(
    x: np.ndarray, y: np.ndarray, max_points: int
) -> Tuple[np.ndarray, np.ndarray, int]:
    if x.size <= max_points:
        return x, y, 1
    stride = int(math.ceil(x.size / max_points))
    return x[::stride], y[::stride], stride


def add_empty_message(ax, text: str) -> None:
    ax.text(0.5, 0.5, text, ha="center", va="center", transform=ax.transAxes)
    ax.set_xticks([])
    ax.set_yticks([])


def plot_gap_series(
    path: Path,
    topic: str,
    label: str,
    timestamps_ns: np.ndarray,
    max_plot_points: int,
) -> None:
    gaps_ms = gap_values_s(timestamps_ns) * 1000.0
    fig, ax = plt.subplots(figsize=(13, 5))
    stats = numeric_stats(gaps_ms)
    if stats is None:
        add_empty_message(ax, f"Fewer than two usable {label.lower()} timestamps")
    else:
        x = (timestamps_ns[1:] - timestamps_ns[0]).astype(np.float64) * 1e-9
        plot_x, plot_y, stride = downsample_xy(x, gaps_ms, max_plot_points)
        ax.plot(plot_x, plot_y, linewidth=0.8, alpha=0.85)
        ax.axhline(stats.mean, color="tab:orange", linestyle="--", linewidth=1.3)
        min_i = int(np.argmin(gaps_ms))
        max_i = int(np.argmax(gaps_ms))
        ax.scatter(
            [x[min_i], x[max_i]],
            [gaps_ms[min_i], gaps_ms[max_i]],
            c=["tab:green", "tab:red"],
            s=34,
            zorder=3,
            label=(
                f"mean ± stddev = {stats.mean:.3f} ± {stats.std:.3f} ms"
            ),
        )
        if stride > 1:
            ax.text(
                0.99,
                0.98,
                f"displayed every {stride}th interval; statistics use all data",
                ha="right",
                va="top",
                transform=ax.transAxes,
                fontsize=9,
            )
        ax.legend(loc="best")
        ax.set_xlabel(f"Time since first usable {label.lower()} timestamp [s]")
        ax.set_ylabel("Gap to previous message [ms]")
        ax.grid(True, alpha=0.25)
    ax.set_title(f"{topic}: continuity of {label.lower()} timestamps")
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def plot_aggregate_gaps(
    path: Path,
    samples: Sequence[TopicSamples],
    use_header: bool,
    max_plot_points: int,
) -> None:
    label = "Header" if use_header else "Recorder-arrival"
    fig, ax = plt.subplots(figsize=(15, 7))
    plotted = 0
    for item in samples:
        timestamps = item.header_array() if use_header else item.arrival_array()
        gaps_ms = gap_values_s(timestamps) * 1000.0
        stats = numeric_stats(gaps_ms)
        if stats is None:
            continue
        x = (timestamps[1:] - timestamps[0]).astype(np.float64) * 1e-9
        plot_x, plot_y, _ = downsample_xy(x, gaps_ms, max_plot_points)
        line = ax.plot(
            plot_x,
            plot_y,
            linewidth=0.8,
            alpha=0.72,
            label=(
                f"{item.info.name} | mean ± stddev "
                f"{stats.mean:.3f} ± {stats.std:.3f} ms"
            ),
        )[0]
        color = line.get_color()
        ax.axhline(stats.mean, color=color, linestyle="--", linewidth=0.75, alpha=0.7)
        min_i = int(np.argmin(gaps_ms))
        max_i = int(np.argmax(gaps_ms))
        ax.scatter(
            [x[min_i], x[max_i]],
            [gaps_ms[min_i], gaps_ms[max_i]],
            color=color,
            s=25,
            zorder=3,
        )
        plotted += 1
    if plotted == 0:
        add_empty_message(ax, f"No topics have two usable {label.lower()} timestamps")
    else:
        ax.set_xlabel(f"Time since each topic's first {label.lower()} timestamp [s]")
        ax.set_ylabel("Gap to previous message [ms]")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best", fontsize=8)
    ax.set_title(
        f"{label} timestamp continuity for all selected topics\n"
        "Dashed lines show means; markers identify each topic's minimum and maximum"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def plot_topic_throughput(
    path: Path,
    topic: str,
    label: str,
    timestamps_ns: np.ndarray,
    windows_s: Sequence[float],
    max_bins: int,
    max_plot_points: int,
) -> None:
    fig, axes = plt.subplots(
        len(windows_s), 1, figsize=(14, max(3.0 * len(windows_s), 4.5))
    )
    if len(windows_s) == 1:
        axes = [axes]
    for ax, window_s in zip(axes, windows_s):
        try:
            centers, counts = bin_counts(timestamps_ns, window_s, max_bins)
        except ValueError as exc:
            add_empty_message(ax, str(exc))
            continue
        stats = burst_stats(counts, window_s)
        if stats is None:
            add_empty_message(ax, "No usable timestamps")
            continue
        plot_x, plot_y, stride = downsample_xy(centers, counts, max_plot_points)
        ax.plot(plot_x, plot_y, linewidth=1.0)
        ax.axhline(stats.mean_count, color="tab:orange", linestyle="--", linewidth=1.0)
        min_i = int(np.argmin(counts))
        max_i = int(np.argmax(counts))
        ax.scatter(
            [centers[min_i], centers[max_i]],
            [counts[min_i], counts[max_i]],
            c=["tab:green", "tab:red"],
            s=28,
            zorder=3,
        )
        suffix = f" | displayed every {stride}th bin" if stride > 1 else ""
        ax.set_title(
            f"window={window_s:g}s | mean/min/max="
            f"{stats.mean_count:.3f}/{stats.min_count}/{stats.max_count} msg/bin | "
            f"CV={stats.coefficient_of_variation:.3f}{suffix}"
        )
        ax.set_ylabel("Messages / bin")
        ax.grid(True, alpha=0.25)
    axes[-1].set_xlabel(f"Time since first usable {label.lower()} timestamp [s]")
    fig.suptitle(f"{topic}: throughput using {label.lower()} timestamps")
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def plot_aggregate_throughput(
    path: Path,
    samples: Sequence[TopicSamples],
    use_header: bool,
    windows_s: Sequence[float],
    max_bins: int,
    max_plot_points: int,
) -> None:
    label = "Header" if use_header else "Recorder-arrival"
    fig, axes = plt.subplots(
        len(windows_s), 1, figsize=(16, max(3.7 * len(windows_s), 5.0))
    )
    if len(windows_s) == 1:
        axes = [axes]
    for ax, window_s in zip(axes, windows_s):
        plotted = 0
        for item in samples:
            timestamps = item.header_array() if use_header else item.arrival_array()
            try:
                centers, counts = bin_counts(timestamps, window_s, max_bins)
            except ValueError:
                continue
            stats = burst_stats(counts, window_s)
            if stats is None:
                continue
            plot_x, plot_y, _ = downsample_xy(centers, counts, max_plot_points)
            line = ax.plot(
                plot_x,
                plot_y,
                linewidth=0.9,
                alpha=0.78,
                label=(
                    f"{item.info.name} | mean ± stddev "
                    f"{stats.mean_count:.2f} ± {stats.std_count:.2f} msg/bin"
                ),
            )[0]
            color = line.get_color()
            ax.axhline(
                stats.mean_count,
                color=color,
                linestyle="--",
                linewidth=0.7,
                alpha=0.65,
            )
            min_i = int(np.argmin(counts))
            max_i = int(np.argmax(counts))
            ax.scatter(
                [centers[min_i], centers[max_i]],
                [counts[min_i], counts[max_i]],
                color=color,
                s=22,
                zorder=3,
            )
            plotted += 1
        if plotted == 0:
            add_empty_message(ax, f"No plottable data for {window_s:g}s windows")
            continue
        ax.set_title(f"Window size: {window_s:g}s")
        ax.set_ylabel("Messages / bin")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best", fontsize=7)
    axes[-1].set_xlabel(f"Time since each topic's first {label.lower()} timestamp [s]")
    fig.suptitle(
        f"Throughput using {label.lower()} timestamps for all selected topics\n"
        "Legend values are mean ± stddev messages per bin; dashed lines show "
        "means and markers show extrema"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def source_latency_arrays(
    samples: TopicSamples,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    arrivals, headers = samples.paired_arrays()
    if arrivals.size == 0:
        empty = np.asarray([], dtype=np.float64)
        return empty, empty, empty
    elapsed_s = (arrivals - arrivals[0]).astype(np.float64) * 1e-9
    raw_latency_s = (arrivals - headers).astype(np.float64) * 1e-9
    normalized_s = raw_latency_s - raw_latency_s[0]
    return elapsed_s, raw_latency_s, normalized_s


def plot_topic_source_latency(
    path: Path, samples: TopicSamples, max_plot_points: int
) -> None:
    elapsed_s, raw_s, normalized_s = source_latency_arrays(samples)
    fig, ax = plt.subplots(figsize=(13, 5))
    raw_stats = numeric_stats(raw_s * 1000.0)
    normalized_stats = numeric_stats(normalized_s * 1000.0)
    if raw_stats is None or normalized_stats is None:
        add_empty_message(ax, "No messages with a usable msg.header.stamp")
    else:
        x, y, stride = downsample_xy(
            elapsed_s, normalized_s * 1000.0, max_plot_points
        )
        ax.plot(x, y, linewidth=0.9)
        ax.axhline(
            normalized_stats.mean,
            color="tab:orange",
            linestyle="--",
            linewidth=1.2,
        )
        min_i = int(np.argmin(normalized_s))
        max_i = int(np.argmax(normalized_s))
        ax.scatter(
            [elapsed_s[min_i], elapsed_s[max_i]],
            [normalized_s[min_i] * 1000.0, normalized_s[max_i] * 1000.0],
            c=["tab:green", "tab:red"],
            s=34,
            zorder=3,
            label=(
                "raw recorder-header latency mean ± stddev = "
                f"{raw_stats.mean:.3f} ± {raw_stats.std:.3f} ms"
            ),
        )
        ax.legend(loc="best")
        if stride > 1:
            ax.text(
                0.99,
                0.98,
                f"displayed every {stride}th sample; statistics use all data",
                ha="right",
                va="top",
                transform=ax.transAxes,
                fontsize=9,
            )
        ax.set_xlabel("Time since topic's first recorder-arrival timestamp [s]")
        ax.set_ylabel("Normalized recorder − header latency [ms]")
        ax.grid(True, alpha=0.25)
    ax.set_title(
        f"{samples.info.name}: source-to-recorder latency drift\n"
        "Normalized latency = current latency − first valid latency"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def plot_aggregate_source_latency(
    path: Path, samples: Sequence[TopicSamples], max_plot_points: int
) -> None:
    fig, ax = plt.subplots(figsize=(15, 7))
    plotted = 0
    for item in samples:
        elapsed_s, raw_s, normalized_s = source_latency_arrays(item)
        raw_stats = numeric_stats(raw_s * 1000.0)
        norm_stats = numeric_stats(normalized_s * 1000.0)
        if raw_stats is None or norm_stats is None:
            continue
        x, y, _ = downsample_xy(elapsed_s, normalized_s * 1000.0, max_plot_points)
        line = ax.plot(
            x,
            y,
            linewidth=0.9,
            alpha=0.78,
            label=(
                f"{item.info.name} | normalized mean ± stddev "
                f"{norm_stats.mean:.3f} ± {norm_stats.std:.3f} ms | "
                f"raw mean ± stddev {raw_stats.mean:.3f} ± {raw_stats.std:.3f} ms"
            ),
        )[0]
        color = line.get_color()
        ax.axhline(norm_stats.mean, color=color, linestyle="--", linewidth=0.7)
        min_i = int(np.argmin(normalized_s))
        max_i = int(np.argmax(normalized_s))
        ax.scatter(
            [elapsed_s[min_i], elapsed_s[max_i]],
            [normalized_s[min_i] * 1000.0, normalized_s[max_i] * 1000.0],
            color=color,
            s=25,
            zorder=3,
        )
        plotted += 1
    if plotted == 0:
        add_empty_message(ax, "No selected topics have usable msg.header.stamp values")
    else:
        ax.set_xlabel("Time since each topic's first recorder-arrival timestamp [s]")
        ax.set_ylabel("Normalized recorder − header latency [ms]")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best", fontsize=7)
    ax.set_title(
        "Source-to-recorder latency drift for all selected topics\n"
        "Normalized latency = current latency − first valid latency; "
        "dashed lines show means and markers show min/max"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def write_topic_csv(path: Path, samples: TopicSamples) -> None:
    arrivals = samples.arrival_array()
    headers = np.asarray(samples.header_ns, dtype=np.int64)
    first_arrival = int(arrivals[0]) if arrivals.size else 0
    valid_headers = headers[headers != 0]
    first_header = int(valid_headers[0]) if valid_headers.size else 0
    paired_latency_ns = arrivals[headers != 0] - headers[headers != 0]
    first_latency = int(paired_latency_ns[0]) if paired_latency_ns.size else 0
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "message_index",
                "recorder_arrival_timestamp_ns",
                "header_timestamp_ns",
                "recorder_arrival_elapsed_s",
                "header_elapsed_s",
                "recorder_minus_header_latency_s",
                "normalized_latency_s",
            ]
        )
        for index, (arrival, header) in enumerate(zip(arrivals, headers), start=1):
            arrival_i = int(arrival)
            header_i = int(header)
            if header_i:
                latency_ns = arrival_i - header_i
                header_elapsed = f"{(header_i - first_header) * 1e-9:.9f}"
                latency = f"{latency_ns * 1e-9:.9f}"
                normalized = f"{(latency_ns - first_latency) * 1e-9:.9f}"
                header_text = str(header_i)
            else:
                header_elapsed = ""
                latency = ""
                normalized = ""
                header_text = ""
            writer.writerow(
                [
                    index,
                    arrival_i,
                    header_text,
                    f"{(arrival_i - first_arrival) * 1e-9:.9f}",
                    header_elapsed,
                    latency,
                    normalized,
                ]
            )


def format_numeric_stats(stats: Optional[NumericStats], unit: str) -> str:
    if stats is None:
        return "unavailable"
    return (
        f"count={stats.count}, mean={stats.mean:.6f} {unit}, "
        f"stddev={stats.std:.6f} {unit}, min={stats.minimum:.6f} {unit}, "
        f"p50={stats.p50:.6f} {unit}, p95={stats.p95:.6f} {unit}, "
        f"p99={stats.p99:.6f} {unit}, max={stats.maximum:.6f} {unit}"
    )


def write_inventory(
    path: Path, inventory: Sequence[TopicInfo], selected_names: Sequence[str]
) -> None:
    selected = set(selected_names)
    with path.open("w") as stream:
        stream.write("Topics with nonzero messages\n")
        stream.write("selected\tindex\tmessages\ttopic\ttype\n")
        for item in inventory:
            stream.write(
                f"{'yes' if item.name in selected else 'no'}\t{item.index}\t"
                f"{item.message_count}\t{item.name}\t{item.type_name}\n"
            )


def write_summary(
    path: Path,
    bag_dir: Path,
    metadata,
    samples: Sequence[TopicSamples],
    windows_s: Sequence[float],
    max_bins: int,
) -> None:
    with path.open("w") as stream:
        stream.write("ROS 2 bag timestamp, continuity, throughput, and latency analysis\n")
        stream.write("=================================================================\n")
        stream.write(f"Bag directory: {bag_dir}\n")
        stream.write(f"Storage identifier: {metadata.storage_identifier}\n")
        stream.write(f"Bag message count (all topics): {metadata.message_count}\n")
        stream.write(f"Selected topics: {len(samples)}\n")
        stream.write(
            "Throughput windows [s]: " + ", ".join(f"{x:g}" for x in windows_s) + "\n"
        )
        stream.write(
            "Recorder-arrival timestamp means the stored rosbag timestamp supplied "
            "by rosbag2 when the message was recorded.\n"
        )
        stream.write(
            "Header timestamp means msg.header.stamp. Messages without that field, "
            "with a zero stamp, or that cannot be deserialized are excluded from "
            "header and source-latency analysis.\n"
        )
        stream.write(
            "Normalized source latency = (recorder timestamp - header timestamp) "
            "minus that topic's first valid latency.\n"
        )
        stream.write(
            "Throughput bins cover the full timestamp extent, including a possibly "
            "partial final bin.\n\n"
        )

        for item in samples:
            arrivals = item.arrival_array()
            headers = item.header_array()
            stream.write(f"Topic: {item.info.name}\n")
            stream.write("-" * (7 + len(item.info.name)) + "\n")
            stream.write(f"Type: {item.info.type_name}\n")
            stream.write(f"Metadata message count: {item.info.message_count}\n")
            stream.write(f"Messages read: {arrivals.size}\n")
            stream.write(f"Usable header timestamps: {headers.size}\n")
            stream.write(
                f"Messages without header: {item.messages_without_header}; "
                f"zero header stamps: {item.zero_header_stamps}; "
                f"deserialization errors: {item.deserialization_errors}\n"
            )
            if item.message_type_error:
                stream.write(f"Message type import error: {item.message_type_error}\n")

            for label, timestamps in (
                ("Recorder-arrival", arrivals),
                ("Header", headers),
            ):
                stream.write(f"\n{label} timestamp analysis\n")
                if timestamps.size:
                    span_s = (int(np.max(timestamps)) - int(np.min(timestamps))) * 1e-9
                    effective_hz = (
                        (timestamps.size - 1) / span_s if span_s > 0 and timestamps.size > 1 else 0.0
                    )
                    stream.write(
                        f"  first_ns={int(timestamps[0])}, last_ns={int(timestamps[-1])}, "
                        f"extent={span_s:.6f} s, effective_frequency={effective_hz:.6f} Hz\n"
                    )
                gaps = gap_stats(timestamps)
                if gaps is None:
                    stream.write("  Gaps: unavailable\n")
                else:
                    stream.write(
                        "  Gaps: " + format_numeric_stats(gaps.values, "s") + "\n"
                    )
                    stream.write(
                        f"  Non-monotonic (negative) gaps: {gaps.negative_count}; "
                        f"zero gaps: {gaps.zero_count}\n"
                    )
                stream.write("  Throughput/burstiness:\n")
                for window_s in windows_s:
                    try:
                        _, counts = bin_counts(timestamps, window_s, max_bins)
                        burst = burst_stats(counts, window_s)
                    except ValueError as exc:
                        stream.write(f"    {window_s:g}s: skipped ({exc})\n")
                        continue
                    if burst is None:
                        stream.write(f"    {window_s:g}s: unavailable\n")
                    else:
                        stream.write(
                            f"    {window_s:g}s: bins={burst.n_bins}, "
                            f"mean={burst.mean_count:.6f}, stddev={burst.std_count:.6f}, "
                            f"min={burst.min_count}, max={burst.max_count}, "
                            f"mean_rate={burst.mean_rate_hz:.6f} Hz, "
                            f"peak_rate={burst.peak_rate_hz:.6f} Hz, "
                            f"peak/mean={burst.peak_to_mean:.6f}, "
                            f"CV={burst.coefficient_of_variation:.6f}, "
                            f"empty_bins={burst.zero_bin_count}/{burst.n_bins} "
                            f"({100.0 * burst.zero_bin_fraction:.3f}%)\n"
                        )

            _, raw_latency_s, normalized_s = source_latency_arrays(item)
            stream.write("\nSource-to-recorder latency analysis\n")
            stream.write(
                "  Raw recorder-header latency: "
                + format_numeric_stats(numeric_stats(raw_latency_s * 1000.0), "ms")
                + "\n"
            )
            stream.write(
                "  Normalized latency drift: "
                + format_numeric_stats(numeric_stats(normalized_s * 1000.0), "ms")
                + "\n\n"
            )


def create_outputs(
    stats_dir: Path,
    bag_dir: Path,
    metadata,
    inventory: Sequence[TopicInfo],
    sample_map: Dict[str, TopicSamples],
    windows_s: Sequence[float],
    max_bins: int,
    max_plot_points: int,
) -> None:
    samples = list(sample_map.values())
    output_count = 6 * len(samples) + 7
    progress = ProgressDisplay("Generating outputs", output_count, "file")
    completed = 0
    progress.update(completed, "loading Matplotlib", force=True)
    load_plotting()
    subdirs = {
        "header_gaps": stats_dir / "header_gaps",
        "arrival_gaps": stats_dir / "arrival_gaps",
        "throughput_header": stats_dir / "throughput_header",
        "throughput_arrival": stats_dir / "throughput_arrival",
        "source_latency": stats_dir / "source_latency",
        "data": stats_dir / "data",
    }
    stats_dir.mkdir(parents=True, exist_ok=True)
    for directory in subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

    def step(status, function, *function_args):
        nonlocal completed
        progress.update(completed, status, force=True)
        function(*function_args)
        completed += 1

    step(
        "writing topic inventory",
        write_inventory,
        stats_dir / "topic_inventory.txt",
        inventory,
        [item.info.name for item in samples],
    )
    for item in samples:
        slug = safe_slug(item.info.name)
        arrivals = item.arrival_array()
        headers = item.header_array()
        step(
            f"{item.info.name}: writing timestamp CSV",
            write_topic_csv,
            subdirs["data"] / f"{slug}_timestamps.csv",
            item,
        )
        step(
            f"{item.info.name}: plotting recorder-arrival gaps",
            plot_gap_series,
            subdirs["arrival_gaps"] / f"{slug}_arrival_gaps.png",
            item.info.name,
            "Recorder-arrival",
            arrivals,
            max_plot_points,
        )
        step(
            f"{item.info.name}: plotting header gaps",
            plot_gap_series,
            subdirs["header_gaps"] / f"{slug}_header_gaps.png",
            item.info.name,
            "Header",
            headers,
            max_plot_points,
        )
        step(
            f"{item.info.name}: plotting recorder-arrival throughput",
            plot_topic_throughput,
            subdirs["throughput_arrival"] / f"{slug}_arrival_throughput.png",
            item.info.name,
            "Recorder-arrival",
            arrivals,
            windows_s,
            max_bins,
            max_plot_points,
        )
        step(
            f"{item.info.name}: plotting header throughput",
            plot_topic_throughput,
            subdirs["throughput_header"] / f"{slug}_header_throughput.png",
            item.info.name,
            "Header",
            headers,
            windows_s,
            max_bins,
            max_plot_points,
        )
        step(
            f"{item.info.name}: plotting normalized source latency",
            plot_topic_source_latency,
            subdirs["source_latency"] / f"{slug}_source_latency.png",
            item,
            max_plot_points,
        )

    step(
        "plotting all-topic header timestamp gaps",
        plot_aggregate_gaps,
        stats_dir / "all_topics_header_timestamp_gaps.png",
        samples,
        True,
        max_plot_points,
    )
    step(
        "plotting all-topic recorder-arrival timestamp gaps",
        plot_aggregate_gaps,
        stats_dir / "all_topics_arrival_timestamp_gaps.png",
        samples,
        False,
        max_plot_points,
    )
    step(
        "plotting all-topic header throughput",
        plot_aggregate_throughput,
        stats_dir / "all_topics_header_throughput.png",
        samples,
        True,
        windows_s,
        max_bins,
        max_plot_points,
    )
    step(
        "plotting all-topic recorder-arrival throughput",
        plot_aggregate_throughput,
        stats_dir / "all_topics_arrival_throughput.png",
        samples,
        False,
        windows_s,
        max_bins,
        max_plot_points,
    )
    step(
        "plotting all-topic normalized source latency",
        plot_aggregate_source_latency,
        stats_dir / "all_topics_source_latency.png", samples, max_plot_points
    )
    step(
        "writing summary.txt",
        write_summary,
        stats_dir / "summary.txt",
        bag_dir,
        metadata,
        samples,
        windows_s,
        max_bins,
    )
    progress.finish(completed, f"complete; wrote {completed} result files")


def parse_cli_args() -> argparse.Namespace:
    argv = remove_ros_args(args=sys.argv)[1:]
    parser = argparse.ArgumentParser(
        description=(
            "Analyze full-duration ROS 2 bag continuity, throughput/burstiness, "
            "and source-to-recorder latency using recorder and msg.header timestamps."
        )
    )
    parser.add_argument(
        "bag",
        type=Path,
        help="Rosbag directory containing metadata.yaml and MCAP/DB3 data",
    )
    parser.add_argument(
        "--select",
        nargs="+",
        metavar="TOPIC_OR_INDEX",
        help=(
            "Non-interactive selection: 'all', 1-based indices/ranges, or topic "
            "names. Without this option, an interactive index prompt is shown."
        ),
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        type=float,
        default=DEFAULT_WINDOWS_S,
        metavar="SECONDS",
        help=(
            "Throughput/bin widths in seconds "
            f"(default: {' '.join(map(str, DEFAULT_WINDOWS_S))})"
        ),
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print nonempty topics and exit without prompting or analyzing",
    )
    parser.add_argument(
        "--max-plot-points",
        type=int,
        default=100_000,
        help=(
            "Maximum displayed samples per topic/line; statistics always use all "
            "samples (default: 100000)"
        ),
    )
    parser.add_argument(
        "--max-bins",
        type=int,
        default=5_000_000,
        help=(
            "Safety limit for bins in one topic/window analysis "
            "(default: 5000000)"
        ),
    )
    args = parser.parse_args(argv)
    if not args.windows or any(value <= 0.0 for value in args.windows):
        parser.error("all --windows values must be > 0")
    if args.max_plot_points < 100:
        parser.error("--max-plot-points must be >= 100")
    if args.max_bins < 1:
        parser.error("--max-bins must be >= 1")
    args.windows = sorted(set(args.windows), reverse=True)
    return args


def main() -> int:
    args = parse_cli_args()
    try:
        print("Status: validating rosbag directory and reading metadata ...", flush=True)
        bag_dir = normalize_bag_dir(args.bag)
        metadata = read_metadata(bag_dir)
        inventory = inventory_from_metadata(metadata)
        if not inventory:
            print("The bag metadata reports no topics with nonzero messages.")
            return 1
        print(f"Bag: {bag_dir}")
        print(f"Storage: {metadata.storage_identifier}")
        print_inventory(inventory)
        if args.list_only:
            return 0
        print("Status: waiting for topic selection ...", flush=True)
        selected = select_topics(inventory, args.select)
        print("\nSelected topics:")
        for item in selected:
            print(f"  [{item.index}] {item.name} ({item.message_count:,} messages)")
        sample_map = read_selected_topics(
            bag_dir, str(metadata.storage_identifier), selected
        )
        stats_dir = bag_dir / "stats"
        create_outputs(
            stats_dir,
            bag_dir,
            metadata,
            inventory,
            sample_map,
            args.windows,
            args.max_bins,
            args.max_plot_points,
        )
        print(f"\nAnalysis complete: {stats_dir}")
        print(f"Summary: {stats_dir / 'summary.txt'}")
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
