#!/usr/bin/env python3
"""Launch one live topic_stats process for every configured/selected topic."""

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import statistics
from typing import Any, Dict, List, Optional, Sequence

import yaml

from topic_stats.live_topic_selector import (
    discover_live_topics,
    interactively_select_live_topics,
)


DEFAULT_DURATION_S = 30.0
DEFAULT_WINDOWS_S = [2.0, 1.0, 0.5, 0.25, 0.1]
MAX_AGGREGATE_PLOT_POINTS = 20_000


@dataclass
class AggregateTopicSamples:
    topic: str
    arrival_times_s: List[float]
    header_times_s: List[float]
    drift_times_s: List[float]
    drift_ms: List[float]
    duration_s: float


def load_config(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    config_path = path.expanduser().resolve()
    try:
        with config_path.open() as stream:
            loaded = yaml.safe_load(stream)
    except Exception as exc:
        raise RuntimeError(f"Could not read config {config_path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("Config root must be a YAML mapping")
    return loaded


def config_value(config: Dict[str, Any], *names: str, default=None):
    for name in names:
        if name in config:
            return config[name]
    return default


def resolve_settings(args: argparse.Namespace, config: Dict[str, Any]) -> Dict[str, Any]:
    topics = args.topics
    if topics is None:
        topics = config_value(config, "topics", default=None)
    if topics is not None:
        if not isinstance(topics, list) or not all(isinstance(x, str) for x in topics):
            raise ValueError("topics must be a YAML list of ROS topic names")
        topics = list(dict.fromkeys(topics))

    duration = args.duration_s
    if duration is None:
        duration = config_value(
            config,
            "duration",
            "duration_s",
            "recording_time_window",
            default=DEFAULT_DURATION_S,
        )
    windows = args.windows_s
    if windows is None:
        windows = config_value(
            config, "windows", "window_sizes", default=DEFAULT_WINDOWS_S
        )
    output_dir = args.output_dir
    if output_dir is None:
        configured_output = config_value(config, "output_dir", default=None)
        output_dir = Path(configured_output) if configured_output else None
    qos_depth = args.qos_depth
    if qos_depth is None:
        qos_depth = config_value(config, "qos_depth", default=1000)
    qos_reliability = args.qos_reliability
    if qos_reliability is None:
        qos_reliability = config_value(config, "qos_reliability", default="auto")
    loop = args.loop
    if loop is None:
        loop = bool(config_value(config, "loop", default=False))
    measure_bytes = args.measure_bytes
    if measure_bytes is None:
        measure_bytes = bool(config_value(config, "measure_bytes", default=False))
    discovery_timeout = args.discovery_timeout
    if discovery_timeout is None:
        discovery_timeout = config_value(config, "discovery_timeout", default=2.0)
    include_system_topics = args.include_system_topics
    if include_system_topics is None:
        include_system_topics = bool(
            config_value(config, "include_system_topics", default=False)
        )

    duration = float(duration)
    windows = sorted({float(value) for value in windows}, reverse=True)
    qos_depth = int(qos_depth)
    discovery_timeout = float(discovery_timeout)
    if duration <= 0.0:
        raise ValueError("duration must be > 0")
    if not windows or any(value <= 0.0 for value in windows):
        raise ValueError("all window sizes must be > 0")
    if any(value > duration for value in windows):
        raise ValueError("window sizes cannot exceed duration")
    if qos_depth <= 0:
        raise ValueError("qos_depth must be > 0")
    if qos_reliability not in {"auto", "reliable", "best_effort"}:
        raise ValueError("qos_reliability must be auto, reliable, or best_effort")
    if discovery_timeout <= 0.0:
        raise ValueError("discovery_timeout must be > 0")
    return {
        "topics": topics,
        "duration": duration,
        "windows": windows,
        "output_dir": output_dir,
        "qos_depth": qos_depth,
        "qos_reliability": qos_reliability,
        "loop": loop,
        "measure_bytes": measure_bytes,
        "discovery_timeout": discovery_timeout,
        "include_system_topics": include_system_topics,
    }


def topic_slug(topic: str) -> str:
    value = topic.strip("/").replace("/", "_") or "topic"
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)


def find_source_package_dir() -> Path:
    """Resolve the source package without falling back to install/share."""
    this_file = Path(__file__).resolve()
    for parent in [this_file.parent, *this_file.parents]:
        if (parent / "package.xml").is_file():
            return parent
    try:
        from ament_index_python.packages import get_package_prefix

        prefix = Path(get_package_prefix("topic_stats")).resolve()
        if prefix.parent.name == "install":
            candidate = prefix.parent.parent / "src" / "topic_stats"
            if (candidate / "package.xml").is_file():
                return candidate
    except Exception:
        pass
    raise RuntimeError(
        "Could not locate the topic_stats source package. Expected a source "
        "checkout containing package.xml or <workspace>/src/topic_stats."
    )


def find_package_dir() -> Path:
    """Resolve the package source for default output, falling back to cwd."""
    try:
        return find_source_package_dir()
    except RuntimeError:
        return Path.cwd().resolve()


def default_config_path() -> Path:
    path = find_source_package_dir() / "config" / "live_topic_stats.yaml"
    if not path.is_file():
        raise RuntimeError(f"Default source config does not exist: {path}")
    return path


def create_multitopic_output_dir(settings: Dict[str, Any]) -> tuple[Path, str]:
    configured = settings["output_dir"]
    base_dir = (
        configured.expanduser().resolve()
        if configured is not None
        else find_package_dir() / "out"
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%y%m%d_%H%M%S")
    output_dir = base_dir / f"multitopic_stats_{stamp}"
    suffix = 1
    while output_dir.exists():
        output_dir = base_dir / f"multitopic_stats_{stamp}_{suffix:02d}"
        suffix += 1
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir, stamp


def create_topic_output_prefixes(topics: Sequence[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    used_names = set()
    for topic in topics:
        base_name = topic_slug(topic)
        name = base_name
        suffix = 1
        while name in used_names:
            suffix += 1
            name = f"{base_name}_{suffix:02d}"
        used_names.add(name)
        result[topic] = f"{name}_"
    return result


def load_aggregate_samples(
    topics: Sequence[str],
    multitopic_output_dir: Path,
    topic_output_prefixes: Dict[str, str],
    capture_duration_s: float,
) -> List[AggregateTopicSamples]:
    samples = []
    for topic in topics:
        prefix = topic_output_prefixes[topic]
        direct_csv = multitopic_output_dir / "data" / f"{prefix}timestamps.csv"
        if direct_csv.is_file():
            csv_paths = [direct_csv]
        else:
            csv_paths = sorted(
                multitopic_output_dir.glob(f"*/data/{prefix}timestamps.csv")
            )

        arrivals: List[float] = []
        headers: List[float] = []
        drift_times: List[float] = []
        drift_values: List[float] = []
        for capture_number, path in enumerate(csv_paths):
            offset_s = capture_number * capture_duration_s
            with path.open(newline="") as stream:
                for row in csv.DictReader(stream):
                    try:
                        arrival_s = float(row["arrival_time_s"]) + offset_s
                    except (KeyError, TypeError, ValueError):
                        continue
                    arrivals.append(arrival_s)
                    header_elapsed = row.get("header_elapsed_s", "")
                    drift_ms = row.get("normalized_arrival_minus_header_ms", "")
                    if header_elapsed:
                        try:
                            headers.append(float(header_elapsed) + offset_s)
                        except ValueError:
                            pass
                    if drift_ms:
                        try:
                            drift_times.append(arrival_s)
                            drift_values.append(float(drift_ms))
                        except ValueError:
                            pass

        if csv_paths:
            samples.append(
                AggregateTopicSamples(
                    topic=topic,
                    arrival_times_s=arrivals,
                    header_times_s=headers,
                    drift_times_s=drift_times,
                    drift_ms=drift_values,
                    duration_s=len(csv_paths) * capture_duration_s,
                )
            )
    return samples


def downsample_xy(
    x_values: Sequence[float], y_values: Sequence[float]
) -> tuple[Sequence[float], Sequence[float]]:
    if len(x_values) <= MAX_AGGREGATE_PLOT_POINTS:
        return x_values, y_values
    stride = (
        len(x_values) + MAX_AGGREGATE_PLOT_POINTS - 1
    ) // MAX_AGGREGATE_PLOT_POINTS
    return x_values[::stride], y_values[::stride]


def add_empty_message(axis, text: str) -> None:
    axis.text(0.5, 0.5, text, ha="center", va="center", transform=axis.transAxes)
    axis.set_xticks([])
    axis.set_yticks([])


def plot_aggregate_gaps(
    path: Path,
    samples: Sequence[AggregateTopicSamples],
    *,
    use_header: bool,
) -> None:
    import matplotlib.pyplot as plt

    label = "Header" if use_header else "Subscriber-arrival"
    fig, axis = plt.subplots(figsize=(15, 7))
    plotted = 0
    for item in samples:
        timestamps = item.header_times_s if use_header else item.arrival_times_s
        if len(timestamps) < 2:
            continue
        gaps_ms = [
            (timestamps[index] - timestamps[index - 1]) * 1000.0
            for index in range(1, len(timestamps))
        ]
        x_values, y_values = downsample_xy(timestamps[1:], gaps_ms)
        mean = statistics.mean(gaps_ms)
        stddev = statistics.pstdev(gaps_ms)
        line = axis.plot(
            x_values,
            y_values,
            linewidth=0.8,
            alpha=0.72,
            label=f"{item.topic} | mean ± stddev {mean:.3f} ± {stddev:.3f} ms",
        )[0]
        axis.axhline(
            mean,
            color=line.get_color(),
            linestyle="--",
            linewidth=0.75,
            alpha=0.7,
        )
        plotted += 1
    if plotted == 0:
        add_empty_message(axis, f"No topics have two usable {label.lower()} timestamps")
    else:
        axis.set_xlabel(f"Time since each topic's first {label.lower()} timestamp [s]")
        axis.set_ylabel("Gap to previous message [ms]")
        axis.grid(True, alpha=0.25)
        axis.legend(loc="best", fontsize=8)
    axis.set_title(f"{label} timestamp continuity for all analyzed topics")
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def fixed_bin_counts(
    timestamps_s: Sequence[float], duration_s: float, window_s: float
) -> tuple[List[float], List[int]]:
    n_bins = max(1, int(duration_s / window_s + 1e-12))
    counts = [0] * n_bins
    for timestamp in timestamps_s:
        index = int(timestamp / window_s)
        if 0 <= index < n_bins:
            counts[index] += 1
    centers = [(index + 0.5) * window_s for index in range(n_bins)]
    return centers, counts


def plot_aggregate_throughput(
    path: Path,
    samples: Sequence[AggregateTopicSamples],
    *,
    use_header: bool,
    windows_s: Sequence[float],
) -> None:
    import matplotlib.pyplot as plt

    label = "Header" if use_header else "Subscriber-arrival"
    fig, axes = plt.subplots(
        len(windows_s), 1, figsize=(16, max(3.7 * len(windows_s), 5.0))
    )
    if len(windows_s) == 1:
        axes = [axes]
    for axis, window_s in zip(axes, windows_s):
        plotted = 0
        for item in samples:
            timestamps = item.header_times_s if use_header else item.arrival_times_s
            if not timestamps:
                continue
            centers, counts = fixed_bin_counts(
                timestamps, item.duration_s, window_s
            )
            x_values, y_values = downsample_xy(centers, counts)
            mean = statistics.mean(counts)
            stddev = statistics.pstdev(counts)
            line = axis.plot(
                x_values,
                y_values,
                linewidth=0.9,
                alpha=0.78,
                label=(
                    f"{item.topic} | mean ± stddev "
                    f"{mean:.2f} ± {stddev:.2f} msg/bin"
                ),
            )[0]
            axis.axhline(
                mean,
                color=line.get_color(),
                linestyle="--",
                linewidth=0.7,
                alpha=0.65,
            )
            plotted += 1
        if plotted == 0:
            add_empty_message(axis, f"No plottable data for {window_s:g}s windows")
            continue
        axis.set_title(f"Window size: {window_s:g}s")
        axis.set_ylabel("Messages / bin")
        axis.grid(True, alpha=0.25)
        axis.legend(loc="best", fontsize=7)
    axes[-1].set_xlabel(f"Time since each topic's first {label.lower()} timestamp [s]")
    fig.suptitle(
        f"Throughput using {label.lower()} timestamps for all analyzed topics\n"
        "Legend values are mean ± stddev messages per bin"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def plot_aggregate_source_latency(
    path: Path, samples: Sequence[AggregateTopicSamples]
) -> None:
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(15, 7))
    plotted = 0
    for item in samples:
        if not item.drift_ms:
            continue
        x_values, y_values = downsample_xy(item.drift_times_s, item.drift_ms)
        mean = statistics.mean(item.drift_ms)
        stddev = statistics.pstdev(item.drift_ms)
        line = axis.plot(
            x_values,
            y_values,
            linewidth=0.9,
            alpha=0.78,
            label=(
                f"{item.topic} | mean ± stddev "
                f"{mean:.3f} ± {stddev:.3f} ms"
            ),
        )[0]
        axis.axhline(
            mean,
            color=line.get_color(),
            linestyle="--",
            linewidth=0.7,
            alpha=0.7,
        )
        plotted += 1
    if plotted == 0:
        add_empty_message(axis, "No analyzed topics have usable msg.header.stamp values")
    else:
        axis.set_xlabel("Time since each topic's recording start [s]")
        axis.set_ylabel("Normalized arrival − header drift [ms]")
        axis.grid(True, alpha=0.25)
        axis.legend(loc="best", fontsize=7)
    axis.set_title(
        "Normalized source-latency drift for all analyzed topics\n"
        "Drift = arrival elapsed time − header elapsed time"
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def generate_all_topic_plots(
    output_dir: Path,
    topics: Sequence[str],
    topic_output_prefixes: Dict[str, str],
    settings: Dict[str, Any],
) -> List[Path]:
    print("Status: loading live timestamp data for all-topic plots ...", flush=True)
    samples = load_aggregate_samples(
        topics, output_dir, topic_output_prefixes, settings["duration"]
    )
    if not samples:
        print("Status: no saved topic data is available for all-topic plots.")
        return []
    try:
        import matplotlib

        matplotlib.use("Agg")
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is required for all-topic plotting. On Ubuntu/ROS, "
            "install it with 'sudo apt install python3-matplotlib'."
        ) from exc

    outputs = [
        output_dir / "all_topics_arrival_timestamp_gaps.png",
        output_dir / "all_topics_header_timestamp_gaps.png",
        output_dir / "all_topics_arrival_throughput.png",
        output_dir / "all_topics_header_throughput.png",
        output_dir / "all_topics_source_latency.png",
    ]
    print("Status: plotting all-topic subscriber-arrival gaps ...", flush=True)
    plot_aggregate_gaps(outputs[0], samples, use_header=False)
    print("Status: plotting all-topic header gaps ...", flush=True)
    plot_aggregate_gaps(outputs[1], samples, use_header=True)
    print("Status: plotting all-topic subscriber-arrival throughput ...", flush=True)
    plot_aggregate_throughput(
        outputs[2], samples, use_header=False, windows_s=settings["windows"]
    )
    print("Status: plotting all-topic header throughput ...", flush=True)
    plot_aggregate_throughput(
        outputs[3], samples, use_header=True, windows_s=settings["windows"]
    )
    print("Status: plotting all-topic normalized source latency ...", flush=True)
    plot_aggregate_source_latency(outputs[4], samples)
    return outputs


def write_multitopic_summary(
    output_dir: Path,
    topics: Sequence[str],
    topic_output_prefixes: Dict[str, str],
    settings: Dict[str, Any],
) -> Optional[Path]:
    sections = []
    for topic in topics:
        prefix = topic_output_prefixes[topic]
        direct = output_dir / "data" / f"{prefix}summary.txt"
        paths = [direct] if direct.is_file() else sorted(
            output_dir.glob(f"*/data/{prefix}summary.txt")
        )
        for path in paths:
            sections.append((topic, path))
    if not sections:
        return None

    summary_path = output_dir / "summary.txt"
    with summary_path.open("w") as stream:
        stream.write("ROS 2 live multi-topic timing and throughput analysis\n")
        stream.write("=====================================================\n")
        stream.write(f"Analyzed topics: {len(topics)}\n")
        stream.write(f"Loop mode: {settings['loop']}\n")
        stream.write(f"Capture duration: {settings['duration']:g} s\n")
        stream.write(
            "Throughput windows [s]: "
            + ", ".join(f"{value:g}" for value in settings["windows"])
            + "\n\n"
        )
        for topic, path in sections:
            stream.write(f"Topic: {topic}\n")
            stream.write(f"Capture data: {path.relative_to(output_dir)}\n")
            stream.write("-" * 80 + "\n")
            stream.write(path.read_text())
            stream.write("\n")
    return summary_path


def child_command(
    topic: str,
    index: int,
    settings: Dict[str, Any],
    multitopic_output_dir: Path,
    output_prefix: str,
    capture_stamp_base: str,
) -> List[str]:
    command = [
        sys.executable,
        "-m",
        "topic_stats.topic_stats",
        topic,
        "--duration",
        f"{settings['duration']:g}",
        "--windows",
        *[f"{value:g}" for value in settings["windows"]],
        "--qos-depth",
        str(settings["qos_depth"]),
        "--qos-reliability",
        settings["qos_reliability"],
        "--node-name",
        f"topic_stats_{index}_{topic_slug(topic)[:80]}",
        "--session-output-dir",
        str(multitopic_output_dir),
        "--output-prefix",
        output_prefix,
        "--capture-stamp-base",
        capture_stamp_base,
        "--overview-plots-only",
    ]
    if settings["loop"]:
        command.append("--loop")
    if settings["measure_bytes"]:
        command.append("--measure-bytes")
    return command


def forward_output(stream, prefix: str, lock: threading.Lock) -> None:
    buffer: List[str] = []
    while True:
        char = stream.read(1)
        if char == "":
            break
        if char in "\r\n":
            if buffer:
                text = "".join(buffer).strip()
                if text:
                    with lock:
                        print(f"[{prefix}] {text}", flush=True)
                buffer.clear()
        else:
            buffer.append(char)
    if buffer:
        text = "".join(buffer).strip()
        if text:
            with lock:
                print(f"[{prefix}] {text}", flush=True)


def launch_analyzers(topics: Sequence[str], settings: Dict[str, Any]) -> int:
    multitopic_output_dir, capture_stamp_base = create_multitopic_output_dir(settings)
    topic_output_prefixes = create_topic_output_prefixes(topics)
    print("\nLaunching live analyzers")
    print("=" * 80, flush=True)
    print(f"Topics:          {len(topics)}")
    print(f"Duration:        {settings['duration']:g} s")
    print(f"Windows:         {' '.join(f'{x:g}' for x in settings['windows'])} s")
    print(f"QoS reliability: {settings['qos_reliability']}")
    print(f"QoS depth:       {settings['qos_depth']}")
    print(f"Loop mode:       {settings['loop']}")
    print(f"Measure bytes:   {settings['measure_bytes']}")
    print(f"Output directory: {multitopic_output_dir}")
    print("=" * 80)

    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    output_lock = threading.Lock()
    children = []
    readers = []
    for index, topic in enumerate(topics, start=1):
        command = child_command(
            topic,
            index,
            settings,
            multitopic_output_dir,
            topic_output_prefixes[topic],
            capture_stamp_base,
        )
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=0,
            env=environment,
            start_new_session=True,
        )
        assert process.stdout is not None
        thread = threading.Thread(
            target=forward_output,
            args=(process.stdout, topic, output_lock),
            daemon=True,
        )
        thread.start()
        children.append((topic, process))
        readers.append(thread)
        print(f"Started [{process.pid}] {topic}")

    interrupted = False
    reported = set()
    try:
        while True:
            running = 0
            for topic, process in children:
                return_code = process.poll()
                if return_code is None:
                    running += 1
                elif topic not in reported:
                    print(f"Analyzer finished: {topic} (exit {return_code})")
                    reported.add(topic)
            if running == 0:
                break
            time.sleep(0.2)
    except KeyboardInterrupt:
        interrupted = True
        print("\nStopping all live analyzers ...", flush=True)
        for _, process in children:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        for _, process in children:
            try:
                process.wait(timeout=30.0)
            except subprocess.TimeoutExpired:
                process.terminate()
        print("All analyzer processes stopped.")
    finally:
        for thread in readers:
            thread.join(timeout=2.0)

    aggregate_failed = False
    try:
        summary_path = write_multitopic_summary(
            multitopic_output_dir, topics, topic_output_prefixes, settings
        )
        if summary_path is not None:
            print(f"Multitopic summary: {summary_path}")
        aggregate_paths = generate_all_topic_plots(
            multitopic_output_dir, topics, topic_output_prefixes, settings
        )
        if aggregate_paths:
            print("All-topic condensed plots:")
            for path in aggregate_paths:
                print(f"  {path.name}")
    except Exception as exc:
        aggregate_failed = True
        print(f"Failed to generate all-topic condensed plots: {exc}")

    if interrupted:
        return 130
    failed = [(topic, process.returncode) for topic, process in children if process.returncode]
    if failed:
        print("One or more analyzers failed:")
        for topic, return_code in failed:
            print(f"  {topic}: exit {return_code}")
        return 1
    if aggregate_failed:
        return 1
    print("All live topic analyses completed successfully.")
    print(f"Multitopic output directory: {multitopic_output_dir}")
    return 0


def filter_discovered_topics(
    candidates: Sequence[str], settings: Dict[str, Any]
) -> List[str]:
    timeout_s = settings["discovery_timeout"]
    print(
        f"Status: checking {len(candidates)} configured topic(s) for active "
        f"publishers during a {timeout_s:g} second discovery window ...",
        flush=True,
    )
    discovered = discover_live_topics(
        timeout_s=timeout_s,
        include_system_topics=settings["include_system_topics"],
    )
    discovered_names = {item.name for item in discovered}
    active = [topic for topic in candidates if topic in discovered_names]
    skipped = [topic for topic in candidates if topic not in discovered_names]

    print("\nConfigured topic discovery result")
    print("=" * 80)
    if active:
        print("Active topics (will analyze):")
        for topic in active:
            print(f"  + {topic}")
    if skipped:
        print("Not discovered (skipped):")
        for topic in skipped:
            print(f"  - {topic}")
    print("=" * 80, flush=True)

    if not active:
        raise RuntimeError(
            "None of the configured topics had an active publisher during the "
            f"{timeout_s:g} second discovery window; no analyzers were launched"
        )
    return active


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactively select multiple live ROS 2 topics or load them from YAML, "
            "then launch one topic_stats analyzer process per topic."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        help=(
            "YAML settings file (default: source package "
            "config/live_topic_stats.yaml)"
        ),
    )
    parser.add_argument("--topics", nargs="+", help="Topic names; skips selection prompt")
    parser.add_argument("--duration", dest="duration_s", type=float)
    parser.add_argument("--windows", dest="windows_s", nargs="+", type=float)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--qos-depth", type=int)
    parser.add_argument(
        "--qos-reliability", choices=["auto", "reliable", "best_effort"]
    )
    parser.add_argument("--loop", dest="loop", action="store_true", default=None)
    parser.add_argument("--no-loop", dest="loop", action="store_false")
    parser.add_argument(
        "--measure-bytes", dest="measure_bytes", action="store_true", default=None
    )
    parser.add_argument(
        "--no-measure-bytes", dest="measure_bytes", action="store_false"
    )
    parser.add_argument(
        "--discovery-timeout",
        type=float,
        help=(
            "seconds to discover selectable topics and validate configured topics "
            "before launching analyzers"
        ),
    )
    parser.add_argument(
        "--include-system-topics",
        dest="include_system_topics",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--exclude-system-topics",
        dest="include_system_topics",
        action="store_false",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_cli_args()
        config_path = args.config if args.config is not None else default_config_path()
        print(f"Using config: {config_path.expanduser().resolve()}")
        config = load_config(config_path)
        settings = resolve_settings(args, config)
        topics = settings["topics"]
        if not topics:
            selected = interactively_select_live_topics(
                timeout_s=settings["discovery_timeout"],
                allow_multiple=True,
                include_system_topics=settings["include_system_topics"],
            )
            topics = [item.name for item in selected]
        else:
            topics = filter_discovered_topics(topics, settings)
        return launch_analyzers(topics, settings)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
