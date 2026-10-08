#!/usr/bin/env python3
"""
Measure how bursty a ROS 2 topic is.

The node records message *arrival* times for a fixed duration (30 s by default),
then generates line plots showing the number of received messages in several
measurement/bin widths (5 s, 1 s, 0.5 s, 0.1 s by default).

Examples
--------
ros2 run topic_stats topic_stats /radar/points

ros2 run topic_stats topic_stats /radar/points \
    --duration 60 \
    --windows 10 5 1 0.5 0.1 0.05 \
    --output-dir /tmp/radar_topic_stats

# Repeated 30 s captures; stop after one completely silent capture:
ros2 run topic_stats topic_stats /radar/points --loop

By default, a non-looping run is written directly to:
    <pkg_dir>/out/stats_<topic_name>_YYMMDD_HHMMSS/

Looping runs additionally store each capture in a timestamped subdirectory and
write cross-capture results to a consolidated/ subdirectory.

When --output-dir is supplied, it is treated as the base directory and the
same stats_<topic_name>_YYMMDD_HHMMSS run folder is created underneath it.

# If you need to force subscriber reliability:
ros2 run topic_stats topic_stats /radar/points \
    --qos-reliability best_effort

Important: timestamps are taken with time.monotonic_ns() in the subscription
callback, so these plots show the arrival pattern seen by this subscriber.
That is usually what you want when investigating rosbag/DDS burstiness.
"""

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import csv
import math
import re
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)
from rclpy.utilities import remove_ros_args

from topic_stats.live_topic_selector import interactively_select_live_topics

try:
    from rosidl_runtime_py.utilities import get_message
except Exception:
    get_message = None

try:
    from rclpy.serialization import serialize_message
except Exception:
    serialize_message = None


DEFAULT_DURATION_S = 30.0
DEFAULT_WINDOWS_S = [2.0, 1.0, 0.5, 0.25, 0.1]


def topic_to_slug(topic: str) -> str:
    """Convert a ROS topic name into a filesystem-safe folder component."""
    slug = topic.strip("/").replace("/", "_")
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", slug).strip("_.-")
    return slug or "topic"


def create_session_output_dir(base_output_dir: Path, topic_slug: str) -> Path:
    """Create one top-level run directory for this script invocation."""
    stamp = datetime.now().strftime("%y%m%d_%H%M%S")
    session_dir = base_output_dir / f"stats_{topic_slug}_{stamp}"
    suffix = 1
    while session_dir.exists():
        session_dir = base_output_dir / f"stats_{topic_slug}_{stamp}_{suffix:02d}"
        suffix += 1
    session_dir.mkdir(parents=True, exist_ok=False)
    return session_dir


def find_package_dir() -> Path:
    """
    Best-effort resolution of the ROS package source directory.

    Preference order:
      1. An ancestor of this file containing package.xml (source checkout / symlink install).
      2. <workspace>/src/<package_name>, inferred from the ament install prefix.
      3. The package's ament share directory.
      4. Current working directory as a final fallback.
    """
    this_file = Path(__file__).resolve()

    for parent in [this_file.parent, *this_file.parents]:
        if (parent / "package.xml").is_file():
            return parent

    package_name = (__package__ or this_file.parent.name).split(".")[0]
    try:
        from ament_index_python.packages import (
            get_package_prefix,
            get_package_share_directory,
        )

        prefix = Path(get_package_prefix(package_name)).resolve()
        # Typical colcon layout: <workspace>/install/<pkg>.
        if prefix.parent.name == "install":
            source_candidate = prefix.parent.parent / "src" / package_name
            if (source_candidate / "package.xml").is_file():
                return source_candidate

        share_dir = Path(get_package_share_directory(package_name)).resolve()
        if share_dir.exists():
            return share_dir
    except Exception:
        pass

    return Path.cwd().resolve()


@dataclass
class BinStats:
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


@dataclass
class CaptureResult:
    capture_index: int
    output_dir: Path
    message_count: int
    duration_s: float
    stats: List[BinStats]
    header_message_count: int
    header_stats: List[BinStats]


def fmt_seconds(value: float) -> str:
    if value >= 1.0:
        return f"{value:g}s"
    return f"{value * 1000:g}ms"


def filename_seconds(value: float) -> str:
    return f"{value:g}".replace(".", "p") + "s"


def enum_name(value: Any) -> str:
    """Return a readable ROS enum name while remaining compatible across distros."""
    name = getattr(value, "name", None)
    if name:
        return str(name)
    return str(value)


def duration_to_text(value: Any) -> str:
    """Format rclpy/builtin duration-like values without assuming one ROS distro."""
    if value is None:
        return "n/a"

    nanoseconds = getattr(value, "nanoseconds", None)
    if nanoseconds is not None:
        try:
            ns = int(nanoseconds)
            return f"{ns} ns ({ns * 1e-9:g} s)"
        except Exception:
            pass

    sec = getattr(value, "sec", None)
    nanosec = getattr(value, "nanosec", None)
    if sec is not None and nanosec is not None:
        try:
            ns = int(sec) * 1_000_000_000 + int(nanosec)
            return f"{ns} ns ({ns * 1e-9:g} s)"
        except Exception:
            pass

    return str(value)


def publisher_info_to_dict(info: Any) -> Dict[str, str]:
    """Flatten TopicEndpointInfo + its full exposed QoS profile for logging."""
    qos = info.qos_profile
    gid = getattr(info, "endpoint_gid", None)
    if isinstance(gid, (bytes, bytearray)):
        gid_text = gid.hex()
    elif gid is not None:
        try:
            gid_text = bytes(gid).hex()
        except Exception:
            gid_text = str(gid)
    else:
        gid_text = "n/a"

    row = {
        "node_name": str(getattr(info, "node_name", "n/a")),
        "node_namespace": str(getattr(info, "node_namespace", "n/a")),
        "topic_type": str(getattr(info, "topic_type", "n/a")),
        "endpoint_type": enum_name(getattr(info, "endpoint_type", "n/a")),
        "endpoint_gid": gid_text,
        "qos_history": enum_name(getattr(qos, "history", "n/a")),
        "qos_depth": str(getattr(qos, "depth", "n/a")),
        "qos_reliability": enum_name(getattr(qos, "reliability", "n/a")),
        "qos_durability": enum_name(getattr(qos, "durability", "n/a")),
        "qos_deadline": duration_to_text(getattr(qos, "deadline", None)),
        "qos_lifespan": duration_to_text(getattr(qos, "lifespan", None)),
        "qos_liveliness": enum_name(getattr(qos, "liveliness", "n/a")),
        "qos_liveliness_lease_duration": duration_to_text(
            getattr(qos, "liveliness_lease_duration", None)
        ),
        "qos_avoid_ros_namespace_conventions": str(
            getattr(qos, "avoid_ros_namespace_conventions", "n/a")
        ),
    }
    return row


def format_publisher_info(info: Any, index: int) -> str:
    d = publisher_info_to_dict(info)
    return "\n".join(
        [
            f"Publisher #{index}",
            f"  Node:                         {d['node_namespace'].rstrip('/')}/{d['node_name']}",
            f"  Topic type:                   {d['topic_type']}",
            f"  Endpoint type:                {d['endpoint_type']}",
            f"  Endpoint GID:                 {d['endpoint_gid']}",
            "  QoS:",
            f"    History:                    {d['qos_history']}",
            f"    Depth:                      {d['qos_depth']}",
            f"    Reliability:                {d['qos_reliability']}",
            f"    Durability:                 {d['qos_durability']}",
            f"    Deadline:                   {d['qos_deadline']}",
            f"    Lifespan:                   {d['qos_lifespan']}",
            f"    Liveliness:                 {d['qos_liveliness']}",
            f"    Liveliness lease duration:  {d['qos_liveliness_lease_duration']}",
            f"    Avoid ROS namespace conv.:  {d['qos_avoid_ros_namespace_conventions']}",
        ]
    )


def percentile(values: Sequence[float], q: float) -> Optional[float]:
    """Linear-interpolated percentile, q in [0, 1]."""
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    pos = q * (len(xs) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    frac = pos - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def make_fixed_bins(
    arrival_times_s: Sequence[float],
    recording_duration_s: float,
    window_s: float,
) -> tuple[List[float], List[int]]:
    """
    Count arrivals in fixed-width windows.

    Only complete windows are used. For example, with duration=31 s and
    window=5 s, the plots/statistics use [0, 30) and intentionally ignore the
    final 1 s so that every bin has equal duration.
    """
    n_bins = int(math.floor(recording_duration_s / window_s + 1e-12))
    if n_bins < 1:
        raise ValueError(
            f"Measurement window {window_s:g}s is longer than recording "
            f"duration {recording_duration_s:g}s"
        )

    analysis_duration_s = n_bins * window_s
    counts = [0] * n_bins

    for t in arrival_times_s:
        if t < 0.0 or t >= analysis_duration_s:
            continue
        idx = int(t / window_s)
        if 0 <= idx < n_bins:
            counts[idx] += 1

    centers_s = [(i + 0.5) * window_s for i in range(n_bins)]
    return centers_s, counts


def compute_bin_stats(counts: Sequence[int], window_s: float) -> BinStats:
    if not counts:
        raise ValueError("Cannot compute statistics for zero bins")

    mean_count = statistics.mean(counts)
    std_count = statistics.pstdev(counts) if len(counts) > 1 else 0.0
    min_count = min(counts)
    max_count = max(counts)

    mean_rate_hz = mean_count / window_s
    peak_rate_hz = max_count / window_s
    peak_to_mean = (max_count / mean_count) if mean_count > 0 else math.inf
    coefficient_of_variation = (std_count / mean_count) if mean_count > 0 else math.inf
    zero_bin_count = sum(c == 0 for c in counts)
    zero_bin_fraction = zero_bin_count / len(counts)

    return BinStats(
        window_s=window_s,
        n_bins=len(counts),
        mean_count=mean_count,
        std_count=std_count,
        min_count=min_count,
        max_count=max_count,
        mean_rate_hz=mean_rate_hz,
        peak_rate_hz=peak_rate_hz,
        peak_to_mean=peak_to_mean,
        coefficient_of_variation=coefficient_of_variation,
        zero_bin_count=zero_bin_count,
        zero_bin_fraction=zero_bin_fraction,
    )


class TopicBurstinessNode(Node):
    def __init__(
        self,
        topic_name: str,
        duration_s: float,
        qos_depth: int,
        qos_reliability: str,
        measure_bytes: bool,
        node_name: str = "topic_stats",
    ):
        super().__init__(node_name)
        self.topic = topic_name
        self.duration_s = duration_s
        self.qos_depth = qos_depth
        self.qos_reliability_arg = qos_reliability
        self.measure_bytes = measure_bytes

        self.sub = None
        self.msg_cls = None
        self.topic_type_str: Optional[str] = None
        self.qos_profile: Optional[QoSProfile] = None
        self.publisher_infos_start: List[Any] = []
        self.publisher_infos_end: List[Any] = []

        self.recording_start_ns: Optional[int] = None
        self.arrival_ns: List[int] = []
        # Aligned with arrival_ns. Zero means no usable msg.header.stamp.
        self.header_ns: List[int] = []
        self.serialized_sizes: List[int] = []
        self.capture_index: int = 0

        self.discovery_timer = self.create_timer(0.2, self._try_discover_and_subscribe)

    @property
    def subscribed(self) -> bool:
        return self.sub is not None

    @property
    def elapsed_s(self) -> float:
        if self.recording_start_ns is None:
            return 0.0
        return (time.monotonic_ns() - self.recording_start_ns) * 1e-9

    @property
    def done(self) -> bool:
        return self.subscribed and self.elapsed_s >= self.duration_s

    def relative_arrival_times_s(self) -> List[float]:
        if self.recording_start_ns is None:
            return []
        t0 = self.recording_start_ns
        return [(t - t0) * 1e-9 for t in self.arrival_ns]

    def _import_message_class(self, type_str: str):
        if get_message is not None:
            try:
                return get_message(type_str)
            except Exception:
                pass

        try:
            import importlib

            parts = type_str.split("/")
            if len(parts) != 3 or parts[1] != "msg":
                raise ValueError(f"Unexpected type string format: {type_str}")
            pkg, _, msg_name = parts
            mod = importlib.import_module(f"{pkg}.msg")
            return getattr(mod, msg_name)
        except Exception as exc:
            self.get_logger().error(
                f"Failed to import message class for '{type_str}': {exc}"
            )
            return None

    def _get_publisher_infos(self) -> List[Any]:
        try:
            return list(self.get_publishers_info_by_topic(self.topic))
        except Exception as exc:
            self.get_logger().warning(
                f"Could not query publisher endpoint/QoS details for {self.topic}: {exc}"
            )
            return []

    def _log_publisher_infos(self, infos: Sequence[Any], label: str) -> None:
        self.get_logger().info(
            f"{label}: discovered {len(infos)} publisher endpoint(s) for {self.topic}"
        )
        if not infos:
            self.get_logger().warning(
                "No publisher endpoint information was returned; QoS details unavailable."
            )
            return
        for i, info in enumerate(infos, start=1):
            self.get_logger().info("\n" + format_publisher_info(info, i))

    def snapshot_publishers_at_end(self) -> List[Any]:
        self.publisher_infos_end = self._get_publisher_infos()
        return self.publisher_infos_end

    def start_capture_window(self) -> None:
        """Start a fresh measurement window without recreating the subscription."""
        self.capture_index += 1
        self.arrival_ns = []
        self.header_ns = []
        self.serialized_sizes = []
        self.publisher_infos_end = []
        self.publisher_infos_start = self._get_publisher_infos()
        self._log_publisher_infos(
            self.publisher_infos_start, f"At capture #{self.capture_index} start"
        )
        self.recording_start_ns = time.monotonic_ns()

    def _choose_reliability(self) -> QoSReliabilityPolicy:
        requested = self.qos_reliability_arg
        if requested == "reliable":
            return QoSReliabilityPolicy.RELIABLE
        if requested == "best_effort":
            return QoSReliabilityPolicy.BEST_EFFORT

        # auto: BEST_EFFORT can match both best-effort and reliable publishers,
        # while RELIABLE cannot match a best-effort publisher. Prefer the actual
        # publisher's policy when discoverable, and fall back to BEST_EFFORT.
        try:
            infos = self.get_publishers_info_by_topic(self.topic)
            if infos:
                reliabilities = [info.qos_profile.reliability for info in infos]
                if QoSReliabilityPolicy.BEST_EFFORT in reliabilities:
                    return QoSReliabilityPolicy.BEST_EFFORT
                if QoSReliabilityPolicy.RELIABLE in reliabilities:
                    return QoSReliabilityPolicy.RELIABLE
        except Exception:
            pass

        return QoSReliabilityPolicy.BEST_EFFORT

    def _try_discover_and_subscribe(self) -> None:
        if self.sub is not None:
            return

        topic_type = None
        for name, types in self.get_topic_names_and_types():
            if name == self.topic and types:
                topic_type = types[0]
                break

        if not topic_type:
            return

        msg_cls = self._import_message_class(topic_type)
        if msg_cls is None:
            return

        reliability = self._choose_reliability()
        qos = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=self.qos_depth,
            reliability=reliability,
            durability=QoSDurabilityPolicy.VOLATILE,
        )

        self.topic_type_str = topic_type
        self.msg_cls = msg_cls
        self.qos_profile = qos
        self.sub = self.create_subscription(msg_cls, self.topic, self._on_msg, qos)
        self.start_capture_window()

        try:
            self.discovery_timer.cancel()
        except Exception:
            pass

        self.get_logger().info(
            f"Subscribed to {self.topic} [{self.topic_type_str}] | "
            f"recording={self.duration_s:g}s | QoS={qos}"
        )

    def _on_msg(self, msg) -> None:
        # Keep this callback deliberately lightweight: plotting/statistics are
        # deferred until after capture so the diagnostic tool itself is less
        # likely to perturb a high-rate topic.
        self.arrival_ns.append(time.monotonic_ns())
        header = getattr(msg, "header", None)
        stamp = getattr(header, "stamp", None) if header is not None else None
        if stamp is not None and hasattr(stamp, "sec") and hasattr(stamp, "nanosec"):
            value = int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)
            self.header_ns.append(value if value != 0 else 0)
        else:
            self.header_ns.append(0)

        if self.measure_bytes:
            if serialize_message is None:
                self.serialized_sizes.append(0)
            else:
                try:
                    self.serialized_sizes.append(len(serialize_message(msg)))
                except Exception:
                    self.serialized_sizes.append(0)


def valid_header_times_s(header_timestamps_ns: Sequence[int]) -> List[float]:
    valid = [int(value) for value in header_timestamps_ns if int(value) != 0]
    if not valid:
        return []
    first = valid[0]
    return [(value - first) * 1e-9 for value in valid]


def timestamp_drift_series(
    arrival_times_s: Sequence[float], header_timestamps_ns: Sequence[int]
) -> tuple[List[float], List[float]]:
    paired = [
        (float(arrival), int(header))
        for arrival, header in zip(arrival_times_s, header_timestamps_ns)
        if int(header) != 0
    ]
    if not paired:
        return [], []
    first_arrival, first_header = paired[0]
    x_s = [arrival for arrival, _ in paired]
    drift_ms = [
        ((arrival - first_arrival) - (header - first_header) * 1e-9) * 1000.0
        for arrival, header in paired
    ]
    return x_s, drift_ms


def write_arrivals_csv(
    output_dir: Path,
    arrival_times_s: Sequence[float],
    header_timestamps_ns: Sequence[int],
    serialized_sizes: Sequence[int],
    filename: str = "timestamps.csv",
) -> Path:
    path = output_dir / filename
    valid_headers = [int(value) for value in header_timestamps_ns if int(value) != 0]
    first_header = valid_headers[0] if valid_headers else None
    paired = [
        (float(arrival), int(header))
        for arrival, header in zip(arrival_times_s, header_timestamps_ns)
        if int(header) != 0
    ]
    first_pair_arrival = paired[0][0] if paired else None
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "arrival_time_s",
                "header_timestamp_ns",
                "header_elapsed_s",
                "normalized_arrival_minus_header_ms",
                "serialized_size_bytes",
            ]
        )
        for i, arrival in enumerate(arrival_times_s):
            header = int(header_timestamps_ns[i]) if i < len(header_timestamps_ns) else 0
            size = serialized_sizes[i] if i < len(serialized_sizes) else ""
            if header and first_header is not None and first_pair_arrival is not None:
                header_elapsed_s = (header - first_header) * 1e-9
                drift_ms = (
                    (arrival - first_pair_arrival) - header_elapsed_s
                ) * 1000.0
                header_text = str(header)
                header_elapsed_text = f"{header_elapsed_s:.9f}"
                drift_text = f"{drift_ms:.6f}"
            else:
                header_text = ""
                header_elapsed_text = ""
                drift_text = ""
            writer.writerow(
                [
                    f"{arrival:.9f}",
                    header_text,
                    header_elapsed_text,
                    drift_text,
                    size,
                ]
            )
    return path


def write_publisher_qos_log(
    output_dir: Path,
    topic: str,
    start_infos: Sequence[Any],
    end_infos: Sequence[Any],
    filename_prefix: str = "",
) -> tuple[Path, Path]:
    """Write human-readable and machine-readable publisher endpoint/QoS logs."""
    txt_path = output_dir / f"{filename_prefix}publisher_qos.txt"
    csv_path = output_dir / f"{filename_prefix}publisher_qos.csv"

    with txt_path.open("w") as f:
        f.write(f"Topic: {topic}\n")
        for label, infos in (("capture_start", start_infos), ("capture_end", end_infos)):
            f.write(f"\n=== {label} ({len(infos)} publisher endpoint(s)) ===\n")
            if not infos:
                f.write("No publisher endpoint information available.\n")
            for i, info in enumerate(infos, start=1):
                f.write(format_publisher_info(info, i))
                f.write("\n\n")

    fieldnames = [
        "snapshot",
        "publisher_index",
        "node_name",
        "node_namespace",
        "topic_type",
        "endpoint_type",
        "endpoint_gid",
        "qos_history",
        "qos_depth",
        "qos_reliability",
        "qos_durability",
        "qos_deadline",
        "qos_lifespan",
        "qos_liveliness",
        "qos_liveliness_lease_duration",
        "qos_avoid_ros_namespace_conventions",
    ]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for snapshot, infos in (("capture_start", start_infos), ("capture_end", end_infos)):
            for i, info in enumerate(infos, start=1):
                row = publisher_info_to_dict(info)
                row["snapshot"] = snapshot
                row["publisher_index"] = i
                writer.writerow(row)

    return txt_path, csv_path


def write_summary_csv(
    output_dir: Path,
    stats: Sequence[BinStats],
    filename: str = "arrival_window_summary.csv",
) -> Path:
    path = output_dir / filename
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "window_s",
                "n_bins",
                "mean_messages_per_window",
                "std_messages_per_window",
                "min_messages_per_window",
                "max_messages_per_window",
                "mean_rate_hz",
                "peak_rate_hz",
                "peak_to_mean",
                "coefficient_of_variation",
                "zero_bin_count",
                "zero_bin_fraction",
            ]
        )
        for s in stats:
            writer.writerow(
                [
                    s.window_s,
                    s.n_bins,
                    s.mean_count,
                    s.std_count,
                    s.min_count,
                    s.max_count,
                    s.mean_rate_hz,
                    s.peak_rate_hz,
                    s.peak_to_mean,
                    s.coefficient_of_variation,
                    s.zero_bin_count,
                    s.zero_bin_fraction,
                ]
            )
    return path


def generate_plots(
    throughput_output_dir: Path,
    gap_output_dir: Path,
    topic: str,
    arrival_times_s: Sequence[float],
    recording_duration_s: float,
    windows_s: Sequence[float],
    overview_filename: str,
    gap_filename: str,
    filename_prefix: str = "",
    timestamp_label: str = "subscriber-arrival",
    save_individual_window_plots: bool = True,
) -> tuple[List[Path], List[BinStats]]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is required for plotting. On Ubuntu/ROS, install it "
            "with e.g. 'sudo apt install python3-matplotlib'."
        ) from exc

    topic_label = topic
    plot_paths: List[Path] = []
    all_stats: List[BinStats] = []
    binned_data = []

    for window_s in windows_s:
        centers_s, counts = make_fixed_bins(
            arrival_times_s, recording_duration_s, window_s
        )
        stats = compute_bin_stats(counts, window_s)
        all_stats.append(stats)
        binned_data.append((window_s, centers_s, counts, stats))

        if save_individual_window_plots:
            fig, ax = plt.subplots(figsize=(12, 4.5))
            ax.plot(centers_s, counts, linewidth=1.5)
            ax.axhline(
                stats.mean_count,
                linestyle="--",
                linewidth=1.2,
                label=(
                    f"mean ± stddev = {stats.mean_count:.2f} ± "
                    f"{stats.std_count:.2f} msg/{fmt_seconds(window_s)}"
                ),
            )
            ax.set_title(
                f"{topic_label}: messages per {fmt_seconds(window_s)} window "
                f"using {timestamp_label} timestamps"
            )
            ax.set_xlabel(f"Time since first {timestamp_label} timestamp [s]")
            ax.set_ylabel("Messages in window")
            ax.set_xlim(0, centers_s[-1] + window_s / 2.0)
            ax.grid(True, alpha=0.25)
            ax.legend()
            fig.tight_layout()

            path = throughput_output_dir / (
                f"{filename_prefix}messages_per_{filename_seconds(window_s)}.png"
            )
            fig.savefig(path, dpi=160)
            plt.close(fig)
            plot_paths.append(path)

    # One convenient overview figure containing the same line plots.
    n = len(binned_data)
    fig, axes = plt.subplots(n, 1, figsize=(13, max(3.2 * n, 4.5)), sharex=True)
    if n == 1:
        axes = [axes]

    for ax, (window_s, centers_s, counts, stats) in zip(axes, binned_data):
        ax.plot(centers_s, counts, linewidth=1.25)
        ax.axhline(stats.mean_count, linestyle="--", linewidth=1.0)
        ax.set_ylabel(f"msg / {fmt_seconds(window_s)}")
        ax.set_title(
            f"window={fmt_seconds(window_s)} | mean={stats.mean_count:.2f} | "
            f"std={stats.std_count:.2f} | max={stats.max_count} | "
            f"peak/mean={stats.peak_to_mean:.2f}"
        )
        ax.grid(True, alpha=0.25)

    axes[-1].set_xlabel(f"Time since first {timestamp_label} timestamp [s]")
    fig.suptitle(f"Topic burstiness ({timestamp_label} time): {topic_label}")
    fig.tight_layout()
    overview_path = throughput_output_dir / f"{filename_prefix}{overview_filename}"
    fig.savefig(overview_path, dpi=160)
    plt.close(fig)
    plot_paths.append(overview_path)

    # Inter-arrival time plot: bursts show up as clusters of very small delta-t,
    # while gaps between bursts show up as spikes.
    if len(arrival_times_s) >= 2:
        dt_ms = [
            (arrival_times_s[i] - arrival_times_s[i - 1]) * 1000.0
            for i in range(1, len(arrival_times_s))
        ]
        dt_x = list(arrival_times_s[1:])

        fig, ax = plt.subplots(figsize=(12, 4.5))
        ax.plot(dt_x, dt_ms, linewidth=1.0)
        ax.set_title(f"{topic_label}: {timestamp_label} timestamp gaps")
        ax.set_xlabel(f"Time since first {timestamp_label} timestamp [s]")
        ax.set_ylabel("Δt to previous message [ms]")
        ax.grid(True, alpha=0.25)
        fig.tight_layout()
        dt_path = gap_output_dir / f"{filename_prefix}{gap_filename}"
        fig.savefig(dt_path, dpi=160)
        plt.close(fig)
        plot_paths.append(dt_path)

    return plot_paths, all_stats


def generate_timestamp_drift_plot(
    output_dir: Path,
    topic: str,
    arrival_times_s: Sequence[float],
    header_timestamps_ns: Sequence[int],
    filename_prefix: str = "",
) -> Optional[Path]:
    x_s, drift_ms = timestamp_drift_series(arrival_times_s, header_timestamps_ns)
    if not drift_ms:
        return None
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    mean_drift = statistics.mean(drift_ms)
    min_index = min(range(len(drift_ms)), key=drift_ms.__getitem__)
    max_index = max(range(len(drift_ms)), key=drift_ms.__getitem__)
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(x_s, drift_ms, linewidth=1.0)
    ax.axhline(
        mean_drift,
        color="tab:orange",
        linestyle="--",
        linewidth=1.2,
        label=(
            f"mean ± stddev = {mean_drift:.3f} ± "
            f"{statistics.pstdev(drift_ms):.3f} ms"
        ),
    )
    ax.scatter(
        [x_s[min_index], x_s[max_index]],
        [drift_ms[min_index], drift_ms[max_index]],
        c=["tab:green", "tab:red"],
        s=32,
        zorder=3,
    )
    ax.set_title(
        f"{topic}: normalized header-to-arrival timestamp drift\n"
        "(arrival elapsed time − header elapsed time)"
    )
    ax.set_xlabel("Time since recording start [s]")
    ax.set_ylabel("Normalized arrival − header drift [ms]")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    path = output_dir / f"{filename_prefix}source_latency.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def _series_stats_text(values: Sequence[float], unit: str) -> str:
    if not values:
        return "unavailable"
    return (
        f"count={len(values)}, mean={statistics.mean(values):.6f} {unit}, "
        f"stddev={statistics.pstdev(values):.6f} {unit}, "
        f"min={min(values):.6f} {unit}, "
        f"p50={percentile(values, 0.50):.6f} {unit}, "
        f"p95={percentile(values, 0.95):.6f} {unit}, "
        f"p99={percentile(values, 0.99):.6f} {unit}, "
        f"max={max(values):.6f} {unit}"
    )


def write_timestamp_summary(
    output_dir: Path,
    arrival_times_s: Sequence[float],
    header_timestamps_ns: Sequence[int],
    filename: str = "summary.txt",
) -> Path:
    arrival_gaps_ms = [
        (arrival_times_s[i] - arrival_times_s[i - 1]) * 1000.0
        for i in range(1, len(arrival_times_s))
    ]
    valid_headers = [int(value) for value in header_timestamps_ns if int(value) != 0]
    header_gaps_ms = [
        (valid_headers[i] - valid_headers[i - 1]) * 1e-6
        for i in range(1, len(valid_headers))
    ]
    _, drift_ms = timestamp_drift_series(arrival_times_s, header_timestamps_ns)
    path = output_dir / filename
    with path.open("w") as stream:
        stream.write(f"Messages received: {len(arrival_times_s)}\n")
        stream.write(f"Usable header timestamps: {len(valid_headers)}\n")
        stream.write(
            f"Missing/zero header timestamps: "
            f"{len(arrival_times_s) - len(valid_headers)}\n\n"
        )
        stream.write(
            "Subscriber-arrival timestamp gaps: "
            + _series_stats_text(arrival_gaps_ms, "ms")
            + "\n"
        )
        stream.write(
            f"Subscriber-arrival negative gaps: "
            f"{sum(value < 0.0 for value in arrival_gaps_ms)}\n"
        )
        stream.write(
            "Header timestamp gaps: "
            + _series_stats_text(header_gaps_ms, "ms")
            + "\n"
        )
        stream.write(
            f"Header negative gaps: {sum(value < 0.0 for value in header_gaps_ms)}\n"
        )
        stream.write(
            "Normalized arrival-minus-header drift: "
            + _series_stats_text(drift_ms, "ms")
            + "\n"
        )
        stream.write(
            "Drift definition: (arrival - first arrival) - "
            "(header - first header).\n"
        )
    return path


def parse_cli_args() -> argparse.Namespace:
    # Remove ROS-specific arguments so argparse only sees our own flags.
    argv = remove_ros_args(args=sys.argv)[1:]

    parser = argparse.ArgumentParser(
        description=(
            "Record ROS 2 topic arrival times and generate burstiness plots at "
            "multiple measurement-window sizes."
        )
    )
    parser.add_argument(
        "topic",
        nargs="?",
        help=(
            "ROS 2 topic name. If omitted, active topics are discovered and an "
            "interactive single-topic selector is shown."
        ),
    )
    parser.add_argument(
        "--duration",
        "--recording-window",
        type=float,
        default=DEFAULT_DURATION_S,
        dest="duration_s",
        help=f"Recording duration in seconds (default: {DEFAULT_DURATION_S:g})",
    )
    parser.add_argument(
        "--windows",
        "--measurement-windows",
        type=float,
        nargs="+",
        default=DEFAULT_WINDOWS_S,
        dest="windows_s",
        metavar="SECONDS",
        help=(
            "Measurement/bin widths in seconds "
            f"(default: {' '.join(map(str, DEFAULT_WINDOWS_S))})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Base directory for output runs. A "
            "stats_<topic>_YYMMDD_HHMMSS session folder is created inside it. "
            "Timestamped capture subdirectories are used only with --loop. "
            "Default: <pkg_dir>/out/"
        ),
    )
    parser.add_argument(
        "--session-output-dir",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--output-prefix",
        default="",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--capture-stamp-base",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--overview-plots-only",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--qos-depth",
        type=int,
        default=1000,
        help="Subscriber KEEP_LAST depth (default: 1000)",
    )
    parser.add_argument(
        "--qos-reliability",
        choices=["auto", "reliable", "best_effort"],
        default="auto",
        help=(
            "Subscriber reliability. 'auto' follows discovered publishers when "
            "possible (default: auto)."
        ),
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help=(
            "Repeat back-to-back measurement windows until an entire recording "
            "window receives zero messages. With the default --duration, this "
            "means repeated 30 s captures and termination after 30 s of silence."
        ),
    )
    parser.add_argument(
        "--measure-bytes",
        action="store_true",
        help=(
            "Also serialize every message to estimate message sizes. Disabled by "
            "default because serialization adds callback overhead and can perturb "
            "high-rate measurements."
        ),
    )
    parser.add_argument(
        "--discovery-timeout",
        type=float,
        default=2.0,
        help="Seconds to discover active topics for interactive selection (default: 2)",
    )
    parser.add_argument(
        "--include-system-topics",
        action="store_true",
        help="Include /rosout and /parameter_events in interactive discovery",
    )
    parser.add_argument(
        "--node-name",
        default="topic_stats",
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args(argv)

    if args.duration_s <= 0:
        parser.error("--duration must be > 0")
    if args.qos_depth <= 0:
        parser.error("--qos-depth must be > 0")
    if args.discovery_timeout <= 0:
        parser.error("--discovery-timeout must be > 0")
    if not args.windows_s or any(w <= 0 for w in args.windows_s):
        parser.error("all --windows values must be > 0")
    if any(w > args.duration_s for w in args.windows_s):
        parser.error("measurement-window sizes cannot exceed --duration")

    # Sort from coarsest to finest and remove duplicates while preserving order.
    args.windows_s = sorted(set(args.windows_s), reverse=True)
    return args


def print_summary(
    topic: str,
    duration_s: float,
    arrival_times_s: Sequence[float],
    header_timestamps_ns: Sequence[int],
    stats: Sequence[BinStats],
    header_stats: Sequence[BinStats],
    sizes: Sequence[int],
) -> None:
    n = len(arrival_times_s)
    overall_rate = n / duration_s

    print("\n\n=== Topic burstiness summary ===")
    print(f"Topic:              {topic}")
    print(f"Recording duration: {duration_s:.3f} s")
    print(f"Messages received:  {n}")
    print(f"Overall mean rate:  {overall_rate:.3f} msg/s")

    if len(arrival_times_s) >= 2:
        dt = [
            arrival_times_s[i] - arrival_times_s[i - 1]
            for i in range(1, len(arrival_times_s))
        ]
        print(
            "Inter-arrival Δt:    "
            f"mean={statistics.mean(dt) * 1000:.3f} ms | "
            f"std={statistics.pstdev(dt) * 1000:.3f} ms | "
            f"min={min(dt) * 1000:.3f} ms | "
            f"p50={percentile(dt, 0.50) * 1000:.3f} ms | "
            f"p95={percentile(dt, 0.95) * 1000:.3f} ms | "
            f"p99={percentile(dt, 0.99) * 1000:.3f} ms | "
            f"max={max(dt) * 1000:.3f} ms"
        )

    valid_headers = [int(value) for value in header_timestamps_ns if int(value) != 0]
    print(
        f"Header timestamps:   {len(valid_headers)} usable | "
        f"{len(arrival_times_s) - len(valid_headers)} missing/zero"
    )
    if len(valid_headers) >= 2:
        header_dt_ms = [
            (valid_headers[i] - valid_headers[i - 1]) * 1e-6
            for i in range(1, len(valid_headers))
        ]
        print("Header timestamp Δt: " + _series_stats_text(header_dt_ms, "ms"))
    _, drift_ms = timestamp_drift_series(arrival_times_s, header_timestamps_ns)
    if drift_ms:
        print("Normalized drift:    " + _series_stats_text(drift_ms, "ms"))

    if sizes:
        valid_sizes = [x for x in sizes if x > 0]
        if valid_sizes:
            mib = 1024.0 * 1024.0
            print(
                "Serialized size:      "
                f"mean={statistics.mean(valid_sizes) / mib:.3f} MiB | "
                f"std={statistics.pstdev(valid_sizes) / mib:.3f} MiB | "
                f"min={min(valid_sizes) / mib:.3f} MiB | "
                f"max={max(valid_sizes) / mib:.3f} MiB"
            )

    print("\nSubscriber-arrival per-window counts:")
    print(
        f"{'window':>10} {'bins':>6} {'mean':>10} {'std':>10} {'min':>7} "
        f"{'max':>7} {'mean Hz':>10} {'peak Hz':>10} {'peak/mean':>11} "
        f"{'CV':>8} {'empty':>7} {'empty%':>8}"
    )
    for s in stats:
        peak_mean = "inf" if math.isinf(s.peak_to_mean) else f"{s.peak_to_mean:.3f}"
        cv = (
            "inf"
            if math.isinf(s.coefficient_of_variation)
            else f"{s.coefficient_of_variation:.3f}"
        )
        print(
            f"{fmt_seconds(s.window_s):>10} {s.n_bins:6d} "
            f"{s.mean_count:10.3f} {s.std_count:10.3f} "
            f"{s.min_count:7d} {s.max_count:7d} "
            f"{s.mean_rate_hz:10.3f} {s.peak_rate_hz:10.3f} "
            f"{peak_mean:>11} {cv:>8} {s.zero_bin_count:7d} "
            f"{100.0 * s.zero_bin_fraction:7.2f}%"
        )

    if header_stats:
        print("\nHeader-timestamp per-window counts:")
        print(
            f"{'window':>10} {'bins':>6} {'mean':>10} {'std':>10} {'min':>7} "
            f"{'max':>7} {'mean Hz':>10} {'peak Hz':>10} {'peak/mean':>11} "
            f"{'CV':>8} {'empty':>7} {'empty%':>8}"
        )
        for s in header_stats:
            peak_mean = (
                "inf" if math.isinf(s.peak_to_mean) else f"{s.peak_to_mean:.3f}"
            )
            cv = (
                "inf"
                if math.isinf(s.coefficient_of_variation)
                else f"{s.coefficient_of_variation:.3f}"
            )
            print(
                f"{fmt_seconds(s.window_s):>10} {s.n_bins:6d} "
                f"{s.mean_count:10.3f} {s.std_count:10.3f} "
                f"{s.min_count:7d} {s.max_count:7d} "
                f"{s.mean_rate_hz:10.3f} {s.peak_rate_hz:10.3f} "
                f"{peak_mean:>11} {cv:>8} {s.zero_bin_count:7d} "
                f"{100.0 * s.zero_bin_fraction:7.2f}%"
            )

    print(
        "\nInterpretation: a burstier stream generally has larger std/CV, a "
        "larger peak-to-mean ratio, and (at fine windows) more empty bins."
    )


def save_capture_results(
    *,
    session_output_dir: Path,
    topic_slug: str,
    topic: str,
    duration_s: float,
    windows_s: Sequence[float],
    arrival_times_s: Sequence[float],
    header_timestamps_ns: Sequence[int],
    sizes: Sequence[int],
    publisher_infos_start: Sequence[Any],
    publisher_infos_end: Sequence[Any],
    topic_type: Optional[str],
    qos_profile: Optional[QoSProfile],
    capture_index: int,
    use_capture_subdir: bool,
    output_prefix: str = "",
    allow_shared_capture_dir: bool = False,
    save_individual_window_plots: bool = True,
    stamp: Optional[str] = None,
) -> CaptureResult:
    """Generate all artifacts for one completed capture window."""
    if use_capture_subdir:
        if stamp is None:
            stamp = (
                f"{args.capture_stamp_base}_{capture_index:04d}"
                if args.capture_stamp_base
                else datetime.now().strftime("%y%m%d_%H%M%S")
            )
        output_dir = session_output_dir / stamp
        # Defensive uniqueness when capture folders would share one second.
        if output_dir.exists() and not allow_shared_capture_dir:
            output_dir = session_output_dir / f"{stamp}_{capture_index:04d}"
    else:
        output_dir = session_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    subdirs = {
        "header_gaps": output_dir / "header_gaps",
        "arrival_gaps": output_dir / "arrival_gaps",
        "throughput_header": output_dir / "throughput_header",
        "throughput_arrival": output_dir / "throughput_arrival",
        "source_latency": output_dir / "source_latency",
        "data": output_dir / "data",
        "publisher_info": output_dir / "publisher_info",
    }
    for directory in subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

    arrivals_csv = write_arrivals_csv(
        subdirs["data"],
        arrival_times_s,
        header_timestamps_ns,
        sizes,
        f"{output_prefix}timestamps.csv",
    )
    plot_paths, stats = generate_plots(
        throughput_output_dir=subdirs["throughput_arrival"],
        gap_output_dir=subdirs["arrival_gaps"],
        topic=topic,
        arrival_times_s=arrival_times_s,
        recording_duration_s=duration_s,
        windows_s=windows_s,
        overview_filename="arrival_throughput.png",
        gap_filename="arrival_gaps.png",
        filename_prefix=output_prefix,
        timestamp_label="subscriber-arrival",
        save_individual_window_plots=save_individual_window_plots,
    )
    summary_csv = write_summary_csv(
        subdirs["data"], stats, f"{output_prefix}arrival_window_summary.csv"
    )
    header_times_s = valid_header_times_s(header_timestamps_ns)
    header_stats: List[BinStats] = []
    if header_times_s:
        header_plot_paths, header_stats = generate_plots(
            throughput_output_dir=subdirs["throughput_header"],
            gap_output_dir=subdirs["header_gaps"],
            topic=topic,
            arrival_times_s=header_times_s,
            recording_duration_s=duration_s,
            windows_s=windows_s,
            overview_filename="header_throughput.png",
            gap_filename="header_gaps.png",
            filename_prefix=output_prefix,
            timestamp_label="header",
            save_individual_window_plots=save_individual_window_plots,
        )
        plot_paths.extend(header_plot_paths)
        header_summary_csv = write_summary_csv(
            subdirs["data"],
            header_stats,
            f"{output_prefix}header_window_summary.csv",
        )
    else:
        header_summary_csv = None
    drift_path = generate_timestamp_drift_plot(
        subdirs["source_latency"],
        topic,
        arrival_times_s,
        header_timestamps_ns,
        output_prefix,
    )
    if drift_path is not None:
        plot_paths.append(drift_path)
    summary_output_dir = subdirs["data"] if output_prefix else output_dir
    timestamp_summary = write_timestamp_summary(
        summary_output_dir,
        arrival_times_s,
        header_timestamps_ns,
        f"{output_prefix}summary.txt",
    )
    publisher_qos_txt, publisher_qos_csv = write_publisher_qos_log(
        subdirs["publisher_info"],
        topic,
        publisher_infos_start,
        publisher_infos_end,
        output_prefix,
    )

    print_summary(
        topic,
        duration_s,
        arrival_times_s,
        header_timestamps_ns,
        stats,
        header_stats,
        sizes,
    )
    print(f"Capture #: {capture_index}")
    print(f"Type: {topic_type}")
    print(f"Subscriber QoS used by this tool: {qos_profile}")
    print("\n=== Publisher endpoint/QoS details at capture end ===")
    if publisher_infos_end:
        for i, info in enumerate(publisher_infos_end, start=1):
            print(format_publisher_info(info, i))
    else:
        print("No publisher endpoint information available.")

    print(f"\nOutput directory: {output_dir}")
    print(f"  {arrivals_csv.relative_to(output_dir)}")
    print(f"  {summary_csv.relative_to(output_dir)}")
    if header_summary_csv is not None:
        print(f"  {header_summary_csv.relative_to(output_dir)}")
    print(f"  {timestamp_summary.relative_to(output_dir)}")
    print(f"  {publisher_qos_txt.relative_to(output_dir)}")
    print(f"  {publisher_qos_csv.relative_to(output_dir)}")
    for path in plot_paths:
        print(f"  {path.relative_to(output_dir)}")
    return CaptureResult(
        capture_index=capture_index,
        output_dir=output_dir,
        message_count=len(arrival_times_s),
        duration_s=duration_s,
        stats=list(stats),
        header_message_count=len(header_times_s),
        header_stats=list(header_stats),
    )


def write_consolidated_report(
    *,
    session_output_dir: Path,
    topic_slug: str,
    topic: str,
    capture_results: Sequence[CaptureResult],
    filename_prefix: str = "",
) -> Optional[Path]:
    """Write CSV/text summaries and cross-capture plots for loop-mode runs."""
    if not capture_results:
        return None

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is required for consolidated plotting. Install it with "
            "e.g. 'sudo apt install python3-matplotlib'."
        ) from exc

    captures = sorted(capture_results, key=lambda r: r.capture_index)
    report_dir = session_output_dir / "consolidated"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Long-form CSV: one row per capture x measurement-window size.
    csv_path = report_dir / f"{filename_prefix}consolidated_window_stats.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "capture_index",
                "capture_output_dir",
                "capture_duration_s",
                "messages_received",
                "timestamp_source",
                "usable_timestamp_count",
                "window_s",
                "n_bins",
                "mean_messages_per_bin",
                "std_messages_per_bin",
                "min_messages_per_bin",
                "max_messages_per_bin",
                "mean_rate_hz",
                "peak_rate_hz",
                "peak_to_mean",
                "coefficient_of_variation",
                "zero_bin_count",
                "zero_bin_fraction",
            ]
        )
        for result in captures:
            for source, usable_count, source_stats in (
                ("subscriber_arrival", result.message_count, result.stats),
                ("header", result.header_message_count, result.header_stats),
            ):
                for st in source_stats:
                    writer.writerow(
                        [
                            result.capture_index,
                            str(result.output_dir),
                            result.duration_s,
                            result.message_count,
                            source,
                            usable_count,
                            st.window_s,
                            st.n_bins,
                            st.mean_count,
                            st.std_count,
                            st.min_count,
                            st.max_count,
                            st.mean_rate_hz,
                            st.peak_rate_hz,
                            st.peak_to_mean,
                            st.coefficient_of_variation,
                            st.zero_bin_count,
                            st.zero_bin_fraction,
                        ]
                    )

    # Group by measurement-window size.
    by_window: Dict[float, List[tuple[CaptureResult, BinStats]]] = {}
    for result in captures:
        for st in result.stats:
            by_window.setdefault(st.window_s, []).append((result, st))

    # Human-readable report with emphasis on missing-data bins / worst bins.
    txt_path = report_dir / f"{filename_prefix}consolidated_report.txt"
    with txt_path.open("w") as f:
        f.write(f"Topic: {topic}\n")
        active_captures = [r for r in captures if r.message_count > 0]
        silent_captures = [r for r in captures if r.message_count == 0]
        f.write(f"Completed capture windows: {len(captures)}\n")
        f.write(f"Active capture windows:    {len(active_captures)}\n")
        f.write(f"Fully silent windows:       {len(silent_captures)}\n")
        f.write(
            f"Capture indices: {captures[0].capture_index}..{captures[-1].capture_index}\n"
        )
        if silent_captures:
            f.write(
                "Note: fully silent terminating capture(s) remain in the plots/CSV, "
                "but the aggregate dropout statistics below use active captures only.\n"
            )
        f.write("\n")
        f.write("Cross-capture summary by measurement-bin size\n")
        f.write("===============================================\n")
        for window_s in sorted(by_window.keys(), reverse=True):
            rows = by_window[window_s]
            active_rows = [(r, st) for r, st in rows if r.message_count > 0]
            aggregate_rows = active_rows if active_rows else rows
            total_bins = sum(st.n_bins for _, st in aggregate_rows)
            total_zero = sum(st.zero_bin_count for _, st in aggregate_rows)
            captures_with_zero = sum(st.zero_bin_count > 0 for _, st in aggregate_rows)
            mean_of_means = statistics.mean(st.mean_count for _, st in aggregate_rows)
            mean_of_mins = statistics.mean(st.min_count for _, st in aggregate_rows)
            absolute_min = min(st.min_count for _, st in aggregate_rows)
            worst_min_capture = min(
                aggregate_rows, key=lambda x: (x[1].min_count, x[0].capture_index)
            )
            max_zero_row = max(
                aggregate_rows, key=lambda x: (x[1].zero_bin_count, -x[0].capture_index)
            )

            f.write(f"\nBin size: {fmt_seconds(window_s)}\n")
            f.write(f"  Mean of per-capture mean counts: {mean_of_means:.3f} msg/bin\n")
            f.write(f"  Mean of per-capture minimums:    {mean_of_mins:.3f} msg/bin\n")
            f.write(f"  Lowest bin count observed:       {absolute_min} msg\n")
            f.write(
                f"  Worst minimum occurred in:      capture #{worst_min_capture[0].capture_index}\n"
            )
            f.write(
                f"  Active captures with zero bins: {captures_with_zero}/{len(aggregate_rows)}\n"
            )
            f.write(
                f"  Zero bins across all captures:  {total_zero}/{total_bins} "
                f"({(100.0 * total_zero / total_bins) if total_bins else 0.0:.2f}%)\n"
            )
            f.write(
                f"  Most zero bins in one capture:  {max_zero_row[1].zero_bin_count} "
                f"(capture #{max_zero_row[0].capture_index})\n"
            )

    # Four-panel overview: one line per measurement-window size.
    fig, axes = plt.subplots(4, 1, figsize=(14, 15), sharex=True)
    metrics = [
        ("mean_count", "Mean messages / bin"),
        ("min_count", "Minimum messages in any bin"),
        ("zero_bin_count", "Zero-data bins"),
        ("zero_bin_fraction", "Zero-data bins [%]"),
    ]
    for ax, (field, ylabel) in zip(axes, metrics):
        for window_s in sorted(by_window.keys(), reverse=True):
            rows = by_window[window_s]
            x = [result.capture_index for result, _ in rows]
            if field == "zero_bin_fraction":
                y = [100.0 * getattr(st, field) for _, st in rows]
            else:
                y = [getattr(st, field) for _, st in rows]
            ax.plot(x, y, marker="o", linewidth=1.4, label=fmt_seconds(window_s))
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
        ax.legend(title="Bin size", ncol=min(4, len(by_window)))
    axes[-1].set_xlabel("Capture window index")
    fig.suptitle(f"{topic}: consolidated burstiness across capture windows")
    fig.tight_layout()
    overview_path = report_dir / f"{filename_prefix}consolidated_overview.png"
    fig.savefig(overview_path, dpi=170)
    plt.close(fig)

    # Focused plots for the two diagnostics most relevant to dropped/gapped data.
    for field, ylabel, filename in [
        ("min_count", "Minimum messages in any bin", "minimum_messages_per_bin_by_capture.png"),
        ("zero_bin_count", "Number of zero-data bins", "zero_data_bins_by_capture.png"),
        ("mean_count", "Mean messages per bin", "mean_messages_per_bin_by_capture.png"),
    ]:
        fig, ax = plt.subplots(figsize=(13, 5))
        for window_s in sorted(by_window.keys(), reverse=True):
            rows = by_window[window_s]
            x = [result.capture_index for result, _ in rows]
            y = [getattr(st, field) for _, st in rows]
            ax.plot(x, y, marker="o", linewidth=1.5, label=fmt_seconds(window_s))
        ax.set_title(f"{topic}: {ylabel.lower()} by capture")
        ax.set_xlabel("Capture window index")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
        ax.legend(title="Bin size")
        fig.tight_layout()
        fig.savefig(report_dir / f"{filename_prefix}{filename}", dpi=170)
        plt.close(fig)

    # Capture-level total messages can reveal whole-window throughput changes.
    fig, ax = plt.subplots(figsize=(13, 4.5))
    x = [r.capture_index for r in captures]
    y = [r.message_count for r in captures]
    ax.plot(x, y, marker="o", linewidth=1.5)
    ax.set_title(f"{topic}: total received messages per capture")
    ax.set_xlabel("Capture window index")
    ax.set_ylabel("Messages received")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    messages_path = report_dir / f"{filename_prefix}messages_per_capture.png"
    fig.savefig(messages_path, dpi=170)
    plt.close(fig)

    print("\n=== Consolidated report ===")
    print(f"Output directory: {report_dir}")
    print(f"  {csv_path.name}")
    print(f"  {txt_path.name}")
    print(f"  {overview_path.name}")
    print(f"  {filename_prefix}mean_messages_per_bin_by_capture.png")
    print(f"  {filename_prefix}minimum_messages_per_bin_by_capture.png")
    print(f"  {filename_prefix}zero_data_bins_by_capture.png")
    print(f"  {messages_path.name}")
    return report_dir


def main() -> int:
    args = parse_cli_args()

    if args.topic is None:
        try:
            selected = interactively_select_live_topics(
                timeout_s=args.discovery_timeout,
                allow_multiple=False,
                include_system_topics=args.include_system_topics,
            )
            args.topic = selected[0].name
            print(f"Selected topic: {args.topic}")
        except Exception as exc:
            print(f"Could not select a live topic: {exc}", file=sys.stderr)
            return 1

    topic_slug = topic_to_slug(args.topic)
    if args.session_output_dir is not None:
        session_output_dir = args.session_output_dir.expanduser().resolve()
        session_output_dir.mkdir(parents=True, exist_ok=True)
    else:
        if args.output_dir is None:
            package_dir = find_package_dir()
            base_output_dir = package_dir / "out"
        else:
            base_output_dir = args.output_dir.expanduser().resolve()
        base_output_dir.mkdir(parents=True, exist_ok=True)
        session_output_dir = create_session_output_dir(base_output_dir, topic_slug)
    print(f"Session output directory: {session_output_dir}")

    rclpy.init(args=sys.argv)
    node = TopicBurstinessNode(
        topic_name=args.topic,
        duration_s=args.duration_s,
        qos_depth=args.qos_depth,
        qos_reliability=args.qos_reliability,
        measure_bytes=args.measure_bytes,
        node_name=args.node_name,
    )

    interrupted = False
    completed_captures = 0
    save_futures: List[Future] = []
    capture_results: List[CaptureResult] = []
    # Plotting/writing runs on one worker so the ROS spin loop can continue into
    # the next capture immediately. A single worker also avoids concurrent
    # matplotlib use.
    save_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="topic_stats_save")

    try:
        while rclpy.ok():
            # Wait for discovery/subscription before the first capture starts.
            while rclpy.ok() and not node.subscribed:
                rclpy.spin_once(node, timeout_sec=0.05)
                sys.stdout.write(f"\rWaiting for topic {args.topic} ...")
                sys.stdout.flush()

            if not rclpy.ok():
                break

            last_status_second = -1
            while rclpy.ok() and not node.done:
                rclpy.spin_once(node, timeout_sec=0.02)
                sec = int(node.elapsed_s)
                if sec != last_status_second:
                    last_status_second = sec
                    remaining = max(0.0, args.duration_s - node.elapsed_s)
                    sys.stdout.write(
                        f"\rCapture #{node.capture_index}: {len(node.arrival_ns)} msgs | "
                        f"elapsed {node.elapsed_s:5.1f}/{args.duration_s:g}s | "
                        f"remaining {remaining:5.1f}s"
                    )
                    sys.stdout.flush()

            if not rclpy.ok():
                break

            # Use the exact monotonic boundary rather than 'now'. If the spin
            # call that noticed the boundary also delivered a message just after
            # it, carry that message into the next capture instead of losing it.
            assert node.recording_start_ns is not None
            capture_start_ns = node.recording_start_ns
            boundary_ns = capture_start_ns + int(args.duration_s * 1e9)
            split_idx = len(node.arrival_ns)
            for i, t_ns in enumerate(node.arrival_ns):
                if t_ns >= boundary_ns:
                    split_idx = i
                    break

            completed_abs = list(node.arrival_ns[:split_idx])
            carry_abs = list(node.arrival_ns[split_idx:])
            completed_headers = list(node.header_ns[:split_idx])
            carry_headers = list(node.header_ns[split_idx:])
            if node.serialized_sizes:
                completed_sizes = list(node.serialized_sizes[:split_idx])
                carry_sizes = list(node.serialized_sizes[split_idx:])
            else:
                completed_sizes = []
                carry_sizes = []

            arrival_times_s = [
                (t_ns - capture_start_ns) * 1e-9 for t_ns in completed_abs
            ]
            publisher_infos_start = list(node.publisher_infos_start)
            publisher_infos_end = node.snapshot_publishers_at_end()
            topic_type = node.topic_type_str
            qos_profile = node.qos_profile
            capture_index = node.capture_index
            message_count = len(arrival_times_s)
            completed_captures += 1
            stamp = datetime.now().strftime("%y%m%d_%H%M%S")

            print(
                f"\nCapture #{capture_index} complete: {message_count} messages "
                f"in {args.duration_s:g}s."
            )

            # Roll into the next exact, contiguous measurement window BEFORE
            # plotting/saving the completed one. This avoids a blind gap between
            # 30 s windows. Messages already received beyond the boundary are
            # preserved as the first samples of the next window.
            should_continue = args.loop and message_count > 0
            if should_continue:
                node.capture_index += 1
                node.recording_start_ns = boundary_ns
                node.arrival_ns = carry_abs
                node.header_ns = carry_headers
                node.serialized_sizes = carry_sizes
                node.publisher_infos_end = []
                node.publisher_infos_start = node._get_publisher_infos()
                node._log_publisher_infos(
                    node.publisher_infos_start,
                    f"At capture #{node.capture_index} start",
                )

            save_futures.append(
                save_executor.submit(
                    save_capture_results,
                    session_output_dir=session_output_dir,
                    topic_slug=topic_slug,
                    topic=args.topic,
                    duration_s=args.duration_s,
                    windows_s=args.windows_s,
                    arrival_times_s=arrival_times_s,
                    header_timestamps_ns=completed_headers,
                    sizes=completed_sizes,
                    publisher_infos_start=publisher_infos_start,
                    publisher_infos_end=publisher_infos_end,
                    topic_type=topic_type,
                    qos_profile=qos_profile,
                    capture_index=capture_index,
                    use_capture_subdir=args.loop,
                    output_prefix=args.output_prefix,
                    allow_shared_capture_dir=bool(args.output_prefix),
                    save_individual_window_plots=not args.overview_plots_only,
                    stamp=stamp,
                )
            )

            if not args.loop:
                break

            if message_count == 0:
                print(
                    f"\nNo messages were received for the entire "
                    f"{args.duration_s:g}s recording window. Stopping loop mode."
                )
                break

    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted by user.")
    finally:
        # Preserve a partial in-progress capture on Ctrl-C when there is enough
        # elapsed time for at least one requested analysis bin.
        if interrupted and node.subscribed and node.recording_start_ns is not None:
            elapsed = node.elapsed_s
            arrivals = node.relative_arrival_times_s()
            valid_windows = [w for w in args.windows_s if w <= elapsed]
            if elapsed > 0 and valid_windows:
                try:
                    end_infos = node.snapshot_publishers_at_end()
                    stamp = (
                        f"{args.capture_stamp_base}_{node.capture_index:04d}"
                        if args.capture_stamp_base
                        else datetime.now().strftime("%y%m%d_%H%M%S")
                    )
                    save_futures.append(
                        save_executor.submit(
                            save_capture_results,
                            session_output_dir=session_output_dir,
                            topic_slug=topic_slug,
                            topic=args.topic,
                            duration_s=elapsed,
                            windows_s=valid_windows,
                            arrival_times_s=arrivals,
                            header_timestamps_ns=list(node.header_ns),
                            sizes=list(node.serialized_sizes),
                            publisher_infos_start=list(node.publisher_infos_start),
                            publisher_infos_end=end_infos,
                            topic_type=node.topic_type_str,
                            qos_profile=node.qos_profile,
                            capture_index=node.capture_index,
                            use_capture_subdir=args.loop,
                            output_prefix=args.output_prefix,
                            allow_shared_capture_dir=bool(args.output_prefix),
                            save_individual_window_plots=not args.overview_plots_only,
                            stamp=stamp,
                        )
                    )
                except Exception as exc:
                    print(f"Could not queue partial capture for saving: {exc}")

        node_was_subscribed = node.subscribed

        # Wait for already-completed capture plots/files to finish writing and
        # surface any background errors before destroying the ROS node.
        save_executor.shutdown(wait=True)
        save_failed = False
        for future in save_futures:
            try:
                result = future.result()
                capture_results.append(result)
            except Exception as exc:
                save_failed = True
                print(f"\nFailed to save a capture: {exc}")

        # Cross-capture reports and plots are meaningful only in loop mode.
        if args.loop and capture_results:
            try:
                write_consolidated_report(
                    session_output_dir=session_output_dir,
                    topic_slug=topic_slug,
                    topic=args.topic,
                    capture_results=capture_results,
                    filename_prefix=args.output_prefix,
                )
            except Exception as exc:
                save_failed = True
                print(f"\nFailed to generate consolidated report: {exc}")

        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

    if not node_was_subscribed and completed_captures == 0:
        print(f"\nCould not subscribe to {args.topic}; no output generated.")
        return 1
    if save_failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
