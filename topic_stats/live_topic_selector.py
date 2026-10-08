"""Discovery and interactive selection helpers for live ROS 2 topics."""

from dataclasses import dataclass
import re
import time
from typing import Iterable, List, Sequence

import rclpy
from rclpy.node import Node


SYSTEM_TOPICS = {"/parameter_events", "/rosout"}


@dataclass(frozen=True)
class LiveTopicInfo:
    index: int
    name: str
    type_names: tuple[str, ...]
    publisher_count: int


def discover_live_topics(
    timeout_s: float = 2.0,
    include_system_topics: bool = False,
) -> List[LiveTopicInfo]:
    """Discover topics observed with at least one publisher during the timeout."""
    initialized_here = not rclpy.ok()
    if initialized_here:
        rclpy.init(args=[])
    node = Node("topic_stats_discovery")
    try:
        deadline = time.monotonic() + timeout_s
        discovered_by_name = {}
        while rclpy.ok() and time.monotonic() < deadline:
            rclpy.spin_once(node, timeout_sec=min(0.1, max(0.0, deadline - time.monotonic())))
            for name, type_names in node.get_topic_names_and_types():
                if not include_system_topics and name in SYSTEM_TOPICS:
                    continue
                try:
                    publisher_count = len(node.get_publishers_info_by_topic(name))
                except Exception:
                    publisher_count = 0
                if publisher_count <= 0:
                    continue
                previous = discovered_by_name.get(name)
                if previous is None or publisher_count > previous[1]:
                    discovered_by_name[name] = (tuple(type_names), publisher_count)

        discovered = [
            (name, type_names, publisher_count)
            for name, (type_names, publisher_count) in discovered_by_name.items()
        ]
        discovered.sort(key=lambda row: row[0])
        return [
            LiveTopicInfo(index, name, type_names, publisher_count)
            for index, (name, type_names, publisher_count) in enumerate(
                discovered, start=1
            )
        ]
    finally:
        node.destroy_node()
        if initialized_here and rclpy.ok():
            rclpy.shutdown()


def print_live_topic_inventory(topics: Sequence[LiveTopicInfo]) -> None:
    print("\nLive topics with active publishers")
    print("=" * 100)
    print(f"{'index':>5}  {'pubs':>5}  {'topic':<52}  type(s)")
    print("-" * 100)
    for item in topics:
        print(
            f"{item.index:5d}  {item.publisher_count:5d}  {item.name:<52}  "
            f"{', '.join(item.type_names)}"
        )
    print("=" * 100)


def _expand_token(token: str, topics: Sequence[LiveTopicInfo]) -> Iterable[int]:
    token = token.strip()
    if not token:
        return []
    if token.lower() == "all":
        return range(1, len(topics) + 1)
    if token.isdigit():
        return [int(token)]
    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", token)
    if match:
        start, end = map(int, match.groups())
        step = 1 if end >= start else -1
        return range(start, end + step, step)
    by_name = {item.name: item.index for item in topics}
    if token in by_name:
        return [by_name[token]]
    raise ValueError(f"Unknown topic index, range, or name: {token!r}")


def parse_live_selection(
    text: str,
    topics: Sequence[LiveTopicInfo],
    *,
    allow_multiple: bool,
) -> List[LiveTopicInfo]:
    tokens = [part for part in re.split(r"[\s,]+", text.strip()) if part]
    if not tokens:
        raise ValueError("No topics selected")
    indices: List[int] = []
    for token in tokens:
        indices.extend(_expand_token(token, topics))
    invalid = sorted({index for index in indices if index < 1 or index > len(topics)})
    if invalid:
        raise ValueError(f"Topic indices out of range: {invalid}")
    wanted = set(indices)
    selected = [item for item in topics if item.index in wanted]
    if not allow_multiple and len(selected) != 1:
        raise ValueError("Select exactly one topic for the single-topic analyzer")
    return selected


def interactively_select_live_topics(
    *,
    timeout_s: float,
    allow_multiple: bool,
    include_system_topics: bool = False,
) -> List[LiveTopicInfo]:
    print(f"Status: discovering live topics for {timeout_s:g} seconds ...", flush=True)
    topics = discover_live_topics(timeout_s, include_system_topics)
    if not topics:
        raise RuntimeError("No live topics with active publishers were discovered")
    print_live_topic_inventory(topics)
    example = "1 3-5, topic names, or 'all'" if allow_multiple else "one index or topic name"
    while True:
        try:
            answer = input(f"Select {example}: ")
        except EOFError as exc:
            raise RuntimeError(
                "Interactive input is unavailable; provide topic names on the command line "
                "or in a config file."
            ) from exc
        try:
            return parse_live_selection(
                answer, topics, allow_multiple=allow_multiple
            )
        except ValueError as exc:
            print(f"Invalid selection: {exc}")
