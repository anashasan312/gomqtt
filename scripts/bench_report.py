#!/usr/bin/env python3
"""Render the benchmark JSON files as Markdown tables.

Reads what bench.sh wrote and prints tables ready to paste into the README.
Kept separate from bench.sh so the tables can be regenerated from an existing
result set without re-running the benchmark.
"""

import json
import pathlib
import platform
import sys


def read(path: pathlib.Path) -> dict:
    with path.open() as handle:
        return json.load(handle)


def collect(results_dir: pathlib.Path, prefix: str) -> dict:
    """Group result files by scenario label, keyed by broker, preserving order."""
    scenarios: dict[str, dict[str, dict]] = {}

    for path in sorted(results_dir.glob(f"{prefix}-*.json")):
        stem = path.name[len(prefix) + 1 :]
        broker = "gomqtt" if stem.startswith("gomqtt") else "mosquitto"
        data = read(path)
        scenarios.setdefault(data["label"], {})[broker] = data

    return scenarios


def fmt_rate(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1000:
        return f"{value / 1000:.1f}k"
    return f"{value:.0f}"


def print_latency_table(scenarios: dict, have_mosquitto: bool) -> None:
    print("| Scenario | Offered load | Broker | p50 | p95 | p99 | Delivery |")
    print("|---|---:|---|---:|---:|---:|---:|")

    for label, brokers in scenarios.items():
        first = True
        for broker in ("gomqtt", "mosquitto"):
            data = brokers.get(broker)
            if data is None:
                continue

            print(
                "| {scenario} | {load} | {broker} | {p50:.2f} ms | {p95:.2f} ms | "
                "{p99:.2f} ms | {delivery:.1f}% |".format(
                    scenario=label if first else "",
                    load=f"{fmt_rate(data['target_rate_msgs_per_sec'])} msg/s" if first else "",
                    broker="**GoMQTT**" if broker == "gomqtt" else "Mosquitto",
                    p50=data["latency_p50_ms"],
                    p95=data["latency_p95_ms"],
                    p99=data["latency_p99_ms"],
                    delivery=data["delivery_ratio"] * 100,
                )
            )
            first = False

    if not have_mosquitto:
        print()
        print("_Mosquitto was not installed, so only GoMQTT was measured._")


def print_throughput_table(scenarios: dict) -> None:
    print("| Scenario | Broker | Max sustained publish rate | Delivered (fan-out) | p99 |")
    print("|---|---|---:|---:|---:|")

    for label, brokers in scenarios.items():
        first = True
        for broker in ("gomqtt", "mosquitto"):
            data = brokers.get(broker)
            if data is None:
                continue

            print(
                "| {scenario} | {broker} | {pub} msg/s | {deliver} msg/s | {p99:.2f} ms |".format(
                    scenario=label if first else "",
                    broker="**GoMQTT**" if broker == "gomqtt" else "Mosquitto",
                    pub=fmt_rate(data["publish_throughput_msgs_per_sec"]),
                    deliver=fmt_rate(data["deliver_throughput_msgs_per_sec"]),
                    p99=data["latency_p99_ms"],
                )
            )
            first = False


def print_environment() -> None:
    print("Measured on:")
    print()
    print(f"- {platform.system()} {platform.release()}, {platform.machine()}")
    try:
        import os

        print(f"- {os.cpu_count()} logical CPUs")
    except Exception:  # pragma: no cover - informational only
        pass
    print("- Both brokers on loopback, load generator in a separate process")


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: bench_report.py <results-dir>", file=sys.stderr)
        return 1

    results_dir = pathlib.Path(sys.argv[1])

    latency = collect(results_dir, "lat")
    throughput = collect(results_dir, "max")

    if not latency and not throughput:
        print(f"no result files in {results_dir}", file=sys.stderr)
        return 1

    have_mosquitto = any("mosquitto" in b for b in latency.values()) or any(
        "mosquitto" in b for b in throughput.values()
    )

    print()
    print("### Latency at a fixed offered load")
    print()
    print(
        "Both brokers are given the same message rate, well below saturation, "
        "so what is measured is delivery latency rather than queueing."
    )
    print()
    print_latency_table(latency, have_mosquitto)

    if throughput:
        print()
        print("### Maximum sustained throughput")
        print()
        print(
            "The highest offered rate at which the broker still delivered "
            "≥99% of messages with a p99 under 50 ms."
        )
        print()
        print_throughput_table(throughput)

    print()
    print_environment()
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
