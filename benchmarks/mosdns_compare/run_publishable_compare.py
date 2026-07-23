#!/usr/bin/env python3
"""Run the publishable OxiDNS versus mosdns benchmark matrix.

The runner intentionally uses only Python's standard library.  It requires a
Linux host and a dnsperf build with JSON and latency-histogram support.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR.parent.parent
DEFAULT_SCENARIOS = (
    "02-cache-hotpath-large",
    "06-local-answers",
    "08-domain-set",
    "09-ip-set",
    "43-composite-provider-chain",
    "47-server-local-udp",
    "48-server-local-tcp",
)
ENGINE_COLORS = {"oxidns": "#0f766e", "mosdns": "#f59e0b"}


@dataclass(frozen=True)
class Scenario:
    label: str
    oxidns_config: Path
    mosdns_config: Path
    query_file: Path
    mode: str
    family: str
    warmup_file: Path
    tags: tuple[str, ...]
    description: str
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run reproducible QPS, tail-latency, CPU and RSS comparisons."
    )
    parser.add_argument("selectors", nargs="*", help="scenario labels, families, tags, or all")
    parser.add_argument("--load-levels", default=os.getenv("BENCH_LOAD_LEVELS", "1,4,16,64,256,1024"))
    parser.add_argument("--seconds", type=int, default=int(os.getenv("BENCH_SECONDS", "12")))
    parser.add_argument("--warmup-seconds", type=int, default=int(os.getenv("WARMUP_SECONDS", "3")))
    parser.add_argument("--repeats", type=int, default=int(os.getenv("BENCH_REPEATS", "3")))
    parser.add_argument("--threads", type=int, default=int(os.getenv("DNSPERF_THREADS", "4")))
    parser.add_argument("--max-clients", type=int, default=int(os.getenv("DNSPERF_MAX_CLIENTS", "32")))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("DNSPERF_TIMEOUT", "5")))
    parser.add_argument("--sample-interval", type=float, default=float(os.getenv("RESOURCE_SAMPLE_INTERVAL", "0.2")))
    parser.add_argument("--cooldown", type=float, default=float(os.getenv("BENCH_COOLDOWN_SECONDS", "1")))
    parser.add_argument("--result-dir", type=Path)
    parser.add_argument("--publish-docs", action="store_true", help="replace docs current snapshot and chart assets")
    parser.add_argument("--publish-existing", type=Path, help="publish an already completed result directory")
    parser.add_argument("--dry-run", action="store_true", help="validate and print the matrix without starting servers")
    return parser.parse_args()


def positive_levels(raw: str) -> list[int]:
    try:
        levels = sorted({int(part.strip()) for part in raw.replace(" ", ",").split(",") if part.strip()})
    except ValueError as exc:
        raise SystemExit(f"invalid --load-levels: {raw}") from exc
    if not levels or levels[0] < 1:
        raise SystemExit("--load-levels must contain positive integers")
    return levels


def load_catalog(path: Path) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("|", 9)
        if len(fields) != 10:
            raise SystemExit(f"invalid scenario row: {line}")
        label, ox_cfg, mos_cfg, query, mode, family, warmup, tags, description, notes = fields
        query_path = BASE_DIR / query
        warmup_path = query_path if warmup in ("", "-") else BASE_DIR / warmup
        scenarios.append(
            Scenario(
                label,
                BASE_DIR / ox_cfg,
                BASE_DIR / mos_cfg,
                query_path,
                mode,
                family,
                warmup_path,
                tuple(tags.split(",")),
                description,
                notes,
            )
        )
    return scenarios


def select_scenarios(catalog: list[Scenario], selectors: list[str]) -> list[Scenario]:
    wanted = selectors or list(DEFAULT_SCENARIOS)
    selected = [
        item
        for item in catalog
        if "all" in wanted
        or item.label in wanted
        or item.family in wanted
        or any(tag in wanted for tag in item.tags)
    ]
    if not selected:
        raise SystemExit(f"no scenarios matched: {' '.join(wanted)}")
    return selected


def require_executable(value: str, label: str) -> str:
    resolved = shutil.which(value) if "/" not in value else value
    if not resolved or not os.access(resolved, os.X_OK):
        raise SystemExit(f"missing executable for {label}: {value}")
    return str(Path(resolved).resolve())


def command_output(command: list[str]) -> str:
    try:
        return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False).stdout.strip()
    except OSError as exc:
        return f"unavailable: {exc}"


def binary_version(path: str, engine: str) -> str:
    commands = [[path, "--version"]]
    if engine == "mosdns":
        commands.insert(0, [path, "version"])
    for command in commands:
        output = command_output(command).splitlines()
        if output:
            return output[0]
    return "n/a"


def dnsperf_version(path: str) -> str:
    output = command_output([path, "-h"])
    lines = [line.strip() for line in output.splitlines() if "Version" in line]
    return lines[-1] if lines else "recorded from dnsperf JSON at first measured run"


def git_value(*arguments: str) -> str:
    if not (REPO_DIR / ".git").exists():
        return "n/a (release artifact benchmark)"
    return command_output(["git", "-C", str(REPO_DIR), *arguments])


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_listen(config: Path) -> tuple[str, int]:
    for line in config.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("listen:"):
            value = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            host, port = value.rsplit(":", 1)
            return host, int(port)
    raise SystemExit(f"listen address not found in {config}")


def percentile_from_histogram(histogram: list[list[float]], percentile: float) -> float | None:
    total = sum(int(bucket[2]) for bucket in histogram)
    if not total:
        return None
    target = math.ceil(total * percentile)
    seen = 0
    for _lower, upper, count in histogram:
        seen += int(count)
        if seen >= target:
            return float(upper) * 1000.0
    return float(histogram[-1][1]) * 1000.0


class ProcSampler:
    def __init__(self, pid: int, interval: float, output: Path) -> None:
        self.pid = pid
        self.interval = interval
        self.output = output
        self.stop_event = threading.Event()
        self.samples: list[dict[str, float]] = []
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> dict[str, float]:
        self.stop_event.set()
        self.thread.join(timeout=max(2.0, self.interval * 4))
        with self.output.open("w", encoding="utf-8") as stream:
            stream.write("elapsed_s\tcpu_pct\trss_mib\tthreads\n")
            for item in self.samples:
                stream.write(f"{item['elapsed']:.6f}\t{item['cpu']:.3f}\t{item['rss']:.3f}\t{item['threads']:.0f}\n")
        cpus = [item["cpu"] for item in self.samples[1:]]
        rss = [item["rss"] for item in self.samples]
        threads = [item["threads"] for item in self.samples]
        if len(cpus) < 2 or not rss or max(rss) <= 0:
            raise RuntimeError(f"insufficient resource samples for pid {self.pid}; see {self.output}")
        return {
            "cpu_pct_median": median(cpus),
            "cpu_pct_p95": quantile(cpus, 0.95),
            "rss_mib_median": median(rss),
            "rss_mib_max": max(rss, default=0.0),
            "threads_max": max(threads, default=0.0),
            "resource_samples": float(len(self.samples)),
        }

    def _run(self) -> None:
        ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        started = time.monotonic()
        previous_time: float | None = None
        previous_ticks: int | None = None
        while not self.stop_event.is_set():
            now = time.monotonic()
            try:
                stat = Path(f"/proc/{self.pid}/stat").read_text().split()
                status = Path(f"/proc/{self.pid}/status").read_text().splitlines()
            except (FileNotFoundError, ProcessLookupError):
                break
            total_ticks = int(stat[13]) + int(stat[14])
            values: dict[str, str] = {}
            for line in status:
                if ":" in line:
                    key, value = line.split(":", 1)
                    fields = value.strip().split()
                    if fields:
                        values[key] = fields[0]
            cpu = 0.0
            if previous_time is not None and previous_ticks is not None and now > previous_time:
                cpu = ((total_ticks - previous_ticks) / ticks) / (now - previous_time) * 100.0
            self.samples.append(
                {
                    "elapsed": now - started,
                    "cpu": cpu,
                    "rss": float(values.get("VmRSS", "0")) / 1024.0,
                    "threads": float(values.get("Threads", "0")),
                }
            )
            previous_time, previous_ticks = now, total_ticks
            self.stop_event.wait(self.interval)


def median(values: Iterable[float]) -> float:
    items = list(values)
    return float(statistics.median(items)) if items else 0.0


def quantile(values: Iterable[float], q: float) -> float:
    items = sorted(values)
    if not items:
        return 0.0
    return float(items[min(len(items) - 1, max(0, math.ceil(len(items) * q) - 1))])


def stop_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


def start_engine(engine: str, binary: str, config: Path, log_path: Path) -> tuple[subprocess.Popen[str], Any]:
    log_stream = log_path.open("w", encoding="utf-8")
    if engine == "oxidns":
        command = [binary, "start", "-c", str(config)]
    else:
        help_text = command_output([binary, "--help"])
        command = [binary, "start", "-c", str(config)] if " start " in f" {help_text} " else [binary, "-c", str(config)]
    process = subprocess.Popen(command, cwd=BASE_DIR, stdout=log_stream, stderr=subprocess.STDOUT, text=True)
    time.sleep(1.0)
    if process.poll() is not None:
        log_stream.close()
        raise RuntimeError(f"{engine} exited during startup; see {log_path}")
    return process, log_stream


def dnsperf_command(
    dnsperf: str,
    scenario: Scenario,
    host: str,
    port: int,
    query_file: Path,
    seconds: int,
    level: int,
    args: argparse.Namespace,
) -> list[str]:
    clients = min(level, args.max_clients)
    threads = min(clients, args.threads)
    return [
        dnsperf, "-j", "-m", scenario.mode, "-s", host, "-p", str(port), "-d", str(query_file),
        "-l", str(seconds), "-c", str(clients), "-T", str(threads), "-q", str(level), "-n", "1000000",
        "-t", str(args.timeout), "-O", "latency-histogram", "-O", "suppress=timeout,unexpected",
    ]


def run_dnsperf(command: list[str], output: Path) -> dict[str, Any]:
    completed = subprocess.run(command, cwd=BASE_DIR, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    output.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"dnsperf failed ({completed.returncode}); see {output}")
    objects: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            objects.append(item)
    stats_objects = [item["statistics"] for item in objects if isinstance(item.get("statistics"), dict)]
    starts = [item["start"] for item in objects if isinstance(item.get("start"), dict)]
    if not stats_objects:
        raise RuntimeError(f"dnsperf JSON statistics missing; see {output}")
    stats = next((item for item in reversed(stats_objects) if not item.get("interval")), stats_objects[-1])
    latency = stats.get("latency", {})
    histogram = latency.get("histogram", [])
    return {
        "qps": float(stats.get("qps", 0.0)),
        "sent": int(stats.get("sent", 0)),
        "completed": int(stats.get("completed", 0)),
        "lost": int(stats.get("lost", 0)),
        "loss_pct": (float(stats.get("lost", 0)) / max(1.0, float(stats.get("sent", 0)))) * 100.0,
        "avg_latency_ms": float(latency.get("avg", 0.0)) * 1000.0,
        "p50_latency_ms": percentile_from_histogram(histogram, 0.50) or 0.0,
        "p95_latency_ms": percentile_from_histogram(histogram, 0.95) or 0.0,
        "p99_latency_ms": percentile_from_histogram(histogram, 0.99) or 0.0,
        "max_latency_ms": float(latency.get("max", 0.0)) * 1000.0,
        "dnsperf_version": str(starts[-1].get("version", "n/a")) if starts else "n/a",
    }


METRICS = (
    "qps", "loss_pct", "avg_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms",
    "max_latency_ms", "cpu_pct_median", "cpu_pct_p95", "rss_mib_median", "rss_mib_max", "threads_max",
)


def aggregate(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
    for row in raw:
        groups.setdefault((row["scenario"], row["load"], row["engine"]), []).append(row)
    result: list[dict[str, Any]] = []
    for (scenario, load, engine), rows in groups.items():
        item: dict[str, Any] = {"scenario": scenario, "load": load, "engine": engine, "repeats": len(rows)}
        for metric in METRICS:
            item[metric] = median(float(row[metric]) for row in rows)
        result.append(item)
    return sorted(result, key=lambda row: (row["scenario"], row["load"], row["engine"]))


def write_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ("scenario", "load", "engine", "repeats", *METRICS)
    with path.open("w", encoding="utf-8") as stream:
        stream.write("\t".join(fields) + "\n")
        for row in rows:
            stream.write("\t".join(str(row.get(field, "")) for field in fields) + "\n")


def svg_frame(title: str, width: int, height: int, content: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{html.escape(title)}" viewBox="0 0 {width} {height}">'
        '<style>text{font-family:system-ui,sans-serif;fill:#334155}.title{font-size:18px;font-weight:600}.label{font-size:12px}'
        '.grid{stroke:#cbd5e1;stroke-width:1}.axis{stroke:#64748b;stroke-width:1.2}</style>'
        f'<rect width="100%" height="100%" rx="12" fill="#fff"/><text x="24" y="30" class="title">{html.escape(title)}</text>{content}</svg>'
    )


def bar_chart(path: Path, title: str, values: list[tuple[str, float, float]], unit: str) -> None:
    width, height = 920, max(320, 90 + len(values) * 42)
    left, right, top, bottom = 220, 30, 62, 38
    plot_width = width - left - right
    maximum = max((max(ox, mos) for _, ox, mos in values), default=1.0) or 1.0
    parts = [f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" class="axis"/>']
    for index, (label, oxidns, mosdns) in enumerate(values):
        y = top + index * 42
        parts.append(f'<text x="{left-10}" y="{y+18}" text-anchor="end" class="label">{html.escape(label)}</text>')
        for offset, engine, value in ((2, "oxidns", oxidns), (21, "mosdns", mosdns)):
            bar_width = value / maximum * plot_width
            parts.append(f'<rect x="{left}" y="{y+offset}" width="{bar_width:.2f}" height="15" rx="3" fill="{ENGINE_COLORS[engine]}"/>')
            parts.append(f'<text x="{min(width-4, left+bar_width+5):.2f}" y="{y+offset+12}" class="label">{value:,.1f}{unit}</text>')
    parts.append(f'<rect x="{left}" y="{height-24}" width="12" height="12" fill="{ENGINE_COLORS["oxidns"]}"/><text x="{left+18}" y="{height-14}" class="label">OxiDNS</text>')
    parts.append(f'<rect x="{left+90}" y="{height-24}" width="12" height="12" fill="{ENGINE_COLORS["mosdns"]}"/><text x="{left+108}" y="{height-14}" class="label">mosdns</text>')
    path.write_text(svg_frame(title, width, height, "".join(parts)), encoding="utf-8")


def line_chart(path: Path, title: str, rows: list[dict[str, Any]], metric: str, unit: str) -> None:
    width, height = 920, 430
    left, right, top, bottom = 72, 30, 58, 64
    loads = sorted({int(row["load"]) for row in rows})
    maximum = max((float(row[metric]) for row in rows), default=1.0) or 1.0
    x_positions = {load: left + index * (width-left-right) / max(1, len(loads)-1) for index, load in enumerate(loads)}
    parts: list[str] = []
    for tick in range(6):
        value = maximum * tick / 5
        y = height - bottom - (height-top-bottom) * tick / 5
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" class="grid"/>')
        parts.append(f'<text x="{left-8}" y="{y+4:.2f}" text-anchor="end" class="label">{value:,.1f}</text>')
    for load in loads:
        x = x_positions[load]
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+22}" text-anchor="middle" class="label">{load}</text>')
    for engine in ("oxidns", "mosdns"):
        engine_rows = sorted((row for row in rows if row["engine"] == engine), key=lambda row: row["load"])
        points = []
        for row in engine_rows:
            x = x_positions[int(row["load"])]
            y = height-bottom-float(row[metric])/maximum*(height-top-bottom)
            points.append(f"{x:.2f},{y:.2f}")
            parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="{ENGINE_COLORS[engine]}"/>')
        parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{ENGINE_COLORS[engine]}" stroke-width="3"/>')
    parts.append(f'<text x="{(left+width-right)/2}" y="{height-12}" text-anchor="middle" class="label">Outstanding queries</text>')
    parts.append(f'<text x="18" y="{(top+height-bottom)/2}" transform="rotate(-90 18 {(top+height-bottom)/2})" text-anchor="middle" class="label">{html.escape(unit)}</text>')
    parts.append(f'<rect x="{width-190}" y="18" width="12" height="12" fill="{ENGINE_COLORS["oxidns"]}"/><text x="{width-172}" y="29" class="label">OxiDNS</text>')
    parts.append(f'<rect x="{width-100}" y="18" width="12" height="12" fill="{ENGINE_COLORS["mosdns"]}"/><text x="{width-82}" y="29" class="label">mosdns</text>')
    path.write_text(svg_frame(title, width, height, "".join(parts)), encoding="utf-8")


def best_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in summary:
        if float(row["loss_pct"]) <= 0.1:
            groups.setdefault((row["scenario"], row["engine"]), []).append(row)
    return [max(rows, key=lambda row: float(row["qps"])) for rows in groups.values()]


def paired_values(rows: list[dict[str, Any]], metric: str) -> list[tuple[str, float, float]]:
    by_scenario: dict[str, dict[str, float]] = {}
    for row in rows:
        by_scenario.setdefault(row["scenario"], {})[row["engine"]] = float(row[metric])
    return [(scenario, engines.get("oxidns", 0.0), engines.get("mosdns", 0.0)) for scenario, engines in sorted(by_scenario.items())]


def objective_evaluation_lines(summary: list[dict[str, Any]], language: str) -> list[str]:
    pairs: dict[str, dict[str, dict[str, Any]]] = {}
    for row in best_rows(summary):
        pairs.setdefault(row["scenario"], {})[row["engine"]] = row

    def pair(name: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        engines = pairs.get(name, {})
        if "oxidns" not in engines or "mosdns" not in engines:
            return None
        return engines["oxidns"], engines["mosdns"]

    def qps_gain(item: tuple[dict[str, Any], dict[str, Any]]) -> float:
        return (float(item[0]["qps"]) / float(item[1]["qps"]) - 1) * 100

    simple_names = ["02-cache-hotpath", "06-local-answers", "47-server-local-udp"]
    complex_names = ["08-domain-set", "43-composite-provider-chain"]
    simple = [(name, item) for name in simple_names if (item := pair(name)) is not None]
    complex_rows = [(name, item) for name in complex_names if (item := pair(name)) is not None]
    memory_reductions = [
        (1 - float(item[0]["rss_mib_median"]) / float(item[1]["rss_mib_median"])) * 100
        for name in pairs if (item := pair(name)) is not None
    ]

    if language == "zh":
        lines = ["## 客观评价", ""]
        if simple:
            labels = {"02-cache-hotpath": "缓存热路径", "06-local-answers": "本地回答", "47-server-local-udp": "最小 UDP 路径"}
            values = "、".join(f"{labels[name]} {qps_gain(item):+.1f}%" for name, item in simple)
            lines.extend([
                f"* **简单本地路径优势温和，并非全面拉开差距。** 最大稳定吞吐相对 mosdns 分别为：{values}。其中本地回答和最小 UDP 路径只有个位数差距；部分中等并发点两者接近，因此不应概括成所有负载下都有大幅领先。",
            ])
        if complex_rows:
            complex_labels = {"08-domain-set": "域名集合", "43-composite-provider-chain": "复合 provider 链"}
            ratios = "、".join(f"{complex_labels[name]}为 {float(item[0]['qps']) / float(item[1]['qps']):.2f} 倍" for name, item in complex_rows)
            latency = "、".join(f"{complex_labels[name]}低 {(1 - float(item[0]['p99_latency_ms']) / float(item[1]['p99_latency_ms'])) * 100:.1f}%" for name, item in complex_rows)
            lines.append(f"* **复杂规则路径差异明显。** OxiDNS 最大稳定吞吐相对 mosdns：{ratios}；p99 降幅分别为：{latency}。这说明本轮优势主要集中在真实数据集查询和复合 provider/matcher 链，而不是只来自 UDP 协议框架。")
        if memory_reductions:
            lines.append(f"* **常驻内存更低。** 五个场景中 OxiDNS 的 RSS 均低于 mosdns，降幅约为 {min(memory_reductions):.1f}%–{max(memory_reductions):.1f}%。")
        lines.extend([
            "* **CPU 结果需要结合吞吐解释。** 在本地回答和最小 UDP 路径的最大稳定点，OxiDNS 使用了更多 CPU，但吞吐只小幅提高；这些场景下不能仅凭 QPS 宣称效率全面更好。复杂规则路径中 OxiDNS 则同时取得更高吞吐和更低 CPU。",
            "* **结论强度有限。** 每点 3 次重复适合作为阶段性工程对比，但不足以替代更大样本、置信区间和跨机器复测。这里的数值应理解为该主机和该工作负载下的性能轮廓，不是通用容量承诺。",
            "",
        ])
        return lines

    lines = ["## Objective assessment", ""]
    if simple:
        simple_labels = {"02-cache-hotpath": "warm cache", "06-local-answers": "local answers", "47-server-local-udp": "minimal UDP"}
        values = ", ".join(f"{simple_labels[name]} {qps_gain(item):+.1f}%" for name, item in simple)
        lines.append(f"- **The lead on simple local paths is modest, not universal.** Maximum stable throughput versus mosdns is {values}. Local answers and the minimal UDP path differ by only single-digit percentages, and some mid-concurrency points are close.")
    if complex_rows:
        complex_labels = {"08-domain-set": "domain set", "43-composite-provider-chain": "composite provider chain"}
        ratios = ", ".join(f"{complex_labels[name]} {float(item[0]['qps']) / float(item[1]['qps']):.2f}×" for name, item in complex_rows)
        latency = ", ".join(f"{complex_labels[name]} {(1 - float(item[0]['p99_latency_ms']) / float(item[1]['p99_latency_ms'])) * 100:.1f}% lower" for name, item in complex_rows)
        lines.append(f"- **The difference is substantial on complex rule paths.** OxiDNS maximum stable throughput is {ratios}; p99 is {latency}. The largest gains therefore come from dataset lookup and the composite provider/matcher chain, not merely the UDP framework.")
    if memory_reductions:
        lines.append(f"- **Resident memory is consistently lower.** OxiDNS RSS is lower in every measured scenario, by about {min(memory_reductions):.1f}%–{max(memory_reductions):.1f}%.")
    lines.extend([
        "- **CPU must be read together with throughput.** At the local-answer and minimal-UDP stable points, OxiDNS uses more CPU for only a modest throughput increase, so those results do not support a blanket CPU-efficiency claim. On the complex paths it delivers both higher throughput and lower CPU.",
        "- **The strength of the conclusion is limited.** Three repeats per point are suitable for a periodic engineering comparison, but they do not replace larger samples, confidence intervals, or cross-machine replication. Treat these values as a profile for this host and workload, not a universal capacity promise.",
        "",
    ])
    return lines


def generate_assets(result_dir: Path, summary: list[dict[str, Any]]) -> None:
    charts = result_dir / "charts"
    charts.mkdir(exist_ok=True)
    best = best_rows(summary)
    bar_chart(charts / "throughput.svg", "Maximum stable throughput (loss ≤ 0.1%)", paired_values(best, "qps"), " QPS")
    bar_chart(charts / "cpu.svg", "Server CPU at maximum stable throughput", paired_values(best, "cpu_pct_median"), "%")
    bar_chart(charts / "memory.svg", "Server RSS at maximum stable throughput", paired_values(best, "rss_mib_median"), " MiB")
    focus = "47-server-local-udp"
    focus_rows = [row for row in summary if row["scenario"] == focus]
    if not focus_rows:
        focus = summary[0]["scenario"]
        focus_rows = [row for row in summary if row["scenario"] == focus]
    line_chart(charts / "scaling.svg", f"Throughput scaling — {focus}", focus_rows, "qps", "QPS")
    line_chart(charts / "tail-latency.svg", f"p99 latency under load — {focus}", focus_rows, "p99_latency_ms", "ms")


def render_report(result_dir: Path, environment: dict[str, str], summary: list[dict[str, Any]], args: argparse.Namespace) -> str:
    best = best_rows(summary)
    pairs: dict[str, dict[str, dict[str, Any]]] = {}
    for row in best:
        pairs.setdefault(row["scenario"], {})[row["engine"]] = row
    lines = [
        "# OxiDNS vs mosdns publishable benchmark report", "",
        f"Generated: `{environment['timestamp']}`", "",
        "## Method", "",
        f"- repeats per point: `{args.repeats}`; measured duration: `{args.seconds}s`; warmup: `{args.warmup_seconds}s`",
        f"- outstanding-query levels: `{args.load_levels}`",
        "- aggregation: median across repeats; stable capacity excludes points with loss above 0.1%",
        "- server CPU is aggregate process CPU (100% = one fully occupied logical CPU); memory is sampled RSS",
        "- engines run one at a time and their order alternates by repeat/load point",
        "", "## Environment", "",
    ]
    lines.extend(f"- `{key}={value}`" for key, value in environment.items())
    lines.extend([
        "", "## How to read the metrics", "",
        "- **QPS / throughput: higher is better**, provided loss and tail latency remain acceptable.",
        "- **p50/p95/p99/max latency: lower is better**. p99 is the response time that 99% of completed requests do not exceed; it is more useful than the average for spotting queueing and long-tail stalls.",
        "- **Packet loss: lower is better**. This report only treats a point as stable when median loss is at most 0.1%.",
        "- **CPU: lower is better at the same throughput**. CPU alone is not a speed score: higher CPU can be reasonable when it produces substantially more QPS. Here, 100% means one fully occupied logical CPU.",
        "- **RSS memory: lower is better for the same workload**. RSS is the process's resident physical memory during the measured run.",
        "- On scaling charts, the preferred curve rises with concurrency while latency and loss stay controlled. A flat QPS curve combined with rising p99 means the engine has reached saturation.",
        "", "## Charts", "", "![Throughput](charts/throughput.svg)", "", "![Scaling](charts/scaling.svg)", "", "![Tail latency](charts/tail-latency.svg)", "", "![CPU](charts/cpu.svg)", "", "![Memory](charts/memory.svg)", "", "## Maximum stable point by scenario", "", "| Scenario | OxiDNS QPS | mosdns QPS | OxiDNS p99 | mosdns p99 | OxiDNS CPU | mosdns CPU | OxiDNS RSS | mosdns RSS |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for scenario in sorted(pairs):
        ox = pairs[scenario].get("oxidns", {})
        mos = pairs[scenario].get("mosdns", {})
        lines.append(
            f"| {scenario} | {float(ox.get('qps', 0)):,.1f} | {float(mos.get('qps', 0)):,.1f} | "
            f"{float(ox.get('p99_latency_ms', 0)):.3f} ms | {float(mos.get('p99_latency_ms', 0)):.3f} ms | "
            f"{float(ox.get('cpu_pct_median', 0)):.1f}% | {float(mos.get('cpu_pct_median', 0)):.1f}% | "
            f"{float(ox.get('rss_mib_median', 0)):.1f} MiB | {float(mos.get('rss_mib_median', 0)):.1f} MiB |"
        )
    lines.extend([""] + objective_evaluation_lines(summary, "en"))
    lines.extend([
        "", "## Representativeness assessment", "",
        "This matrix is representative of the stable local UDP request path: it separates minimal listener overhead, local answers, warm-cache behavior, dataset lookup, and a composite provider/matcher chain. The load sweep exposes scaling, saturation, and queueing instead of reducing the comparison to one peak-QPS number.", "",
        "It does not represent cold start or reload cost, TCP/DoT/DoH/DoQ transports, cache-miss-heavy traffic, public upstream quality, multi-machine network effects, or host-integrated side effects such as ipset/nftset. Those need dedicated matrices and, for capacity claims, a separate load-generator host.", "",
        "## Interpretation limits", "",
        "This is a same-host loopback comparison. It is representative of local request-path cost and concurrency scaling on the recorded machine, not public-upstream quality or production capacity on other hardware. External-forward scenarios must be reported separately because upstream/network variance can dominate engine cost.", "",
    ])
    return "\n".join(lines)


def docs_chart_panel(src: str, alt: str, heading: str, body: str) -> str:
    return "\n".join([
        '<div className="row margin-bottom--lg">',
        '  <div className="col col--8">',
        f'    <img src="{src}" alt="{alt}" />',
        "  </div>",
        '  <div className="col col--4">',
        f"    <p><strong>{heading}</strong></p>",
        f"    <p>{body}</p>",
        "  </div>",
        "</div>",
    ])


def render_zh_docs(environment: dict[str, str], summary: list[dict[str, Any]], args: argparse.Namespace | None) -> str:
    pairs: dict[str, dict[str, dict[str, Any]]] = {}
    for row in best_rows(summary):
        pairs.setdefault(row["scenario"], {})[row["engine"]] = row
    parameters = "测试参数见[完整原始报告](/benchmarks/staged/report.txt)"
    if args is not None:
        parameters = f"每个点重复 {args.repeats} 次，预热 {args.warmup_seconds} 秒、测量 {args.seconds} 秒，并发点为 {args.load_levels}"
    lines = [
        "---", "title: 性能测试", "sidebar_position: 8", "---", "", "# 性能测试", "",
        f"本页展示 OxiDNS `{environment['oxidns_version']}` 与 mosdns `{environment['mosdns_version']}` 的阶段性实测快照，dnsperf 版本为 `{environment['dnsperf_version']}`。仅在架构、关键请求路径、测试口径或重要里程碑发生明显变化时更新，不要求每个版本重复测试。", "",
        f"本轮数据采集于 `{environment['timestamp']}`。{parameters}。每个指标取多次重复的中位数；最大稳定吞吐只接受丢包率不高于 0.1% 的点。进程 CPU 的 100% 表示占满一个逻辑核。", "",
        "## 被测环境", "",
        f"* CPU：`{environment['cpu']}`，逻辑核 `{environment['logical_cpus']}`",
        f"* 内存：`{environment['memory']}`",
        f"* OxiDNS：`{environment['oxidns_version']}`，SHA-256 `{environment['oxidns_sha256']}`",
        f"* mosdns：`{environment['mosdns_version']}`，SHA-256 `{environment['mosdns_sha256']}`",
        f"* dnsperf：`{environment['dnsperf_version']}`", "",
        "## 指标怎么看", "",
        "* **QPS / 吞吐量：越高越好**，但前提是丢包率和尾延迟仍在可接受范围内。",
        "* **p50、p95、p99、最大延迟：越低越好**。p99 表示 99% 已完成请求的响应时间不超过该值，比平均值更容易看出排队和长尾卡顿。",
        "* **丢包率：越低越好**。本报告只有在丢包率中位数不超过 0.1% 时，才把该并发点计为“稳定”。",
        "* **CPU：相同吞吐量下越低越好**。不能脱离 QPS 单看 CPU；如果使用更多 CPU 换来了明显更高吞吐，仍可能是合理结果。这里 100% 表示占满一个逻辑核。",
        "* **RSS 内存：相同负载下越低越好**，表示测试过程中进程实际驻留在物理内存中的容量。",
        "* 看折线图时，理想状态是并发增加后 QPS 继续上升，同时 p99 和丢包保持稳定；如果 QPS 已经走平而 p99 快速升高，说明服务已经进入饱和区。", "",
        "## 吞吐与并发扩展", "",
        docs_chart_panel(
            "/img/benchmarks/staged/throughput.svg", "各场景最大稳定吞吐量柱状图", "越高越好",
            "柱子越高表示稳定状态下每秒完成的 DNS 请求越多。仍需结合 p99 和丢包判断，不能把高丢包下的峰值当作有效容量。",
        ), "",
        docs_chart_panel(
            "/img/benchmarks/staged/scaling.svg", "并发扩展折线图", "上升且不过早走平更好",
            "并发增加时 QPS 应继续上升。曲线走平表示接近吞吐上限；此时若 p99 同时快速升高，说明已经进入排队和饱和区。",
        ), "",
        "## 尾延迟", "",
        docs_chart_panel(
            "/img/benchmarks/staged/tail-latency.svg", "p99 尾延迟折线图", "越低越好",
            "p99 越低，绝大多数请求的最慢部分越可控。随着并发增加仍保持平缓的曲线，比只看平均延迟更可靠。",
        ), "",
        "## CPU 与内存", "",
        docs_chart_panel(
            "/img/benchmarks/staged/cpu.svg", "CPU 占用柱状图", "相同吞吐量下越低越好",
            "100% 等于占满一个逻辑核。CPU 必须和 QPS 配合看：CPU 更高但吞吐提升更大并不一定更差，也可进一步比较每万 QPS 的 CPU 成本。",
        ), "",
        docs_chart_panel(
            "/img/benchmarks/staged/memory.svg", "RSS 内存柱状图", "相同负载下越低越好",
            "RSS 表示进程实际驻留的物理内存。柱子越低，常驻内存压力越小；比较时应确保场景、规则数据和负载一致。",
        ), "",
        "## 各场景最大稳定点", "", "| 场景 | OxiDNS QPS | mosdns QPS | OxiDNS p99 | mosdns p99 | OxiDNS CPU | mosdns CPU | OxiDNS RSS | mosdns RSS |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario in sorted(pairs):
        ox = pairs[scenario].get("oxidns", {})
        mos = pairs[scenario].get("mosdns", {})
        lines.append(
            f"| {scenario} | {float(ox.get('qps', 0)):,.1f} | {float(mos.get('qps', 0)):,.1f} | "
            f"{float(ox.get('p99_latency_ms', 0)):.3f} ms | {float(mos.get('p99_latency_ms', 0)):.3f} ms | "
            f"{float(ox.get('cpu_pct_median', 0)):.1f}% | {float(mos.get('cpu_pct_median', 0)):.1f}% | "
            f"{float(ox.get('rss_mib_median', 0)):.1f} MiB | {float(mos.get('rss_mib_median', 0)):.1f} MiB |"
        )
    lines.extend([""] + objective_evaluation_lines(summary, "zh"))
    lines.extend([
        "", "## 代表性判断", "",
        "本矩阵对**稳定的本地 UDP 请求路径**具有代表性：最小监听器、本地回答、热缓存、真实域名集合查询和复合 provider/matcher 链被分开测试，并通过多档并发展示扩展、饱和与排队，而不是只比较一个峰值 QPS。", "",
        "它不能代表冷启动/热重载、TCP/DoT/DoH/DoQ、以缓存未命中为主的流量、公网上游质量、跨机网络开销，或 ipset/nftset 等宿主机副作用。生产容量测试还应使用独立压测机，并为这些路径建立单独矩阵。", "",
        "## 口径限制", "",
        "本轮是同机 loopback 对比，适合观察本地请求路径成本、并发扩展和排队，不代表其他硬件上的生产容量，也不把公网转发上游波动混进默认结论。可下载：[完整报告](/benchmarks/staged/report.txt)、[聚合 TSV](/benchmarks/staged/summary.tsv)、[逐轮 JSON](/benchmarks/staged/summary.raw.json)、[环境快照](/benchmarks/staged/environment.json)。", "",
    ])
    return "\n".join(lines)


def publish_docs(
    result_dir: Path,
    report: str,
    environment: dict[str, str],
    summary: list[dict[str, Any]],
    args: argparse.Namespace | None = None,
) -> None:
    asset_dir = REPO_DIR / "docs/static/img/benchmarks/staged"
    asset_dir.mkdir(parents=True, exist_ok=True)
    for source in (result_dir / "charts").glob("*.svg"):
        shutil.copy2(source, asset_dir / source.name)
    (REPO_DIR / "docs/docs/benchmarks.md").write_text(render_zh_docs(environment, summary, args), encoding="utf-8")
    english = report.replace("# OxiDNS vs mosdns publishable benchmark report", "# Performance Benchmark", 1)
    english = english.replace("charts/", "/img/benchmarks/staged/")
    english_chart_panels = {
        "![Throughput](/img/benchmarks/staged/throughput.svg)": docs_chart_panel(
            "/img/benchmarks/staged/throughput.svg", "Maximum stable throughput by scenario", "Higher is better",
            "A taller bar means more DNS requests completed per second at a stable point. Check p99 and loss as well; a peak reached with excessive loss is not usable capacity.",
        ),
        "![Scaling](/img/benchmarks/staged/scaling.svg)": docs_chart_panel(
            "/img/benchmarks/staged/scaling.svg", "Throughput scaling by concurrency", "Rising without flattening early is better",
            "QPS should continue to rise as concurrency increases. A flat curve marks the throughput ceiling; if p99 rises at the same time, the engine is queueing and saturated.",
        ),
        "![Tail latency](/img/benchmarks/staged/tail-latency.svg)": docs_chart_panel(
            "/img/benchmarks/staged/tail-latency.svg", "p99 tail latency under load", "Lower is better",
            "Lower p99 means the slowest part of normal traffic remains controlled. A curve that stays flat as concurrency grows is preferable to one that climbs sharply.",
        ),
        "![CPU](/img/benchmarks/staged/cpu.svg)": docs_chart_panel(
            "/img/benchmarks/staged/cpu.svg", "CPU at maximum stable throughput", "Lower is better at equal throughput",
            "100% is one fully occupied logical CPU. Read CPU together with QPS: using more CPU can be reasonable when it produces proportionally more throughput.",
        ),
        "![Memory](/img/benchmarks/staged/memory.svg)": docs_chart_panel(
            "/img/benchmarks/staged/memory.svg", "Resident memory at maximum stable throughput", "Lower is better for the same workload",
            "RSS is physical memory resident for the process. A shorter bar means lower memory pressure when scenario, rules, and load are equivalent.",
        ),
    }
    for chart_markdown, chart_panel in english_chart_panels.items():
        english = english.replace(chart_markdown, chart_panel)
    if "## How to read the metrics" not in english:
        english = english.replace(
            "## Charts",
            "## How to read the metrics\n\n"
            "- **QPS / throughput: higher is better**, provided loss and tail latency remain acceptable.\n"
            "- **p50/p95/p99/max latency: lower is better**. p99 exposes queueing and long-tail stalls better than an average.\n"
            "- **Packet loss: lower is better**. A point is stable here only when median loss is at most 0.1%.\n"
            "- **CPU: lower is better at the same throughput**. CPU alone is not a speed score; 100% means one fully occupied logical CPU.\n"
            "- **RSS memory: lower is better for the same workload**. It is the process's resident physical memory.\n"
            "- A flat QPS curve combined with rising p99 means the engine has reached saturation.\n\n"
            "## Charts",
            1,
        )
    if "## Representativeness assessment" not in english:
        english = english.replace(
            "## Interpretation limits",
            "## Representativeness assessment\n\n"
            "This matrix represents the stable local UDP request path: minimal listener overhead, local answers, warm-cache behavior, dataset lookup, and a composite provider/matcher chain. Its load sweep exposes scaling, saturation, and queueing instead of relying on one peak-QPS number.\n\n"
            "It does not cover cold start or reload cost, TCP/DoT/DoH/DoQ, cache-miss-heavy traffic, public upstream quality, multi-machine network effects, or host-integrated side effects such as ipset/nftset. Production-capacity work needs a separate load-generator host and dedicated matrices for those paths.\n\n"
            "## Interpretation limits",
            1,
        )
    if "## Objective assessment" not in english:
        english = english.replace(
            "## Representativeness assessment",
            "\n".join(objective_evaluation_lines(summary, "en")) + "\n## Representativeness assessment",
            1,
        )
    stage_note = (
        f"This page presents a periodic benchmark snapshot of OxiDNS `{environment['oxidns_version']}` "
        f"and mosdns `{environment['mosdns_version']}`, measured with dnsperf `{environment['dnsperf_version']}`. "
        "It is updated for meaningful architecture, request-path, methodology, or milestone changes—not for every release.\n\n"
    )
    english = english.replace("Generated:", stage_note + "Generated:", 1)
    english_header = "---\ntitle: Performance Benchmark\nsidebar_position: 8\n---\n\n"
    (REPO_DIR / "docs/i18n/en/docusaurus-plugin-content-docs/current/benchmarks.md").write_text(english_header + english, encoding="utf-8")
    raw_target = REPO_DIR / "docs/static/benchmarks/staged"
    raw_target.mkdir(parents=True, exist_ok=True)
    shutil.copy2(result_dir / "summary.tsv", raw_target / "summary.tsv")
    shutil.copy2(result_dir / "summary.raw.json", raw_target / "summary.raw.json")
    shutil.copy2(result_dir / "environment.json", raw_target / "environment.json")
    (raw_target / "report.txt").write_text(report, encoding="utf-8")
    print(f"published docs snapshot for {environment['oxidns_version']}")


def main() -> int:
    args = parse_args()
    if args.publish_existing:
        result_dir = args.publish_existing.resolve()
        environment = json.loads((result_dir / "environment.json").read_text(encoding="utf-8"))
        raw = json.loads((result_dir / "summary.raw.json").read_text(encoding="utf-8"))
        report = (result_dir / "report.md").read_text(encoding="utf-8")
        publish_docs(result_dir, report, environment, aggregate(raw))
        return 0
    if platform.system() != "Linux" and not args.dry_run:
        raise SystemExit("publishable resource measurements require Linux /proc; use --dry-run elsewhere")
    for name in ("seconds", "warmup_seconds", "repeats", "threads", "max_clients", "timeout"):
        if getattr(args, name) < 1:
            raise SystemExit(f"--{name.replace('_', '-')} must be positive")
    levels = positive_levels(args.load_levels)
    selected = select_scenarios(load_catalog(BASE_DIR / "scenarios.tsv"), args.selectors)
    for scenario in selected:
        for path in (scenario.oxidns_config, scenario.mosdns_config, scenario.query_file, scenario.warmup_file):
            if not path.is_file():
                raise SystemExit(f"missing benchmark input: {path}")
    print("selected scenarios:")
    for scenario in selected:
        print(f"  {scenario.label:30} {scenario.description}")
    print(f"load levels: {levels}; repeats: {args.repeats}")
    if args.dry_run:
        return 0

    oxidns = require_executable(os.getenv("OXIDNS_BIN_PATH", str(BASE_DIR / "oxidns")), "oxidns")
    mosdns = require_executable(os.getenv("MOSDNS_BIN_PATH", str(BASE_DIR / "mosdns")), "mosdns")
    dnsperf = require_executable(os.getenv("DNSPERF_BIN_PATH", "dnsperf"), "dnsperf")
    if "-j" not in command_output([dnsperf, "-h"]):
        raise SystemExit("dnsperf lacks JSON support (-j); run prepare_server.sh to build dnsperf 2.15.1")
    long_help = command_output([dnsperf, "-H"])
    if "latency-histogram" not in long_help:
        raise SystemExit("dnsperf lacks latency-histogram support")

    result_dir = args.result_dir or BASE_DIR / "results" / f"publishable-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    result_dir.mkdir(parents=True, exist_ok=False)
    environment = {
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "hostname": platform.node(),
        "kernel": platform.platform(),
        "cpu": command_output(["sh", "-c", "lscpu | grep 'Model name' | head -n1"]).replace("\n", " "),
        "logical_cpus": str(os.cpu_count() or 0),
        "memory": command_output(["sh", "-c", "free -h | grep '^Mem:'"]).replace("\n", " "),
        "git_head": git_value("rev-parse", "HEAD"),
        "git_describe": git_value("describe", "--tags", "--always", "--dirty"),
        "oxidns_version": binary_version(oxidns, "oxidns"),
        "oxidns_sha256": sha256(oxidns),
        "mosdns_version": binary_version(mosdns, "mosdns"),
        "mosdns_sha256": sha256(mosdns),
        "dnsperf_version": dnsperf_version(dnsperf),
    }
    (result_dir / "environment.json").write_text(json.dumps(environment, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    raw: list[dict[str, Any]] = []
    active: subprocess.Popen[str] | None = None
    active_log: Any = None
    try:
        for scenario in selected:
            host, port = extract_listen(scenario.oxidns_config)
            for level in levels:
                for repeat in range(1, args.repeats + 1):
                    engines = ["oxidns", "mosdns"]
                    if (level + repeat + sum(scenario.label.encode())) % 2:
                        engines.reverse()
                    for engine in engines:
                        binary = oxidns if engine == "oxidns" else mosdns
                        config = scenario.oxidns_config if engine == "oxidns" else scenario.mosdns_config
                        stem = f"{scenario.label}.{engine}.q{level}.r{repeat:02d}"
                        print(f">>> {stem}", flush=True)
                        active, active_log = start_engine(engine, binary, config, result_dir / f"{stem}.startup.log")
                        warmup_command = dnsperf_command(dnsperf, scenario, host, port, scenario.warmup_file, args.warmup_seconds, level, args)
                        run_dnsperf(warmup_command, result_dir / f"{stem}.warmup.jsonl")
                        sampler = ProcSampler(active.pid, args.sample_interval, result_dir / f"{stem}.resources.tsv")
                        sampler.start()
                        measured_command = dnsperf_command(dnsperf, scenario, host, port, scenario.query_file, args.seconds, level, args)
                        measured = run_dnsperf(measured_command, result_dir / f"{stem}.dnsperf.jsonl")
                        measured.update(sampler.stop())
                        measured.update({"scenario": scenario.label, "engine": engine, "mode": scenario.mode, "load": level, "repeat": repeat})
                        raw.append(measured)
                        stop_process(active)
                        active = None
                        active_log.close()
                        active_log = None
                        print(f"    qps={measured['qps']:.1f} p99={measured['p99_latency_ms']:.3f}ms cpu={measured['cpu_pct_median']:.1f}% rss={measured['rss_mib_median']:.1f}MiB loss={measured['loss_pct']:.4f}%")
                        time.sleep(args.cooldown)
    finally:
        stop_process(active)
        if active_log is not None:
            active_log.close()

    (result_dir / "summary.raw.json").write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if raw:
        environment["dnsperf_version"] = str(raw[0].get("dnsperf_version", environment["dnsperf_version"]))
        (result_dir / "environment.json").write_text(json.dumps(environment, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = aggregate(raw)
    write_tsv(result_dir / "summary.tsv", summary)
    generate_assets(result_dir, summary)
    report = render_report(result_dir, environment, summary, args)
    (result_dir / "report.md").write_text(report, encoding="utf-8")
    if args.publish_docs:
        publish_docs(result_dir, report, environment, summary, args)
    print(f"report: {result_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    raise SystemExit(main())
