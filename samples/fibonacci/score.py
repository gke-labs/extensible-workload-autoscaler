#!/usr/bin/env python3
"""Live autoscaler scorer for the Fibonacci comparison sample.

Samples the fibonacci Deployment replica count and scrapes Prometheus metrics
from the load-generator Service in each target namespace via the Kubernetes API,
computing a running total score:

    Score = Replica-Seconds
          + (W_slow * Slow Queries)
          + (W_failed * Failed Queries)
          + (W_churn * Replica Churn)
"""

import argparse
from dataclasses import dataclass
import re
import subprocess
import sys
import time
from typing import Dict, Optional, Tuple


METRIC_RE = re.compile(
    r'^loadgen_queries_total\{status="(pass|slow|failed)"\}\s+([0-9.eE+-]+)',
    re.MULTILINE,
)
QPS_RE = re.compile(
    r"^loadgen_target_qps\s+([0-9.eE+-]+)",
    re.MULTILINE,
)


@dataclass
class NamespaceState:
    namespace: str
    last_sample_time: Optional[float] = None
    last_replicas: Optional[int] = None
    replica_seconds: float = 0.0
    replica_churn: int = 0
    initial_counts: Optional[Dict[str, float]] = None
    pass_queries: int = 0
    slow_queries: int = 0
    failed_queries: int = 0
    target_qps: float = 0.0

    def score(self, w_slow: float, w_failed: float, w_churn: float) -> float:
        return (
            self.replica_seconds
            + (w_slow * self.slow_queries)
            + (w_failed * self.failed_queries)
            + (w_churn * self.replica_churn)
        )


def get_replicas(
    namespace: str, deployment: str, context: Optional[str] = None
) -> Optional[int]:
    cmd = ["kubectl"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend([
        "get",
        f"deployment/{deployment}",
        "-n",
        namespace,
        "-o",
        "jsonpath={.status.replicas}",
    ])
    try:
        res = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=5.0
        )
    except subprocess.TimeoutExpired:
        return None
    if res.returncode != 0:
        return None
    val = res.stdout.strip()
    return int(val) if val.isdigit() else 0


def get_loadgen_metrics(
    namespace: str, service: str, context: Optional[str] = None
) -> Optional[Tuple[Dict[str, float], float]]:
    proxy_path = f"/api/v1/namespaces/{namespace}/services/{service}:80/proxy/metrics"
    cmd = ["kubectl"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(["get", "--raw", proxy_path])
    try:
        res = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=5.0
        )
    except subprocess.TimeoutExpired:
        return None
    if res.returncode != 0:
        return None

    counts = {"pass": 0.0, "slow": 0.0, "failed": 0.0}
    for match in METRIC_RE.finditer(res.stdout):
        counts[match.group(1)] = float(match.group(2))

    qps_match = QPS_RE.search(res.stdout)
    target_qps = float(qps_match.group(1)) if qps_match else 0.0
    return counts, target_qps


def sample_namespace(
    state: NamespaceState,
    fib_deployment: str,
    loadgen_service: str,
    context: Optional[str] = None,
) -> bool:
    now = time.monotonic()
    replicas = get_replicas(state.namespace, fib_deployment, context=context)
    metrics = get_loadgen_metrics(state.namespace, loadgen_service, context=context)
    if replicas is None or metrics is None:
        return False

    counts, target_qps = metrics
    state.target_qps = target_qps

    if state.last_sample_time is not None:
        dt = now - state.last_sample_time
        state.replica_seconds += replicas * dt
    state.last_sample_time = now

    if state.last_replicas is not None:
        state.replica_churn += abs(replicas - state.last_replicas)
    state.last_replicas = replicas

    # Assumes the load-generator pod does not restart during a scoring run.
    if state.initial_counts is None:
        state.initial_counts = dict(counts)

    state.pass_queries = int(round(counts["pass"] - state.initial_counts["pass"]))
    state.slow_queries = int(round(counts["slow"] - state.initial_counts["slow"]))
    state.failed_queries = int(round(counts["failed"] - state.initial_counts["failed"]))
    return True


def format_table(
    states: Dict[str, NamespaceState],
    elapsed: float,
    w_slow: float,
    w_failed: float,
    w_churn: float,
) -> str:
    lines = [
        f"Elapsed: {elapsed:6.1f}s | Weights: replica_s=1, slow={w_slow:g}, failed={w_failed:g}, churn={w_churn:g}",
        f"{'NAMESPACE':<18} {'QPS':>5} {'REPLICAS':>8} {'REPLICA_S':>10} {'PASS':>8} {'SLOW':>8} {'FAILED':>8} {'CHURN':>7} {'SCORE':>12}",
        "-" * 92,
    ]
    for ns, st in states.items():
        if st.last_replicas is None:
            lines.append(f"{ns:<18} {'(waiting for deployment/service...)':<73}")
            continue
        score = st.score(w_slow, w_failed, w_churn)
        lines.append(
            f"{ns:<18} {st.target_qps:>5.0f} {st.last_replicas:>8d} "
            f"{st.replica_seconds:>10.1f} {st.pass_queries:>8d} "
            f"{st.slow_queries:>8d} {st.failed_queries:>8d} "
            f"{st.replica_churn:>7d} {score:>12.1f}"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Live scorer for Fibonacci autoscaling sample.")
    parser.add_argument(
        "--namespaces",
        nargs="+",
        default=["fibonacci-hpa"],
        help="Namespaces to monitor (default: fibonacci-hpa).",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds.")
    parser.add_argument("--duration", type=float, default=0.0, help="Total duration in seconds (0 = until Ctrl+C).")
    parser.add_argument("--fib-deployment", default="fibonacci", help="Fibonacci deployment name.")
    parser.add_argument("--loadgen-service", default="load-generator", help="Load generator service name.")
    parser.add_argument("--context", default=None, help="Kubernetes context to query (optional).")
    parser.add_argument("--weight-slow", type=float, default=10.0, help="Penalty per slow query (2X..5X).")
    parser.add_argument("--weight-failed", type=float, default=50.0, help="Penalty per failed/timeout query (>5X).")
    parser.add_argument("--weight-churn", type=float, default=1.0, help="Penalty per replica count change.")
    args = parser.parse_args()

    states = {ns: NamespaceState(namespace=ns) for ns in args.namespaces}
    start_time = time.monotonic()

    try:
        while True:
            loop_start = time.monotonic()
            elapsed = loop_start - start_time
            if args.duration > 0 and elapsed >= args.duration:
                break

            for st in states.values():
                sample_namespace(
                    st,
                    args.fib_deployment,
                    args.loadgen_service,
                    context=args.context,
                )

            table = format_table(states, elapsed, args.weight_slow, args.weight_failed, args.weight_churn)
            # Clear screen and redraw table when running in an interactive terminal.
            if sys.stdout.isatty():
                sys.stdout.write("\033[2J\033[H" + table + "\n")
            else:
                sys.stdout.write(table + "\n\n")
            sys.stdout.flush()

            sleep_time = args.interval - (time.monotonic() - loop_start)
            if sleep_time > 0:
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
