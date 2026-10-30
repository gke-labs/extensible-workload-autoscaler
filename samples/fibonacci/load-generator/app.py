import aiohttp
import argparse
import asyncio
import os
import schedule
import signal
import statistics
import sys
import time

FIB_SERVICE_URL = os.environ.get("FIB_SERVICE_URL", "http://fibonacci")

async def send_request(session, log_stream=sys.stdout):
    req_start = time.monotonic()
    try:
        async with session.get(FIB_SERVICE_URL) as response:
            elapsed = time.monotonic() - req_start
            if response.status == 200:
                print(".", end="", file=log_stream, flush=True)
                return elapsed
            else:
                print(f"E({response.status})", end="", file=log_stream, flush=True)
                return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"X({e})", end="", file=log_stream, flush=True)
        return None

async def generate_load(duration_seconds, qps_func, log_stream=sys.stdout):
    start_time = time.monotonic()
    pending_tasks = set()
    latencies = []

    def on_task_done(t):
        pending_tasks.discard(t)
        if not t.cancelled() and t.exception() is None:
            result = t.result()
            if result is not None:
                latencies.append(result)

    connector = aiohttp.TCPConnector(force_close=True, limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        while time.monotonic() - start_time < duration_seconds:
            loop_start_time = time.monotonic()
            elapsed_in_loop = loop_start_time - start_time
            target_qps = qps_func(elapsed_in_loop)

            if target_qps > 0:
                for _ in range(int(target_qps)):
                    task = asyncio.create_task(send_request(session, log_stream=log_stream))
                    pending_tasks.add(task)
                    task.add_done_callback(on_task_done)

            loop_end_time = time.monotonic()
            sleep_duration = 1.0 - (loop_end_time - loop_start_time)
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)

        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)

    return latencies


def run_load_cycle():
    print(f"\nStarting new load cycle at {time.ctime()}")

    # Ramp-up and sustain phase (15 minutes)
    def ramp_and_sustain_qps(t):
        # Quick ramp up in the first 10 seconds
        if t < 10:
            return (t / 10) * 25
        return 25

    print("\n--- Phase 1: Ramp-up and Sustain (15 mins) ---")
    asyncio.run(generate_load(900, ramp_and_sustain_qps))

    # Decay phase (15 minutes)
    def decay_qps(t):
        # t in decay phase is from 0 to 900
        return (1 - (t / 900)) * 25

    print("\n--- Phase 2: Decay (15 mins) ---")
    asyncio.run(generate_load(900, decay_qps))

    print(f"\nLoad cycle finished at {time.ctime()}. Waiting for next hour.")


def run_baseline(duration_seconds=30):
    if duration_seconds <= 0:
        print("Error: --duration must be a positive integer.", file=sys.stderr)
        sys.exit(1)
    print(f"Measuring baseline latency at 1 QPS for {duration_seconds}s against {FIB_SERVICE_URL}...", file=sys.stderr)
    latencies = asyncio.run(generate_load(duration_seconds, lambda _: 1, log_stream=sys.stderr))
    print("", file=sys.stderr)
    if not latencies:
        print("Error: no successful requests during baseline measurement.", file=sys.stderr)
        sys.exit(1)

    mean_s = statistics.mean(latencies)
    median_s = statistics.median(latencies)
    min_s = min(latencies)
    max_s = max(latencies)
    print(
        f"Baseline results ({len(latencies)} samples): "
        f"min={min_s:.4f}s, median={median_s:.4f}s, mean={mean_s:.4f}s, max={max_s:.4f}s",
        file=sys.stderr,
    )
    print(f"{median_s:.4f}")


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    parser = argparse.ArgumentParser(description="Fibonacci Load Generator")
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Measure 1-QPS baseline latency and print median latency (seconds) to stdout.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration in seconds for baseline measurement (default: 30).",
    )
    args = parser.parse_args()

    if args.baseline:
        run_baseline(args.duration)
        return
    print("Load generator started. Scheduling job for the start of every hour.")
    schedule.every().hour.at(":00").do(run_load_cycle)
    # For testing, you can uncomment the line below to run every minute
    # schedule.every().minute.at(":00").do(run_load_cycle)

    # Initial run for immediate testing if desired
    # run_load_cycle()

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
