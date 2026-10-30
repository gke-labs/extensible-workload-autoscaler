import aiohttp
import asyncio
import os
import schedule
import signal
import sys
import time

FIB_SERVICE_URL = os.environ.get("FIB_SERVICE_URL", "http://fibonacci")

async def send_request(session):
    try:
        async with session.get(FIB_SERVICE_URL) as response:
            if response.status == 200:
                print(".", end="", flush=True)
            else:
                print(f"E({response.status})", end="", flush=True)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"X({e})", end="", flush=True)

async def generate_load(duration_seconds, qps_func):
    start_time = time.monotonic()
    pending_tasks = set()
    connector = aiohttp.TCPConnector(force_close=True, limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        while time.monotonic() - start_time < duration_seconds:
            loop_start_time = time.monotonic()
            elapsed_in_loop = loop_start_time - start_time
            target_qps = qps_func(elapsed_in_loop)

            if target_qps > 0:
                for _ in range(int(target_qps)):
                    task = asyncio.create_task(send_request(session))
                    pending_tasks.add(task)
                    task.add_done_callback(pending_tasks.discard)

            loop_end_time = time.monotonic()
            sleep_duration = 1.0 - (loop_end_time - loop_start_time)
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)

        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)


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


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
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
