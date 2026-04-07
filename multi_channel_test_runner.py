#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime

import pika

RMQ_HOST = os.getenv("RMQ_HOST", "192.168.0.150")
RMQ_PORT = int(os.getenv("RMQ_PORT", "5672"))
RMQ_USER = os.getenv("RMQ_USER", "guest")
RMQ_PASS = os.getenv("RMQ_PASS", "guest")
RMQ_QUEUE = os.getenv("RMQ_QUEUE", "dvr_events")

TEST_CHANNELS = [
    int(x.strip()) for x in os.getenv("TEST_CHANNELS", "1,2,7,8").split(",") if x.strip()
]
LOSS_TIMEOUT = float(os.getenv("LOSS_TIMEOUT", "20"))
RESTORE_TIMEOUT = float(os.getenv("RESTORE_TIMEOUT", "20"))
RESULTS_FILE = os.getenv("VIDEO_TEST_RESULTS_FILE", "video_test_results.jsonl")
DEBUG_RAW = os.getenv("DEBUG_RAW", "1") == "1"


def now_iso() -> str:
    return datetime.now().isoformat()


def log(msg: str) -> None:
    line = f"{now_iso()} {msg}"
    print(line, flush=True)


def write_result(result: dict) -> None:
    with open(RESULTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(result) + "\n")


def connect_rabbit():
    creds = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    params = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        credentials=creds,
        heartbeat=60,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=RMQ_QUEUE, durable=True)
    return conn, ch


class ChannelTest:
    def __init__(self, target_channel: int, loss_timeout: float, restore_timeout: float):
        self.target_channel = target_channel
        self.loss_timeout = loss_timeout
        self.restore_timeout = restore_timeout

        self.phase = "waiting_for_loss"
        self.test_started_at = time.time()
        self.loss_detected_at = None
        self.restore_detected_at = None
        self.completed = False
        self.result = None

    def handle_message(self, msg: dict) -> None:
        msg_ts = msg.get("ts")
        if msg_ts is not None and msg_ts < self.test_started_at:
            return

        event = msg.get("event", {})
        event_type = event.get("event_type")

        if event_type not in {"video_loss_snapshot", "video_loss_changed"}:
            return

        if self.phase == "waiting_for_loss":
            if event_type == "video_loss_changed":
                started = set(event.get("watched_started", []))
                if self.target_channel in started:
                    self.loss_detected_at = time.time()
                    dt = self.loss_detected_at - self.test_started_at
                    log(f"PASS: CH{self.target_channel} video loss detected in {dt:.2f}s")
                    log(f"WAITING: reconnect CH{self.target_channel} and waiting for restore")
                    self.phase = "waiting_for_restore"

        elif self.phase == "waiting_for_restore":
            if event_type == "video_loss_changed":
                cleared = set(event.get("watched_cleared", []))
                if self.target_channel in cleared:
                    self.restore_detected_at = time.time()
                    restore_dt = self.restore_detected_at - self.loss_detected_at
                    total_dt = self.restore_detected_at - self.test_started_at
                    self.complete(
                        passed=True,
                        message=f"CH{self.target_channel} loss and restore test passed",
                        total_duration=total_dt,
                    )
                    log(f"PASS: CH{self.target_channel} video restored in {restore_dt:.2f}s")
                    log(f"TEST RESULT: PASS - CH{self.target_channel} loss and restore test passed")

    def check_timeouts(self) -> None:
        now = time.time()

        if self.phase == "waiting_for_loss":
            elapsed = now - self.test_started_at
            if elapsed > self.loss_timeout:
                self.complete(
                    passed=False,
                    message=f"Timeout waiting for CH{self.target_channel} video loss start",
                    total_duration=elapsed,
                )
                log(f"TEST RESULT: FAIL - Timeout waiting for CH{self.target_channel} video loss start")

        elif self.phase == "waiting_for_restore":
            elapsed = now - self.loss_detected_at
            total = now - self.test_started_at
            if elapsed > self.restore_timeout:
                self.complete(
                    passed=False,
                    message=f"Timeout waiting for CH{self.target_channel} video restore",
                    total_duration=total,
                )
                log(f"TEST RESULT: FAIL - Timeout waiting for CH{self.target_channel} video restore")

    def complete(self, passed: bool, message: str, total_duration: float) -> None:
        if self.completed:
            return

        self.completed = True
        self.result = {
            "ts": now_iso(),
            "channel": self.target_channel,
            "passed": passed,
            "message": message,
            "loss_timeout": self.loss_timeout,
            "restore_timeout": self.restore_timeout,
            "test_started_at": self.test_started_at,
            "loss_detected_at": self.loss_detected_at,
            "restore_detected_at": self.restore_detected_at,
            "total_duration_sec": round(total_duration, 3),
        }

        write_result(self.result)


def drain_queue(channel) -> None:
    drained = 0
    while True:
        method, properties, body = channel.basic_get(queue=RMQ_QUEUE, auto_ack=False)
        if body is None:
            break
        channel.basic_ack(delivery_tag=method.delivery_tag)
        drained += 1
    log(f"Drained {drained} stale message(s) from queue before next test")


def run_single_test(channel_obj, test: ChannelTest) -> dict:
    while not test.completed:
        method, properties, body = channel_obj.basic_get(queue=RMQ_QUEUE, auto_ack=False)

        if body is not None:
            try:
                msg = json.loads(body.decode("utf-8"))
                if DEBUG_RAW:
                    print("RAW:", msg, flush=True)
                test.handle_message(msg)
            except Exception as e:
                log(f"BAD MESSAGE: {e}")
            finally:
                channel_obj.basic_ack(delivery_tag=method.delivery_tag)
        else:
            time.sleep(0.2)

        test.check_timeouts()

    return test.result


def main():
    log(f"Connecting to RabbitMQ at {RMQ_HOST}:{RMQ_PORT}, queue={RMQ_QUEUE}")
    conn, ch = connect_rabbit()
    log("Consumer connected")
    log(f"TEST PLAN: channels {TEST_CHANNELS}")

    results = []

    try:
        for idx, test_channel in enumerate(TEST_CHANNELS, start=1):
            drain_queue(ch)

            log("")
            log(f"=== TEST {idx}/{len(TEST_CHANNELS)}: CH{test_channel} ===")
            log(f"TEST START: CH{test_channel} video loss test")
            log(f"WAITING: unplug or trigger video loss on CH{test_channel}")

            test = ChannelTest(
                target_channel=test_channel,
                loss_timeout=LOSS_TIMEOUT,
                restore_timeout=RESTORE_TIMEOUT,
            )

            result = run_single_test(ch, test)
            results.append(result)

            log(f"COMPLETED: CH{test_channel} -> {'PASS' if result['passed'] else 'FAIL'}")

        passed = sum(1 for r in results if r["passed"])
        failed = len(results) - passed

        log("")
        log("=== TEST SUMMARY ===")
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            log(f"{status}: CH{r['channel']} - {r['message']}")

        log(f"SUMMARY: {passed} passed, {failed} failed, total {len(results)}")

    finally:
        conn.close()

    if any(not r["passed"] for r in results):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
