#!/usr/bin/env python3
import json
import os
import time
from datetime import datetime, timezone

import pika

RMQ_HOST = os.getenv("RMQ_HOST", "192.168.0.150")
RMQ_PORT = int(os.getenv("RMQ_PORT", "5672"))
RMQ_USER = os.getenv("RMQ_USER", "guest")
RMQ_PASS = os.getenv("RMQ_PASS", "guest")
RMQ_QUEUE = os.getenv("RMQ_QUEUE", "dvr_events")

LOG_FILE = os.getenv("VIDEO_MONITOR_LOG", "video_loss_monitor.jsonl")


def now():
    return datetime.now(timezone.utc).isoformat()


def log(msg):
    print(f"{now()} {msg}", flush=True)


def write_event(event):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


def connect():
    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    params = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        credentials=credentials,
        heartbeat=60
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=RMQ_QUEUE, durable=True)

    return connection, channel


def main():
    log(f"Connecting to RabbitMQ {RMQ_HOST}:{RMQ_PORT}")

    connection, channel = connect()

    log("Video Loss Monitor Started")

    active_losses = {}

    while True:
        method, properties, body = channel.basic_get(
            queue=RMQ_QUEUE,
            auto_ack=True
        )

        if body:
            try:
                msg = json.loads(body.decode())

                event = msg.get("event", {})
                event_type = event.get("event_type")

                if event_type != "video_loss_changed":
                    continue

                started = event.get("watched_started", [])
                cleared = event.get("watched_cleared", [])

                # Loss Started
                for ch in started:
                    if ch not in active_losses:
                        active_losses[ch] = time.time()

                        log(f"VIDEO LOSS STARTED CH{ch}")

                        write_event({
                            "ts": now(),
                            "channel": ch,
                            "event": "loss_start"
                        })

                # Loss Cleared
                for ch in cleared:
                    start = active_losses.pop(ch, None)

                    if start:
                        duration = round(time.time() - start, 2)

                        log(f"VIDEO LOSS CLEARED CH{ch} ({duration}s)")

                        write_event({
                            "ts": now(),
                            "channel": ch,
                            "event": "loss_cleared",
                            "duration": duration
                        })

            except Exception as e:
                log(f"ERROR: {e}")

        else:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
