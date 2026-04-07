#!/usr/bin/env python3
import json
import os
import sys
import time

import pika

RMQ_HOST = os.getenv("RMQ_HOST", "192.168.0.150")
RMQ_PORT = int(os.getenv("RMQ_PORT", "5672"))
RMQ_USER = os.getenv("RMQ_USER", "guest")
RMQ_PASS = os.getenv("RMQ_PASS", "guest")
RMQ_QUEUE = os.getenv("RMQ_QUEUE", "dvr_events")

WATCH_CHANNELS = {1, 2, 7, 8}


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


def handle_event(body: bytes) -> None:
    try:
        msg = json.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"BAD MESSAGE: {e}")
        return

    event = msg.get("event", {})
    event_type = event.get("event_type")

    if event_type == "video_loss_snapshot":
        channels = event.get("channels", [])
        watched = [ch for ch in channels if ch in WATCH_CHANNELS]
        if watched:
            print(f"SNAPSHOT: active video loss on watched channels {watched}")
        else:
            print("SNAPSHOT: no watched channels currently in video loss")
        return

    if event_type == "video_loss_changed":
        started = event.get("watched_started", [])
        cleared = event.get("watched_cleared", [])

        for ch in started:
            print(f"ALERT: CH{ch} VIDEO LOSS STARTED")

        for ch in cleared:
            print(f"ALERT: CH{ch} VIDEO LOSS CLEARED")

        if not started and not cleared:
            all_channels = event.get("channels", [])
            print(f"INFO: video loss changed, but not on watched channels. Current channels: {all_channels}")
        return

    # Optional: keep this if you want to see other event types
    if event_type:
        print(f"IGNORED EVENT: {event_type}")


def main():
    print(f"Connecting to RabbitMQ at {RMQ_HOST}:{RMQ_PORT}, queue={RMQ_QUEUE}")
    conn, ch = connect_rabbit()
    print("Consumer connected. Waiting for messages...")

    def callback(channel, method, properties, body):
        handle_event(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=RMQ_QUEUE, on_message_callback=callback)

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        try:
            ch.stop_consuming()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
