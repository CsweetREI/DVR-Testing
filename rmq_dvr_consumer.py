import json
import sqlite3
import time
from typing import Any

import pika

RABBITMQ_HOST = "192.168.0.150"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "/"
RABBITMQ_USERNAME = "dvrtest"
RABBITMQ_PASSWORD = "Reiadmin123"
RABBITMQ_QUEUE = "dvr_events"
USE_TLS = False

DB_PATH = "/home/dvr-ai/dvr_events.sqlite"


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_rmq_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            routing_key TEXT,
            exchange_name TEXT,
            payload TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS normalized_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            channel INTEGER,
            state TEXT,
            endpoint TEXT,
            payload TEXT NOT NULL
        )
        """
    )

    conn.commit()


def safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def extract_channel(payload_obj: Any) -> int | None:
    if isinstance(payload_obj, dict):
        for key in ("Channel", "channel", "ch", "camera"):
            if key in payload_obj:
                try:
                    return int(payload_obj[key])
                except Exception:
                    return None

        for value in payload_obj.values():
            channel = extract_channel(value)
            if channel is not None:
                return channel

    elif isinstance(payload_obj, list):
        for item in payload_obj:
            channel = extract_channel(item)
            if channel is not None:
                return channel

    return None


def classify_event(payload_text: str, payload_obj: Any) -> tuple[str, int | None, str | None]:
    channel = extract_channel(payload_obj)

    if isinstance(payload_obj, dict):
        endpoint = payload_obj.get("endpoint", "")
        event = payload_obj.get("event", {})
        data = event.get("data", {}) if isinstance(event, dict) else {}

        if endpoint == "/normalized/event" and isinstance(data, dict):
            event_type = data.get("event_type", "normalized_event")
            state = data.get("state")
            channel = data.get("channel", channel)
            return event_type, channel, state

        if endpoint == "/devapi/v1/basic/voilationinfo":
            if isinstance(data, dict) and data.get("savChMask") == 0:
                return "violation_save_disabled", channel, "active"
            return "violation_info", channel, None

        if endpoint == "/devapi/v1/basic/serverinfo":
            return "server_info", channel, None

        if endpoint == "/devapi/v1/basic/systemtime":
            return "system_time", channel, None

    text = payload_text.lower()

    if 'eventid":"vl"' in text or 'eventid\\":\\"vl\\"' in text:
        return "video_loss", channel, "start"

    if "video loss" in text:
        return "video_loss", channel, "start"

    if "vl_alarm" in text and "stop" in text:
        return "video_loss", channel, "stop"

    if "main record stop" in text:
        return "record_stop", channel, "stop"

    if "main record start" in text:
        return "record_start", channel, "start"

    if "alarm" in text:
        return "alarm", channel, None

    return "raw_message", channel, None


def store_raw(conn: sqlite3.Connection, routing_key: str, exchange_name: str, payload_text: str) -> None:
    conn.execute(
        """
        INSERT INTO raw_rmq_events (ts, routing_key, exchange_name, payload)
        VALUES (?, ?, ?, ?)
        """,
        (time.time(), routing_key, exchange_name, payload_text),
    )


def store_normalized(
    conn: sqlite3.Connection,
    event_type: str,
    channel: int | None,
    state: str | None,
    payload_text: str,
) -> None:
    now = time.time()

    conn.execute(
        """
        INSERT INTO normalized_events (ts, source, event_type, channel, state, endpoint, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            "rabbitmq",
            event_type,
            channel,
            state,
            "/rabbitmq/event",
            payload_text,
        ),
    )

    wrapped_payload = {
        "ts": now,
        "source": "rabbitmq",
        "endpoint": "/rabbitmq/event",
        "event": {
            "data": {
                "event_type": event_type,
                "channel": channel,
                "state": state,
            },
            "raw": payload_text,
            "errorcode": 200,
        },
    }

    conn.execute(
        """
        INSERT INTO events (ts, endpoint, payload)
        VALUES (?, ?, ?)
        """,
        (now, "/rabbitmq/event", json.dumps(wrapped_payload)),
    )


def on_message(ch, method, properties, body, conn: sqlite3.Connection) -> None:
    try:
        payload_text = body.decode("utf-8", errors="replace")
    except Exception:
        payload_text = repr(body)

    payload_obj = safe_json_loads(payload_text)

    store_raw(conn, method.routing_key, method.exchange, payload_text)

    event_type, channel, state = classify_event(payload_text, payload_obj)
    store_normalized(conn, event_type, channel, state, payload_text)

    conn.commit()

    print(
        f"[RMQ] event_type={event_type} channel={channel} "
        f"state={state} routing_key={method.routing_key}"
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def main() -> None:
    db_conn = sqlite3.connect(DB_PATH)
    init_db(db_conn)

    while True:
        try:
            connection = make_connection()
            channel = connection.channel()
            channel.basic_qos(prefetch_count=20)
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

            print(f"[INIT] Connected to RabbitMQ. Queue={RABBITMQ_QUEUE}")

            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=lambda ch, method, properties, body: on_message(
                    ch, method, properties, body, db_conn
                ),
            )

            channel.start_consuming()

        except KeyboardInterrupt:
            print("\n[STOP] Exiting.")
            try:
                db_conn.close()
            except Exception:
                pass
            return

        except Exception as e:
            print(f"[ERROR] RabbitMQ connection/consume failed: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()