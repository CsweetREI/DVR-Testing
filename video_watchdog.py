import json
import sqlite3
import time
from typing import Optional

import cv2

# ----------------------------
# CONFIG
# ----------------------------
DB_PATH = "/home/dvr-ai/dvr_events.sqlite"

# Put your real stream URLs here
CAMERAS = [
    {"channel": 1, "name": "cam1", "url": "rtsp://admin:10231981@162.190.128.40/channel1"},
    {"channel": 2, "name": "cam2", "url": "rtsp://admin:10231981@162.190.128.40/channel2"},
    
    # Add more channels if needed
]

LOSS_SECONDS = 10          # no valid frames for this long => video loss
RECONNECT_SECONDS = 2      # wait before reconnect attempt
LOOP_SLEEP = 0.2           # watchdog loop delay


# ----------------------------
# DB HELPERS
# ----------------------------
def ensure_events_table(conn: sqlite3.Connection) -> None:
    # Assumes your events table already exists.
    # This just verifies it.
    conn.execute("SELECT 1 FROM events LIMIT 1")


def insert_event(conn: sqlite3.Connection, endpoint: str, payload: dict) -> None:
    conn.execute(
        "INSERT INTO events (endpoint, payload) VALUES (?, ?)",
        (endpoint, json.dumps(payload)),
    )
    conn.commit()


# ----------------------------
# CAMERA WATCHER
# ----------------------------
class CameraState:
    def __init__(self, channel: int, name: str, url: str):
        self.channel = channel
        self.name = name
        self.url = url
        self.cap: Optional[cv2.VideoCapture] = None
        self.last_good_ts = 0.0
        self.loss_active = False
        self.last_open_attempt = 0.0

    def open(self) -> None:
        if self.cap is not None:
            self.cap.release()
        self.cap = cv2.VideoCapture(self.url)
        self.last_open_attempt = time.time()

    def close(self) -> None:
        if self.cap is not None:
            self.cap.release()
            self.cap = None


def make_payload(source: str, endpoint: str, channel: int, name: str, status: str, reason: str) -> dict:
    return {
        "ts": time.time(),
        "source": source,
        "endpoint": endpoint,
        "event": {
            "data": {
                "channel": channel,
                "camera_name": name,
                "status": status,
                "reason": reason,
            },
            "errorcode": 200,
        },
    }


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    ensure_events_table(conn)

    cameras = [CameraState(c["channel"], c["name"], c["url"]) for c in CAMERAS]

    for cam in cameras:
        cam.open()
        cam.last_good_ts = time.time()

    print("[INIT] Video watchdog started.")

    try:
        while True:
            now = time.time()

            for cam in cameras:
                if cam.cap is None:
                    if now - cam.last_open_attempt >= RECONNECT_SECONDS:
                        cam.open()
                    continue

                ok, frame = cam.cap.read()

                if ok and frame is not None and getattr(frame, "size", 0) > 0:
                    if cam.loss_active:
                        endpoint = "/local/video/restore"
                        payload = make_payload(
                            source="video_watchdog",
                            endpoint=endpoint,
                            channel=cam.channel,
                            name=cam.name,
                            status="restored",
                            reason="valid frames detected again",
                        )
                        insert_event(conn, endpoint, payload)
                        print(f"[RESTORE] channel={cam.channel} name={cam.name}")
                        cam.loss_active = False

                    cam.last_good_ts = now

                else:
                    elapsed = now - cam.last_good_ts

                    if elapsed >= LOSS_SECONDS and not cam.loss_active:
                        endpoint = "/local/video/loss"
                        payload = make_payload(
                            source="video_watchdog",
                            endpoint=endpoint,
                            channel=cam.channel,
                            name=cam.name,
                            status="lost",
                            reason=f"no valid frames for {LOSS_SECONDS} seconds",
                        )
                        insert_event(conn, endpoint, payload)
                        print(f"[LOSS] channel={cam.channel} name={cam.name}")
                        cam.loss_active = True

                    # Reconnect occasionally if stream looks bad
                    if now - cam.last_open_attempt >= RECONNECT_SECONDS:
                        cam.open()

            time.sleep(LOOP_SLEEP)

    except KeyboardInterrupt:
        print("\n[STOP] Video watchdog stopped.")
    finally:
        for cam in cameras:
            cam.close()
        conn.close()


if __name__ == "__main__":
    main()