import json
import sqlite3
import time
from pathlib import Path

import requests

DB_PATH = "/home/dvr-ai/dvr_events.sqlite"
LLM_URL = "http://localhost:8000/api/chat"
MODEL = "qwen2.5-instruct:1.5b"
POLL_SECONDS = 10
MAX_EVENTS = 5
STATE_FILE = Path("/tmp/dvr_agent_last_id.txt")


def load_last_id() -> int:
    if STATE_FILE.exists():
        try:
            return int(STATE_FILE.read_text().strip())
        except Exception:
            return 0
    return 0


def save_last_id(last_id: int) -> None:
    STATE_FILE.write_text(str(last_id))


def call_llm(system_prompt: str, user_prompt: str) -> str:
    payload = {
        "model": MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    r = requests.post(LLM_URL, json=payload, timeout=180)
    r.raise_for_status()
    data = r.json()
    return data["message"]["content"]


def fetch_new_events(db_path: str, last_id: int, limit: int = MAX_EVENTS) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, endpoint, payload
        FROM events
        WHERE id > ?
          AND endpoint NOT LIKE '%systemtime%'
        ORDER BY id DESC
        LIMIT ?
        """,
        (last_id, limit * 4),
    )
    rows = cur.fetchall()
    conn.close()

    events = []
    for row in rows:
        payload_preview = row["payload"] or ""
        if len(payload_preview) > 500:
            payload_preview = payload_preview[:500] + "...[truncated]"

        text = f"{row['endpoint']} {payload_preview}".lower()
        priority = 0

        if "/rabbitmq/event" in text and "video_loss" in text:
            priority = 120
        elif "/rabbitmq/event" in text and "violation_save_disabled" in text:
            priority = 100
        elif "/normalized/event" in text and "violation_save_disabled" in text:
            priority = 90
        elif "savchmask" in text and "0" in text:
            priority = 80
        elif "eventid\":\"vl\"" in text or "eventid\\\":\\\"vl\\\"" in text:
            priority = 120
        elif "video loss" in text:
            priority = 110
        elif "main record stop" in text:
            priority = 70
        elif "serverinfo" in text:
            priority = 20
        elif "systemtime" in text:
            priority = 10

        events.append(
            {
                "id": row["id"],
                "endpoint": row["endpoint"],
                "payload": payload_preview,
                "priority": priority,
            }
        )

    events.sort(key=lambda e: (-e["priority"], -e["id"]))
    return events[:limit]


def diagnose_events(events: list[dict]) -> str:
    system_prompt = (
        "You are a DVR diagnostic assistant. "
        "Respond in English only. "
        "Do not use Chinese. "
        "Do not use any non-English headings. "
        "Return only valid JSON with exactly these keys: "
        "summary, likely_issue, evidence, next_step. "
        "Each value must be a short English string."
    )

    user_prompt = (
        "Analyze these DVR events. "
        "Return JSON only. English only.\n\n"
        f"{json.dumps(events, indent=2)}"
    )

    raw = call_llm(system_prompt, user_prompt).strip()

    try:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1:
            raw = raw[start:end + 1]
        data = json.loads(raw)

        return (
            f"Summary:\n{data.get('summary', '')}\n\n"
            f"Likely issue:\n{data.get('likely_issue', '')}\n\n"
            f"Evidence:\n{data.get('evidence', '')}\n\n"
            f"Next step:\n{data.get('next_step', '')}"
        )
    except Exception:
        return f"[RAW MODEL OUTPUT]\n{raw}"

    user_prompt = (
        "Respond in English only. \n\n"
        f"Analyze these DVR events:\n{json.dumps(events, indent=2)}"
    )
    return call_llm(system_prompt, user_prompt)
    

def choose_action(diagnosis: str) -> dict:
    text = diagnosis.lower()

    if "video loss" in text or ("eventid" in text and "vl" in text):
        return {
            "action": "query_sqlite",
            "reason": "Need correlated rows around the suspected video loss event.",
            "command": (
                "sqlite3 /home/dvr-ai/dvr_events.sqlite "
                "\"select id, endpoint, substr(payload,1,500) "
                "from events order by id desc limit 20;\""
            ),
        }

    if "savchmask" in text or "save channels" in text or "violation" in text:
        return {
            "action": "query_sqlite",
            "reason": "Need related violation rows for confirmation.",
            "command": (
                "sqlite3 /home/dvr-ai/dvr_events.sqlite "
                "\"select id, endpoint, substr(payload,1,500) "
                "from events where endpoint like '%voilationinfo%' "
                "or payload like '%savChMask%' "
                "order by id desc limit 20;\""
            ),
        }

    return {
        "action": "none",
        "reason": "No follow-up action selected.",
        "command": "",
    }


def execute_action(action_obj: dict) -> str:
    action = action_obj.get("action", "none")
    command = action_obj.get("command", "")

    if action == "none" or not command.strip():
        return f"No action executed. Reason: {action_obj.get('reason', 'none')}"

    if action != "query_sqlite":
        return f"Blocked unsupported action: {action}"

    import subprocess

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        return output[:3000]
    except Exception as e:
        return f"Error executing action: {e}"


def main() -> None:
    last_id = load_last_id()
    print(f"[INIT] Starting agent loop. Last seen event id: {last_id}")

    while True:
        try:
            events = fetch_new_events(DB_PATH, last_id, MAX_EVENTS)

            if not events:
                print("[INFO] No new events.")
                time.sleep(POLL_SECONDS)
                continue

            last_id = max(event["id"] for event in events)
            save_last_id(last_id)

            print(f"\n[LOG AGENT] Found {len(events)} new events. Latest id={last_id}")

            diagnosis = diagnose_events(events)
            print("\n[DIAGNOSTIC AGENT]")
            print(diagnosis)

            action_obj = choose_action(diagnosis)
            print("\n[ACTION AGENT]")
            print(json.dumps(action_obj, indent=2))

            action_result = execute_action(action_obj)
            print("\n[EXECUTION RESULT]")
            print(action_result)

        except requests.RequestException as e:
            print(f"[ERROR] LLM request failed: {e}")
        except sqlite3.Error as e:
            print(f"[ERROR] SQLite failed: {e}")
        except KeyboardInterrupt:
            print("\n[STOP] Exiting.")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected failure: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()