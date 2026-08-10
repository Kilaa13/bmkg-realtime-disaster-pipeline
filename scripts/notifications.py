import os
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram_msg(message: str):
    """Mengirim pesan teks ke Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram credentials not configured.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"[ALERT ERROR] Gagal mengirim pesan Telegram: {e}")

def on_failure_callback(context):
    """Callback otomatis saat ada task Airflow yang FAILED."""
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    alert_msg = (
        f"❌ *AIRFLOW PIPELINE FAILED*\n\n"
        f"📌 *DAG:* `{dag_id}`\n"
        f"⚙️ *Task:* `{task_id}`\n"
        f"⏰ *Waktu:* `{execution_date}`\n"
        f"💥 *Error:* `{exception}`"
    )
    send_telegram_msg(alert_msg)