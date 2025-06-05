import os
import time
import json
import threading
import multiprocessing

from uuid import uuid4
from flask import Flask, request, jsonify, abort
from werkzeug.exceptions import HTTPException
from .producer import proceed_to_deliver

# Константы
HOST: str = "0.0.0.0"
PORT: int = int(os.getenv("CONNECTION_PORT", 6005))  
MODULE_NAME: str = os.getenv("MODULE_NAME", "connection")
CONTENT_HEADER = {"Content-Type": "application/json"}


# Очереди задач и ответов
_requests_queue: multiprocessing.Queue = None
_response_queue: multiprocessing.Queue = None

app = Flask(__name__)


def send_to_channel_encryption(details: dict):
    """
    Отправляет сообщение в Kafka в модуль channel_encryption.
    """
    if not details:
        abort(400)

    details["deliver_to"] = "channel_encryption"
    details["source"] = MODULE_NAME
    details["id"] = str(uuid4())

    try:
        proceed_to_deliver(details["id"], details)
    except Exception as e:
        print(f"[{MODULE_NAME} ERROR] Не удалось отправить в Kafka: {e}")
        abort(500)


@app.route("/receive_route_sheet", methods=["POST"])
def receive_route_sheet():
    """
    Принимает маршрутный лист от planningSystem по HTTP,
    упаковывает и пересылает в Kafka в channel_encryption.
    """
    try:
        content = request.json
    except Exception as e:
        print(f"[{MODULE_NAME} ERROR] неверный JSON: {e}")
        abort(400)

    if not content:
        return jsonify({"error": "Требуется корректный маршрутный лист"}), 400

    print(f"[{MODULE_NAME}] Получен маршрутный лист: {json.dumps(content, ensure_ascii=False)}")

    details_to_send = {
        "operation": "receive_route_sheet",
        "data": content
    }
    send_to_channel_encryption(details_to_send)

    return jsonify({
        "operation": "receive_route_sheet",
        "status": "received",
        "received_at": int(time.time())
    }), 200


@app.route("/route_permission", methods=["POST"])
def route_permission():
    """
    Эндпоинт для получения от ЦОДД разрешения на выезд.
    Ожидает JSON вида: {"approved": true/false }
    """
    try:
        data = request.json
    except Exception as e:
        print(f"[{MODULE_NAME} ERROR] неверный JSON в /route_permission: {e}")
        abort(400)

    if not data or "approved" not in data:
        return jsonify({"error": "Требуется поле 'approved'"}), 400

    approved = data["approved"]

    permission_message = {
        "operation": "route_permission",
        "approved": approved,
    }
    send_to_channel_encryption(permission_message)

    return jsonify({"status": "route_permission processed"}), 200


@app.errorhandler(HTTPException)
def handle_exception(e):
    return jsonify({
        "status": e.code,
        "name": e.name,
    }), e.code


def start_web(requests_queue, response_queue):
    global _requests_queue
    global _response_queue

    _requests_queue = requests_queue
    _response_queue = response_queue

    threading.Thread(target=lambda: app.run(
        host=HOST, port=PORT, debug=True, use_reloader=False
    )).start()
