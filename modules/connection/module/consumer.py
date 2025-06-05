import os
import json
import threading
import multiprocessing
from uuid import uuid4

import requests
from confluent_kafka import Consumer, OFFSET_BEGINNING
from .producer import proceed_to_deliver  

MODULE_NAME = os.getenv("MODULE_NAME", "connection")
TSODD_URL = os.getenv("TSODD_URL", "http://tsodd:6003")

_response_queue: multiprocessing.Queue = None


def send_route_permission_to_channel_encryption(approved: bool):
    """
    Отправляет сообщение о разрешении в модуль channel_encryption.
    """
    message = {
        "id": str(uuid4()),
        "operation": "route_permission",
        "deliver_to": "channel_encryption",
        "source": MODULE_NAME,
        "approved": approved
    }

    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Отправлено в channel_encryption: route_permission (approved={approved})")


def handle_encrypted_route_ready(id, details):
    """
    Обрабатывает операцию 'encrypted_route_ready': отправляет маршрут в ЦОДД.
    После получения ответа отправляет разрешение в channel_encryption и task_orchestrator.
    """
    route_info = details.get("encrypted_route", {})

    if not route_info:
        print(f"[{MODULE_NAME}_WARNING] Отсутствует encrypted_route в details")
        return

    print(f"[{MODULE_NAME}_INFO] Получен спланированный маршрут для отправки в ЦОДД: {route_info}")

    try:

        response = requests.post(
            f"{TSODD_URL}/process_route",
            json={"route_sheet": route_info},
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if response.status_code == 200:
            result = response.json()
            approved = result.get("approved", False)
            print(f"[{MODULE_NAME}_INFO] Ответ ЦОДД: {result}")

            send_route_permission_to_channel_encryption(approved)

        else:
            print(f"[{MODULE_NAME}_ERROR] ЦОДД вернул статус {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[{MODULE_NAME}_ERROR] Ошибка при отправке маршрута в ЦОДД: {e}")


def handle_route_permission(id, details):
    """
    Обрабатывает операцию 'route_permission': пересылает разрешение в оба модуля.
    """
    approved = details.get("approved")
    print(f"[{MODULE_NAME}_INFO] Получено разрешение от ЦОДД: approved={approved}")

    send_route_permission_to_channel_encryption(approved)


def handle_mission_completed(id, details):
    """
    Обрабатывает Mission_completed_returned_to_base:
    отправляет POST в planning_system на /mission_complete
    """
    print(f"[{MODULE_NAME}_INFO] Получено сообщение о завершении миссии")
    try:
        response = requests.post(
            "http://planningsystem:6004/mission_complete",
            json=details,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if response.ok:
            print(f"[{MODULE_NAME}_INFO] Уведомление о завершении миссии отправлено в planning_system")
        else:
            print(f"[{MODULE_NAME}_ERROR] Ошибка от planning_system: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[{MODULE_NAME}_ERROR] Ошибка при уведомлении planning_system: {e}")



def handle_event(id, details_str):
    """
    Диспетчер операций: вызывает соответствующие функции-обработчики.
    """
    details = json.loads(details_str)
    operation = details.get("operation")

    if operation == "encrypted_route_ready":
        handle_encrypted_route_ready(id, details)
    elif operation == "route_permission":
        handle_route_permission(id, details)
    elif operation == "Mission_completed_returned_to_base":
        handle_mission_completed(id, details)
    else:
        print(f"[{MODULE_NAME}_WARNING] Неизвестная операция: {operation}")


def consumer_job(args, config):
    consumer = Consumer(config)

    def reset_offset(verifier_consumer, partitions):
        if not args.reset:
            return
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        verifier_consumer.assign(partitions)

    topic = MODULE_NAME
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{MODULE_NAME}_ERROR] {msg.error()}")
                continue
            try:
                id = msg.key().decode("utf-8")
                details_str = msg.value().decode("utf-8")
                handle_event(id, details_str)
            except Exception as e:
                print(f"[{MODULE_NAME}_ERROR] Malformed event: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config, response_queue):
    global _response_queue
    _response_queue = response_queue

    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config)).start()
