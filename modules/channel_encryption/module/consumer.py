import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

def handle_route_sheet(id, details):
    filtered = {
        "data": details.get("data")
    }
    print(f"[{MODULE_NAME}] Получено сообщение: {json.dumps(filtered, ensure_ascii=False)}")

    data = details.get("data", {})
    encrypted_route = {
        "load_point": data.get("load_point"),
        "unload_point": data.get("unload_point"),
        "cargo_type": data.get("cargo_type")
    }

    message = {
        "id": str(uuid4()),
        "operation": "receive_route_sheets", 
        "deliver_to": "task_orchestrator",
        "encrypted_route": encrypted_route
    }

    proceed_to_deliver(message["id"], message)

def handle_encrypt_route(id, details):
    """
    Получает спланированный маршрут, шифрует (символически),
    и пересылает в блок connection.
    """
    route_info = details.get("route_info")
    if not route_info:
        print(f"[{MODULE_NAME}_WARNING] Нет route_info в сообщении для шифрования")
        return

    print(f"[{MODULE_NAME}_INFO] Получен спланированный маршрут для шифрования")

    encrypted_data = route_info 

    message = {
        "id": str(uuid4()),
        "operation": "encrypted_route_ready",
        "deliver_to": "connection",
        "encrypted_route": encrypted_data
    }

    proceed_to_deliver(message["id"], message)

def handle_route_permission(id, details):
    """
    Получает ответ о разрешении маршрута от connection
    и пересылает его в оркестратор.
    """
    approved = details.get("approved")
    print(f"[{MODULE_NAME}_INFO] Получено разрешение от ЦОДД: approved={approved}")

    message = {
        "id": str(uuid4()),
        "operation": "route_permission",
        "deliver_to": "task_orchestrator",
        "approved": approved,
    }
    proceed_to_deliver(message["id"], message)


def handle_mission_completed(id, details):
    """
    Обработка команды Mission_completed_returned_to_base:
    пересылает её в блок connection.
    """
    message_text = details.get("message", "Вернулись на базу. Миссия завершена")
    print(f"[{MODULE_NAME}_INFO] Завершение миссии: {message_text}")

    message = {
        "id": str(uuid4()),
        "operation": "Mission_completed_returned_to_base",
        "deliver_to": "connection",
        "message": message_text,
    }

    proceed_to_deliver(message["id"], message)


commands = {
    "receive_route_sheet": handle_route_sheet,
    "encrypt_route": handle_encrypt_route,
    "route_permission": handle_route_permission,
    "Mission_completed_returned_to_base": handle_mission_completed,
}

def handle_event(id, details_str):
    details = json.loads(details_str)

    source = details.get("source")
    deliver_to = details.get("deliver_to")
    operation = details.get("operation")

    # print(f"[{MODULE_NAME}_INFO] id={id} {source}->{deliver_to}: {operation}")

    command = commands.get(operation)
    if command:
        command(id, details)
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
                pass
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    id = msg.key().decode('utf-8')
                    details_str = msg.value().decode('utf-8')
                    handle_event(id, details_str)
                except Exception as e:
                    print(f"[error] Malformed event received from " \
                          f"topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config)).start()
