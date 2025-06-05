import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

data_cache = {
    "set_coords": None,
    "cardo_loades": None,
    "route_sheet":None,
}

def set_cargo_loades(id,details):
    data_cache['cardo_loades'] = details.get("cargo")
    print(f"[{MODULE_NAME}_DEBUG] Получено состояние груза: {data_cache['cardo_loades']}")
    data_cache["cardo_loades"] = None


def set_coordinates(id, details):
    data_cache["set_coords"] = details.get("coords")
    print(f"[{MODULE_NAME}_DEBUG] Получены координаты: {data_cache['set_coords']}")
    data_cache["set_coords"] = None
#     try_send_processed_data(id)

def set_route_sheet(id, details):
    data_cache['route_sheet'] = details.get("route_info")
    print(f"[{MODULE_NAME}_DEBUG] Получен маршрутный лист: {data_cache['route_sheet']}")
    data_cache["route_sheet"] = None

def set_emergency_alert(id, details):
    """
    Обрабатывает сообщение 'emergency_alert' и пересылает его в emergency_speed_reduction.
    """
    alert_message = details.get("message")
    print(f"[{MODULE_NAME}_INFO] Получено оповещение об аварийной остановке: {alert_message}")

    msg_to_speed = {
        "id": str(uuid4()),
        "operation": "emergency_speed_reduction",
        "deliver_to": "emergency_speed_reduction",
        "message": alert_message
    }
    proceed_to_deliver(msg_to_speed["id"], msg_to_speed)
    print(f"[{MODULE_NAME}_INFO] Отправлена команда на аварийную остановку в блок аварийного торможения")


commands = {
    "set_coords":        set_coordinates,
    "cargo_load":        set_cargo_loades,
    "route_ready":       set_route_sheet,
    "emergency_alert":   set_emergency_alert,
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
