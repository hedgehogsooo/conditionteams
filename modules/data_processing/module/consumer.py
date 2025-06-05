import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

# Временное хранилище для данных с разных сенсоров
data_cache = {
    "datacam": None,
    "data_lidar": None,
    "ir_datas": None
}


def set_camera(id, details):
    data_cache["datacam"] = details.get("datacam")
    print(f"[{MODULE_NAME}_DEBUG] Камера: {data_cache['datacam']}")
    # handle_correction_request(id)


def set_lidar(id, details):
    data_cache["data_lidar"] = details.get("data_lidar")
    print(f"[{MODULE_NAME}_DEBUG] Лидар: {data_cache['data_lidar']}")
    # handle_correction_request(id)


def set_ir(id, details):
    data_cache["ir_datas"] = details.get("ir_datas")
    print(f"[{MODULE_NAME}_DEBUG] ИК-датчики: {data_cache['ir_datas']}")
    # handle_correction_request(id)

def handle_correction_request(id, details):
    """Вызывается при получении команды на корректировку"""
    print(f"[{MODULE_NAME}_INFO] Получен запрос на 'корректировку управляющих команд' от модуля принятия решений")
    try_send_processed_data()

def try_send_processed_data():
    """Проверяет готовность всех сенсоров и отправляет обработанные данные"""
    if data_cache["datacam"] and data_cache["data_lidar"] and data_cache["ir_datas"]:
        processed = {
            "result": "Обработанные данные",
            "camers": data_cache["datacam"],
            "lidars": data_cache["data_lidar"],
            "ir_sensors": data_cache["ir_datas"]
        }
        proceed_to_deliver(str(uuid4()), {
            "deliver_to": "decision_making",
            "operation": "processed_datas",
            "processed_datas": processed
        })
        print(f"[{MODULE_NAME}_INFO] Отправлены обработанные даныне в модуль принятия решений")

        # Очистка
        data_cache["datacam"] = None
        data_cache["data_lidar"] = None
        data_cache["ir_datas"] = None


commands = {
    "data_camers": set_camera,
    "data_lidars": set_lidar,
    "data_ir_sensors": set_ir,
    "corection_comands": handle_correction_request
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
