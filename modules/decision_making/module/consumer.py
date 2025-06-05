import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

data_cache = {
    "processed_datas": None,
    "comands" : "Управляющие команды"
}


def set_processed_data(id, details):
    data_cache["processed_datas"] = details.get("processed_datas")
    print(f"[{MODULE_NAME}_DEBUG] Получены обработанные данные: {data_cache['processed_datas']}")
    send_control_commands_back(id)


def send_control_commands_back(id):
    """
    Отправляет обратно в motion_control команду 'control_commands',
    используя уже принятые обработанные данные.
    """
    processed = data_cache["processed_datas"]
    if not processed:
        return
    comand = data_cache["comands"]
    message = {
        "id": str(uuid4()),
        "deliver_to": "motion_control",
        "operation": "control_commands",
        "control_commands": comand
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Отправлено управляющие команды в блок управления движением")

    # Очистка кеша
    data_cache["processed_datas"] = None


def handle_command_correction(id, details):
    """
    Обрабатывает сообщение 'корректировка управляющих команд' от motion_control
    и перенаправляет его в data_processing для получения новых обработанных данных.
    """
    print(f"[{MODULE_NAME}_INFO] Получен запрос на 'корректировку управляющих команд' от блока управление движением")

    message = {
        "id": str(uuid4()),
        "operation": "corection_comands",
        "deliver_to": "data_processing",
        "source": MODULE_NAME
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Отправлен запрос на 'корректировку управляющих команд' в блок обработки данных")


commands = {
    "processed_datas": set_processed_data,
    "corection_comands": handle_command_correction,
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
