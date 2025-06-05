import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")
STATUS_PATH: str = "/shared/emulation_status"


def check_system_state(id, details):
    """
    Читает STATUS_PATH:
    - если там "1", сбрасывает в "0",
    - в любом случае отправляет 'systems_ok' обратно в task_orchestrator.
    """
    try:
        with open(STATUS_PATH, "r") as f:
            status = f.read().strip()
    except Exception as e:
        print(f"[{MODULE_NAME}_ERROR] Не удалось прочитать {STATUS_PATH}: {e}")
        status = "0"

    if status == "1":
        # Сбросить файл в "0"
        try:
            with open(STATUS_PATH, "w") as f:
                f.write("0")
            print(f"[{MODULE_NAME}_INFO] STATUS_PATH сброшен в 0 после проверки")
        except Exception as e:
            print(f"[{MODULE_NAME}_ERROR] Не удалось обновить {STATUS_PATH}: {e}")

    # Отправить сообщение обратно в task_orchestrator, что все системы в порядке
    msg_ok = {
        "id": str(uuid4()),
        "operation": "systems_ok",
        "deliver_to": "task_orchestrator",
        "message": "все системы в норме"
    }
    proceed_to_deliver(msg_ok["id"], msg_ok)
    print(f"[{MODULE_NAME}_INFO] Отправлено все системы в норме в оркестратор")


commands = {
    "check_system_state": check_system_state,
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
