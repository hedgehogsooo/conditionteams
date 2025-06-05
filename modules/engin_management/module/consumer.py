import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

# Временное хранилище для данных 
data_cache = {
    "control_commands": None,
}

def set_control_commands(id, details):
    data_cache["control_commands"] = details.get("control_commands")
    print(f"[{MODULE_NAME}_DEBUG] Получены : {data_cache['control_commands']}")
    try_send_processed_data(id)


def try_send_processed_data(id):
    if data_cache["control_commands"]:
        processed = {
            "result": "управляющих команд"
        }

        proceed_to_deliver(str(uuid4()), {
            "deliver_to": "acceleration_and_deceleration",
            "operation": "control_commands",
            "control_commands": processed
        })

        # Очистка
        data_cache["processed"] = None

commands = {
    "control_commands": set_control_commands,
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
