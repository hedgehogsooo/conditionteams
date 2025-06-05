import os
import json
import requests
import threading

from uuid import uuid4
from time import sleep
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
INIT_PATH: str = "/shared/init"

data_cache = {
    "ins": None,
    "gnss": None,
}


def try_send_or_log_avg():
    """
    Если в data_cache есть и INS-, и GNSS-координаты,
    вычисляем среднее, логируем его и сбрасываем кэш.
    """
    ins = data_cache["ins"]
    gnss = data_cache["gnss"]

    if ins is None or gnss is None:
        return  

    avg_coords = [(ins[i] + gnss[i]) / 2 for i in range(2)]
    print(f"[{MODULE_NAME}_DEBUG] Средние координаты: {avg_coords}")

    proceed_to_deliver(str(uuid4()), {
        "deliver_to": "emergency_unit",
        "operation": "set_coords",
        "coords": avg_coords
    })
    proceed_to_deliver(str(uuid4()), {
        "deliver_to": "route_planner",
        "operation": "set_coords",
        "coords": avg_coords
    })

    data_cache["ins"] = None
    data_cache["gnss"] = None


def set_ins_coords(_id, details: dict):

    coords = details.get("set_ins_coords")
    if coords is None:
        print(f"[{MODULE_NAME}_WARNING] В set_ins_coords пришёл пустой coords")
        return

    print(f"[{MODULE_NAME}_INFO] Получены INS-координаты: {coords}")
    data_cache["ins"] = coords
    try_send_or_log_avg()


def set_gnss_coords(_id, details: dict):
    coords = details.get("set_gnss_coords")
    if coords is None:
        print(f"[{MODULE_NAME}_WARNING] В set_gnss_coords пришёл пустой coords")
        return


    print(f"[{MODULE_NAME}_INFO] Получены GNSS-координаты: {coords}")
    data_cache["gnss"] = coords
    try_send_or_log_avg()


commands = {
    "set_ins_coords": set_ins_coords,
    "set_gnss_coords": set_gnss_coords,
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