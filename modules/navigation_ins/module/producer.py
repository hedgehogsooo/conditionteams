import os
import json
import threading
import multiprocessing
import random
from uuid import uuid4
import time
from confluent_kafka import Producer

_requests_queue: multiprocessing.Queue = None

COORDS_PATH: str = "/shared/coords"
MODULE_NAME: str = os.getenv("MODULE_NAME")
STATUS_PATH: str = "/shared/emulation_status"


def read_coords(error):
    """
    Читает 2D-координаты (широта, долгота) из файла COORDS_PATH.
    Возвращает базовые coords + error, или просто error, если парсинг неуспешен.
    """
    try:
        with open(COORDS_PATH, "r", encoding="utf-8") as file:
            content = file.read().strip()
            parts = content.split(",")
            if len(parts) != 2:
                raise ValueError("Invalid coord format")
            base = list(map(float, parts))
            noisy = [round(base[i] + error[i], 6) for i in range(2)]
            print(f"[INS_DEBUG] Base coords: {base}, Error: {error}, Result: {noisy}")
            return noisy
    except Exception as e:
        print(f"[INS_ERROR] Failed to read/parse coords file: {e}")
        return error


def get_emulation_status() -> bool:
    try:
        with open(STATUS_PATH, "r", encoding="utf-8") as f:
            return f.read().strip() == "1"
    except Exception as e:
        print(f"[INS_ERROR] Failed to read status file: {e}")
        return False


def generate_coordinates():
    """
    Постоянно генерирует INS-координаты с шумом, читая текущие coords каждую итерацию.
    """
    while True:
        if not get_emulation_status():
            time.sleep(random.randint(5, 10))
            continue

        new_x = random.randint(-1, 1)
        new_y = random.randint(-1, 1)

        coords = read_coords((new_x, new_y))

        print(f"[INS_DEBUG] Coords: {coords}")

        proceed_to_deliver(uuid4().__str__(), {
            "deliver_to": "integration",
            "operation": "set_ins_coords",
            "set_ins_coords": coords
        })

        time.sleep(random.randint(5, 10))


def proceed_to_deliver(id, details):
    details["id"] = id
    details["source"] = MODULE_NAME
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    threading.Thread(target=generate_coordinates).start()

    def delivery_callback(err, msg):
        if err:
            print(f"[{MODULE_NAME}_ERROR] Message failed delivery: {err}")

    topic = "monitor"
    while True:
        event_details = requests_queue.get()
        producer.produce(
            topic,
            json.dumps(event_details),
            event_details["id"],
            callback=delivery_callback
        )
        producer.poll(0)
        producer.flush()


def start_producer(args, config, requests_queue):
    print(f"{MODULE_NAME}_producer started")

    global _requests_queue
    _requests_queue = requests_queue

    threading.Thread(
        target=lambda: producer_job(args, config, requests_queue)
    ).start()
