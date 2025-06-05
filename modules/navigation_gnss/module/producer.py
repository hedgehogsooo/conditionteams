import os
import json
import threading
import multiprocessing

import random
from uuid import uuid4
import time
from time import sleep

from confluent_kafka import Producer

_requests_queue: multiprocessing.Queue = None

STATUS_PATH: str = "/shared/emulation_status"
COORDS_PATH: str = "/shared/coords"
MODULE_NAME: str = os.getenv("MODULE_NAME")

def read_coords(error):
    """Читает 2D-координаты (широта, долгота)."""
    with open(COORDS_PATH, "a+", encoding="utf-8") as file:
        pass  

    with open(COORDS_PATH, "r", encoding="utf-8") as file:
        content = file.read().strip()
        coords = [x.strip() for x in content.split(",") if x.strip()]

    # Проверяем, что координат ровно две
    if len(coords) != 2:
        return error

    try:
        coords = list(map(float, coords))
    except ValueError:
        return error

    print(f"[GNSS_DEBUG] Read coords: {coords}")
    return [coords[i] + error[i] for i in range(2)] 


def get_emulation_status() -> bool:
    try:
        with open(STATUS_PATH, "r") as f:
            status = f.read().strip()
            return status == "1"
    except Exception as e:
        print(f"[GNSS_ERROR] Failed to read status file: {e}")
        return False

def generate_coordinates():

    while True:
        if not get_emulation_status():
            sleep(random.randint(5, 10))
            continue

        # Генерируем только две ошибки
        new_x: int = random.randint(-1, 1)
        new_y: int = random.randint(-1, 1)

        coords = read_coords((new_x, new_y))

        print("[GNSS_DEBUG] Coords:", coords)

        proceed_to_deliver(uuid4().__str__(), {
            "deliver_to": "integration",
            "operation": "set_gnss_coords",
            "set_gnss_coords": coords
        })

        sleep(random.randint(5, 10))



def proceed_to_deliver(id, details):
    details["id"] = id
    details["source"] = MODULE_NAME
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    threading.Thread(target=generate_coordinates).start()

    def delivery_callback(err, msg):
        if err:
            print("[error] Message failed delivery: {}".format(err))

    topic = "monitor"
    while True:
        event_details = requests_queue.get()
        producer.produce(
            topic,
            json.dumps(event_details),
            event_details["id"],
            callback=delivery_callback
        )

        producer.poll(10000)
        producer.flush()


def start_producer(args, config, requests_queue):
    print(f"{MODULE_NAME}_producer started")

    global _requests_queue

    _requests_queue = requests_queue
    threading.Thread(
        target=lambda: producer_job(args, config, requests_queue)
    ).start()