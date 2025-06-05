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

MODULE_NAME: str = os.getenv("MODULE_NAME")
STATUS_PATH: str = "/shared/emulation_status"


def get_emulation_status() -> bool:
    try:
        with open(STATUS_PATH, "r") as f:
            status = f.read().strip()
            return status == "1"
    except Exception as e:
        print(f"[CAMERS_ERROR] Failed to read status file: {e}")
        return False


def generate_data_camers():
    """Имитирует поведение камеры, установленной на автомобиле."""

    simulated_frames = [
        "Данные с камер"
    ]

    while True:
        if not get_emulation_status():
            # print("[CAMERS_INFO] Emulation status OFF — skipping data sending")
            sleep(random.randint(5, 10))
            continue
        datacam = {
            "camera_view": simulated_frames
        }

        print("[CAMERS_DEBUG] Data:", datacam)

        proceed_to_deliver(uuid4().__str__(), {
            "deliver_to": "data_processing",
            "operation": "data_camers",
            "datacam": datacam
        })
        sleep(random.randint(10, 15))


def proceed_to_deliver(id, details):
    details["id"] = id
    details["source"] = MODULE_NAME
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    threading.Thread(target=generate_data_camers).start()

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