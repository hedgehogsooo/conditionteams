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

CARGO_PATH: str = "/shared/cargo"
MODULE_NAME: str = os.getenv("MODULE_NAME")
STATUS_PATH: str = "/shared/emulation_status"


def read_cargo() -> bool:
    with open(CARGO_PATH, "a+") as file:
        pass

    with open(CARGO_PATH, "r") as file:
        status = file.read()

    return status == "1"


def get_emulation_status() -> bool:
    try:
        with open(STATUS_PATH, "r") as f:
            status = f.read().strip()
            return status == "1"
    except Exception as e:
        print(f"[CAMERS_ERROR] Failed to read status file: {e}")
        return False


def generate_cargo_monitor():

    while True:
        if not get_emulation_status():
            # print("[LIDAR_INFO] Emulation status OFF — skipping data sending")
            sleep(random.randint(5, 10))
            continue    
        # Эмуляция работы системы мониторинга груза 
        cargo_loaded = read_cargo()
        cargo = "Груз загружен" if cargo_loaded else "Груз не загружен"

        print("[CARGO_DEBUG] Data:", cargo)

        proceed_to_deliver(uuid4().__str__(), {
            "deliver_to": "emergency_unit",
            "operation": "cargo_load",
            "cargo": cargo
        })

        sleep(random.randint(5, 10))


def proceed_to_deliver(id, details):
    details["id"] = id
    details["source"] = MODULE_NAME
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    threading.Thread(target=generate_cargo_monitor).start()

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