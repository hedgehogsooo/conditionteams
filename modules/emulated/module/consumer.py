import os
import json
import time
import threading
import math
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver

MODULE_NAME: str = os.getenv("MODULE_NAME")

COORDS_PATH: str = "/shared/coords"
CARGO_PATH: str = "/shared/cargo"
CHECKPOINT_PATH: str = "/shared/checkpoint_status"
STATUS_PATH: str = "/shared/emulation_status"

data_cache = {
    "start_coords": [55.7558, 37.6176],
    "load_point": None,
    "unload_point": None,
    "route_received": False,
    "emulation_started": False,
}

# Параметры адаптивного шага
BASE_STEP = 0.05
ARRIVAL_THRESHOLD = 0.0001


def read_coords():
    """
    Читает текущие координаты из файла COORDS_PATH.
    Если файла нет или содержимое некорректно, возвращает None.
    Формат в файле: "lat,lon"
    """
    try:
        with open(COORDS_PATH, "r", encoding="utf-8") as f:
            content = f.read().strip()
            parts = content.split(",")
            if len(parts) != 2:
                raise ValueError("Invalid coord format")
            lat, lon = map(float, parts)
            return [lat, lon]
    except Exception:
        return None


def write_coords(coords):
    """
    Записывает координаты в файл COORDS_PATH в формате "lat,lon".
    """
    with open(COORDS_PATH, "w", encoding="utf-8") as f:
        f.write(f"{coords[0]},{coords[1]}")


def write_cargo(status: int):
    with open(CARGO_PATH, "w", encoding="utf-8") as f:
        f.write(str(status))


def write_checkpoint(status: int):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        f.write(str(status))


def move_towards(start, end, base_step=BASE_STEP, arrival_threshold=ARRIVAL_THRESHOLD):
    """
    Двигает координаты от `start` к `end` с адаптивным шагом.
    Приближение становится медленнее по мере уменьшения расстояния.
    """
    lat1, lon1 = start
    lat2, lon2 = end

    lat_diff = lat2 - lat1
    lon_diff = lon2 - lon1
    distance = math.hypot(lat_diff, lon_diff)

    if distance < arrival_threshold:
        return end

    step = min(base_step, distance / 2)
    new_lat = lat1 + (lat_diff / distance) * step
    new_lon = lon1 + (lon_diff / distance) * step

    return [round(new_lat, 6), round(new_lon, 6)]


def emulate_movement():
    print(f"[{MODULE_NAME}_INFO] Эмуляция движения начата")

    current = data_cache["start_coords"]
    write_coords(current)

    # 1. Движение к точке загрузки
    print(f"[{MODULE_NAME}_INFO] Движение к точке загрузки")
    while current != data_cache["load_point"]:
        current = move_towards(current, data_cache["load_point"])
        write_coords(current)
        time.sleep(2)

    print(f"[{MODULE_NAME}_INFO] Прибыл в точку загрузки")
    write_cargo(1)
    write_checkpoint(1)
    time.sleep(5)  # ожидание 5 секунд в точке загрузки

    # 2. Движение к точке разгрузки
    print(f"[{MODULE_NAME}_INFO] Движение к точке разгрузки")
    while current != data_cache["unload_point"]:
        current = move_towards(current, data_cache["unload_point"])
        write_coords(current)
        time.sleep(2)

    print(f"[{MODULE_NAME}_INFO] Прибыл в точку разгрузки")
    write_cargo(0)
    write_checkpoint(2)
    time.sleep(5)  # ожидание 5 секунд в точке разгрузки

    # 3. Возврат на базу
    print(f"[{MODULE_NAME}_INFO] Возврат на базу")
    while current != data_cache["start_coords"]:
        current = move_towards(current, data_cache["start_coords"])
        write_coords(current)
        time.sleep(2)

    print(f"[{MODULE_NAME}_INFO] Прибыл на базу. Эмуляция завершена.")
    write_checkpoint(3)
    time.sleep(5)  # ожидание 5 секунд после возврата на базу
    data_cache["emulation_started"] = False



def emulated_status_loop():
    """
    Постоянно читает STATUS_PATH. Если в нём '1' — запускает 'эмуляцию'.
    """
    while True:
        try:
            with open(STATUS_PATH, "r", encoding="utf-8") as f:
                status = f.read().strip()
        except Exception as e:
            print(f"[{MODULE_NAME}_ERROR] Не удалось прочитать {STATUS_PATH}: {e}")
            status = "0"

        if (
            status == "1"
            and not data_cache["emulation_started"]
            and data_cache["route_received"]
        ):
            print(f"[{MODULE_NAME}_INFO] Получен сигнал на запуск эмуляции")
            data_cache["emulation_started"] = True
            threading.Thread(target=emulate_movement).start()

        time.sleep(1)


def handle_route_sheet(id, details):
    task_route = details.get("task_route")
    if not task_route:
        print(f"[{MODULE_NAME}_WARNING] Нет task_route в сообщении")
        return

    # Считываем начальные координаты из файла coords
    current_coords = read_coords()
    if current_coords:
        data_cache["start_coords"] = current_coords
    else:
        data_cache["start_coords"] = task_route.get("load_point")

    data_cache["load_point"] = task_route.get("load_point")
    data_cache["unload_point"] = task_route.get("unload_point")
    data_cache["route_received"] = True

    print(f"[{MODULE_NAME}_DEBUG] Получен маршрутный лист:")
    print(f"  Загрузка: {data_cache['load_point']}")
    print(f"  Разгрузка: {data_cache['unload_point']}")


commands = {
    "receive_route_sheet": handle_route_sheet,
}


def handle_event(id, details_str):
    details = json.loads(details_str)
    operation = details.get("operation")

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
                continue
            elif msg.error():
                print(f"[{MODULE_NAME}_ERROR] {msg.error()}")
            else:
                try:
                    id = msg.key().decode("utf-8")
                    details_str = msg.value().decode("utf-8")
                    handle_event(id, details_str)
                except Exception as e:
                    print(f"[{MODULE_NAME}_ERROR] Malformed event: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config)).start()
    threading.Thread(target=emulated_status_loop).start()
