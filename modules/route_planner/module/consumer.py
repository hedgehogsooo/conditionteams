import os
import json
import threading
from uuid import uuid4
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")
CHECKPOINT_PATH: str = "/shared/checkpoint_status"

data_cache = {
    "set_coords": None,
    "receive_route_sheets": None,
}


def set_coordinates(id, details):
    """
    Сохраняет координаты во временный кеш и сразу же очищает его.
    """
    data_cache["set_coords"] = details.get("coords")
    print(f"[{MODULE_NAME}_DEBUG] Получены координаты: {data_cache['set_coords']}")
    data_cache["set_coords"] = None


def store_route_sheet_in_cache(task_route):
    """
    Сохраняет маршрутный лист во временный кеш и логирует его.
    """
    data_cache["receive_route_sheets"] = task_route
    # print(f"[{MODULE_NAME}_DEBUG] Сохраняем в кеш маршрутный лист: {task_route}")
    data_cache["receive_route_sheets"] = None


def handle_route_sheet(id, details):
    """
    Получает поле 'task_route', сохраняет его в кеш, 
    и эмулирует планирование маршрута.
    Затем отправляет результат в orchestrator.
    """
    task_route = details.get("task_route")
    if not task_route:
        print(f"[{MODULE_NAME}_WARNING] Нет task_route в сообщении")
        return

    print(f"[{MODULE_NAME}_INFO] Получен маршрутный лист")

    # Сохраняем маршрутный лист
    store_route_sheet_in_cache(task_route)

    # Эмулируем планирование маршрута
    print(f"[{MODULE_NAME}_INFO] Маршрут успешно спланирован")

    # Отправляем результат в orchestrator
    message = {
        "id": str(uuid4()),
        "operation": "route_planned",
        "deliver_to": "task_orchestrator",
        "source": MODULE_NAME,
        "planned_route": task_route
    }
    proceed_to_deliver(message["id"], message)


def handle_calculate_checkpoint_route(id, details):

    print(f"[{MODULE_NAME}_INFO] Получен запрос на рассчет маршрута до контрольной точки")

    # Эмулированный маршрут до контрольной точки
    checkpoint_route = "Рассчитанный маршрут до контрольной точки"

    message = {
        "id": str(uuid4()),
        "operation": "checkpoint_route_ready",
        "deliver_to": "task_orchestrator",
        "source": MODULE_NAME,
        "checkpoint_route": checkpoint_route
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Рассчитанный маршрут до контрольной точки")


def monitor_checkpoint_status():
    """
    Следит за CHECKPOINT_PATH и при изменении статуса отправляет
    сообщение с соответствующей operation и описанием.
    """
    last_status = None

    operations_map = {
        "1": {
            "operation": "Arrived_at_the_loading",
            "message": "Прибыли в точку загрузки"
        },
        "2": {
            "operation": "Arrived_at_the_unloading",
            "message": "Прибыли в точку разгрузки"
        },
        "3": {
            "operation": "Mission_completed_returned_to_base",
            "message": "Вернулись на базу. Миссия завершена"
        }
    }

    while True:
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                status = f.read().strip()
        except Exception as e:
            print(f"[{MODULE_NAME}_ERROR] Ошибка чтения {CHECKPOINT_PATH}: {e}")
            time.sleep(1)
            continue

        if status != last_status and status in operations_map:
            last_status = status
            operation_data = operations_map[status]

            print(f"[{MODULE_NAME}_INFO] {operation_data['message']}")

            message = {
                "id": str(uuid4()),
                "operation": operation_data["operation"],
                "deliver_to": "task_orchestrator",
                "source": MODULE_NAME,
                "message": operation_data["message"]
            }
            proceed_to_deliver(message["id"], message)

        time.sleep(1)


commands = {
    "set_coords": set_coordinates,
    "receive_route_sheet": handle_route_sheet,
    "calculate_checkpoint_route": handle_calculate_checkpoint_route,
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
    threading.Thread(target=monitor_checkpoint_status, daemon=True).start()
