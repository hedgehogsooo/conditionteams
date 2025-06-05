import os
import json
import threading
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")
STATUS_PATH: str = "/shared/emulation_status"

data_cache = {
    "receive_route_sheets": None,
}


def store_route_sheet_in_cache(task_route):
    data_cache["receive_route_sheets"] = task_route
    data_cache["receive_route_sheets"] = None


def handle_route_sheet(id, details):
    """
    Получает 'encrypted_route', сохраняет во временный кеш, 
    и пересылает его в route_planner.
    """
    task_route = details.get("encrypted_route")
    if not task_route:
        print(f"[{MODULE_NAME}_WARNING] Нет encrypted_route в сообщении")
        return

    print(f"[{MODULE_NAME}_DEBUG] Получен маршрутный лист")
    store_route_sheet_in_cache(task_route)

    message = {
        "id": str(uuid4()),
        "operation": "receive_route_sheet",
        "deliver_to": "route_planner",
        "task_route": task_route
    }

    message_to_emulated = {
        "id": str(uuid4()),
        "operation": "receive_route_sheet",
        "deliver_to": "emulated",
        "task_route": task_route
    }
    proceed_to_deliver(message_to_emulated["id"], message_to_emulated)

    proceed_to_deliver(message["id"], message)


def handle_planned_route(id, details):
    """
    Обработка готового маршрута от route_planner.
    Пересылает его в emergency_unit и chanel_encryption.
    """
    planned_route = details.get("planned_route")
    if not planned_route:
        print(f"[{MODULE_NAME}_WARNING] Нет planned_route в сообщении")
        return

    # Отправка в emergency_unit
    msg_to_emergency = {
        "id": str(uuid4()),
        "operation": "route_ready",
        "deliver_to": "emergency_unit",
        "route_info": planned_route
    }
    proceed_to_deliver(msg_to_emergency["id"], msg_to_emergency)

    # Отправка в chanel_encryption
    msg_to_encryption = {
        "id": str(uuid4()),
        "operation": "encrypt_route",
        "deliver_to": "channel_encryption",
        "route_info": planned_route
    }
    proceed_to_deliver(msg_to_encryption["id"], msg_to_encryption)


def handle_route_permission(id, details):
    """
    Получает разрешение от channel_encryption и отправляет команду 
    'check_system_state' в self_diagnostic вместо прямого выполнения.
    """
    approved = details.get("approved")
    if approved is None:
        print(f"[{MODULE_NAME}_WARNING] Нет поля 'approved' в сообщении")
        return

    print(f"[{MODULE_NAME}_INFO] Получено разрешение от ЦОДД: approved={approved}")

    if approved:
        # Отправляем запрос на проверку состояния системы в self_diagnostic
        message = {
            "id": str(uuid4()),
            "operation": "check_system_state",
            "deliver_to": "self_diagnostic",
            "source": MODULE_NAME
        }
        proceed_to_deliver(message["id"], message)
        print(f"[{MODULE_NAME}_INFO] Отправлен запрос на состояние систем в блок диагностики")
    else:
        print(f"[{MODULE_NAME}_INFO] Разрешение отклонено, дальнейшие команды не отправляются")


def handle_checkpoint_route_ready(id, details):
    """
    Получает сообщение 'checkpoint_route_ready' и 
    отправляет команду 'start_motion' в motion_control.
    """
    checkpoint_route = details.get("checkpoint_route")
    if not checkpoint_route:
        print(f"[{MODULE_NAME}_WARNING] Нет checkpoint_route в сообщении")
        return

    print(f"[{MODULE_NAME}_INFO] Получен рассчитанный маршрут до контрольной точки")

    message = {
        "id": str(uuid4()),
        "operation": "start_motion",
        "deliver_to": "motion_control",
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Начать движение к контрольной точке")


def handle_emergency_alert(id, details):
    """
    Получает сообщение об аварийной обстановке и пересылает его в emergency_unit.
    """
    alert_message = details.get("message")
    print(f"[{MODULE_NAME}_INFO] Получено оповещение об аварийной остановке: {alert_message}")

    msg_to_emergency = {
        "id": str(uuid4()),
        "operation": "emergency_alert",
        "deliver_to": "emergency_unit",
        "message": alert_message
    }
    proceed_to_deliver(msg_to_emergency["id"], msg_to_emergency)
    print(f"[{MODULE_NAME}_INFO] Отправлено 'emergency_alert' в emergency_unit")


def handle_systems_ok(id, details):
    """
    Получает от self_diagnostic сообщение 'systems_ok' 
    и выводит, что системы в норме, после чего можно выполнять остальные команды.
    """
    print(f"[{MODULE_NAME}_INFO] Получено сообщение от самодиагностики: все системы в норме")
    message = {
        "id": str(uuid4()),
        "operation": "calculate_checkpoint_route",
        "deliver_to": "route_planner",
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Отправлен запрос на расчет маршрута к контрольной точке в блок планирования маршрута")


def handle_arrived_loading(id, details):
    print(f"[{MODULE_NAME}_INFO] Прибыли в точку загрузки")


def handle_arrived_unloading(id, details):
    print(f"[{MODULE_NAME}_INFO] Прибыли в точку разгрузки")


def handle_mission_completed(id, details):
    print(f"[{MODULE_NAME}_INFO] Вернулись на базу. Миссия завершена")

    # Меняем статус в STATUS_PATH
    try:
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            f.write("0")
        print(f"[{MODULE_NAME}_INFO] Статус в {STATUS_PATH} изменён на 0")
    except Exception as e:
        print(f"[{MODULE_NAME}_ERROR] Не удалось записать в {STATUS_PATH}: {e}")
        return

    # Отправка сообщения в channel_encryption
    message = {
        "id": str(uuid4()),
        "operation": "Mission_completed_returned_to_base",
        "deliver_to": "channel_encryption",
        "message": "Вернулись на базу. Миссия завершена",
        "source": MODULE_NAME,
    }
    proceed_to_deliver(message["id"], message)
    print(f"[{MODULE_NAME}_INFO] Сообщение о завершении миссии отправлено в channel_encryption")


commands = {
    "receive_route_sheets":      handle_route_sheet,
    "route_planned":             handle_planned_route,
    "route_permission":          handle_route_permission,
    "checkpoint_route_ready":    handle_checkpoint_route_ready,
    "emergency_alert":           handle_emergency_alert,
    "systems_ok":                handle_systems_ok,
    "Arrived_at_the_loading": handle_arrived_loading,
    "Arrived_at_the_unloading": handle_arrived_unloading,
    "Mission_completed_returned_to_base": handle_mission_completed,
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
