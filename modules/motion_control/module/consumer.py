import os
import json
import threading
from uuid import uuid4
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver 

MODULE_NAME: str = os.getenv("MODULE_NAME")

STATUS_PATH: str = "/shared/emulation_status"
EMERGENCY_STATUS_PATH: str = "/shared/emergency_status"


data_cache = {
    "control_commands": "Управляющие команды",
    "pause_loop": False,
    "emergency_sent": False 
}

def set_control_commands(id, details):
    data_cache["control_commands"] = details.get("control_commands")
    data_cache["pause_loop"] = False  
    print(f"[{MODULE_NAME}_DEBUG] Управляющие команды получены: {data_cache['control_commands']}")


def emergency_loop():
    """
    Постоянно читает EMERGENCY_STATUS_PATH.
    Если там '1' и команда ещё не отправлена, отправляет 'Аварийная обстановка' в task_orchestrator.
    """
    while True:
        try:
            with open(EMERGENCY_STATUS_PATH, "r") as f:
                emergency_status = f.read().strip()
        except Exception as e:
            print(f"[{MODULE_NAME}_ERROR] Не удалось прочитать {EMERGENCY_STATUS_PATH}: {e}")
            emergency_status = "0"

        if emergency_status == "1":
            if not data_cache["emergency_sent"]:
                message = {
                    "id": str(uuid4()),
                    "operation": "emergency_alert",
                    "deliver_to": "task_orchestrator",
                    "message": "Аварийная обстановка"
                }
                proceed_to_deliver(message["id"], message)
                print(f"[{MODULE_NAME}_INFO] Отправлено сообщение об авариной остановке в оркестратор")
                data_cache["emergency_sent"] = True  
        else:
            data_cache["emergency_sent"] = False 

        time.sleep(1)


def motion_loop():
    counter = 0
    while True:
        try:
            with open(STATUS_PATH, "r") as f:
                status = f.read().strip()
        except Exception as e:
            print(f"[{MODULE_NAME}_ERROR] Не удалось прочитать {STATUS_PATH}: {e}")
            break

        if status != "1":
            print(f"[{MODULE_NAME}_INFO] STATUS_PATH != 1 (={status}), остановка цикла")
            break

        if not data_cache["control_commands"]:
            print(f"[{MODULE_NAME}_INFO] Ожидание управляющих команд...")
            time.sleep(0.5)
            continue

        # Ждём, пока не разрешено отправлять
        if data_cache["pause_loop"]:
            time.sleep(0.5)
            continue

        # Выполняем отправку
        processed = data_cache["control_commands"]
        for subsystem in ("taxiing", "engin_management", "alarm_systems"):
            proceed_to_deliver(str(uuid4()), {
                "operation": "control_commands",
                "deliver_to": subsystem,
                "control_commands": processed
            })
        print(f"[{MODULE_NAME}_INFO] Отправлены управляющие команды в подсистемы (№{counter + 1})")
        time.sleep(10)
        counter += 1

        # После двух отправок — пауза и запрос на корректировку
        if counter >= 2:
            message = {
                "id": str(uuid4()),
                "operation": "corection_comands",
                "deliver_to": "decision_making",
                "source": MODULE_NAME
            }
            proceed_to_deliver(message["id"], message)
            print(f"[{MODULE_NAME}_INFO] Отправлена запрос на корректировку управляющих команд в блок принятия решений")

            # Ставим паузу, ждём новых команд
            data_cache["pause_loop"] = True
            counter = 0

        time.sleep(1)


def receive_start_motion_command(id, details):
    print(f"[{MODULE_NAME}_INFO] Получена команда начать движение")

    try:
        with open(STATUS_PATH, "w") as f:
            f.write("1")
        print(f"[{MODULE_NAME}_INFO] Статус в {STATUS_PATH} обновлён на 1")
    except Exception as e:
        print(f"[{MODULE_NAME}_ERROR] Ошибка при обновлении {STATUS_PATH}: {e}")
        return

    # Запуск фонового потока
    threading.Thread(target=motion_loop, daemon=True).start()
    threading.Thread(target=emergency_loop, daemon=True).start()

commands = {
    "control_commands": set_control_commands,
    "start_motion": receive_start_motion_command,
}

def handle_event(id, details_str):
    details = json.loads(details_str)

    source = details.get("source")
    deliver_to = details.get("deliver_to")
    operation = details.get("operation")

    print(f"[{MODULE_NAME}_INFO] id={id} {source}->{deliver_to}: {operation}")

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
