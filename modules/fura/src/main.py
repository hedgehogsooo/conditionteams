from flask import Flask, jsonify, request
import threading
import time
import requests
import os
import math

HOST = '0.0.0.0'
PORT = int(os.getenv('FURA_PORT', 6005))

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Теперь PlanningSystem слушает на 6004
PLANNING_SYSTEM_URL = "http://planningsystem:6004"
TSODD_URL = "http://tsodd:6003"

class Truck:
    def __init__(self):
        self.route_sheet = None
        self.truck_id = "truck_123"
        self.current_position = [55.7558, 37.6176]  # Москва
        self.speed = 0
        self.current_stage = "waiting"  # waiting -> to_load -> loading -> to_unload -> unloading -> completed
        self.base_step = 0.1              # Базовый шаг движения (в градусах)
        self.arrival_threshold = 0.0001   # Порог (~10 м)
        self.is_moving = False
        self.current_target = None
        self.mission_completed = False

    def receive_approved_route(self, route_sheet):
        """
        Приходит JSON {"truck_id": "...", "load_point": [lat,lon], "unload_point": [lat,lon], "cargo_type": "..."}.
        Сохраняем и сразу стартуем поток движения.
        """
        self.route_sheet = route_sheet
        self.truck_id = route_sheet.get("truck_id", self.truck_id)
        self.current_stage = "to_load"
        self.current_target = route_sheet["load_point"]
        self.mission_completed = False
        print(f"Фура: Получен маршрут. Цель — load_point {self.current_target}")
        self.is_moving = True
        threading.Thread(target=self.move_to_target, daemon=True).start()

    def move_to_target(self):
        """
        Цикл движения:
        1) Доехали до load_point → остановились → 5 сек «загрузка» → переходим к to_unload.
        2) Доехали до unload_point → остановились → 5 сек «разгрузка» → completed → отправляем mission_complete.
        """
        while self.is_moving and not self.mission_completed:
            distance = self.calculate_distance()
            if distance <= self.arrival_threshold:
                # Прибыли прямо на точку
                self.current_position = self.current_target.copy()
                self.speed = 0

                if self.current_stage == "to_load":
                    # На load_point
                    print(f"Фура: Прибыли в load_point {self.current_position}. Загрузка (5 сек).")
                    self.current_stage = "loading"
                    self.is_moving = False
                    time.sleep(5)  # имитация загрузки
                    # После загрузки едем к unload_point
                    self.current_stage = "to_unload"
                    self.current_target = self.route_sheet["unload_point"]
                    print(f"Фура: Загрузка завершена. Едем к unload_point {self.current_target}")
                    self.is_moving = True
                    continue  # сразу переходим к движению к новой цели

                elif self.current_stage == "to_unload":
                    # На unload_point
                    print(f"Фура: Прибыли в unload_point {self.current_position}. Разгрузка (5 сек).")
                    self.current_stage = "unloading"
                    self.is_moving = False
                    time.sleep(5)  # имитация разгрузки
                    # После разгрузки завершаем миссию
                    self.current_stage = "completed"
                    self.complete_mission()
                    return  # выходим из цикла

            # Пока не доехали — обновляем позицию
            self.update_position(distance)
            time.sleep(1)

    def calculate_distance(self):
        """Расстояние (двумерное) между current_position и current_target."""
        lat_diff = self.current_target[0] - self.current_position[0]
        lon_diff = self.current_target[1] - self.current_position[1]
        return math.sqrt(lat_diff**2 + lon_diff**2)

    def update_position(self, distance):
        """Перемещаемся на шаг step=min(base_step, distance/2), считаем speed."""
        lat_diff = self.current_target[0] - self.current_position[0]
        lon_diff = self.current_target[1] - self.current_position[1]
        step = min(self.base_step, distance / 2) if distance > 0 else 0
        if distance > 0:
            self.current_position[0] += (lat_diff / distance) * step
            self.current_position[1] += (lon_diff / distance) * step
        self.speed = 60 if distance > self.arrival_threshold * 2 else max(5, 60 * (distance / (self.arrival_threshold * 2)))
        print(f"Фура: Позиция={self.current_position}, Скорость={self.speed:.1f} км/ч, Осталось={distance:.6f}°")

    def complete_mission(self):
        """Формируем JSON и шлём POST /mission_complete в PlanningSystem."""
        self.mission_completed = True
        self.is_moving = False

        mission_data = {
            "truck_id": self.truck_id,
            "status": "completed",
            "final_position": self.current_position,
            "timestamp": time.time()
        }
        try:
            requests.post(
                f"{PLANNING_SYSTEM_URL}/mission_complete",
                json=mission_data,
                timeout=5
            )
            print("Фура: mission_complete отправлен в PlanningSystem")
        except Exception as e:
            print(f"Фура: не удалось отправить mission_complete: {e}")

# Создаём экземпляр Truck
truck = Truck()

@app.route('/receive_approved_route', methods=['POST'])
def receive_approved_route():
    data = request.json
    if not data or "truck_id" not in data or "load_point" not in data or "unload_point" not in data:
        return jsonify({"error": "Нужен полный маршрутный лист (truck_id, load_point, unload_point, cargo_type)"}), 400

    # 👉 Отправка маршрута в ЦОДД
    try:
        print("Фура: Отправляем маршрут в ЦОДД для проверки...")
        response = requests.post(
            f"{TSODD_URL}/process_route",
            json={"route_sheet": data},
            timeout=5
        )
        if response.status_code == 200:
            result = response.json()
            if not result.get("approved", False):
                print("Фура: ЦОДД НЕ одобрил маршрут:", result.get("message"))
                return jsonify({"error": "ЦОДД не одобрил маршрут", "details": result}), 400
            print("Фура: ЦОДД одобрил маршрут.")
        else:
            print("Фура: Ошибка от ЦОДД:", response.text)
            return jsonify({"error": "Ошибка от ЦОДД"}), 500
    except Exception as e:
        print(f"Фура: Ошибка при запросе к ЦОДД: {e}")
        return jsonify({"error": "Ошибка при связи с ЦОДД"}), 500

    truck.receive_approved_route(data)
    return jsonify({"message": "Маршрут одобрен ЦОДД, начинаем движение"}), 200


def start_web():
    threading.Thread(
        target=lambda: app.run(host=HOST, port=PORT, debug=True, use_reloader=False)
    ).start()

if __name__ == '__main__':
    start_web()
