from flask import Flask, jsonify, request
import requests
import os
import json
from werkzeug.exceptions import HTTPException
import threading
import time
from pathlib import Path

# Конфигурация
HOST = '0.0.0.0'
PORT = int(os.getenv('CUSTOMER_PORT', 6001))
BASE_URL = os.getenv('BASE_URL', 'http://base:6002')
ROUTES_FILE = 'routes.json' 
app = Flask(__name__)

class Customer:
    def __init__(self):
        """
        Инициализация класса Customer.
        """
        self.mission_info = None
        self.route_sheet = None
        self.routes = self._load_routes()
        self.current_route_index = 0

    def _load_routes(self):
        """
        Загружает маршруты из JSON файла.
        """
        try:
            file_path = Path(__file__).parent / ROUTES_FILE
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка загрузки файла маршрутов: {str(e)}")
            return []

    def create_mission(self, cargo_type):
        """
        Создает миссию с координатами из файла.
        :param cargo_type: Тип груза.
        :return: Информация о миссии.
        """
        if not self.routes or self.current_route_index >= len(self.routes):
            print("Нет доступных маршрутов в файле")
            return None

        route = self.routes[self.current_route_index]
        self.current_route_index = (self.current_route_index + 1) % len(self.routes)  # Циклический перебор

        self.mission_info = {
            "load_coordinates": route["load_point"],
            "unload_coordinates": route["unload_point"],
            "cargo_type": cargo_type
        }
        print("Миссия создана:", self.mission_info)
        return self.mission_info

    def generate_route_sheet(self, cargo_type):
        """
        Генерирует маршрутный лист на основе данных миссии.
        :param cargo_type: Тип груза.
        :return: Маршрутный лист.
        """
        if not self.create_mission(cargo_type):
            return None

        self.route_sheet = {
            "load_point": self.mission_info["load_coordinates"],
            "unload_point": self.mission_info["unload_coordinates"],
            "cargo_type": self.mission_info["cargo_type"]
        }
        print("Маршрутный лист сформирован:", self.route_sheet)
        return self.route_sheet

    def send_route_sheet_to_base(self, base_url):
        """
        Отправляет маршрутный лист в систему Base.
        :param base_url: URL системы Base.
        :return: Ответ от системы Base.
        """
        if self.route_sheet is None:
            print("Ошибка: маршрутный лист не сформирован")
            return None

        try:
            print("Отправка маршрутного листа в систему Base...")
            response = requests.post(f"{base_url}/route_sheets", json=self.route_sheet)
            if response.status_code == 201:
                print("Маршрутный лист успешно отправлен в систему Base")
            else:
                print(f"Ошибка при отправке маршрутного листа: {response.status_code}")
            return response
        except Exception as e:
            print(f"Ошибка при отправке маршрутного листа: {str(e)}")
            return None

customer = Customer()

@app.route('/generate_and_send_route_sheet', methods=['POST'])
def generate_and_send_route_sheet():
    """
    Эндпоинт для генерации маршрутного листа и его отправки в систему Base.
    """
    data = request.json
    cargo_type = data.get('cargo_type')

    if not cargo_type:
        return jsonify({"error": "Необходимо указать тип груза"}), 400

    # Генерация маршрутного листа
    route_sheet = customer.generate_route_sheet(cargo_type)
    if not route_sheet:
        return jsonify({"error": "Ошибка при создании маршрутного листа"}), 500

    # Отправка маршрутного листа в систему Base
    response = customer.send_route_sheet_to_base(BASE_URL)
    if response and response.status_code == 201:
        return jsonify({
            "message": "Маршрутный лист успешно сгенерирован и отправлен в систему Base",
            "route_sheet": route_sheet
        }), 201
    else:
        return jsonify({"error": "Ошибка при отправке маршрутного листа"}), 500

# Обработка ошибок
@app.errorhandler(HTTPException)
def handle_exception(e):
    """
    Обработка исключений HTTP.
    """
    response = e.get_response()
    return jsonify({
        "status": e.code,
        "name": e.name,
    }), e.code

# Запуск веб-сервера
def start_web():
    threading.Thread(target=lambda: app.run(
        host=HOST, port=PORT, debug=True, use_reloader=False
    )).start()

if __name__ == '__main__':
    start_web()
