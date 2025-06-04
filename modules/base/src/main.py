from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException
import threading
import os
import requests
import time

HOST = '0.0.0.0'
PORT = int(os.getenv('BASE_PORT', 6002)) 
PLANNING_SYSTEM_URL = os.getenv('PLANNING_SYSTEM_URL', 'http://planningsystem:6004')

app = Flask(__name__)

class Base:
    def __init__(self):
        self.route_sheets = [] 
        self.planning_system_url = PLANNING_SYSTEM_URL

    def receive_route_sheet(self, route_sheet):
        """
        Принимает маршрутный лист от заказчика.
        """
        required_fields = ["load_point", "unload_point", "cargo_type"]
        if not all(field in route_sheet for field in required_fields):
            return None

        self.route_sheets.append(route_sheet)
        print("Маршрутный лист получен от заказчика:", route_sheet)
        return route_sheet

    def send_route_sheet_to_planning_system(self, route_sheet):
        """
        Отправляет маршрутный лист в систему планирования.
        :param route_sheet: Маршрутный лист для отправки.
        :return: True, если отправка успешна, иначе False.
        """
        retries = 2 
        for attempt in range(retries):
            try:
                print("Отправка маршрутного листа в систему планирования...")
                response = requests.post(
                    f"{self.planning_system_url}/receive_route_sheet",
                    json=route_sheet
                )
                if response.status_code == 200:
                    print("Маршрутный лист успешно передан в систему планирования:", route_sheet)
                    return True
            except Exception as e:
                print(f"Ошибка при отправке маршрутного листа: {str(e)}")

            if attempt < retries - 1:
                time.sleep(2)
        return False

base = Base()

@app.route('/route_sheets', methods=['POST'])
def receive_route_sheet():
    """
    Принимает маршрутный лист от заказчика и сразу передает его в систему планирования.
    """
    data = request.json
    if not data:
        return jsonify({"error": "Необходимо передать маршрутный лист"}), 400

    route_sheet = base.receive_route_sheet(data)
    if not route_sheet:
        return jsonify({"error": "Маршрутный лист не содержит всех необходимых полей"}), 400

    success = base.send_route_sheet_to_planning_system(route_sheet)
    if success:
        return jsonify({"message": "Маршрутный лист получен и передан в систему планирования", "route_sheet": route_sheet}), 201
    else:
        return jsonify({"error": "Ошибка при передаче маршрутного листа в систему планирования"}), 500

@app.route('/route_sheets', methods=['GET'])
def get_route_sheets():
    """
    Возвращает список всех маршрутных листов.
    """
    return jsonify({"route_sheets": base.route_sheets})

@app.route('/mission_complete', methods=['POST'])
def mission_complete():
    data = request.json
    print(f"\nПолучено уведомление о завершении миссии:")
    print(f"Фура: {data.get('truck_id')}")
    print(f"Статус: {data.get('status')}")
    print(f"Маршрут: {data.get('route', {}).get('load_point')} -> {data.get('route', {}).get('unload_point')}")
    print(f"Груз: {data.get('route', {}).get('cargo_type')}\n")
    
    return jsonify({"message": "Уведомление о завершении получено"}), 200

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
    """
    Запуск веб-сервера.
    """
    app.run(host=HOST, port=PORT, debug=True, use_reloader=False)
