from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException
import threading
import os
import requests
import time

HOST = '0.0.0.0'
PORT = int(os.getenv('PLANNING_SYSTEM_PORT', 6004))
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

TRUCK_URL = "http://fura:6005/receive_approved_route"
TSODD_URL = "http://tsodd:6003"

class PlanningSystem:
    def __init__(self):
        self.route_sheet = None
        self.approved_route = None
        self.truck_telemetry = None
        self.completed_missions = []
        self.active_missions = []
        self.truck_status = {}

    def receive_route_sheet(self, route_sheet):
        truck_id = 'truck_123' 
        route_sheet['truck_id'] = truck_id  
        self.route_sheet = route_sheet
        self.active_missions.append(route_sheet)
        self.truck_status[truck_id] = {
            'status': 'route_received',
            'current_stage': 'to_load',
            'last_update': time.time()
        }
        return route_sheet


    def send_approved_route_to_connection(self):
        if not self.route_sheet:
            return False
        try:
            response = requests.post(TRUCK_URL, json=self.route_sheet, timeout=5, headers={"Content-Type": "application/json"})
            if response.ok:
                truck_id = self.route_sheet.get('truck_id', 'truck_123')
                self.truck_status[truck_id].update({
                    'status': 'route_sent_to_connection',
                    'current_stage': 'to_load'
                })
                return True
            return False
        except Exception as e:
            print(f"Ошибка отправки маршрута в connection: {e}")
            return False

    def handle_mission_completion(self, completion_data):
        truck_id = 'truck_123' 
        if truck_id not in self.truck_status:
            print(f"Ошибка: фура {truck_id} не найдена в системе")
            return False

        active_mission = next((m for m in self.active_missions 
                            if m.get('truck_id', 'truck_123') == truck_id), None)

        if active_mission:
            # Формируем сообщение о завершении
            completion_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print(
                f"\nМиссия завершена!\n"
                f"Фура: {truck_id}\n"
                f"Маршрут: {active_mission.get('load_point', 'неизвестно')} -> {active_mission.get('unload_point', 'неизвестно')}\n"
                f"Груз: {active_mission.get('cargo_type', 'неизвестно')}\n"
            )

            # Отправляем уведомление в систему base
            try:
                requests.post(
                    "http://base:6002/mission_complete",
                    json={
                        'truck_id': truck_id,
                        'status': 'completed',
                        'route': {
                            'load_point': active_mission.get('load_point'),
                            'unload_point': active_mission.get('unload_point'),
                            'cargo_type': active_mission.get('cargo_type')
                        }
                    },
                    timeout=3
                )
            except Exception as e:
                print(f"Ошибка при отправке уведомления в base: {str(e)}")

            # Оригинальная логика обработки
            self.completed_missions.append({
                **active_mission,
                'completion_data': completion_data,
            })
            self.active_missions.remove(active_mission)
            self.truck_status[truck_id].update({
                'status': 'mission_completed',
                'current_stage': 'completed',
            })
            return True
        
        print(f"Ошибка: активная миссия для фуры {truck_id} не найдена")
        return False

planning_system = PlanningSystem()

@app.route('/receive_route_sheet', methods=['POST'])
def receive_route_sheet():
    data = request.json
    if not data:
        return jsonify({"error": "Требуется маршрутный лист"}), 400

    route_sheet = planning_system.receive_route_sheet(data)
    
    if planning_system.send_approved_route_to_connection():
        return jsonify({"message": "Маршрутный лист отправлен в connection"}), 200
    return jsonify({"error": "Ошибка обработки маршрута"}), 400

@app.route('/mission_complete', methods=['POST'])
def mission_complete():
    data = request.json
    if not data:
        return jsonify({"error": "Необходимы данные миссии"}), 400

    if planning_system.handle_mission_completion(data):
        return jsonify({
            "message": "Миссия завершена",
            "truck_id": "truck_123",
            "status": "completed"
        }), 200
    return jsonify({"error": "Ошибка обработки завершения миссии"}), 400

@app.route('/missions', methods=['GET'])
def get_missions():
    return jsonify({
        "active_missions": planning_system.active_missions,
        "completed_missions": planning_system.completed_missions
    })


@app.errorhandler(HTTPException)
def handle_exception(e):
    return jsonify({
        "error": e.name,
        "message": e.description
    }), e.code

def start_web():
    app.run(host=HOST, port=PORT, debug=True, use_reloader=False)

if __name__ == '__main__':
    start_web()