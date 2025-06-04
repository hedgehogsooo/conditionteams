from flask import Flask, jsonify, request
import threading
import time
import os 

HOST = '0.0.0.0'
PORT = int(os.getenv('TSODD_PORT', 6003))
MODULE_NAME = os.getenv('MODULE_NAME')

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

class TSODD:

    def check_route(self, route_sheet):
        load_point = route_sheet.get("load_point")
        unload_point = route_sheet.get("unload_point")
        cargo_type = route_sheet.get("cargo_type")

        if not (load_point and unload_point):
            print("ЦОДД: Маршрут не одобрен.")
            return {"approved": False, "message": "Маршрут некорректен."}

        print("ЦОДД: Маршрут одобрен.")
        return {"approved": True, "message": "Маршрут одобрен."}


# Создаем экземпляр ЦОДД
tsodd = TSODD()


@app.route('/process_route', methods=['POST'])
def process_route():
    data = request.json
    if not data or "route_sheet" not in data:
        return jsonify({"error": "Нужно передать route_sheet"}), 400

    route_sheet = data["route_sheet"]

    result = tsodd.check_route(route_sheet)
    return jsonify(result)


# Запуск веб-сервера
def start_web():
    threading.Thread(target=lambda: app.run(
        host=HOST, port=PORT, debug=True, use_reloader=False
    )).start()

if __name__ == '__main__':
    start_web()
