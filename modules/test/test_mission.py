import requests
import pytest
from time import sleep
import subprocess

CUSTOMER_URL = 'http://localhost:6001/generate_and_send_route_sheet'
BASE_LOG_URL = 'http://localhost:6002/route_sheets'
PLANNING_URL = 'http://localhost:6004/missions'

@pytest.fixture
def get_logs():
    def _get_logs(container_name):
        return subprocess.check_output(
            ['docker-compose', 'logs', '--no-color', container_name],
            text=True,
        )
    return _get_logs

@pytest.fixture
def send_route_sheet():
    payload = {
        "cargo_type": "fuel"
    }
    response = requests.post(CUSTOMER_URL, json=payload)
    return response

def test_customer_to_base(send_route_sheet, get_logs):
    response = send_route_sheet
    assert response.status_code == 201
    assert "route_sheet" in response.json()
    print("test_customer_to_base passed")

    # Проверяем логи customer
    logs = get_logs('customer')
    assert "Миссия создана" in logs or "Маршрутный лист сформирован" in logs
    print("Customer log check passed")

def test_base_to_planning(get_logs):
    response = requests.get(BASE_LOG_URL)
    assert response.status_code == 200
    data = response.json()
    assert len(data.get("route_sheets", [])) > 0
    print("test_base_to_planning passed")

    # Проверяем логи base
    logs = get_logs('base')
    assert "Маршрутный лист получен от заказчика" in logs
    print("Base log check passed")

def test_tsodd_approved(get_logs):
    # Даем немного времени, чтобы запрос дошел до ЦОДД
    sleep(35)
    logs = get_logs('tsodd')
    assert "ЦОДД: Маршрут одобрен" in logs
    print("test_tsodd_approved passed")

def test_planning_received_route(get_logs):
    response = requests.get(PLANNING_URL)
    assert response.status_code == 200
    data = response.json()
    assert len(data.get("active_missions", [])) > 0
    print("test_planning_received_route passed")

def test_full_mission_flow(get_logs):
    # Даем время системе полностью выполнить миссию (движение фуры, загрузка/разгрузка и уведомления)
    sleep(90)

    # Проверяем, что в логах base зафиксировано получение уведомления
    base_logs = get_logs('base')
    assert "Получено уведомление о завершении миссии" in base_logs
    print("test_full_mission_flow passed")
