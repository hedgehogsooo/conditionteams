#Политика безопансости 
policies =(
    {"src": "customer", "dst": "base" }, # Заказчик -> база
   
    {"src": "base", "dst": "palningsystem"}, # база -> система планирования 
    {"src": "palningsystem", "dst": "tsodd"}, # система планирования -> тсод
    {"src": "tsodd", "dst": "palningsystem"}, # тсод -> система планирования
   
    {"src": "palningsystem", "dst": "connection"}, # система планирования -> связь 
    {"src": "connection", "dst": "palningsystem"}, # связь -> система планирования
    {"src": "tsodd", "dst": "connection"}, # тсодд -> связь 
    {"src": "connection", "dst": "tsodd"}, # связь -> тсодд
   
    {"src": "connection", "dst": "channel_encryption"}, # связь -> шифрование каналов  
    {"src": "channel_encryption", "dst": "connection"}, # шифрование каналов -> связь
   
    {"src": "channel_encryption", "dst": "task_orchestrator"}, # шифрование каналов -> оркестратор задач 
    {"src": "task_orchestrator", "dst": "channel_encryption"}, # орекестратор задач -> шифрование каналов
    {"src": "task_orchestrator", "dst": "emulated"}, # орекестратор задач -> шифрование каналов
   
    {"src": "task_orchestrator", "dst": "emergency_unit"}, # орекестратор задач -> аварийный блок 
    {"src": "emergency_unit", "dst": "emergency_speed_reduction"}, # аварийный блок -> аварийное снижение скорости 
    {"src": "cargo_monitoring_system", "dst": "emergency_unit"}, # система мониторинга груза -> аварийный блок
    
    
    {"src": "task_orchestrator", "dst": "self_diagnostic"}, # орекестратор задач -> самодиангостика
    {"src": "self_diagnostic", "dst": "task_orchestrator"}, # самодиангостика -> орекестратор задач

    {"src": "task_orchestrator", "dst": "route_planner"}, # орекестратор задач -> планировщик маршрута
    {"src": "route_planner", "dst": "task_orchestrator"}, # планировщик маршрута -> орекестратор задач
    {"src": "navigation_ins", "dst": "integration"}, # навигация инс -> комплексирование 
    {"src": "navigation_gnss", "dst": "integration"}, # навигация гнсс -> комплексирование 
    {"src": "integration", "dst": "route_planner"}, # комплексирование -> планировщик маршрута
    {"src": "integration", "dst": "emergency_unit"}, # комплексирование -> аварийный блок
    

    {"src": "task_orchestrator", "dst": "motion_control"}, # орекестратор задач -> управление движением
    {"src": "motion_control", "dst": "task_orchestrator"}, # управление движением -> орекестратор задач
    {"src": "motion_control", "dst": "taxiing"}, # управление движением -> руление 
    {"src": "taxiing", "dst": "taxiing_system"}, # руление -> система руления
    {"src": "motion_control", "dst": "engin_management"}, # управление движением -> управление двигателем
    {"src": "engin_management", "dst": "acceleration_and_deceleration"}, # управление двигателем -> разног и тормоджение
    {"src": "motion_control", "dst": "alarm_systems"}, # управление движением -> сигнальные системы

    {"src": "motion_control", "dst": "decision_making"}, # управление движением -> принятие решений 
    {"src": "decision_making", "dst": "motion_control"}, # принятие решений -> управление движением
    
    {"src": "decision_making", "dst": "data_processing"}, # принятие решений -> обработка данных 

    {"src": "data_processing", "dst": "decision_making"}, # обработка данных -> принятие решений

    {"src": "camers", "dst": "data_processing"}, # камеры -> обработка данных
    {"src": "lidar", "dst": "data_processing"}, # лидар -> обработка данных 
    {"src": "ir_sensors", "dst": "data_processing"}, # ИК-датчики -> обработка данных

)

def check_operation(id, details) -> bool:
    """ Проверка возможности совершения обращения. """
    src: str = details.get("source")
    dst: str = details.get("deliver_to")

    if not all((src, dst)):
        return False

    print(f"[info] checking policies for event {id}, {src}->{dst}")

    return {"src": src, "dst": dst} in policies