SHELL := bash
PIPENV := $(shell which pipenv)

MODULES := monitor \
			connection \
			channel_encryption \
			task_orchestrator \
			route_planner \
			self_diagnostic \
			emergency_unit \
			emergency_speed_reduction \
			cargo_monitoring_system\
			integration \
			navigation_gnss \
			navigation_ins \
			camers \
			lidar \
			ir_sensors \
			data_processing \
			decision_making \
			motion_control \
			taxiing \
			taxiing_system \
			engin_management \
			acceleration_and_deceleration \
			alarm_systems \


SLEEP_TIME := 20

run:
	docker-compose up --build -d
	sleep ${SLEEP_TIME}

	for MODULE in ${MODULES}; do \
		echo Creating $${MODULE} topic; \
		docker exec broker \
			kafka-topics --create --if-not-exists \
			--topic $${MODULE} \
			--bootstrap-server localhost:9092 \
			--replication-factor 1 \
			--partitions 1; \
	done

all: clean pipenv run delay30s test

delay30s:
	sleep 30

clean:
	docker-compose down

logs:
	docker-compose logs -f --tail 100

pipenv:
	pipenv install -r requirements.txt

test: 
	cd modules/test && $(PIPENV) run pytest -sv