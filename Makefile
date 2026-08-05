.PHONY: install up topic produce test down

install:
	python3 -m pip install -r requirements.txt

up:
	docker compose up -d

topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic security-events --partitions 3 --replication-factor 1

produce:
	python3 producer/generate_events.py --count 500 --anomaly-rate 0.12

test:
	pytest -q

down:
	docker compose down

