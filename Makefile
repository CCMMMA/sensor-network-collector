build:
	docker build -t sensor-network-collector .

run:
	docker run --network=host -v ./config.json:/config/config.json -d sensor-network-collector
