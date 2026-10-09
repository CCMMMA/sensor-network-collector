build:
	docker build -t sensor-network-collector .

run:
	docker run --network=host -v $(CURDIR)/config.json:/app/config.json -d sensor-network-collector
