# How To Join

This guide describes how to join the Sensor Network with:

- a Davis Vantage Pro2 weather station
- optional Davis AirLink
- a USB connection from the console to an edge machine
- an ASUS PN41-S1 mini PC
  - Intel Celeron N5100
  - 4 GB RAM
  - 128 GB NVMe
  - Ubuntu Linux

The target architecture is:

```text
Vantage Pro2 console --USB--> ASUS PN41 edge machine
  -> ser2net exposes console on tcp:127.0.0.1:22222
  -> vantage-publisher reads live data
  -> MQTT broker publishes flat packets on topic=<uuid>
  -> sensor-network-collector subscribes and stores/forwards data
```

This document is aligned with:

- [`vantage-publisher`](https://github.com/CCMMMA/vantage-publisher)
- the current collector configuration model used by this repository
- the AirLink helper flow expected by the publisher (`http://<mqttBroker>:8088/get_airlink/<uuid>`)

## 1. What you need

Hardware:

- Davis Vantage Pro2 console with USB connectivity
- optional Davis AirLink
- ASUS PN41-S1 mini PC
- stable power supply
- Ethernet connectivity strongly recommended

Operating system:

- Ubuntu Linux 22.04 LTS or newer

Software on the edge machine:

- `ser2net`
- Python 3
- `git`
- optionally Docker and Docker Compose
- cloned [`vantage-publisher`](https://github.com/CCMMMA/vantage-publisher)

Network services reachable from the edge machine:

- MQTT broker
- optionally InfluxDB
- optionally Signal K server
- optionally the AirLink helper service from `weather-initialization`

## 2. Edge machine sizing

The PN41-S1 with N5100, 4 GB RAM, and 128 GB NVMe is adequate for one Vantage Pro2 edge node.

Typical workload on the PN41:

- one `ser2net` service
- one `vantage-publisher` process or container
- optional lightweight local logging

Recommended storage layout:

- operating system and packages on NVMe
- local CSV cache only if needed
- if long retention is required, prefer publishing over MQTT and storing centrally

Recommended baseline:

- keep the edge machine focused on acquisition and publishing
- avoid heavy local analytics or browser-facing workloads on the PN41
- use the central `sensor-network-collector` for aggregation, web UI, CSV archival, InfluxDB, and Signal K forwarding

## 3. Ubuntu preparation

Update the system:

```bash
sudo apt update
sudo apt upgrade -y
```

Install required packages:

```bash
sudo apt install -y \
  git \
  python3 \
  python3-pip \
  python3-venv \
  ser2net \
  curl \
  ca-certificates
```

Optional Docker-based deployment:

```bash
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
```

Then log out and back in.

## 4. Connect the Vantage Pro2 console over USB

Plug the Vantage Pro2 USB interface into the PN41.

Check which serial device appears:

```bash
ls -l /dev/ttyUSB*
dmesg | tail -n 50
```

Typical result:

```text
/dev/ttyUSB0
```

Confirm permissions:

```bash
ls -l /dev/ttyUSB0
groups
```

If needed, add the runtime user to `dialout`:

```bash
sudo usermod -aG dialout $USER
```

Then log out and back in.

## 5. Configure `ser2net`

`vantage-publisher` expects the station to be reachable as:

```text
tcp:127.0.0.1:22222
```

The `vantage-publisher` repository already includes a matching example in:

- [`util/ser2net.yaml`](/Users/raffaelemontella/Documents/New project 2/vantage-publisher/util/ser2net.yaml)

The relevant block is:

```yaml
connection: &weather
    accepter: tcp,22222
    enable: on
    options:
      kickolduser: true
      telnet-brk-on-sync: true
    connector: serialdev,
              /dev/ttyUSB0,
              19200n81,local
```

Install this configuration, adapting the device path if needed:

```bash
sudo cp /etc/ser2net.yaml /etc/ser2net.yaml.bak
sudo nano /etc/ser2net.yaml
```

Minimal working example:

```yaml
connection: &weather
    accepter: tcp,22222
    enable: on
    options:
      kickolduser: true
      telnet-brk-on-sync: true
    connector: serialdev,
              /dev/ttyUSB0,
              19200n81,local
```

Restart and enable the service:

```bash
sudo systemctl restart ser2net
sudo systemctl enable ser2net
sudo systemctl status ser2net
```

Verify the TCP endpoint is open:

```bash
ss -ltnp | grep 22222
```

The edge machine should now expose the console on `127.0.0.1:22222`.

## 6. Clone and install `vantage-publisher`

Example working directory:

```bash
mkdir -p ~/ccmmma
cd ~/ccmmma
git clone https://github.com/CCMMMA/vantage-publisher.git
cd vantage-publisher
```

Python deployment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 7. Create the station configuration

Start from the sample:

```bash
cp config.json.sample config.json
```

The sample config in `vantage-publisher` uses this style:

```json
{
  "uuid": "it.uniparthenope.meteo.wsN",
  "name": "New Weather Station",
  "lon": 0,
  "lat": 0,
  "storage": true,
  "mqtt": false,
  "signalk": false,
  "usbPort": 22222,
  "usbPollInterval": 1.0,
  "delay": 10,
  "timeout": 60,
  "pathStorage": "/storage/vantage-pro/",
  "mqttBroker": "",
  "mqttPort": 1883,
  "mqttUser": "",
  "mqttPass": "",
  "mqttQos": 1,
  "mqttFormat": "flat",
  "airlinkIntervalSec": 300
}
```

For joining this sensor network, set at least:

- `uuid`
- `name`
- `lat`
- `lon`
- `usbPort: 22222`
- `mqtt: true`
- `mqttBroker`
- `mqttPort`
- `mqttFormat: "flat"`

Recommended edge-node configuration:

```json
{
  "uuid": "it.uniparthenope.meteo.ws9",
  "name": "Example Station",
  "lon": 14.3000,
  "lat": 40.8500,

  "storage": true,
  "mqtt": true,
  "signalk": false,

  "usbPort": 22222,
  "usbPollInterval": 1.0,
  "delay": 10,
  "timeout": 60,

  "pathStorage": "/storage/vantage-pro/",

  "mqttBroker": "mqtt.network.meteo.uniparthenope.it",
  "mqttPort": 1883,
  "mqttUser": "",
  "mqttPass": "",
  "mqttQos": 1,
  "mqttFormat": "flat",

  "signalkServerUrl": "",
  "signalkToken": "",
  "signalkContext": "meteo.it.uniparthenope.meteo.ws9",
  "signalkPathMap": {},

  "httpEnabled": false,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "httpUser": "",
  "httpPass": "",
  "httpRoot": "/storage/vantage-pro/",

  "offlineMaxMessages": 200000,
  "offlineMaxAgeSec": 604800,
  "airlinkIntervalSec": 300
}
```

Notes:

- MQTT topic is the station `uuid`
- `mqttFormat` should be `flat` for the collector flow described in this repository
- `pathStorage` is optional but recommended for local diagnostics and resilience
- `signalk` is usually left `false` on the edge machine when using `sensor-network-collector` centrally

## 8. Optional `parameters.json`

If you want to filter fields before publishing, create `parameters.json`.

Behavior:

- `true`: include field
- `false`: exclude field
- if the file is missing, all fields are included

Example:

```json
{
  "TempOut": true,
  "HumOut": true,
  "WindSpeed": true,
  "WindDir": true,
  "Barometer": true,
  "ForecastIcon": false
}
```

## 9. Optional AirLink integration

`vantage-publisher` has built-in optional AirLink enrichment.

From the code path:

- it derives the station device name from `uuid`
- it queries:
  - `http://<mqttBroker>:8088/get_airlink/<uuid>`
- if that service returns an `airlinkID`, it periodically fetches AirLink data and merges it into the outgoing packet

This means that for AirLink support you need:

1. a Davis AirLink associated with the station
2. a helper service reachable on port `8088`
3. that helper service must return the AirLink identifier for the station UUID

This helper flow is the part expected from the `weather-initialization` side of the ecosystem.

Operationally:

- if the helper service returns `404`, the publisher logs that AirLink is not available and continues normally
- if AirLink is available, particulate and AQI-related fields are merged into the MQTT packet
- `airlinkIntervalSec` controls how often AirLink data is refreshed

## 10. Run `vantage-publisher`

Python mode:

```bash
cd ~/ccmmma/vantage-publisher
source .venv/bin/activate
python3 vantage-publisher.py --config config.json --mqtt true --storage true
```

Dry-run validation:

```bash
python3 vantage-publisher.py --config config.json --dry
```

Check for:

- successful USB connection on `tcp:127.0.0.1:22222`
- packets being generated
- MQTT packets being published on topic `uuid`
- optional local CSV rows under `pathStorage`

## 11. Run as a service

Example systemd service:

```ini
[Unit]
Description=Vantage Publisher
After=network-online.target ser2net.service
Wants=network-online.target

[Service]
User=weather
Group=weather
WorkingDirectory=/home/weather/ccmmma/vantage-publisher
ExecStart=/home/weather/ccmmma/vantage-publisher/.venv/bin/python /home/weather/ccmmma/vantage-publisher/vantage-publisher.py --config /home/weather/ccmmma/vantage-publisher/config.json --mqtt true --storage true
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vantage-publisher
sudo systemctl status vantage-publisher
```

## 12. Optional Docker deployment on the edge machine

The `vantage-publisher` repository also includes a simple Docker Compose example.

From the local repo:

- [`docker-compose.yml`](/Users/raffaelemontella/Documents/New project 2/vantage-publisher/docker-compose.yml)

That sample uses:

- a `vantage-publisher` container
- mounted `config.json`
- mounted `parameters.json`
- mounted storage under `/storage`

For an edge device, Python + systemd is usually simpler than Docker because:

- USB and `ser2net` debugging is easier
- fewer moving parts
- lower operational complexity on a 4 GB RAM node

Use Docker only if your deployment standard already requires it.

## 13. Register the station in the central collector

On the central server running `sensor-network-collector`, ensure:

- MQTT subscription covers the new topic
  - example: `it.uniparthenope.meteo/#`
- storage is enabled if archival is required
- InfluxDB and/or Signal K are enabled if needed

Example central collector behavior:

- subscribes to flat MQTT packets
- stores CSV under its own `pathStorage`
- optionally stores in InfluxDB
- optionally forwards to Signal K
- exposes the station in the web GUI map and station browser

## 14. Validation checklist

USB and edge machine:

- `ser2net` is active
- `127.0.0.1:22222` is listening
- the Vantage console is stable on USB

Publisher:

- `vantage-publisher` starts without USB read errors
- MQTT messages are published on topic `uuid`
- optional CSV files are created locally

Network:

- MQTT broker is reachable from the PN41
- DNS and NTP are correct
- firewall allows outbound MQTT and any required HTTPS calls

AirLink optional path:

- helper service on `http://<mqttBroker>:8088/get_airlink/<uuid>` is reachable
- AirLink ID is returned for the station
- AirLink metrics appear in published packets

Central collector:

- the new station appears in the home page map
- data is visible in station browser and public dashboard
- optional InfluxDB and Signal K sinks receive data

## 15. Recommended operating practices

- use a stable UUID naming convention such as `it.uniparthenope.meteo.wsN`
- document station name, exact coordinates, installation height, and maintenance notes
- keep the PN41 on a UPS if possible
- prefer Ethernet over Wi-Fi
- enable unattended security updates on Ubuntu
- monitor disk usage if local CSV storage is enabled
- keep the edge node simple: acquisition + publish first, visualization centrally

## 16. Troubleshooting

No USB data:

- verify `/dev/ttyUSB0`
- verify `ser2net` configuration
- verify `ss -ltnp | grep 22222`
- verify the user can access the serial device

No MQTT data:

- verify `mqttBroker` and `mqttPort`
- check broker ACLs
- run the publisher with `--dry` and compare generated packets with broker traffic

AirLink not present:

- check the helper endpoint on port `8088`
- verify station UUID mapping to AirLink ID
- verify outbound HTTPS access to WeatherLink APIs

Station not visible in collector:

- verify the topic matches the collector subscription
- verify the packet contains `name` and `position`
- verify collector storage or InfluxDB is enabled as expected

## 17. Files you will typically maintain

On the edge machine:

- `ser2net.yaml`
- `vantage-publisher/config.json`
- `vantage-publisher/parameters.json`
- systemd service file for `vantage-publisher`

On the central collector:

- `sensor-network-collector/config.json`
- optional web policies and station access assignments

