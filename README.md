# MQTT Sensor Simulator

Simple Flask web app for simulating devices and sensor tags that publish MQTT payloads.

## Features

- Creates devices and publishes `<org_id>/newSensorCreated`
- Creates sensor tags and publishes `<org_id>/<device_name>/newTag`
- Simulates sensor values and publishes `<org_id>/<device_name>/<tag_id>`
- Stores state in `data/state.json`
- Exposes advanced settings for organisation ID, MQTT host, and publish interval

## Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

The app listens on `http://localhost:9999`.

## Notes

- If `paho-mqtt` is not installed, the UI still works and shows the publish failure reason.
- The initial `newSensorCreated` payload publishes with an empty `tags` array because tags are created in a later step.
