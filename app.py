import json
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from flask import Flask, jsonify, render_template, request

MQTT_IMPORT_ERROR = None
MQTT_VERSION = None

try:
    import paho.mqtt.client as mqtt
    try:
        import paho.mqtt
        MQTT_VERSION = getattr(paho.mqtt, "__version__", "unknown")
    except Exception:
        MQTT_VERSION = "unknown"
except ImportError as exc:
    mqtt = None
    MQTT_IMPORT_ERROR = str(exc)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATE_FILE = DATA_DIR / "state.json"

DEFAULT_ORG_ID = "69e9f78b199e523b543ad39e"
DEFAULT_MQTT_HOST = "mqtt.tasknitter.com"
DEFAULT_INTERVAL_SECONDS = 15
SOURCE_NAME = "fuxa"

SENSOR_PROFILES = {
    "temperature": {
        "label": "Temperature",
        "unit": "°C",
        "min": 18.0,
        "max": 95.0,
        "initial": (28.0, 42.0),
        "step": 1.2,
        "decimals": 2,
    },
    "humidity": {
        "label": "Humidity",
        "unit": "%",
        "min": 20.0,
        "max": 85.0,
        "initial": (35.0, 60.0),
        "step": 2.5,
        "decimals": 1,
    },
    "vibration": {
        "label": "Vibration",
        "unit": "mm/s",
        "min": 0.0,
        "max": 12.0,
        "initial": (1.5, 5.0),
        "step": 0.65,
        "decimals": 2,
    },
    "battery": {
        "label": "Battery",
        "unit": "%",
        "min": 0.0,
        "max": 100.0,
        "initial": (75.0, 98.0),
        "step": 1.0,
        "decimals": 0,
    },
    "pressure": {
        "label": "Pressure",
        "unit": "bar",
        "min": 0.8,
        "max": 12.0,
        "initial": (2.0, 4.5),
        "step": 0.25,
        "decimals": 2,
    },
}


def now_ms() -> int:
    return int(time.time() * 1000)


def generated_id(prefix: str) -> str:
    token = uuid4().hex
    return f"{prefix}_{token[:8]}-{token[8:16]}"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class MqttPublisher:
    def __init__(self, host: str) -> None:
        self.host = host
        self._client = None
        self._connected = False
        self._lock = threading.Lock()
        self._setup_client()

    def _setup_client(self) -> None:
        if mqtt is None:
            return
        client = mqtt.Client()
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        try:
            client.connect_async(self.host, 1883, 60)
            client.loop_start()
            self._client = client
        except Exception:
            self._client = None
            self._connected = False

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self._connected = reason_code == 0

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None) -> None:
        self._connected = False

    def set_host(self, host: str) -> None:
        with self._lock:
            self.host = host
            if self._client is not None:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass
            self._client = None
            self._connected = False
            self._setup_client()

    def publish(self, topic: str, payload: dict[str, Any]) -> dict[str, Any]:
        encoded_payload = json.dumps(payload)
        with self._lock:
            if self._client is None:
                reason = "paho-mqtt not installed"
                if MQTT_IMPORT_ERROR:
                    reason = f"{reason}: {MQTT_IMPORT_ERROR}"
                return {
                    "sent": False,
                    "reason": reason,
                    "mqtt_version": MQTT_VERSION,
                }
            try:
                result = self._client.publish(topic, encoded_payload)
                ok = result.rc == 0
                return {
                    "sent": ok,
                    "reason": None if ok else f"mqtt rc={result.rc}",
                    "mqtt_version": MQTT_VERSION,
                }
            except Exception as exc:
                return {
                    "sent": False,
                    "reason": str(exc),
                    "mqtt_version": MQTT_VERSION,
                }


class AppState:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.state = self._load_state()
        self.publisher = MqttPublisher(self.state["settings"]["mqtt_host"])

    def _default_state(self) -> dict[str, Any]:
        return {
            "settings": {
                "org_id": DEFAULT_ORG_ID,
                "mqtt_host": DEFAULT_MQTT_HOST,
                "publish_interval_seconds": DEFAULT_INTERVAL_SECONDS,
                "theme": "light",
            },
            "broadcast_enabled": True,
            "devices": [],
            "last_publish": None,
        }

    def _load_state(self) -> dict[str, Any]:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if not STATE_FILE.exists():
            state = self._default_state()
            STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
            return state
        loaded = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        default = self._default_state()
        default["settings"].update(loaded.get("settings", {}))
        default["broadcast_enabled"] = loaded.get("broadcast_enabled", True)
        default["devices"] = loaded.get("devices", [])
        default["last_publish"] = loaded.get("last_publish")
        return default

    def save(self) -> None:
        with self._lock:
            STATE_FILE.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self.state))

    def update_settings(self, org_id: str, mqtt_host: str, interval_seconds: int, theme: str) -> dict[str, Any]:
        with self._lock:
            self.state["settings"]["org_id"] = org_id.strip() or DEFAULT_ORG_ID
            self.state["settings"]["mqtt_host"] = mqtt_host.strip() or DEFAULT_MQTT_HOST
            self.state["settings"]["publish_interval_seconds"] = max(1, interval_seconds)
            self.state["settings"]["theme"] = theme if theme in {"light", "dark"} else "light"
            self.publisher.set_host(self.state["settings"]["mqtt_host"])
            self.save()
            return self.snapshot()

    def create_device(self, device_display_name: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        timestamp = now_ms()
        device_name = generated_id("d")
        device = {
            "device": device_name,
            "device_name": device_name,
            "device_display_name": device_display_name.strip(),
            "timestamp": timestamp,
            "source": SOURCE_NAME,
            "tags": [],
        }
        payload = {
            "device": device["device"],
            "device_name": device["device_name"],
            "timestamp": timestamp,
            "source": SOURCE_NAME,
            "tags": [],
            "device_display_name": device["device_display_name"],
        }
        topic = f'{self.state["settings"]["org_id"]}/newSensorCreated'
        publish_result = self.publisher.publish(topic, payload)
        with self._lock:
            self.state["devices"].append(device)
            self.state["last_publish"] = {
                "topic": topic,
                "payload": payload,
                "result": publish_result,
                "timestamp": timestamp,
            }
            self.save()
        return device, payload, publish_result

    def create_tag(self, device_name: str, sensor_display_name: str, sensor_type: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        profile = SENSOR_PROFILES[sensor_type]
        timestamp = now_ms()
        tag_name = generated_id("t")
        with self._lock:
            device = next((item for item in self.state["devices"] if item["device_name"] == device_name), None)
            if device is None:
                raise ValueError("Device not found")
            initial_value = random.uniform(*profile["initial"])
            tag = {
                "device": device["device"],
                "device_name": device["device_name"],
                "tag_id": tag_name,
                "tag_name": tag_name,
                "timestamp": timestamp,
                "source": SOURCE_NAME,
                "sensor_display_name": sensor_display_name.strip(),
                "sensor_type": sensor_type,
                "sensor_type_label": profile["label"],
                "unit": profile["unit"],
                "current_value": round(initial_value, profile["decimals"]),
                "last_published_at": 0,
            }
            payload = {
                "device": device["device"],
                "device_name": device["device_name"],
                "tag_id": tag["tag_id"],
                "tag_name": tag["tag_name"],
                "timestamp": timestamp,
                "source": SOURCE_NAME,
                "sensor_display_name": tag["sensor_display_name"],
            }
            topic = f'{self.state["settings"]["org_id"]}/{device["device_name"]}/newTag'
            publish_result = self.publisher.publish(topic, payload)
            device["tags"].append(tag)
            self.state["last_publish"] = {
                "topic": topic,
                "payload": payload,
                "result": publish_result,
                "timestamp": timestamp,
            }
            self.save()
            return tag, payload, publish_result

    def set_broadcast_enabled(self, enabled: bool) -> dict[str, Any]:
        with self._lock:
            self.state["broadcast_enabled"] = bool(enabled)
            self.save()
            return self.snapshot()

    def _next_sensor_value(self, tag: dict[str, Any]) -> float:
        profile = SENSOR_PROFILES[tag["sensor_type"]]
        current = float(tag["current_value"])
        drift = random.uniform(-profile["step"], profile["step"])
        midpoint = (profile["min"] + profile["max"]) / 2
        mean_reversion = (midpoint - current) * 0.04
        if tag["sensor_type"] == "battery":
            drift = random.uniform(-0.6, 0.1)
            mean_reversion = (85 - current) * 0.02
        next_value = clamp(current + drift + mean_reversion, profile["min"], profile["max"])
        return round(next_value, profile["decimals"])

    def publish_tag_value(self, device: dict[str, Any], tag: dict[str, Any]) -> None:
        timestamp = now_ms()
        tag["current_value"] = self._next_sensor_value(tag)
        tag["last_published_at"] = timestamp
        payload = {
            "device": device["device"],
            "device_name": device["device_name"],
            "tag_id": tag["tag_id"],
            "tag_name": tag["tag_name"],
            "value": tag["current_value"],
            "timestamp": timestamp,
            "source": SOURCE_NAME,
            "sensor_display_name": tag["sensor_display_name"],
        }
        topic = f'{self.state["settings"]["org_id"]}/{device["device_name"]}/{tag["tag_id"]}'
        publish_result = self.publisher.publish(topic, payload)
        self.state["last_publish"] = {
            "topic": topic,
            "payload": payload,
            "result": publish_result,
            "timestamp": timestamp,
        }

    def simulate_forever(self) -> None:
        while True:
            time.sleep(1)
            changed = False
            with self._lock:
                if not self.state.get("broadcast_enabled", True):
                    continue
                interval_ms = self.state["settings"]["publish_interval_seconds"] * 1000
                for device in self.state["devices"]:
                    for tag in device.get("tags", []):
                        if now_ms() - int(tag.get("last_published_at", 0)) >= interval_ms:
                            self.publish_tag_value(device, tag)
                            changed = True
                if changed:
                    self.save()


state = AppState()
app = Flask(__name__)


@app.get("/")
def index():
    return render_template("index.html", sensor_profiles=SENSOR_PROFILES)


@app.get("/api/state")
def get_state():
    snapshot = state.snapshot()
    snapshot["runtime"] = {
        "python_executable": sys.executable,
        "python_version": sys.version,
        "mqtt_import_error": MQTT_IMPORT_ERROR,
        "mqtt_version": MQTT_VERSION,
    }
    return jsonify(snapshot)


@app.post("/api/settings")
def update_settings():
    payload = request.get_json(force=True)
    snapshot = state.update_settings(
        org_id=payload.get("org_id", DEFAULT_ORG_ID),
        mqtt_host=payload.get("mqtt_host", DEFAULT_MQTT_HOST),
        interval_seconds=int(payload.get("publish_interval_seconds", DEFAULT_INTERVAL_SECONDS)),
        theme=payload.get("theme", "light"),
    )
    return jsonify(snapshot)


@app.post("/api/broadcast")
def update_broadcast():
    payload = request.get_json(force=True)
    snapshot = state.set_broadcast_enabled(bool(payload.get("broadcast_enabled", True)))
    return jsonify(snapshot)


@app.post("/api/devices")
def create_device():
    payload = request.get_json(force=True)
    display_name = (payload.get("device_display_name") or "").strip()
    if not display_name:
        return jsonify({"error": "device_display_name is required"}), 400
    device, published_payload, publish_result = state.create_device(display_name)
    return jsonify({"device": device, "published_payload": published_payload, "publish_result": publish_result}), 201


@app.post("/api/tags")
def create_tag():
    payload = request.get_json(force=True)
    device_name = (payload.get("device_name") or "").strip()
    sensor_display_name = (payload.get("sensor_display_name") or "").strip()
    sensor_type = (payload.get("sensor_type") or "").strip()
    if not device_name or not sensor_display_name or sensor_type not in SENSOR_PROFILES:
        return jsonify({"error": "device_name, sensor_display_name, and valid sensor_type are required"}), 400
    try:
        tag, published_payload, publish_result = state.create_tag(device_name, sensor_display_name, sensor_type)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify({"tag": tag, "published_payload": published_payload, "publish_result": publish_result}), 201


def start_background_worker() -> None:
    thread = threading.Thread(target=state.simulate_forever, daemon=True)
    thread.start()


start_background_worker()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9999, debug=True)
