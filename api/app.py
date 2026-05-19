import logging
import os
import time
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

try:
    from .kafka_utils import BOOTSTRAP_SERVERS, TOPIC, latest_reading, publish_reading
    from .lake_utils import DATALAKE_ROOT, daily_stats, recent_anomalies
except ImportError:
    from kafka_utils import BOOTSTRAP_SERVERS, TOPIC, latest_reading, publish_reading
    from lake_utils import DATALAKE_ROOT, daily_stats, recent_anomalies


SENSORS = {
    "temperature": {"unit": "C", "plausible": (-40.0, 85.0)},
    "humidity": {"unit": "%", "plausible": (0.0, 100.0)},
    "pressure": {"unit": "hPa", "plausible": (870.0, 1100.0)},
}


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["BOOTSTRAP_SERVERS"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS)
    app.config["KAFKA_TOPIC"] = os.getenv("KAFKA_TOPIC", TOPIC)
    app.config["DATALAKE_ROOT"] = os.getenv("DATALAKE_ROOT", DATALAKE_ROOT)
    logging.basicConfig(level=logging.INFO)

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException):
        return error_response(error.code or 500, error.name, error.description)

    @app.errorhandler(Exception)
    def handle_unexpected_error(error: Exception):
        app.logger.exception("Unhandled API error")
        return error_response(500, "internal_server_error", "An unexpected server-side error occurred.")

    @app.get("/api/v1/health")
    def health():
        return success_response({"status": "ok", "service": "aerosense-api"})

    @app.get("/api/v1/sensors")
    def sensors():
        return success_response({"sensors": list(SENSORS.keys())})

    @app.get("/api/v1/sensors/<sensor_type>/latest")
    def latest(sensor_type: str):
        validation = validate_sensor(sensor_type)
        if validation:
            return validation
        reading = latest_reading(
            sensor_type,
            bootstrap_servers=app.config["BOOTSTRAP_SERVERS"],
            topic=app.config["KAFKA_TOPIC"],
        )
        if reading is None:
            return error_response(404, "not_found", f"No Kafka reading found for sensor '{sensor_type}'.")
        return success_response(reading)

    @app.get("/api/v1/sensors/<sensor_type>/stats")
    def stats(sensor_type: str):
        validation = validate_sensor(sensor_type)
        if validation:
            return validation
        days_value = request.args.get("days", "7")
        days = parse_int(days_value, "days", 1, 90)
        if isinstance(days, tuple):
            return days
        rows = daily_stats(sensor_type, days, app.config["DATALAKE_ROOT"])
        if rows is None:
            return error_response(404, "not_found", "Curated data lake zone is not available.")
        if not rows:
            return error_response(404, "not_found", f"No statistics found for sensor '{sensor_type}'.")
        return success_response({"sensor": sensor_type, "days": days, "statistics": rows})

    @app.get("/api/v1/anomalies")
    def anomalies():
        sensor_type = request.args.get("sensor")
        if sensor_type:
            validation = validate_sensor(sensor_type)
            if validation:
                return validation
        limit = parse_int(request.args.get("limit", "50"), "limit", 1, 1000)
        if isinstance(limit, tuple):
            return limit
        rows = recent_anomalies(sensor_type, limit, app.config["DATALAKE_ROOT"])
        if rows is None:
            return error_response(404, "not_found", "Curated data lake zone is not available.")
        return success_response({"items": rows, "count": len(rows), "limit": limit})

    @app.post("/api/v1/readings")
    def readings():
        if not request.is_json:
            return error_response(400, "bad_request", "Request body must be JSON.")
        payload = request.get_json(silent=True)
        if payload is None:
            return error_response(400, "bad_request", "Malformed JSON body.")
        event_or_error = validate_reading_payload(payload)
        if isinstance(event_or_error, tuple):
            return event_or_error
        metadata = publish_reading(
            event_or_error,
            bootstrap_servers=app.config["BOOTSTRAP_SERVERS"],
            topic=app.config["KAFKA_TOPIC"],
        )
        return success_response({"event": event_or_error, "kafka": metadata}, status=201)

    return app


def success_response(data: Dict[str, Any], status: int = 200):
    return jsonify({"status": "success", "data": data, "error": None}), status


def error_response(status: int, code: str, message: str):
    return jsonify({"status": "error", "data": None, "error": {"code": code, "message": message}}), status


def validate_sensor(sensor_type: str):
    if sensor_type not in SENSORS:
        return error_response(422, "invalid_sensor", f"Unsupported sensor type '{sensor_type}'.")
    return None


def parse_int(raw_value: str, name: str, minimum: int, maximum: int):
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return error_response(400, "bad_request", f"Query parameter '{name}' must be an integer.")
    if not minimum <= value <= maximum:
        return error_response(400, "bad_request", f"Query parameter '{name}' must be between {minimum} and {maximum}.")
    return value


def validate_reading_payload(payload: Dict[str, Any]):
    required = {"sensor", "value", "source"}
    missing = sorted(required - set(payload))
    if missing:
        return error_response(400, "bad_request", f"Missing required field(s): {', '.join(missing)}.")

    sensor_type = payload.get("sensor")
    if not isinstance(sensor_type, str):
        return error_response(400, "bad_request", "Field 'sensor' must be a string.")
    if sensor_type not in SENSORS:
        return error_response(422, "invalid_sensor", f"Unsupported sensor type '{sensor_type}'.")

    try:
        value = float(payload.get("value"))
    except (TypeError, ValueError):
        return error_response(400, "bad_request", "Field 'value' must be numeric.")

    low, high = SENSORS[sensor_type]["plausible"]
    if not low <= value <= high:
        return error_response(422, "invalid_value", f"Value {value} is outside the plausible physical range.")

    source = payload.get("source")
    if not isinstance(source, str) or not source.strip():
        return error_response(400, "bad_request", "Field 'source' must be a non-empty string.")

    expected_unit = SENSORS[sensor_type]["unit"]
    unit = payload.get("unit", expected_unit)
    if unit != expected_unit:
        return error_response(422, "invalid_unit", f"Sensor '{sensor_type}' must use unit '{expected_unit}'.")

    timestamp = payload.get("timestamp", int(time.time() * 1000))
    if not isinstance(timestamp, int):
        return error_response(400, "bad_request", "Field 'timestamp' must be an epoch millisecond integer.")

    event = {
        "sensor": sensor_type,
        "value": round(value, 2),
        "unit": expected_unit,
        "timestamp": timestamp,
        "source": source.strip(),
        "anomaly": compute_anomaly(sensor_type, value),
    }
    return event


def compute_anomaly(sensor_type: str, value: float) -> bool:
    return (
        (sensor_type == "temperature" and value > 35.0)
        or (sensor_type == "humidity" and value > 90.0)
        or (sensor_type == "pressure" and (value < 990.0 or value > 1030.0))
    )


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
