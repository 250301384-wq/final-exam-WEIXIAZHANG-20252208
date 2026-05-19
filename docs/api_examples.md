# API Examples

Start the API:

```bash
python api/app.py
```

Health check:

```bash
curl -s http://localhost:5000/api/v1/health | python3 -m json.tool
```

Expected status: `200`.

List sensors:

```bash
curl -s http://localhost:5000/api/v1/sensors | python3 -m json.tool
```

Expected status: `200`.

Latest Kafka reading:

```bash
curl -s http://localhost:5000/api/v1/sensors/temperature/latest | python3 -m json.tool
```

Expected status: `200` if a temperature message exists in Kafka, otherwise `404`.

Daily stats:

```bash
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool
```

Expected status: `200` when curated Parquet data exists, `400` for invalid `days`, or `404` when no stats are available.

Recent anomalies:

```bash
curl -s "http://localhost:5000/api/v1/anomalies?sensor=temperature&limit=10" | python3 -m json.tool
```

Expected status: `200` for valid query parameters or `400` for invalid `sensor` / `limit` query parameters.

Publish a reading:

```bash
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":36.8,"unit":"C","source":"site-A-rack-12"}' \
  | python3 -m json.tool
```

Expected status: `201`.

Malformed POST body:

```bash
curl -s -i -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":'
```

Expected status: `400`.

Semantically invalid POST body:

```bash
curl -s -i -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":500,"unit":"C","source":"site-A-rack-12"}'
```

Expected status: `422`.

Unknown path sensor:

```bash
curl -s -i http://localhost:5000/api/v1/sensors/noise/latest
```

Expected status: `404`.

