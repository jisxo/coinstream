#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="${1:-cdc/infra/debezium/register_connector.json}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Connector config not found: ${CONFIG_FILE}" >&2
  exit 1
fi

CONNECTOR_NAME="$(python3 - <<'PY' "${CONFIG_FILE}"
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    payload = json.load(f)
print(payload["name"])
PY
)"

echo "[cdc] connector=${CONNECTOR_NAME}"
if curl -fsS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null; then
  echo "[cdc] connector exists, updating config"
  python3 - <<'PY' "${CONFIG_FILE}" > /tmp/cdc_connector_config.json
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    payload = json.load(f)
print(json.dumps(payload["config"]))
PY
  curl -fsS -X PUT \
    -H "Content-Type: application/json" \
    --data @/tmp/cdc_connector_config.json \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
else
  echo "[cdc] connector not found, creating"
  curl -fsS -X POST \
    -H "Content-Type: application/json" \
    --data @"${CONFIG_FILE}" \
    "${CONNECT_URL}/connectors"
fi

echo
echo "[cdc] connector registration done"
