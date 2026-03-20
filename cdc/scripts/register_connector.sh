#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "$#" -gt 0 ]]; then
  "${SCRIPT_DIR}/register_debezium_connector.sh" "$@"
else
  "${SCRIPT_DIR}/register_debezium_connector.sh" "cdc/infra/debezium/register_connector.json"
fi
