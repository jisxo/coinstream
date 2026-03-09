#!/usr/bin/env bash
set -euo pipefail

BROKERS=${BROKERS:-redpanda:9092}
TP=${TP:-12}

echo "Creating topics on $BROKERS (partitions=$TP)..."
rpk topic create trades_raw -p "$TP" -r 1 --brokers "$BROKERS" || true
rpk topic create trades_dlq -p 3 -r 1 --brokers "$BROKERS" || true
echo "Done."
