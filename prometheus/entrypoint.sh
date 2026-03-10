#!/bin/sh
set -eu

mkdir -p /tmp/alertmanager

if [ -n "${ALERT_EMAIL_TO:-}" ] && [ -n "${SMTP_FROM:-}" ] && [ -n "${SMTP_AUTH_USERNAME:-}" ] && [ -n "${SMTP_AUTH_PASSWORD:-}" ]; then
  : "${SMTP_SMARTHOST:=smtp.gmail.com:587}"
  : "${SMTP_REQUIRE_TLS:=true}"
  cat > /tmp/alertmanager.yml <<EOF
global:
  smtp_smarthost: '${SMTP_SMARTHOST}'
  smtp_from: '${SMTP_FROM}'
  smtp_auth_username: '${SMTP_AUTH_USERNAME}'
  smtp_auth_password: '${SMTP_AUTH_PASSWORD}'
  smtp_require_tls: ${SMTP_REQUIRE_TLS}

route:
  receiver: 'email'
  group_by: ['alertname', 'job']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 2h

receivers:
  - name: 'email'
    email_configs:
      - to: '${ALERT_EMAIL_TO}'
        send_resolved: true
EOF
else
  cat > /tmp/alertmanager.yml <<'EOF'
route:
  receiver: 'null'
receivers:
  - name: 'null'
EOF
fi

alertmanager \
  --config.file=/tmp/alertmanager.yml \
  --storage.path=/tmp/alertmanager \
  --web.listen-address=127.0.0.1:9093 &

exec /bin/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --web.console.libraries=/usr/share/prometheus/console_libraries \
  --web.console.templates=/usr/share/prometheus/consoles
