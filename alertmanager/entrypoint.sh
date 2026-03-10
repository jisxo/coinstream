#!/bin/sh
set -eu

: "${SMTP_SMARTHOST:=smtp.gmail.com:587}"
: "${SMTP_FROM:=}"
: "${SMTP_AUTH_USERNAME:=}"
: "${SMTP_AUTH_PASSWORD:=}"
: "${SMTP_REQUIRE_TLS:=true}"
: "${ALERT_EMAIL_TO:=}"

cat > /etc/alertmanager/alertmanager.yml <<EOF
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

exec alertmanager \
  --config.file=/etc/alertmanager/alertmanager.yml \
  --storage.path=/alertmanager
