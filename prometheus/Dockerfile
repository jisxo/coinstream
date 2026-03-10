FROM prom/alertmanager:v0.28.1 AS alertmanager

FROM prom/prometheus:v2.55.1

COPY --from=alertmanager /bin/alertmanager /bin/alertmanager
COPY prometheus/prometheus.render.yml /etc/prometheus/prometheus.yml
COPY docs/observability/alert_rules.yml /etc/prometheus/alert_rules.yml
COPY --chmod=755 prometheus/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
