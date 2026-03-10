FROM prom/alertmanager:v0.28.1

COPY alertmanager/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
