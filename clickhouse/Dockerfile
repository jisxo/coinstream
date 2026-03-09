FROM clickhouse/clickhouse-server:24.6

# Keep custom user config, but avoid initdb scripts on Render where
# entrypoint init shutdown can fail due process permission constraints.
COPY clickhouse/users.d/default-user.xml /etc/clickhouse-server/users.d/default-user.xml
