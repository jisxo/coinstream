# Prometheus + Grafana (Render)

## 1) Prometheus 서비스 생성 (Prometheus + Alertmanager 동시 실행)
- Type: Private Service
- Runtime: Dockerfile from Git
- Dockerfile Path: `prometheus/Dockerfile`
- Internal Port: `9090`
- Environment Variables (이메일 알림용):
  - `SMTP_SMARTHOST` (예: `smtp.gmail.com:587`)
  - `SMTP_FROM`
  - `SMTP_AUTH_USERNAME`
  - `SMTP_AUTH_PASSWORD` (앱 비밀번호)
  - `ALERT_EMAIL_TO`
  - `SMTP_REQUIRE_TLS=true`

> `prometheus/prometheus.render.yml`의 타겟명(`coinstream-ingest`, `coinstream-processor`, `coinstream-redpanda`)은 Render 서비스명과 같아야 합니다.

## 2) Grafana 서비스 생성
- Type: Web Service (또는 Private Service + 내부 접근)
- Runtime: Dockerfile from Git
- Dockerfile Path: `grafana/Dockerfile`
- Internal Port: `3000`
- Environment Variables:
  - `PROMETHEUS_URL=http://coinstream-prometheus:9090`
  - `GF_SECURITY_ADMIN_USER=admin`
  - `GF_SECURITY_ADMIN_PASSWORD=<secure-password>`

## 3) 검증
1. Prometheus targets에서 `ingest`, `processor`, `redpanda`가 `UP`인지 확인
2. Prometheus rules API에서 alert rule이 로드됐는지 확인
3. Grafana 로그인 후 `CoinStream / CoinStream Overview` 대시보드가 보이는지 확인
4. 패널 값이 비면 `KAFKA_BROKERS`, `METRICS_PORT`, 서비스명/타겟명을 우선 점검
