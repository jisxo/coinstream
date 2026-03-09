# Parquet replay script

`python3 scripts/replay_parquet.py`를 실행하면 MinIO에 저장된 Parquet 파일을 Kafka `replay` 토픽으로 재전송할 수 있습니다. 현재 Parquet은 `processor/app.py`에서 생성된 OHLCV/volatility row이므로, 재생 시에는 동일한 구조의 JSON을 새로운 topic으로 보내서 downstream 테스트나 reprocessing 시나리오를 구성할 수 있습니다.

기본 환경변수
- `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`: MinIO 접속 정보
- `S3_PREFIX`: Parquet이 쌓인 경로 (기본 `ohlc_1m`)
- `KAFKA_BROKERS`: Kafka 브로커(기본 `redpanda:9092`)
- `REPLAY_TOPIC`: 재전송할 토픽 (기본 `TOPIC_TRADES`)

옵션
```
python3 scripts/replay_parquet.py --prefix ohlc_1m/dt=2024-01-01 --limit 5
```

`--limit`을 주면 지정 수량만 재생하므로 개발 중 반복 테스트에 유용합니다.
