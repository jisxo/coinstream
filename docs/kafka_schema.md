# Kafka topics & schema

이 프로젝트는 Redpanda(Kafka API)를 통해 실시간 aggTrade 메시지를 전달합니다. 주요 설정은 `shared/config.py`/`.env.example`에서 확인할 수 있습니다.

## 브로커
- `redpanda:9092` (Docker compose 내 `redpanda` 서비스)

## 토픽
1. `trades_raw` – ingest가 전달한 실시간 aggTrade 이벤트(메인 파이프라인)
2. `trades_dlq` – ingest에서 처리 중 예외가 발생했을 때 보관하는 DLQ

## 메시지 스키마 (`ingest/normalize_event` 결과)
```json
{
  "event_time_ms": 1690012345678,   // Binance aggTrade의 이벤트 타임
  "ingest_time_ms": 1690012345680,  // ingest 측 타임스탬프
  "symbol": "BTCUSDT",
  "agg_trade_id": 123456789,
  "price": 42900.5,
  "quantity": 0.01,
  "is_buyer_maker": 1,
  "stream": "btcusdt@aggTrade"
}
```

### 소비자 위치
- Processor는 `processor/app.py`에서 `group.id`로 consumer group을 설정하고 `auto.offset.reset`을 `earliest`로, `enable.auto.commit`은 `True`로 유지합니다.
- Checkpoint 기능은 소비 오프셋과 상태를 `processor/checkpoint.py`로 저장하기 때문에 다운/재시작해도 `offsets` 재설정과 dedup state 복원이 가능합니다.
