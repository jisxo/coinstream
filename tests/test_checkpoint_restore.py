from typing import List

from confluent_kafka import TopicPartition

from processor.checkpoint_restore import CheckpointRestoreHandler


class _StubCounter:
    def __init__(self) -> None:
        self.count = 0

    def inc(self) -> None:
        self.count += 1


class _StubMetrics:
    def __init__(self) -> None:
        self.checkpoint_restores = _StubCounter()


class _StubConsumer:
    def __init__(self) -> None:
        self.assigned: List[List[TopicPartition]] = []

    def assign(self, partitions: List[TopicPartition]) -> None:
        self.assigned.append(partitions)


def test_checkpoint_restore_handler_applies_offsets_once() -> None:
    metrics = _StubMetrics()
    consumer = _StubConsumer()
    handler = CheckpointRestoreHandler(
        [
            {"topic": "trades_raw", "partition": 0, "offset": 101},
            {"topic": "trades_raw", "partition": 1, "offset": 202},
        ],
        metrics,
    )
    partitions = [
        TopicPartition("trades_raw", 0),
        TopicPartition("trades_raw", 1),
        TopicPartition("trades_raw", 2),
    ]

    handler.on_assign(consumer, partitions)

    assert len(consumer.assigned) == 1
    first_assignment = consumer.assigned[0]
    assert [(tp.topic, tp.partition, tp.offset) for tp in first_assignment] == [
        ("trades_raw", 0, 101),
        ("trades_raw", 1, 202),
        ("trades_raw", 2, partitions[2].offset),
    ]
    assert metrics.checkpoint_restores.count == 1

    handler.on_assign(consumer, partitions)

    assert len(consumer.assigned) == 2
    second_assignment = consumer.assigned[1]
    assert [(tp.topic, tp.partition, tp.offset) for tp in second_assignment] == [
        ("trades_raw", 0, partitions[0].offset),
        ("trades_raw", 1, partitions[1].offset),
        ("trades_raw", 2, partitions[2].offset),
    ]
    assert metrics.checkpoint_restores.count == 1


def test_checkpoint_restore_handler_without_saved_offsets_uses_default_assignment() -> None:
    metrics = _StubMetrics()
    consumer = _StubConsumer()
    handler = CheckpointRestoreHandler(None, metrics)
    partitions = [TopicPartition("trades_raw", 0)]

    handler.on_assign(consumer, partitions)

    assert len(consumer.assigned) == 1
    assert [(tp.topic, tp.partition, tp.offset) for tp in consumer.assigned[0]] == [
        ("trades_raw", 0, partitions[0].offset),
    ]
    assert metrics.checkpoint_restores.count == 0
