from typing import Dict, List, Optional, Tuple

from confluent_kafka import Consumer, TopicPartition

from processor.metrics import ProcessorMetrics


class CheckpointRestoreHandler:
    def __init__(self, saved_offsets: Optional[List[Dict[str, int]]], metrics: ProcessorMetrics) -> None:
        self._offsets_map: Dict[Tuple[str, int], int] = {
            (entry["topic"], entry["partition"]): entry["offset"]
            for entry in (saved_offsets or [])
        }
        self._metrics = metrics
        self._applied = False

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        if self._applied or not self._offsets_map:
            consumer.assign(partitions)
            return

        restored_partitions: List[TopicPartition] = []
        restored_count = 0
        for tp in partitions:
            offset = self._offsets_map.get((tp.topic, tp.partition))
            if offset is None:
                restored_partitions.append(tp)
                continue
            restored_partitions.append(TopicPartition(tp.topic, tp.partition, offset))
            restored_count += 1

        consumer.assign(restored_partitions)
        self._applied = True
        self._metrics.checkpoint_restores.inc()
        print(
            f"[checkpoint] restored offsets for {restored_count}/{len(partitions)} assigned partitions",
            flush=True,
        )
