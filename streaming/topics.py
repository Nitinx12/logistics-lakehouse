# Declares Kafka topics and creates them idempotently (ARCHITECTURE.md §8.2).
import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


def topic_definitions() -> dict[str, dict[str, str]]:
    return {
        env("KAFKA_TOPIC_DELIVERY_EVENTS", "lh.delivery_events.v1"): {
            "key": "trip_id",
            "partitions": env("KAFKA_PARTITIONS_DELIVERY_EVENTS", "6"),
            "cleanup": "delete",
            "retention_ms": env("KAFKA_RETENTION_MS_DELIVERY_EVENTS", "604800000"),
        },
        env("KAFKA_TOPIC_DELIVERY_EVENTS_DLQ", "lh.delivery_events.v1.dlq"): {
            "key": "trip_id",
            "partitions": "1",
            "cleanup": "delete",
            "retention_ms": env("KAFKA_RETENTION_MS_DLQ", "2592000000"),
        },
        env("KAFKA_TOPIC_TRIP_STATUS", "lh.trip_status.v1"): {
            "key": "trip_id",
            "partitions": env("KAFKA_PARTITIONS_DELIVERY_EVENTS", "6"),
            "cleanup": "compact",
            "retention_ms": "-1",
        },
        env("KAFKA_TOPIC_DELIVERY_ALERTS", "lh.delivery_alerts.v1"): {
            "key": "trip_id",
            "partitions": "3",
            "cleanup": "delete",
            "retention_ms": env("KAFKA_RETENTION_MS_DELIVERY_EVENTS", "604800000"),
        },
    }


def main() -> int:
    from confluent_kafka.admin import AdminClient, NewTopic

    from src.utils.logger import get_logger

    logger = get_logger(__name__)
    admin = AdminClient(
        {"bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}
    )
    definitions = topic_definitions()
    existing = set(admin.list_topics(timeout=10).topics.keys())
    pending = [
        NewTopic(
            name,
            num_partitions=int(spec["partitions"]),
            config={
                "cleanup.policy": spec["cleanup"],
                **(
                    {"retention.ms": spec["retention_ms"]}
                    if spec["retention_ms"] != "-1"
                    else {}
                ),
            },
        )
        for name, spec in definitions.items()
        if name not in existing
    ]
    if not pending:
        logger.info("topics ready count=%s", len(definitions))
        return 0
    for name, future in admin.create_topics(pending).items():
        try:
            future.result(timeout=30)
            logger.info("topic created name=%s", name)
        except Exception as exc:  # noqa: BLE001
            if "ALREADY_EXISTS" not in str(exc):
                raise
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
