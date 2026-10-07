# Caches compacted trip status in Redis with a TTL (ARCHITECTURE.md §8.7).
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer  # noqa: E402
from redis import Redis  # noqa: E402
from redis.exceptions import RedisError  # noqa: E402

from src.utils.logger import get_logger  # noqa: E402
from streaming.flink.trip_status_job import consumer_config  # noqa: E402
from streaming.message import decode  # noqa: E402
from streaming.topics import env  # noqa: E402

logger = get_logger(__name__)


def redis_client() -> Redis:
    return Redis(
        host=env("REDIS_CACHE_HOST", "redis-cache"),
        port=int(env("REDIS_CACHE_PORT", "6379")),
        password=env("REDIS_CACHE_PASSWORD", "") or None,
        socket_timeout=5,
        decode_responses=True,
    )


def cache_ttl() -> int:
    return int(env("REDIS_CACHE_TTL_SECONDS", "3600"))


def cache_key(trip_id: str) -> str:
    return f"trip:{trip_id}:status"


def run() -> int:
    import json

    topic = env("KAFKA_TOPIC_TRIP_STATUS", "lh.trip_status.v1")
    ttl = cache_ttl()
    client = redis_client()
    consumer = Consumer(
        consumer_config(env("CONSUMER_GROUP_REDIS_SINK", "lh-redis-sink"))
    )
    consumer.subscribe([topic])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("consumer error %s", message.error())
                continue
            try:
                status = decode(message.value())
                client.setex(
                    cache_key(str(status["trip_id"])),
                    ttl,
                    json.dumps(status, default=str),
                )
            except (ValueError, KeyError, RedisError) as exc:
                logger.warning(
                    "redis sink skipped offset=%s error=%s", message.offset(), exc
                )
            consumer.commit(message=message)
    except KeyboardInterrupt:
        return 0
    finally:
        consumer.close()


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001
        logger.error("redis sink failed error=%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
