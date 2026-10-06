# Caches compacted trip status in Redis with a TTL (ARCHITECTURE.md §8.7).
import socket
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer  # noqa: E402

from src.utils.logger import get_logger  # noqa: E402
from streaming.flink.trip_status_job import consumer_config  # noqa: E402
from streaming.message import decode  # noqa: E402
from streaming.topics import env  # noqa: E402

logger = get_logger(__name__)


def redis_config() -> dict[str, Any]:
    return {
        "host": env("REDIS_CACHE_HOST", "redis-cache"),
        "port": int(env("REDIS_CACHE_PORT", "6379")),
        "password": env("REDIS_CACHE_PASSWORD", ""),
        "ttl": int(env("REDIS_CACHE_TTL_SECONDS", "3600")),
    }


def encode_command(*parts: bytes) -> bytes:
    command = f"*{len(parts)}\r\n".encode("ascii")
    for part in parts:
        command += f"${len(part)}\r\n".encode("ascii") + part + b"\r\n"
    return command


def read_line(sock: socket.socket) -> bytes:
    line = b""
    while not line.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("redis closed the connection")
        line += chunk
    return line


def redis_setex(
    host: str, port: int, password: str, key: str, ttl: int, value: str
) -> None:
    with socket.create_connection((host, port), timeout=5) as sock:
        if password:
            sock.sendall(encode_command(b"AUTH", password.encode("utf-8")))
            if not read_line(sock).startswith(b"+"):
                raise ConnectionError("redis auth rejected")
        sock.sendall(
            encode_command(
                b"SETEX",
                key.encode("utf-8"),
                str(ttl).encode("ascii"),
                value.encode("utf-8"),
            )
        )
        if not read_line(sock).startswith(b"+"):
            raise ConnectionError("redis SETEX rejected")


def cache_key(trip_id: str) -> str:
    return f"trip:{trip_id}:status"


def run() -> int:
    import json

    topic = env("KAFKA_TOPIC_TRIP_STATUS", "lh.trip_status.v1")
    redis = redis_config()
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
                redis_setex(
                    redis["host"],
                    redis["port"],
                    redis["password"],
                    cache_key(str(status["trip_id"])),
                    redis["ttl"],
                    json.dumps(status, default=str),
                )
            except (ValueError, KeyError, ConnectionError, socket.timeout) as exc:
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
