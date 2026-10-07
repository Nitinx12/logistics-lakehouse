# Replays Mongo delivery events into Kafka.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.producer.replay import main

if __name__ == "__main__":
    sys.exit(main())
