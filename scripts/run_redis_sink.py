# Runs the Redis live-state sink.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.sinks.redis_sink import main

if __name__ == "__main__":
    sys.exit(main())
