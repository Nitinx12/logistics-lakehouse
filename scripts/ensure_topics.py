# Creates Kafka topics with the section 8.2 layout.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.topics import main

if __name__ == "__main__":
    sys.exit(main())
