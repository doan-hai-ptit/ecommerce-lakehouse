import os
import sys

# Ensure parent processing/ directory is in sys.path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.bronze_to_silver.orchestrator import main

if __name__ == "__main__":
    main()
