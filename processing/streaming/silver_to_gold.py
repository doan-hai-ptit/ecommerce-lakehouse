import os
import sys

# Ensure parent processing/ directory is in sys.path so we can import core/jobs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.silver_to_gold.orchestrator import main

if __name__ == "__main__":
    main()
