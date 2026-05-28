import os
import sys

# Allow running this file directly from /app/jobs inside the Spark container.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.silver_to_gold import main


if __name__ == "__main__":
    main()
