import logging
import os
from datetime import datetime

def setup_logging():
    """Configure logging to file and console."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()