import logging
import os
from datetime import datetime

curr_time = str(datetime.now().strftime("%d_%m_%Y_%H%M"))

# Create logs directory if it doesn't exist
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set up logging
log_file = os.path.join(log_dir, f"{curr_time}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


# Function to get the logger
def get_logger(name):
    return logging.getLogger(name)
