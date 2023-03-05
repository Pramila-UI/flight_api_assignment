import logging

logging.basicConfig(
    filename="logs/flight_logger.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)
