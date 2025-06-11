import logging
import os


LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def getLogger(filename=None, name=None):

    level = "DEBUG"
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(LOGGING_FORMAT)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if filename is not None:
            log_dir = os.path.join(os.getcwd(), "logs")

            # Create a log directory if it does not already exists
            if not os.path.exists(log_dir):
                os.mkdir(log_dir)

            log_file_path = os.path.join(log_dir, filename)

            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger
