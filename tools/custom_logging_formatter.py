import logging

class Colors:
    grey = "\x1b[0;37m"
    green = "\x1b[1;32m"
    yellow = "\x1b[1;33m"
    red = "\x1b[1;31m"
    purple = "\x1b[1;35m"
    blue = "\x1b[1;34m"
    light_blue = "\x1b[1;36m"
    reset = "\x1b[0m"
    blink_red = "\x1b[5m\x1b[1;31m"
    bold_red = "\x1b[31;1m"
    # grey = "\x1b[38;20m"
    # yellow = "\x1b[33;20m"
    # red = "\x1b[31;20m"
    # reset = "\x1b[0m"

class CustomFormatter(logging.Formatter):


    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: Colors.grey + format +Colors. reset,
        logging.INFO: Colors.grey + format + Colors.reset,
        logging.WARNING: Colors.yellow + format + Colors.reset,
        logging.ERROR: Colors.red + format + Colors.reset,
        logging.CRITICAL: Colors.bold_red + format + Colors.reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)