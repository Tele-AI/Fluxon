from logging import Logger
import logging
import os
from threading import Lock

fluxon_pyo3 = None

def build_format(color):
    reset = "\x1b[0m"
    underline = "\x1b[3m"
    # tips = "\x1b[35;3m"
    gray= "\x1b[90;20m"
    light_gray= "\x1b[37;20m"
    # very_light_gray = "\x1b[38;2;255;255;255m"
    # tips = very_light_gray
    tips = "\x1b[90m"
    bold = "\x1b[1m"  

    return (
        f"{color}[%(asctime)s] Fluxon %(levelname)s:{reset} {tips}(%(funcName)s:%(pathname)s:%(lineno)d){reset}"
        # f"{underline}(%(filename)s:%(lineno)d:%(name)s){reset}  "
        f"\n  {bold}%(message)s{reset}"
    )


class CustomFormatter(logging.Formatter):
    light_blue = "\x1b[94;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: build_format(light_blue),
        logging.INFO: build_format(green),
        logging.WARNING: build_format(yellow),
        logging.ERROR: build_format(bold_red),
        logging.CRITICAL: build_format(red),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_log_level() -> int:
    """
    Try to read FLUXON_LOG from environment variables.
    Could be:
    - DEBUG
    - INFO
    - WARNING
    - ERROR
    - CRITICAL

    If not found, defaults to INFO.
    """
    log_level = os.getenv("FLUXON_LOG", "INFO").upper()
    return getattr(logging, log_level, logging.INFO)


INITED_LOGGERS = []
INITED_LOGGERS_LOCK = Lock()

def init_logger(name: str = "fluxon") -> Logger:
    # Get the logger
    logger = logging.getLogger(name)

    # Clear any existing handlers
    logger.handlers.clear()

    # Prevent propagation to parent loggers
    logger.propagate = False

    # Add our custom handler
    ch = logging.StreamHandler()
    ch.setLevel(get_log_level())
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    # Keep logger enabled for DEBUG so file logging (if attached elsewhere) can always capture full details.
    logger.setLevel(logging.DEBUG)

    # Register to global list
    with INITED_LOGGERS_LOCK:
        if logger not in INITED_LOGGERS:
            INITED_LOGGERS.append(logger)

    return logger


def init_mq_file_logger(name: str = "fluxon_mq") -> Logger:
    """Initialize an MQ-specific logger with an optional file handler.

    Path rule aligned with Rust: third_party_logs_dir() is derived from owner large_file_paths.

    If fluxon_pyo3 is unavailable, falls back to console-only logging.
    """
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False

    # Always keep a console handler for debugging
    ch = logging.StreamHandler()
    ch.setLevel(get_log_level())
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    # Keep third-party file logs under one Fluxon-owned root so observability can discover them.
    log_dir = None
    try:
        from .tool import import_fluxon_pyo3_local

        fp = import_fluxon_pyo3_local()
        client = fp.KvClient()
        third_party_log_dir = client.third_party_logs_dir().unwrap("third_party_logs_dir failed")
        if isinstance(third_party_log_dir, str) and third_party_log_dir:
            log_dir = os.path.join(third_party_log_dir, "mq")
    except ImportError as exc:
        logger.warning("init_mq_file_logger: fluxon_pyo3 not available; MQ file logs disabled: %s", exc)
        log_dir = None
    except Exception as exc:  # noqa: BLE001
        # Keep usable in cases like invalid config or client init failure; use console-only logging.
        logger.warning("init_mq_file_logger: KvClient/third_party_logs_dir failed: %s", exc)
        log_dir = None

    if isinstance(log_dir, str) and log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            file_path = os.path.join(log_dir, f"{name}.log")
            fh = logging.FileHandler(file_path, mode="a", encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(CustomFormatter())
            logger.addHandler(fh)
        except Exception as exc:  # noqa: BLE001
            # Do not let file errors affect the main flow; keep console-only logging.
            logger.warning("init_mq_file_logger: file handler init failed: %s", exc)

    logger.setLevel(logging.DEBUG)

    with INITED_LOGGERS_LOCK:
        if logger not in INITED_LOGGERS:
            INITED_LOGGERS.append(logger)

    return logger

def update_log_level(level_str: str) -> None:
    """
    Update log level for all initialized loggers.
    """
    def get_level_id(level_str:str):
        valid_level = [
            'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
        ]
        level_str = level_str.upper()
        if level_str not in valid_level:
            raise ValueError("No valid logging level provided.")
        return level_str

    level_id=get_level_id(level_str)

    # Keep environment variable in sync so subsequent init_logger() calls
    # within the same process inherit the updated level instead of falling
    # back to the default INFO.
    os.environ["FLUXON_LOG"] = level_id
    with INITED_LOGGERS_LOCK:
        for logger in INITED_LOGGERS:
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    handler.setLevel(logging.DEBUG)
                else:
                    handler.setLevel(getattr(logging, level_id))

if __name__ == "__main__":
    logger = init_logger(__name__)
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")
