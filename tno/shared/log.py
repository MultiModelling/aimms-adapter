import logging
import logging.config

from typing import List, Any
import structlog
from tno.aimms_adapter.settings import EnvSettings
from structlog.threadlocal import merge_threadlocal

timestamper = structlog.processors.TimeStamper(fmt="iso")
shared_processors: List[Any] = [
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    timestamper,
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
]

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.dev.ConsoleRenderer(colors=True),
                "foreign_pre_chain": shared_processors,
            },
            "json": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.processors.JSONRenderer(sort_keys=True),
                "foreign_pre_chain": shared_processors,
            },
        },
        "handlers": {
            "default": {
                "level": "INFO" if EnvSettings.is_production() else "DEBUG",
                "class": "logging.StreamHandler",
                # Also output json in test because output in VSCode doesn't handle it.
                "formatter": "colored" if EnvSettings.env() == "dev" else "json",
            },
            "file": {
                "level": "DEBUG",
                "class": "logging.handlers.WatchedFileHandler",
                "filename": "opera_adapter.log",
            },
        },
        "loggers": {
            "": {"handlers": ["default", "file"], "level": "INFO"},
            "alembic": {"handlers": ["default"], "level": "INFO"},
            "tno": {
                "handlers": ["default", "file"],
                "level": "DEBUG",  # "INFO" if EnvSettings.is_production() else "DEBUG",
                "propagate": False,
            },
        },
    }
)


structlog.configure(
    processors=[merge_threadlocal]
    + shared_processors
    + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


def get_logger(name):
    return structlog.get_logger(name)
