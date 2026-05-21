# utils/logger_setup.py

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
import config_loader  # Import the central config loader

try:
    from colorama import Fore, Style, init as colorama_init
    COLORAMA_AVAILABLE = True
    colorama_init(autoreset=True)
    # Use bright yellow which appears more orange
    ORANGE = Fore.YELLOW
except ImportError:
    COLORAMA_AVAILABLE = False
    # Fallback ANSI color codes if colorama is not available
    class Fore:
        RED = '\033[91m'
        YELLOW = '\033[93m'  # Bright yellow/orange
        RESET = '\033[0m'
    ORANGE = Fore.YELLOW
    class Style:
        RESET_ALL = '\033[0m'

# --- Global logger instance ---
logger = None

# Supported migration item IDs for per-item log levels
MIGRATION_LOG_LEVEL_ITEMS = frozenset(
    {'groups', 'users', 'preflight', 'folders', 'datamodels', 'datasecurity', 'dashboards', 'blox', 'custom_code'}
)
LOG_LEVEL_MAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}


class MigrationLogLevelFilter(logging.Filter):
    """
    Filter that applies per-migration-item log levels.
    Records with migration_item in extra are filtered by migration_log_levels;
    records without it pass through (handler level applies).
    base_level: when a migration item has no override (inherit), use this level so we don't
    pass DEBUG for all items when the handler was lowered to DEBUG for one item.
    """
    def __init__(self, base_level=logging.INFO):
        super().__init__()
        self.base_level = base_level

    def filter(self, record):
        migration_item = getattr(record, 'migration_item', None)
        if migration_item is None:
            return record.levelno >= self.base_level
        if not config_loader.settings:
            return True
        mll = config_loader.settings.get('migration_log_levels') or {}
        level_str = mll.get(migration_item)
        if level_str is None or str(level_str).lower() in ('inherit', 'default', ''):
            return record.levelno >= self.base_level
        level_num = LOG_LEVEL_MAP.get(str(level_str).upper(), logging.INFO)
        return record.levelno >= level_num


def get_logger_for_migration(item: str):
    """
    Returns a LoggerAdapter bound to the given migration item.
    Log messages will be filtered by migration_log_levels[item] if configured.
    """
    if logger is None:
        return logging.getLogger(__name__)
    return logging.LoggerAdapter(logger, extra={'migration_item': item})


def init_logging():
    """
    Initializes and configures the global logger instance based on settings
    from the centrally loaded config_loader.
    """
    global logger

    # Ensure settings are loaded before trying to use them
    if not config_loader.settings:
        print("Configuration not loaded. Cannot initialize logger.", file=sys.stderr)
        # Create a basic logger that just prints to stderr as a fallback
        logger = logging.getLogger(__name__)
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        logger.warning("Using fallback logger because config was not loaded.")
        return logger

    # --- Get settings from config_loader ---
    file_log_level_str = config_loader.settings.get('file_logLevel', 'INFO').upper()
    console_log_level_str = config_loader.settings.get('console_logLevel', 'INFO').upper()
    override_logs = config_loader.settings.get('override_logs', False)

    # --- Per-run log file (when started from server with MIGRATION_LOG_FILE env) ---
    per_run_log = os.environ.get('MIGRATION_LOG_FILE')
    if per_run_log and os.path.isabs(per_run_log):
        log_file_path = per_run_log
        log_dir = os.path.dirname(log_file_path)
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory at {log_dir}: {e}", file=sys.stderr)
        file_mode = 'w'  # Each run gets a fresh file
    else:
        log_file_name = config_loader.settings.get('logFileName', 'migration.log')
        # --- Determine log directory ---
        # Use the application_path from config_loader, which is aware of the bundled state
        log_dir = os.path.join(config_loader.application_path, 'logs')
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory at {log_dir}: {e}", file=sys.stderr)
            # Fallback to a local directory if creating the primary one fails
            log_dir = os.path.join(os.getcwd(), 'logs')
            os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, log_file_name)
        file_mode = 'w' if override_logs else 'a'

    # --- Map string levels to logging constants ---
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    file_log_level = log_levels.get(file_log_level_str, logging.INFO)
    console_log_level = log_levels.get(console_log_level_str, logging.INFO)
    file_log_level_base = file_log_level
    console_log_level_base = console_log_level

    # If any migration_log_levels item is more verbose than the global level, lower the handler
    # level so those records can reach the MigrationLogLevelFilter (which then filters per-item).
    mll = config_loader.settings.get('migration_log_levels') or {}
    for _k, _v in mll.items():
        if _v and str(_v).strip().lower() not in ('inherit', 'default', ''):
            _num = LOG_LEVEL_MAP.get(str(_v).upper(), logging.INFO)
            if _num < file_log_level:
                file_log_level = _num
            if _num < console_log_level:
                console_log_level = _num

    # --- Configure the global logger ---
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all messages

    # --- Clear existing handlers to avoid duplication ---
    if logger.hasHandlers():
        logger.handlers.clear()

    # --- Create Console Handler with Color Formatter ---
    class ColoredFormatter(logging.Formatter):
        """Custom formatter that adds colors to log levels"""
        def format(self, record):
            # Get the default formatted message
            log_message = super().format(record)
            
            # Add colors based on log level
            if record.levelno == logging.WARNING:
                # Orange color for warnings
                if COLORAMA_AVAILABLE:
                    return f"{ORANGE}{log_message}{Style.RESET_ALL}"
                else:
                    return f"{ORANGE}{log_message}{Fore.RESET}"
            elif record.levelno == logging.ERROR:
                # Red color for errors
                if COLORAMA_AVAILABLE:
                    return f"{Fore.RED}{log_message}{Style.RESET_ALL}"
                else:
                    return f"{Fore.RED}{log_message}{Fore.RESET}"
            elif record.levelno == logging.CRITICAL:
                # Red color for critical errors too
                if COLORAMA_AVAILABLE:
                    return f"{Fore.RED}{log_message}{Style.RESET_ALL}"
                else:
                    return f"{Fore.RED}{log_message}{Fore.RESET}"
            else:
                # No color for INFO, DEBUG, etc.
                return log_message
    
    # Use stderr when stdout is not a TTY (e.g. MCP stdio) so we don't corrupt JSON-RPC on stdout
    console_stream = sys.stderr if not sys.stdout.isatty() else sys.stdout
    console_handler = logging.StreamHandler(console_stream)
    console_handler.setLevel(console_log_level)
    console_handler.addFilter(MigrationLogLevelFilter(base_level=console_log_level_base))
    console_formatter = ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # --- Create File Handler ---
    # file_mode set above: 'w' for per-run or override_logs, 'a' otherwise
    # Use RotatingFileHandler for better log management
    file_handler = RotatingFileHandler(
        log_file_path,
        mode=file_mode,
        maxBytes=5*1024*1024,  # 5 MB
        backupCount=2,
        encoding='utf-8'
    )
    file_handler.setLevel(file_log_level)
    file_handler.addFilter(MigrationLogLevelFilter(base_level=file_log_level_base))
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    logger.info("Logger initialized successfully.")
    logger.debug(f"File log level: {file_log_level_str}, Console log level: {console_log_level_str}")
    logger.debug(f"Log file path: {log_file_path}")

    return logger