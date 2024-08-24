# =====================
# Logging Configuration
# =====================
# The logging library is used to provide a flexible framework for emitting log messages from the application.
#
# Configuration:
# - `logging.basicConfig()` is used to set up the default logging configuration.
#   - `level=logging.INFO`: Sets the logging threshold to INFO level. Only messages with severity level INFO and higher (WARNING, ERROR, CRITICAL) will be logged. DEBUG messages will be ignored.
#   - `format='%(asctime)s - %(levelname)s - %(message)s'`: Specifies the format for log messages.
#     - `%(asctime)s`: Timestamp of the log message.
#     - `%(levelname)s`: Severity level of the log message (INFO, WARNING, ERROR, CRITICAL).
#     - `%(message)s`: The actual log message.
#
# - `logging.getLogger(__name__)`: Creates a logger object with the name of the current module. This logger is used to log messages from this module.
#   - By using `__name__`, you can easily identify which module the log messages are coming from, which is helpful for debugging and tracing.
#
# Example usage:
# - Use `logger.info("Your message here")` to log informational messages.
# - Use `logger.warning("Your message here")` for warning messages.
# - Use `logger.error("Your message here")` for error messages.
# - Use `logger.critical("Your message here")` for critical issues.
#
# Note:
# - Adjust the logging level and format as needed for different environments (e.g., development, production).


import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
