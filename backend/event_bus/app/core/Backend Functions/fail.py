import logging
from collections.abc import Mapping, Sequence


_LOGGER = logging.getLogger("event_bus.backend_functions")
if not _LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    _LOGGER.addHandler(_handler)
_LOGGER.setLevel(logging.INFO)
_LOGGER.propagate = False

_REDACTED_KEYS = {
    "password",
    "current_password",
    "new_password",
    "password_hash",
    "session_token",
}


def _sanitize(value):
    if isinstance(value, Mapping):
        return {
            key: ("***REDACTED***" if key in _REDACTED_KEYS else _sanitize(item))
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_sanitize(item) for item in value]
    return value


def _log_success(operation, result):
    _LOGGER.info("Success in %s: %r", operation, _sanitize(result))


def _log_failure(operation, error, message):
    _LOGGER.error("Failure in %s [%s]: %s", operation, error, message)


def _log_result(operation, result):
    if isinstance(result, dict) and result.get("ok") is False:
        _log_failure(operation, result.get("error"), result.get("message"))
    else:
        _log_success(operation, result)


def _fail(error, message):
    return {"ok": False, "error": error, "message": message}
