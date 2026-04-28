"""Bootstrap env before importing the FastAPI app so tests never connect to Kafka."""

from __future__ import annotations

import os

# Hard overrides — win over `.env` because dotenv does not replace existing vars.
_FORCED_ENV = {
    "POLARIS_SKIP_KAFKA_AT_STARTUP": "1",
    "POLARIS_ENABLE_LEGACY_CONSUMER": "0",
    "POLARIS_ENABLE_V2_WORKER": "0",
    "POLARIS_REQUIRE_KAFKA_AT_STARTUP": "0",
    "V2_REQUIRE_JWT": "0",
    "V2_OPERATIONS_REQUIRE_JWT": "0",
}

for _key, _val in _FORCED_ENV.items():
    os.environ[_key] = _val
