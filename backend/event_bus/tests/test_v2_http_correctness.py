"""HTTP-layer correctness: async write accepts and records operation (Kafka + DB mocked)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

# conftest sets env before this import
from app.main import app


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


_SAMPLE_TX = {
    "user_id": 5,
    "market_id": 3,
    "action": "MARKET_TRANSACTION",
    "token_id": 1,
    "side": True,
    "qty": 10,
    "transaction_id": 1776824000001,
    "transaction_type": "BUY",
}


def test_post_markets_transactions_returns_202_with_operation_id(client: TestClient) -> None:
    with (
        patch(
            "app.command_bus.v2_kafka_producer.send_json",
            new_callable=AsyncMock,
            return_value=(2, 100),
        ),
        patch("app.command_bus.insert_operation_pending") as ins,
        patch("app.command_bus.update_operation_kafka_meta") as upd,
    ):
        r = client.post("/v2/markets/transactions", json=_SAMPLE_TX)
    assert r.status_code == 202
    body = r.json()
    assert body.get("accepted") is True
    assert body.get("status") == "queued"
    oid = body.get("operation_id")
    assert oid is not None
    UUID(str(oid))  # raises if invalid
    assert ins.called
    assert upd.called


def test_get_operation_returns_row_when_present(client: TestClient) -> None:
    oid = "550e8400-e29b-41d4-a716-446655440000"
    fake_row = {
        "operation_id": oid,
        "topic": "market.operations",
        "status": "succeeded",
        "error_message": None,
        "kafka_partition": 1,
        "kafka_offset": 42,
        "created_at": None,
        "updated_at": None,
        "envelope": {"metadata": {"sub": None}, "payload": _SAMPLE_TX},
    }
    with patch("app.v2_routes.fetch_operation", return_value=fake_row):
        r = client.get(f"/v2/operations/{oid}")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "succeeded"
    assert data["topic"] == "market.operations"


def test_get_operation_404_when_missing(client: TestClient) -> None:
    oid = "650e8400-e29b-41d4-a716-446655440001"
    with patch("app.v2_routes.fetch_operation", return_value=None):
        r = client.get(f"/v2/operations/{oid}")
    assert r.status_code == 404
