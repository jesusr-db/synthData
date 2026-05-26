# tests/test_ontos_client.py
import json
from unittest.mock import MagicMock, patch, call
import pytest


def _make_client(responses=None):
    """Return an OntosClient with all HTTP mocked out."""
    from src.setup.ontos_client import OntosClient
    client = OntosClient.__new__(OntosClient)
    client.base = "https://fake.ontos"
    client.token = "fake-token"
    client._responses = responses or {}
    return client


# ── get_or_create_schema ────────────────────────────────────────────────────

def test_get_schema_returns_existing_id():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post:
        mock_get.return_value = [{"id": "schema-abc", "name": "guest_order"}]
        client = _make_client()
        sid = client.get_or_create_schema("contract-123", "guest_order",
                                          "jmrdemo.synth_silver.guest_order", "desc")
        assert sid == "schema-abc"
        mock_post.assert_not_called()


def test_create_schema_when_missing():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post:
        mock_get.return_value = []
        mock_post.return_value = {"status": "ok", "schema_name": "guest_order"}
        client = _make_client()
        client.get_or_create_schema("contract-123", "guest_order",
                                    "jmrdemo.synth_silver.guest_order", "desc")
        mock_post.assert_called_once_with(
            "/api/data-contracts/contract-123/schemas",
            {"name": "guest_order",
             "physicalName": "jmrdemo.synth_silver.guest_order",
             "description": "desc"}
        )


# ── fetch_uc_columns ────────────────────────────────────────────────────────

def test_fetch_uc_columns_returns_list():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get:
        mock_get.return_value = [
            {"name": "order_id", "type": "bigint", "comment": "PK"},
            {"name": "channel", "type": "string", "comment": None},
        ]
        client = _make_client()
        cols = client.fetch_uc_columns("jmrdemo", "synth_silver", "guest_order")
        assert len(cols) == 2
        assert cols[0]["name"] == "order_id"
        mock_get.assert_called_once_with(
            "/api/catalogs/jmrdemo/schemas/synth_silver/objects/guest_order/columns"
        )


def test_fetch_uc_columns_returns_empty_on_error():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get:
        mock_get.return_value = None  # _get returns None on HTTP error
        client = _make_client()
        cols = client.fetch_uc_columns("jmrdemo", "synth_silver", "missing_table")
        assert cols == []


# ── upsert_property ─────────────────────────────────────────────────────────

def test_upsert_property_creates_when_not_exists():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post:
        mock_get.return_value = {"items": []}
        mock_post.return_value = {"id": "prop-xyz"}
        client = _make_client()
        client.upsert_property("contract-1", "guest_order", {
            "name": "order_id",
            "logicalType": "integer",
            "primaryKey": True,
            "description": "PK",
        })
        mock_post.assert_called_once()
        call_path = mock_post.call_args[0][0]
        assert "contract-1" in call_path
        assert "guest_order" in call_path


def test_upsert_property_skips_when_exists():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post:
        mock_get.return_value = {"items": [{"name": "order_id", "id": "prop-123"}]}
        client = _make_client()
        client.upsert_property("contract-1", "guest_order", {"name": "order_id"})
        mock_post.assert_not_called()


# ── create_semantic_link ────────────────────────────────────────────────────

def test_create_semantic_link_posts_correct_body():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_post") as mock_post:
        mock_post.return_value = {"id": "link-abc"}
        client = _make_client()
        result = client.create_semantic_link(
            entity_type="uc_column",
            entity_id="jmrdemo.synth_silver.guest_order.order_id",
            iri="http://qsr.synth/ontology#Order",
        )
        mock_post.assert_called_once_with(
            "/api/semantic-links/",
            {
                "entity_type": "uc_column",
                "entity_id": "jmrdemo.synth_silver.guest_order.order_id",
                "iri": "http://qsr.synth/ontology#Order",
            }
        )
        assert result is not None


def test_create_semantic_link_returns_none_on_error():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_post") as mock_post:
        mock_post.return_value = None
        client = _make_client()
        result = client.create_semantic_link("uc_column", "bad-id", "bad-iri")
        assert result is None
