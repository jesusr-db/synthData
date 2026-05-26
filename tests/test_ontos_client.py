# tests/test_ontos_client.py
import json
from unittest.mock import MagicMock, patch, call
import pytest


def _make_client():
    """Return an OntosClient with all HTTP mocked out."""
    from src.setup.ontos_client import OntosClient
    client = OntosClient.__new__(OntosClient)
    client.base = "https://fake.ontos"
    client.token = "fake-token"
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
        assert sid == "guest_order"
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


# ── upload_ttl ──────────────────────────────────────────────────────────────

def test_upload_ttl_returns_model_id():
    from src.setup.ontos_client import OntosClient
    import urllib.request
    mock_resp = MagicMock()
    mock_resp.read.return_value = b'{"id": "model-123"}'
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=mock_resp):
        client = _make_client()
        result = client.upload_ttl(b"@prefix qsr: <http://qsr.synth/ontology#> .", "qsr-ontology")
        assert result == "model-123"


def test_upload_ttl_returns_none_on_error():
    from src.setup.ontos_client import OntosClient
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
        url=None, code=500, msg="Internal Server Error", hdrs=None, fp=None
    )):
        client = _make_client()
        result = client.upload_ttl(b"ttl content", "qsr-ontology")
        assert result is None


# ── get_assets ──────────────────────────────────────────────────────────────

def test_get_assets_returns_items_list():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get:
        mock_get.return_value = {"items": [{"id": "asset-1"}, {"id": "asset-2"}]}
        client = _make_client()
        result = client.get_assets()
        assert len(result) == 2
        mock_get.assert_called_once_with("/api/assets?limit=200")


# ── delete_contract / delete_semantic_link ──────────────────────────────────

def test_delete_contract_calls_delete():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_delete", return_value=True) as mock_del:
        client = _make_client()
        result = client.delete_contract("cid-abc")
        mock_del.assert_called_once_with("/api/data-contracts/cid-abc")
        assert result is True


def test_delete_semantic_link_calls_delete():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_delete", return_value=True) as mock_del:
        client = _make_client()
        result = client.delete_semantic_link("link-xyz")
        mock_del.assert_called_once_with("/api/semantic-links/link-xyz")
        assert result is True


# ── get_semantic_links_for_entity ────────────────────────────────────────────

def test_get_semantic_links_returns_list():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get:
        mock_get.return_value = [{"iri": "http://qsr.synth/ontology#Order"}]
        client = _make_client()
        result = client.get_semantic_links_for_entity("uc_column", "jmrdemo.synth_silver.guest_order.order_id")
        assert len(result) == 1
        mock_get.assert_called_once()


def test_get_semantic_links_returns_empty_on_non_list():
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get:
        mock_get.return_value = None
        client = _make_client()
        result = client.get_semantic_links_for_entity("uc_column", "bad-id")
        assert result == []
