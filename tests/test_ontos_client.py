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


# ── seed_contract_schemas: schema creation strategy ─────────────────────────

def test_seed_schema_skips_when_already_has_properties():
    """If schema exists with propertyCount > 0, skip without re-POSTing."""
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post, \
         patch.object(OntosClient, "_delete") as mock_del, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=[]):
        mock_get.return_value = [{"name": "guest_order", "propertyCount": 5}]
        client = _make_client()
        client.seed_contract_schemas("contract-123", "jmrdemo", "synth_silver", ["guest_order"])
        mock_del.assert_not_called()
        mock_post.assert_not_called()


def test_seed_schema_deletes_and_recreates_empty_schema():
    """If schema exists with 0 properties, delete it then re-POST with properties."""
    from src.setup.ontos_client import OntosClient
    columns = [{"name": "order_id", "type": "bigint", "comment": "PK"}]
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post, \
         patch.object(OntosClient, "_delete") as mock_del, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=columns):
        mock_get.return_value = [{"name": "guest_order", "propertyCount": 0}]
        mock_post.return_value = {"status": "ok"}
        client = _make_client()
        client.seed_contract_schemas("contract-123", "jmrdemo", "synth_silver", ["guest_order"])
        mock_del.assert_called_once_with("/api/data-contracts/contract-123/schemas/guest_order")
        mock_post.assert_called_once()
        body = mock_post.call_args[0][1]
        assert body["name"] == "guest_order"
        assert len(body["properties"]) == 1


def test_seed_schema_creates_new_schema_with_properties():
    """When schema doesn't exist, POST with embedded properties in one call."""
    from src.setup.ontos_client import OntosClient
    columns = [
        {"name": "order_id", "type": "bigint", "comment": "PK"},
        {"name": "channel",  "type": "string",  "comment": "Order channel"},
    ]
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=columns):
        mock_get.return_value = []  # no existing schemas
        mock_post.return_value = {"status": "ok"}
        client = _make_client()
        client.seed_contract_schemas("contract-123", "jmrdemo", "synth_silver", ["guest_order"])
        mock_post.assert_called_once()
        path, body = mock_post.call_args[0]
        assert path == "/api/data-contracts/contract-123/schemas"
        assert body["name"] == "guest_order"
        assert body["physicalName"] == "jmrdemo.synth_silver.guest_order"
        assert len(body["properties"]) == 2


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


# ── seed_contract_schemas: PII and edge cases ────────────────────────────────


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


def test_upload_ttl_idempotent_returns_existing_id():
    """If model already uploaded, return existing id without re-uploading."""
    from src.setup.ontos_client import OntosClient
    with patch.object(OntosClient, "_get", return_value={
        "semantic_models": [{"id": "existing-123", "original_filename": "qsr-ontology.ttl"}]
    }):
        with patch("urllib.request.urlopen") as mock_urlopen:
            client = _make_client()
            result = client.upload_ttl(b"ttl content", "qsr-ontology")
            assert result == "existing-123"
            mock_urlopen.assert_not_called()


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


def test_seed_contract_schemas_marks_pii():
    """Columns in pii_columns set should get classification='pii' and criticalDataElement=True."""
    from src.setup.ontos_client import OntosClient

    columns = [{"name": "email", "type": "string", "comment": "Guest email"}]
    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=columns):
        mock_get.return_value = []  # no existing schemas
        mock_post.return_value = {"status": "ok"}
        client = _make_client()
        client.seed_contract_schemas(
            contract_id="cid-2",
            catalog="jmrdemo",
            schema="synth_silver",
            tables=["guest_profile"],
            pii_columns={"email", "phone"},
        )
        body = mock_post.call_args[0][1]
        prop = body["properties"][0]
        assert prop["classification"] == "pii"
        assert prop["criticalDataElement"] is True


def test_seed_contract_schemas_embeds_all_columns_as_properties():
    """All UC columns are embedded in the POST body properties list."""
    from src.setup.ontos_client import OntosClient

    columns = [
        {"name": "order_id", "type": "bigint", "comment": "PK"},
        {"name": "channel",  "type": "string",  "comment": "Order channel"},
        {"name": "total",    "type": "double",  "comment": None},
    ]
    with patch.object(OntosClient, "_get", return_value=[]), \
         patch.object(OntosClient, "_post", return_value={"status": "ok"}) as mock_post, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=columns):
        client = _make_client()
        client.seed_contract_schemas("cid-4", "jmrdemo", "synth_silver", ["guest_order"])
        body = mock_post.call_args[0][1]
        prop_names = [p["name"] for p in body["properties"]]
        assert prop_names == ["order_id", "channel", "total"]
        # description falls back to column name when comment is None
        assert body["properties"][2]["description"] == "total"


def test_seed_contract_schemas_skips_when_no_columns():
    """When fetch_uc_columns returns [], no POST should be made."""
    from src.setup.ontos_client import OntosClient

    with patch.object(OntosClient, "_get") as mock_get, \
         patch.object(OntosClient, "_post") as mock_post, \
         patch.object(OntosClient, "fetch_uc_columns", return_value=[]):
        mock_get.return_value = []  # no existing schemas
        client = _make_client()
        client.seed_contract_schemas(
            contract_id="cid-3",
            catalog="jmrdemo",
            schema="synth_silver",
            tables=["empty_table"],
        )
        mock_post.assert_not_called()
