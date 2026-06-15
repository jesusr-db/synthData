# src/setup/ontos_client.py
import json
import urllib.request
import urllib.error


class OntosClient:
    def __init__(self, base_url: str, token: str):
        self.base = base_url.rstrip("/")
        self.token = token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _get(self, path):
        req = urllib.request.Request(
            self.base + path, headers=self._headers(), method="GET"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            err_detail = e.read().decode()[:200] if hasattr(e, 'read') else str(e.reason)
            print(f"  [WARN] GET {path}: {err_detail}")
            return None

    def _post(self, path, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            self.base + path, data=data, headers=self._headers(), method="POST"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            err_detail = e.read().decode()[:300] if hasattr(e, 'read') else str(e.reason)
            print(f"  [WARN] POST {path}: {err_detail}")
            return None

    def _delete(self, path):
        req = urllib.request.Request(
            self.base + path, headers=self._headers(), method="DELETE"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                resp.read()
                return True
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            err_detail = e.code if hasattr(e, 'code') else str(e.reason)
            print(f"  [WARN] DELETE {path}: {err_detail}")
            return False

    def fetch_uc_columns(self, catalog: str, schema: str, table: str) -> list:
        """Fetch live column metadata from Unity Catalog via ontos catalog API."""
        result = self._get(f"/api/catalogs/{catalog}/schemas/{schema}/objects/{table}/columns")
        if not isinstance(result, list):
            return []
        return result

    def seed_contract_schemas(
        self,
        contract_id: str,
        catalog: str,
        schema: str,
        tables: list[str],
        pii_columns: set[str] | None = None,
    ):
        """Fetch columns from UC via ontos catalog API and POST each schema with properties in one call.

        The ontos API only exposes GET on /schemas/{name}/properties — properties must be
        embedded in the initial POST /schemas body. If the schema already exists with
        properties, it is skipped. If it exists with 0 properties (e.g. from a prior partial
        run), it is deleted and re-created so properties land correctly.
        """
        pii_columns = pii_columns or set()
        existing_schemas = self._get(f"/api/data-contracts/{contract_id}/schemas") or []
        existing_by_name = {s["name"]: s for s in existing_schemas}

        for table in tables:
            physical_name = f"{catalog}.{schema}.{table}"

            # Check if schema already exists with properties — skip if so
            if table in existing_by_name:
                prop_count = existing_by_name[table].get("propertyCount", 0)
                if prop_count > 0:
                    print(f"    [SKIP] schema {table} already has {prop_count} properties")
                    continue
                # Exists but empty — delete so we can re-create with properties
                print(f"    [INFO] schema {table} exists but has 0 properties — deleting to re-create")
                self._delete(f"/api/data-contracts/{contract_id}/schemas/{table}")

            columns = self.fetch_uc_columns(catalog, schema, table)
            if not columns:
                print(f"  [WARN] No columns returned for {table}, skipping schema creation")
                continue

            properties = []
            for col in columns:
                col_name = col.get("name", "")
                is_pii = col_name in pii_columns
                properties.append({
                    "name": col_name,
                    "logicalType": col.get("type", "string"),
                    "description": col.get("comment") or col_name,
                    "required": False,
                    "criticalDataElement": is_pii,
                    "classification": "pii" if is_pii else None,
                })
                print(f"    ↳ {col_name} ({col.get('type','?')}){' [PII]' if is_pii else ''}")

            result = self._post(
                f"/api/data-contracts/{contract_id}/schemas",
                {
                    "name": table,
                    "physicalName": physical_name,
                    "description": f"QSR silver table {physical_name}",
                    "properties": properties,
                },
            )
            if result:
                print(f"    [OK] schema {table} created with {len(properties)} properties")
            else:
                print(f"  [WARN] Could not create schema for {table}")

    def create_semantic_link(self, entity_type: str, entity_id: str, iri: str):
        """POST a semantic link. Returns result dict or None on error."""
        return self._post(
            "/api/semantic-links/",
            {"entity_type": entity_type, "entity_id": entity_id, "iri": iri},
        )

    def upload_ttl(self, ttl_bytes: bytes, title: str) -> str | None:
        """Upload a Turtle ontology file. Returns model_id or None.

        Idempotent: if a model with filename '{title}.ttl' already exists, returns its id
        without re-uploading.
        """
        filename = f"{title}.ttl"
        existing = self._get("/api/semantic-models") or {}
        for m in existing.get("semantic_models", []):
            if m.get("original_filename") == filename or m.get("name") == filename:
                print(f"    [SKIP] semantic model '{filename}' already uploaded (id={m['id']})")
                return m["id"]

        boundary = "----FormBoundaryQSRONTOS"
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: text/turtle\r\n\r\n"
        ).encode() + ttl_bytes + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(
            self.base + "/api/semantic-models/upload",
            data=body,
            method="POST",
        )
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
                return result.get("id") or result.get("model_id")
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            err_detail = e.read().decode()[:300] if hasattr(e, 'read') else str(e.reason)
            print(f"  [WARN] upload_ttl: {err_detail}")
            return None

    def get_assets(self, limit: int = 200) -> list:
        result = self._get(f"/api/assets?limit={limit}") or {}
        return result.get("items", [])

    def delete_contract(self, contract_id: str) -> bool:
        return self._delete(f"/api/data-contracts/{contract_id}")

    def delete_semantic_link(self, link_id: str) -> bool:
        return self._delete(f"/api/semantic-links/{link_id}")

    def get_semantic_links_for_entity(self, entity_type: str, entity_id: str) -> list:
        result = self._get(f"/api/semantic-links/entity/{entity_type}/{entity_id}")
        return result if isinstance(result, list) else []
