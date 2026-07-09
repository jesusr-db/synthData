#!/usr/bin/env python3
"""Shared BU governed-tag domain layer for the QSR Genie spaces.

Groups the 11 spaces under 4 higher-level Business-Unit domains using governed tags with
parent/child key naming (e.g. 'Store Operations' parent, 'Store Operations/Orders and SOS' child).
Each child tag is applied to (a) the space's underlying UC tables/views and (b) the Genie space
entity via entity-tag-assignments, so Domain membership resolves automatically.

Imported by both the local CLI wrapper (build_domains.sh's python replacement) and the setup-job
notebook (src/setup/apply_bu_domains.py). Callable-injection keeps it transport-agnostic:
pass a `sql(stmt)` runner and a `post(path, body)` / `delete(path)` REST runner.
"""
from ._spaces import DOMAINS

# --- BU parent domain metadata (icon + color for the Discover card) ----------
BUS = {
    "Store Operations":               {"subtitle": "Orders, delivery, labor & demand",
                                        "description": "Store-level operations across orders, Speed-of-Service, delivery, workforce, and demand risk.",
                                        "icon": {"color": "#1B5E20", "name": "BASKET"}},
    "Customer and Loyalty":           {"subtitle": "Loyalty, guests, payments & ML",
                                        "description": "Customer-facing domains: loyalty & rewards, guest lifecycle, payments, and ML features.",
                                        "icon": {"color": "#6A1B9A", "name": "BASKET"}},
    "Supply Chain and Merchandising": {"subtitle": "Inventory, waste & menu",
                                        "description": "Inventory, waste, receiving/replenishment, and menu/product performance.",
                                        "icon": {"color": "#B71C1C", "name": "BASKET"}},
    "Finance and Franchise":          {"subtitle": "Franchisee & executive KPIs",
                                        "description": "Cross-domain franchisee and executive scorecards.",
                                        "icon": {"color": "#0D47A1", "name": "BASKET"}},
}


def child_tag_key(bu, tag):
    """Parent/child governed-tag key, e.g. 'Store Operations/Orders and SOS'."""
    return f"{bu}/{tag}"


def plan():
    """Return the full BU plan: [{bu, subtitle, description, icon, children:[{key, tag, title}]}]."""
    out = []
    for bu, meta in BUS.items():
        children = [{"key": k, "tag": d["tag"], "title": d["title"], "tag_key": child_tag_key(bu, d["tag"])}
                    for k, d in DOMAINS.items() if d["bu"] == bu]
        out.append({"bu": bu, **meta, "children": children})
    return out


# --- transport-agnostic apply/teardown (inject sql/post/delete callables) -----
def create_governed_tags(sql):
    """CREATE GOVERNED TAG for each BU parent + each space child. sql: callable(stmt)->None."""
    for p in plan():
        sql(f"CREATE GOVERNED TAG `{p['bu']}` DESCRIPTION '{p['description'][:200]}'")
        for c in p["children"]:
            sql(f"CREATE GOVERNED TAG `{c['tag_key']}` DESCRIPTION 'Genie space: {c['title']}'")


def apply_tags_to_assets(sql, space_tables):
    """Apply each child governed tag to the UC tables/views its space references.
    space_tables: {space_key: [fully.qualified.identifier, ...]}."""
    for p in plan():
        for c in p["children"]:
            for ident in space_tables.get(c["key"], []):
                obj = "VIEW" if ident.split(".")[1].endswith("genie") or ".metric_" in ident else "TABLE"
                # ALTER TABLE works for both tables and views for SET TAGS; use TABLE form uniformly.
                sql(f"ALTER TABLE {ident} SET TAGS ('{c['tag_key']}' = '')", best_effort=True)


def assign_tags_to_spaces(post, spaces):
    """Attach each child tag to its Genie space entity via entity-tag-assignments.
    spaces: {space_key: {tag, bu, space_id, ...}}  (output of build_all/build notebook)."""
    for p in plan():
        for c in p["children"]:
            sid = spaces.get(c["key"], {}).get("space_id")
            if not sid:
                continue
            post("/api/2.0/entity-tag-assignments",
                 {"entity_type": "geniespaces", "entity_id": sid,
                  "tag_key": c["tag_key"], "tag_value": ""})


def create_domain_cards(post, owner_id):
    """POST /api/2.0/domains for each BU parent + each child (published immediately).

    A top-level (BU parent) domain card takes a plain tag_key (no '/'). A child/subdomain
    card takes the slash-namespaced governed-tag key AND parent_domain_id = the parent's
    domain_id (the API rejects a '/' tag_key without parent_domain_id)."""
    created = {}
    for p in plan():
        r = post("/api/2.0/domains", {
            "tag_key": p["bu"], "subtitle": p["subtitle"], "description": p["description"],
            "technical_owner_ids": [owner_id], "business_owner_ids": [owner_id], "icon": p["icon"]})
        parent_id = (r or {}).get("domain_id", "")
        created[p["bu"]] = {"domain_id": parent_id, "children": {}}
        for c in p["children"]:
            rc = post("/api/2.0/domains", {
                "tag_key": c["tag_key"], "parent_domain_id": parent_id,
                "subtitle": c["title"], "description": f"Genie space: {c['title']}",
                "technical_owner_ids": [owner_id], "business_owner_ids": [owner_id], "icon": p["icon"]})
            created[p["bu"]]["children"][c["key"]] = (rc or {}).get("domain_id", "")
    return created


def teardown(get, delete, sql, drop_tags=True, spaces=None):
    """Delete all BU/child domains, remove space entity-tag-assignments, and (optionally) drop the
    governed tags. Idempotent/best-effort.
    get: callable(path)->json; delete: callable(path)->bool; sql: callable(stmt, best_effort);
    spaces: optional {space_key: {..., space_id}} — if given, unassigns each child tag from its space."""
    import urllib.parse
    # Collect the tag keys we own (BU parents + children).
    owned = set(BUS.keys())
    for p in plan():
        owned.update(c["tag_key"] for c in p["children"])
    # Delete matching domain cards.
    existing = (get("/api/2.0/domains") or {}).get("domains", [])
    for dom in existing:
        if dom.get("tag_key") in owned:
            delete(f"/api/2.0/domains/{dom.get('domain_id')}")
    # Remove entity-tag-assignments from the Genie spaces (so redeploys don't accumulate stale tags).
    if spaces:
        for p in plan():
            for c in p["children"]:
                sid = spaces.get(c["key"], {}).get("space_id")
                if sid:
                    key = urllib.parse.quote(c["tag_key"], safe="")
                    delete(f"/api/2.0/entity-tag-assignments/geniespaces/{sid}/tags/{key}")
    # Drop governed tags (children first, then parents).
    if drop_tags:
        for p in plan():
            for c in p["children"]:
                sql(f"DROP GOVERNED TAG `{c['tag_key']}`", best_effort=True)
        for bu in BUS:
            sql(f"DROP GOVERNED TAG `{bu}`", best_effort=True)
