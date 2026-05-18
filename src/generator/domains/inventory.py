import random
from datetime import datetime
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry
from src.generator.entropy import should_waste

_inv_counter = 0

def _next_inv_id() -> int:
    global _inv_counter
    _inv_counter += 1
    return _inv_counter

def generate_inventory_events(ctx: CausalContext, registry: EntityRegistry,
                               order_rows: list[dict]) -> list[dict]:
    rows = []
    # Decrement on_hand_balance for each order_item sold
    depleted: dict[str, float] = {}
    for row in order_rows:
        if row["event_type"] != "order_item":
            continue
        mid = row["menu_item_id"]
        for bom_row in registry.bom_for_item(mid):
            sku = bom_row["stock_sku"]
            depleted[sku] = depleted.get(sku, 0.0) + bom_row["quantity"] * row["quantity"]

    for sku, qty_used in depleted.items():
        par_level = 20.0
        on_hand = max(0.0, par_level - qty_used + random.uniform(0, 5))
        rows.append({
            "event_type": "on_hand_balance",
            "on_hand_balance_id": _next_inv_id(),
            "unit_id": ctx.unit_id,
            "stock_sku": sku,
            "quantity_on_hand": round(on_hand, 3),
            "quantity_reserved": round(qty_used, 3),
            "par_level": par_level,
            "snapshot_at": ctx.timestamp,
        })
        # Waste events: ~3% of prep volume, skewed late-night
        if should_waste(ctx.waste_probability, ctx.hour_of_day):
            waste_qty = round(qty_used * random.uniform(0.02, 0.06), 3)
            rows.append({
                "event_type": "waste_log",
                "waste_log_id": _next_inv_id(),
                "unit_id": ctx.unit_id,
                "stock_sku": sku,
                "waste_quantity": waste_qty,
                "waste_category": "overproduction",
                "waste_cost": round(waste_qty * 2.5, 2),
                "logged_at": ctx.timestamp,
            })
        # Trigger replenishment if on_hand drops below 25% of PAR
        if on_hand < par_level * 0.25:
            rows.append({
                "event_type": "replenishment_order",
                "replenishment_order_id": _next_inv_id(),
                "unit_id": ctx.unit_id,
                "stock_sku": sku,
                "order_type": "auto_par",
                "order_quantity": round(par_level - on_hand, 3),
                "order_status": "submitted",
                "ordered_at": ctx.timestamp,
            })

    return rows

def generate_daily_receiving(unit_id: int, reg: EntityRegistry,
                              order_date: str) -> list[dict]:
    """Simulate daily supplier delivery restocking PAR levels."""
    rows = []
    for sku in _all_skus(reg):
        rows.append({
            "event_type": "receiving_order",
            "receiving_order_id": _next_inv_id(),
            "unit_id": unit_id,
            "stock_sku": sku,
            "received_quantity": round(random.uniform(15, 30), 2),
            "delivery_date": order_date,
            "quality_inspection_result": "pass",
            "temperature_check_pass": True,
        })
    return rows

def _all_skus(reg: EntityRegistry) -> set[str]:
    skus = set()
    for item in reg._menu_items:
        for bom in reg.bom_for_item(item["menu_item_id"]):
            skus.add(bom["stock_sku"])
    return skus
