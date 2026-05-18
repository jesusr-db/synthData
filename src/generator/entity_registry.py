import random
from datetime import date
from typing import Optional

class EntityRegistry:
    """In-memory lookup of all seeded entity IDs for FK consistency."""

    def __init__(self, units: list[dict], menu_items: list[dict], bom: list[dict],
                 financial_periods: list[dict], num_guests_per_unit: int = 200,
                 item_prices: list[dict] | None = None):
        self._units = units
        self._unit_ids = [u["unit_id"] for u in units]
        self._unit_by_id = {u["unit_id"]: u for u in units}
        self._menu_items = menu_items
        self._menu_by_id = {m["menu_item_id"]: m for m in menu_items}
        self._bom = bom  # list of {menu_item_id, stock_sku, quantity, ...}
        self._bom_by_item: dict[int, list[dict]] = {}
        for row in bom:
            self._bom_by_item.setdefault(row["menu_item_id"], []).append(row)
        self._periods = sorted(financial_periods, key=lambda p: p["start_date"])

        self._item_price_mult: dict[tuple[int, int], float] = {}
        for row in (item_prices or []):
            self._item_price_mult[(row["menu_item_id"], row["financial_period_id"])] = row["price_multiplier"]

        # Pre-generate a guest pool per unit: IDs 1..N * num_units
        self._guest_pool: dict[int, list[int]] = {}
        g_id = 1
        for uid in self._unit_ids:
            self._guest_pool[uid] = list(range(g_id, g_id + num_guests_per_unit))
            g_id += num_guests_per_unit
        self._max_guest_id = g_id - 1

        # ~30% of guests are loyalty members (assigned on init)
        self._member_ids: set[int] = set()
        all_guests = [gid for pool in self._guest_pool.values() for gid in pool]
        self._member_ids = set(random.sample(all_guests, k=int(len(all_guests) * 0.30)))

    def random_unit_id(self) -> int:
        return random.choice(self._unit_ids)

    def unit_by_id(self, unit_id: int) -> dict:
        return self._unit_by_id[unit_id]

    def all_units(self) -> list[dict]:
        return self._units

    def random_menu_item(self, hour: int) -> dict:
        from src.generator.reference.menu_catalog import get_items_for_daypart
        items = get_items_for_daypart(hour)
        return random.choice(items)

    def random_menu_item_id(self, hour: int) -> int:
        return self.random_menu_item(hour)["menu_item_id"]

    def get_menu_item(self, menu_item_id: int) -> dict:
        return self._menu_by_id[menu_item_id]

    def bom_for_item(self, menu_item_id: int) -> list[dict]:
        return self._bom_by_item.get(menu_item_id, [])

    def random_guest_profile_id(self, unit_id: int) -> Optional[int]:
        """40% chance of returning a guest ID, 60% anonymous."""
        if random.random() > 0.40:
            return None
        pool = self._guest_pool.get(unit_id, [])
        return random.choice(pool) if pool else None

    def is_loyalty_member(self, guest_profile_id: Optional[int]) -> bool:
        return guest_profile_id in self._member_ids if guest_profile_id else False

    def random_member_id(self, guest_profile_id: Optional[int]) -> Optional[int]:
        """Return member_id if guest is a loyalty member (same ID as profile for simplicity)."""
        return guest_profile_id if self.is_loyalty_member(guest_profile_id) else None

    def guest_ids_for_unit(self, unit_id: int) -> list[int]:
        return self._guest_pool.get(unit_id, [])

    def unit_price_index(self, unit_id: int) -> float:
        return self._unit_by_id[unit_id].get("market_price_index", 1.0)

    def item_price_multiplier(self, menu_item_id: int, financial_period_id) -> float:
        return self._item_price_mult.get((menu_item_id, financial_period_id), 1.0)

    def financial_period_for_date(self, d: date) -> Optional[int]:
        d_str = d.isoformat()
        for p in self._periods:
            if p["start_date"] <= d_str <= p["end_date"]:
                return p["financial_period_id"]
        return self._periods[-1]["financial_period_id"] if self._periods else None

    @classmethod
    def from_spark(cls, spark, catalog: str, backfill_months: int = 12):
        """Load entity registry from live ref.* Delta tables."""
        units = [r.asDict() for r in spark.table(f"{catalog}.ref.unit").collect()]
        menu = [r.asDict() for r in spark.table(f"{catalog}.ref.menu_item").collect()]
        bom = [r.asDict() for r in spark.table(f"{catalog}.ref.recipe_ingredient").collect()]
        periods = [r.asDict() for r in spark.table(f"{catalog}.ref.financial_period").collect()]
        item_prices = [r.asDict() for r in spark.table(f"{catalog}.ref.item_price").collect()]
        return cls(units=units, menu_items=menu, bom=bom, financial_periods=periods,
                   item_prices=item_prices)
