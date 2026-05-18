from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.domains.orders import generate_orders_for_tick
from src.generator.domains.inventory import generate_inventory_events
from src.generator.domains.guest import generate_new_guest_profiles
from src.generator.domains.loyalty import generate_loyalty_events
from src.generator.domains.workforce import generate_shift_events


@dataclass
class GeneratorConfig:
    catalog_name: str
    num_units: int
    backfill_months: int
    live_tick_seconds: int
    base_orders_per_unit_per_hour: int


def build_tick_rows(
    unit_id: int,
    timestamp: datetime,
    registry: EntityRegistry,
    tick_seconds: int = 60,
    base_orders_per_hour: int = 18,
) -> list[dict]:
    """All domain rows for one unit, one tick."""
    unit = registry.unit_by_id(unit_id)
    ctx = build_context(unit_id, timestamp, unit["unit_volume_bias"], base_orders_per_hour)
    order_rows = generate_orders_for_tick(ctx, registry, tick_seconds)
    inv_rows = generate_inventory_events(ctx, registry, order_rows)
    loyalty_rows = generate_loyalty_events(ctx, registry, order_rows)
    return order_rows + inv_rows + loyalty_rows


def backfill_ticks(
    registry: EntityRegistry,
    backfill_months: int,
    tick_seconds: int = 3600,
    base_orders_per_hour: int = 18,
    start_dt: datetime | None = None,
) -> Iterator[list[dict]]:
    """Yield batches of rows for all units, one hour at a time, from start_dt (or N months ago) to now."""
    from dateutil.relativedelta import relativedelta

    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = start_dt if start_dt is not None else now - relativedelta(months=backfill_months)
    current = start
    while current <= now:
        batch = []
        for unit in registry.all_units():
            uid = unit["unit_id"]
            batch.extend(build_tick_rows(uid, current, registry, tick_seconds, base_orders_per_hour))
            # Daily events on the first tick of each day (10:00 AM)
            if current.hour == 10:
                batch.extend(
                    generate_shift_events(
                        uid,
                        current.date().isoformat(),
                        projected_orders=base_orders_per_hour * 12,
                    )
                )
                batch.extend(generate_new_guest_profiles(uid, current.date().isoformat()))
        yield batch
        current += timedelta(seconds=tick_seconds)


def live_tick(
    registry: EntityRegistry,
    tick_seconds: int = 60,
    base_orders_per_hour: int = 18,
) -> list[dict]:
    """One tick of live data for all units at current time."""
    now = datetime.now()
    rows = []
    for unit in registry.all_units():
        rows.extend(
            build_tick_rows(unit["unit_id"], now, registry, tick_seconds, base_orders_per_hour)
        )
    return rows
