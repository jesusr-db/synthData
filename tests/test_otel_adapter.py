"""Hermetic tests for otel_order_adapter.

No spark, no network, no dbutils. All data injected directly as plain dicts.
Mirrors the style of tests/test_refresh.py.
"""
import json
from datetime import datetime
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# parse_skus
# ---------------------------------------------------------------------------

class TestParseSkus:
    def test_basic(self):
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["41 x5","7 x10"]')
        assert result == [(41, 5), (7, 10)]

    def test_single_sku(self):
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["33 x2"]')
        assert result == [(33, 2)]

    def test_garbage_returns_empty(self):
        from src.refresh.otel_order_adapter import parse_skus
        assert parse_skus("notansku") == []
        assert parse_skus("[]") == []
        assert parse_skus(None) == []

    def test_clamps_below_min(self):
        """menu_item_id 0 is below 1 — should be dropped."""
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["0 x3"]')
        assert result == []

    def test_clamps_above_max(self):
        """menu_item_id 76 is above 75 — should be dropped."""
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["76 x2"]')
        assert result == []

    def test_keeps_boundary_values(self):
        """menu_item_id 1 and 75 are valid."""
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["1 x1","75 x1"]')
        assert result == [(1, 1), (75, 1)]

    def test_drops_zero_qty(self):
        from src.refresh.otel_order_adapter import parse_skus
        result = parse_skus('["10 x0"]')
        assert result == []


# ---------------------------------------------------------------------------
# parse_member_id — web-injected app.order.member_id → synth customer key
# ---------------------------------------------------------------------------

class TestParseMemberId:
    def test_valid_in_range(self):
        from src.refresh.otel_order_adapter import parse_member_id
        assert parse_member_id("12345") == 12345
        assert parse_member_id(12345) == 12345

    def test_boundaries(self):
        from src.refresh.otel_order_adapter import parse_member_id
        assert parse_member_id(1) == 1
        assert parse_member_id(50000) == 50000

    def test_none_and_empty(self):
        from src.refresh.otel_order_adapter import parse_member_id
        assert parse_member_id(None) is None
        assert parse_member_id("") is None

    def test_out_of_range_returns_none(self):
        """Values outside the synth customer space [1,50000] don't reconcile → None."""
        from src.refresh.otel_order_adapter import parse_member_id
        assert parse_member_id(0) is None
        assert parse_member_id(50001) is None
        assert parse_member_id(-5) is None

    def test_non_numeric_returns_none(self):
        from src.refresh.otel_order_adapter import parse_member_id
        assert parse_member_id("abc") is None
        assert parse_member_id("a1b2c3d4-uuid") is None


# ---------------------------------------------------------------------------
# ID bridge: stable + namespaced
# ---------------------------------------------------------------------------

class TestIdBridge:
    def test_guest_order_id_stable(self):
        """Same trace_id always produces the same guest_order_id."""
        from src.generator.id_utils import make_id
        t = "aaaa1111bbbb2222cccc3333dddd4444"
        assert make_id("otel", t) == make_id("otel", t)

    def test_namespaced_vs_synth(self):
        """otel namespace does not collide with the synth 'o' namespace."""
        from src.generator.id_utils import make_id
        t = "aaaa1111bbbb2222cccc3333dddd4444"
        otel_id = make_id("otel", t)
        synth_id = make_id("o", t)
        assert otel_id != synth_id

    def test_item_ids_differ_by_index(self):
        from src.generator.id_utils import make_id
        t = "aaaa1111bbbb2222cccc3333dddd4444"
        assert make_id("otel-item", t, 0) != make_id("otel-item", t, 1)

    def test_different_namespaces_differ(self):
        from src.generator.id_utils import make_id
        t = "aaaa1111bbbb2222cccc3333dddd4444"
        ids = {
            make_id("otel", t),
            make_id("otel-pay", t),
            make_id("otel-deliv", t),
            make_id("otel-status", t, "placed"),
        }
        assert len(ids) == 4  # all distinct


# ---------------------------------------------------------------------------
# map_store_to_unit
# ---------------------------------------------------------------------------

class TestMapStoreToUnit:
    UNIT_IDS_BY_STATE = {"CA": [101, 102, 103], "WA": [201, 202]}
    ALL_UNIT_IDS = [101, 102, 103, 201, 202, 301]

    def test_known_state_uses_state_pool(self):
        from src.refresh.otel_order_adapter import map_store_to_unit
        result = map_store_to_unit(
            "store-CA-042", "CA",
            self.UNIT_IDS_BY_STATE, self.ALL_UNIT_IDS,
        )
        assert result in self.UNIT_IDS_BY_STATE["CA"]

    def test_unknown_state_falls_back_to_all(self):
        from src.refresh.otel_order_adapter import map_store_to_unit
        result = map_store_to_unit(
            "store-TX-007", "TX",
            self.UNIT_IDS_BY_STATE, self.ALL_UNIT_IDS,
        )
        assert result in self.ALL_UNIT_IDS

    def test_deterministic(self):
        """Same store_id and state always map to the same unit."""
        from src.refresh.otel_order_adapter import map_store_to_unit
        r1 = map_store_to_unit("store-CA-042", "CA", self.UNIT_IDS_BY_STATE, self.ALL_UNIT_IDS)
        r2 = map_store_to_unit("store-CA-042", "CA", self.UNIT_IDS_BY_STATE, self.ALL_UNIT_IDS)
        assert r1 == r2


# ---------------------------------------------------------------------------
# reshape_otel_orders — full envelope
# ---------------------------------------------------------------------------

UNIT_IDS_BY_STATE = {"CA": [101, 102, 103], "WA": [201, 202]}
ALL_UNIT_IDS = [101, 102, 103, 201, 202]


def _load_sample():
    logs = _load_fixture("otel_logs_sample.json")
    spans = _load_fixture("otel_spans_sample.json")
    return logs, spans


class TestReshapeOtelOrders:
    def test_empty_inputs_returns_empty(self):
        from src.refresh.otel_order_adapter import reshape_otel_orders
        result = reshape_otel_orders([], [], UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        assert result == []

    def test_filters_load_gen_amount_zero(self):
        """Row with amount=0.0 must not produce any output rows."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        # Collect all guest_order_ids in results
        guest_ids = {r["guest_order_id"] for r in result if r["event_type"] == "guest_order"}
        # trace_id of load-gen row
        from src.generator.id_utils import make_id
        load_gen_id = make_id("otel", "cccc3333dddd4444eeee5555ffff6666")
        assert load_gen_id not in guest_ids

    def test_filters_fee_test_user(self):
        """Row with user.id starting 'fee-test' must be dropped."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        fee_test_id = make_id("otel", "dddd4444eeee5555ffff6666aaaa1111")
        guest_ids = {r["guest_order_id"] for r in result if r["event_type"] == "guest_order"}
        assert fee_test_id not in guest_ids

    def test_total_amount_matches_log_amount(self):
        """guest_order.total_amount must equal app.order.amount from the log."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        guest_orders = [r for r in result if r["event_type"] == "guest_order"]
        # First real log row has amount=54.29
        amounts = {r["total_amount"] for r in guest_orders}
        assert 54.29 in amounts

    def test_subtotal_plus_tax_equals_total(self):
        """subtotal + tax_amount should ≈ total_amount for each guest_order."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        for row in result:
            if row["event_type"] == "guest_order":
                computed = round(row["subtotal"] + row["tax_amount"], 2)
                # Allow ±1 cent rounding tolerance
                assert abs(computed - row["total_amount"]) <= 0.02, (
                    f"subtotal+tax={computed} != total={row['total_amount']}"
                )

    def test_channel_map_delivery(self):
        """Span channel='delivery' → synth channel='own_delivery'."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        # trace aaaa... is delivery
        from src.generator.id_utils import make_id
        delivery_go_id = make_id("otel", "aaaa1111bbbb2222cccc3333dddd4444")
        delivery_order = next(
            r for r in result
            if r["event_type"] == "guest_order" and r["guest_order_id"] == delivery_go_id
        )
        assert delivery_order["channel"] == "own_delivery"
        assert delivery_order["order_type"] == "delivery"

    def test_channel_map_carryout(self):
        """Span channel='carryout' → synth channel='carryout'."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        carryout_go_id = make_id("otel", "bbbb2222cccc3333dddd4444eeee5555")
        carryout_order = next(
            r for r in result
            if r["event_type"] == "guest_order" and r["guest_order_id"] == carryout_go_id
        )
        assert carryout_order["channel"] == "carryout"

    def test_all_order_items_have_positive_unit_price(self):
        """Every order_item must have unit_price > 0 (guards positive_price in DLT)."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        for row in result:
            if row["event_type"] == "order_item":
                assert row["unit_price"] > 0, f"unit_price must be > 0, got {row['unit_price']}"

    def test_source_is_otel(self):
        """All emitted rows must carry source='otel'."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        assert len(result) > 0
        for row in result:
            assert row.get("source") == "otel", (
                f"expected source='otel', got {row.get('source')} in {row['event_type']}"
            )

    def test_delivery_order_emitted_for_delivery(self):
        """A delivery_order row must be emitted for delivery channel orders."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        delivery_go_id = make_id("otel", "aaaa1111bbbb2222cccc3333dddd4444")
        delivery_orders = [
            r for r in result
            if r["event_type"] == "delivery_order" and r["guest_order_id"] == delivery_go_id
        ]
        assert len(delivery_orders) == 1
        assert delivery_orders[0]["platform_order_reference"] == "TRK-CA-001"

    def test_no_delivery_order_for_carryout(self):
        """No delivery_order row should be emitted for carryout orders."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        carryout_go_id = make_id("otel", "bbbb2222cccc3333dddd4444eeee5555")
        delivery_orders = [
            r for r in result
            if r["event_type"] == "delivery_order" and r["guest_order_id"] == carryout_go_id
        ]
        assert len(delivery_orders) == 0

    def test_log_without_span_emits_guest_order_and_payment(self):
        """A log row with no matching span still produces guest_order and payment."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        # Only provide the log row, no spans
        log_only = [
            {
                "trace_id": "zzzz9999zzzz9999zzzz9999zzzz9999",
                "event_ts": "2025-06-12T08:00:00",
                "app.order.amount": 19.99,
                "app.order.items.count": 1,
                "user.id": "user-solo",
            }
        ]
        result = reshape_otel_orders(log_only, [], UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        event_types = {r["event_type"] for r in result}
        assert "guest_order" in event_types
        assert "payment" in event_types

    def test_since_ts_filter_excludes_old_rows(self):
        """Rows with event_ts <= since_ts must be excluded."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        # since_ts = 2025-06-13 (all fixture logs are on 2025-06-12)
        since_ts = datetime(2025, 6, 13, 0, 0, 0)
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS, since_ts=since_ts)
        assert result == []

    def test_since_ts_allows_newer_rows(self):
        """Rows after since_ts are included."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        logs, spans = _load_sample()
        # since_ts = 2025-06-11 (before all fixture logs)
        since_ts = datetime(2025, 6, 11, 0, 0, 0)
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS, since_ts=since_ts)
        # Should have emitted the 2 valid real orders
        guest_orders = [r for r in result if r["event_type"] == "guest_order"]
        assert len(guest_orders) == 2

    def test_injected_member_id_sets_profile_and_member(self):
        """A web-injected app.order.member_id (12345) sets BOTH member_id and
        profile_id on the guest_order (synth semantics: member_id == profile_id)."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        # Row 1 (trace aaaa...) carries app.order.member_id = "12345"
        go_id = make_id("otel", "aaaa1111bbbb2222cccc3333dddd4444")
        go = next(r for r in result if r["event_type"] == "guest_order" and r["guest_order_id"] == go_id)
        assert go["member_id"] == 12345
        assert go["profile_id"] == 12345

    def test_order_without_member_id_stays_anonymous(self):
        """A real order with no app.order.member_id keeps member_id/profile_id None."""
        from src.refresh.otel_order_adapter import reshape_otel_orders
        from src.generator.id_utils import make_id
        logs, spans = _load_sample()
        result = reshape_otel_orders(logs, spans, UNIT_IDS_BY_STATE, ALL_UNIT_IDS)
        # Row 2 (trace bbbb...) has no member_id
        go_id = make_id("otel", "bbbb2222cccc3333dddd4444eeee5555")
        go = next(r for r in result if r["event_type"] == "guest_order" and r["guest_order_id"] == go_id)
        assert go["member_id"] is None
        assert go["profile_id"] is None
