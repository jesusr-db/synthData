from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients

def build_units_df_data(num_units: int = 250, seed: int = 42) -> list[dict]:
    return generate_units(num_units, seed=seed)

def build_franchisees_data(num_units: int = 250) -> list[dict]:
    units = generate_units(num_units)
    franchisee_ids = {u["franchisee_id"] for u in units if u["franchisee_id"]}
    return [
        {
            "franchisee_id": fid,
            "franchisee_name": f"QSR Franchise Group #{fid}",
            "contact_email": f"ops{fid}@qsrfranchise.com",
            "status": "active",
        }
        for fid in sorted(franchisee_ids)
    ]

def build_financial_periods_data(backfill_months: int = 12) -> list[dict]:
    rows = []
    today = date.today()
    start = (today - relativedelta(months=backfill_months)).replace(day=1)
    period_id = 1
    current = start
    while current <= today + relativedelta(months=1):
        end = (current + relativedelta(months=1)) - timedelta(days=1)
        rows.append({
            "financial_period_id": period_id,
            "period_name": current.strftime("%b %Y"),
            "start_date": current.isoformat(),
            "end_date": end.isoformat(),
            "fiscal_year": current.year,
            "fiscal_quarter": (current.month - 1) // 3 + 1,
            "status": "closed" if end < today else "open",
        })
        current += relativedelta(months=1)
        period_id += 1
    return rows

def build_suppliers_data() -> list[dict]:
    return [
        {"supplier_id": 1, "supplier_name": "US Foods", "category": "food_beverage", "status": "active"},
        {"supplier_id": 2, "supplier_name": "Sysco", "category": "food_beverage", "status": "active"},
        {"supplier_id": 3, "supplier_name": "Performance Food Group", "category": "food_beverage", "status": "active"},
        {"supplier_id": 4, "supplier_name": "Ecolab", "category": "cleaning_supplies", "status": "active"},
        {"supplier_id": 5, "supplier_name": "ALSCO", "category": "uniforms", "status": "active"},
        {"supplier_id": 6, "supplier_name": "Domino's Supply Chain", "category": "dough_sauce", "status": "active"},
    ]

def seed_all(spark, catalog: str, num_units: int = 250, backfill_months: int = 12):
    """Write all reference tables to {catalog}.ref.*"""
    from pyspark.sql import Row

    def write(data: list[dict], table: str):
        if not data:
            return
        df = spark.createDataFrame([Row(**r) for r in data])
        df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.ref.{table}")

    write(build_units_df_data(num_units), "unit")
    write(build_franchisees_data(num_units), "franchisee")
    write(build_financial_periods_data(backfill_months), "financial_period")
    write(build_suppliers_data(), "supplier")
    write(get_menu_items(), "menu_item")
    write(get_recipe_ingredients(), "recipe_ingredient")
    # Phase 2 stubs — empty tables
    for stub_table in ("weather_conditions", "local_events"):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.ref.{stub_table}
            (stub_id BIGINT, placeholder STRING)
            USING DELTA
        """)
