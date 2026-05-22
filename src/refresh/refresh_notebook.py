# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog_name             = _widget("catalog_name", "jmrdemo")
schema_prefix            = _widget("schema_prefix", "synth_")
tm_secret_scope          = _widget("ticketmaster_secret_scope", "qsr-synth")
tm_secret_key            = _widget("ticketmaster_secret_key", "ticketmaster_consumer_key")
sg_secret_scope          = _widget("seatgeek_secret_scope", "qsr-synth")
sg_secret_key            = _widget("seatgeek_secret_key", "seatgeek_client_id")

print(f"[INFO] refresh_notebook: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
from datetime import datetime, timedelta, date

from src.refresh.openmeteo_client import fetch_metro_weather
from src.refresh.noaa_client import fetch_state_alerts
from src.refresh.nager_client import fetch_us_holidays
from src.refresh.events_client import fetch_ticketmaster_events, fetch_seatgeek_events
from src.refresh.multiplier_engine import load_config, compute_weather_multipliers, compute_event_multiplier

cfg = load_config()
today = date.today()
start_date = (today - timedelta(days=30)).isoformat()
end_date   = (today + timedelta(days=14)).isoformat()
refreshed_at = datetime.now().isoformat()

# COMMAND ----------
# Load distinct metros from ref.unit
metro_rows = spark.sql(f"""
    SELECT DISTINCT metro_area, state,
           AVG(latitude) AS lat, AVG(longitude) AS lon
    FROM {catalog_name}.{schema_prefix}ref.unit
    GROUP BY metro_area, state
""").collect()
print(f"[INFO] Refreshing {len(metro_rows)} metros")

# COMMAND ----------
# Step 1: Fetch weather + apply NOAA alerts per state
weather_by_metro_date = {}  # (metro, date_str) → dict

for m in metro_rows:
    try:
        rows = fetch_metro_weather(m.metro_area, m.lat, m.lon)
        for r in rows:
            weather_by_metro_date[(m.metro_area, r["forecast_date"])] = r
        print(f"[OK] Open-Meteo: {m.metro_area} ({len(rows)} rows)")
    except Exception as e:
        print(f"[WARN] Open-Meteo failed for {m.metro_area}: {e}")

# Apply NOAA alerts — keyed by state, spread across matching date rows
states = list({m.state for m in metro_rows})
for state in states:
    try:
        alerts = fetch_state_alerts(state)
        state_metros = [m.metro_area for m in metro_rows if m.state == state]
        for alert in alerts:
            onset_str  = alert["onset"][:10] if alert["onset"] else ""
            expires_str = alert["expires"][:10] if alert["expires"] else ""
            for metro in state_metros:
                for (m_area, d_str), row in weather_by_metro_date.items():
                    if m_area == metro and onset_str <= d_str <= expires_str:
                        # Use the highest-severity alert level
                        existing = row.get("alert_level")
                        severity_rank = {"warning": 3, "watch": 2, "advisory": 1}
                        new_rank = severity_rank.get(alert["alert_level"], 0)
                        old_rank = severity_rank.get(existing, 0)
                        if new_rank > old_rank:
                            row["alert_level"] = alert["alert_level"]
        if alerts:
            print(f"[OK] NOAA alerts: {state} — {len(alerts)} classifiable alerts")
    except Exception as e:
        print(f"[WARN] NOAA alerts failed for {state}: {e}")

# Compute multipliers and add to each weather row
for row in weather_by_metro_date.values():
    demand_mult, shift = compute_weather_multipliers(
        row["weather_condition"], row.get("alert_level"), cfg
    )
    row["demand_multiplier"] = demand_mult
    row["channel_shift_delivery"] = shift
    row["refreshed_at"] = refreshed_at

# COMMAND ----------
# Step 2: Write weather_conditions via MERGE
from pyspark.sql import Row

weather_rows = list(weather_by_metro_date.values())
if weather_rows:
    weather_df = spark.createDataFrame([Row(**r) for r in weather_rows])
    weather_df.createOrReplaceTempView("_weather_refresh")
    spark.sql(f"""
        MERGE INTO {catalog_name}.{schema_prefix}ref.weather_conditions t
        USING _weather_refresh s
        ON t.metro_area = s.metro_area AND t.forecast_date = s.forecast_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[OK] Merged {len(weather_rows)} rows into ref.weather_conditions")

# COMMAND ----------
# Step 3: Fetch events — holidays (always) + Ticketmaster + SeatGeek (optional)
event_rows_by_id = {}  # event_id → dict

# Nager.Date holidays for current + next year
for year in [today.year, today.year + 1]:
    for m in metro_rows:
        try:
            rows = fetch_us_holidays(year, m.state)
            for r in rows:
                event_rows_by_id[r["event_id"]] = {**r, "metro_area": m.metro_area,
                                                    "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                                                    "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] Nager holidays failed for {m.state} {year}: {e}")

# Ticketmaster (optional)
try:
    tm_key = dbutils.secrets.get(scope=tm_secret_scope, key=tm_secret_key)
    for m in metro_rows:
        try:
            rows = fetch_ticketmaster_events(m.metro_area, m.state, start_date, end_date, tm_key)
            for r in rows:
                event_rows_by_id[r["event_id"]] = {**r,
                    "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                    "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] Ticketmaster failed for {m.metro_area}: {e}")
    print(f"[OK] Ticketmaster: fetched events for {len(metro_rows)} metros")
except Exception:
    print("[INFO] Ticketmaster secret not configured — skipping (holidays still populated)")

# SeatGeek (optional)
try:
    sg_key = dbutils.secrets.get(scope=sg_secret_scope, key=sg_secret_key)
    for m in metro_rows:
        try:
            rows = fetch_seatgeek_events(m.metro_area, m.state, start_date, end_date, sg_key)
            for r in rows:
                if r["event_id"] not in event_rows_by_id:
                    event_rows_by_id[r["event_id"]] = {**r,
                        "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                        "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] SeatGeek failed for {m.metro_area}: {e}")
    print(f"[OK] SeatGeek: fetched events for {len(metro_rows)} metros")
except Exception:
    print("[INFO] SeatGeek secret not configured — skipping")

# COMMAND ----------
# Step 4: Write local_events via MERGE
event_rows = list(event_rows_by_id.values())
if event_rows:
    event_df = spark.createDataFrame([Row(**r) for r in event_rows])
    event_df.createOrReplaceTempView("_events_refresh")
    spark.sql(f"""
        MERGE INTO {catalog_name}.{schema_prefix}ref.local_events t
        USING _events_refresh s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[OK] Merged {len(event_rows)} rows into ref.local_events")

print("[INFO] Weather & events refresh complete")
