import random
from src.generator.entropy import unit_volume_bias

# Representative US QSR metros with approximate lat/lon and population weight
US_METROS = [
    {"name": "New York-Newark",     "state": "NY", "lat": 40.71, "lon": -74.01, "weight": 10},
    {"name": "Los Angeles",         "state": "CA", "lat": 34.05, "lon": -118.24, "weight": 9},
    {"name": "Chicago",             "state": "IL", "lat": 41.88, "lon": -87.63, "weight": 7},
    {"name": "Dallas-Fort Worth",   "state": "TX", "lat": 32.78, "lon": -96.80, "weight": 6},
    {"name": "Houston",             "state": "TX", "lat": 29.76, "lon": -95.37, "weight": 6},
    {"name": "Atlanta",             "state": "GA", "lat": 33.75, "lon": -84.39, "weight": 5},
    {"name": "Phoenix",             "state": "AZ", "lat": 33.45, "lon": -112.07, "weight": 5},
    {"name": "Philadelphia",        "state": "PA", "lat": 39.95, "lon": -75.17, "weight": 5},
    {"name": "Miami",               "state": "FL", "lat": 25.77, "lon": -80.19, "weight": 4},
    {"name": "Seattle",             "state": "WA", "lat": 47.61, "lon": -122.33, "weight": 4},
    {"name": "Denver",              "state": "CO", "lat": 39.74, "lon": -104.98, "weight": 3},
    {"name": "Boston",              "state": "MA", "lat": 42.36, "lon": -71.06, "weight": 3},
    {"name": "Minneapolis",         "state": "MN", "lat": 44.98, "lon": -93.27, "weight": 3},
    {"name": "San Antonio",         "state": "TX", "lat": 29.42, "lon": -98.49, "weight": 3},
    {"name": "Columbus",            "state": "OH", "lat": 39.96, "lon": -83.00, "weight": 2},
    {"name": "Charlotte",           "state": "NC", "lat": 35.23, "lon": -80.84, "weight": 2},
    {"name": "Indianapolis",        "state": "IN", "lat": 39.77, "lon": -86.16, "weight": 2},
    {"name": "Nashville",           "state": "TN", "lat": 36.17, "lon": -86.78, "weight": 2},
    {"name": "Las Vegas",           "state": "NV", "lat": 36.17, "lon": -115.14, "weight": 2},
    {"name": "Louisville",          "state": "KY", "lat": 38.25, "lon": -85.76, "weight": 1},
]

def _assign_districts(units: list[dict]) -> list[dict]:
    """Assign district_id (5–8 units each) and region_id (3–5 districts each)."""
    metros = list({u["metro_area"] for u in units})
    metro_to_district = {}
    district_id = 1
    region_id = 1
    districts_in_region = 0
    for metro in sorted(metros):
        metro_to_district[metro] = district_id
        district_id += 1
        districts_in_region += 1
        if districts_in_region >= 4:
            region_id += 1
            districts_in_region = 0
    for u in units:
        u["district_id"] = metro_to_district[u["metro_area"]]
        u["region_id"] = (metro_to_district[u["metro_area"]] - 1) // 4 + 1
    return units

def generate_units(num_units: int, seed: int = 42) -> list[dict]:
    random.seed(seed)
    weights = [m["weight"] for m in US_METROS]
    units = []
    # ~20% corporate-owned, ~80% franchised across ~40 franchisees
    franchisee_pool = list(range(1, 41))
    for i in range(1, num_units + 1):
        metro = random.choices(US_METROS, weights=weights, k=1)[0]
        is_franchise = random.random() < 0.80
        franchisee_id = random.choice(franchisee_pool) if is_franchise else None
        lat_jitter = random.uniform(-0.3, 0.3)
        lon_jitter = random.uniform(-0.3, 0.3)
        units.append({
            "unit_id": i,
            "unit_name": f"Domino's #{1000 + i}",
            "city": metro["name"].split("-")[0],
            "state": metro["state"],
            "lat": round(metro["lat"] + lat_jitter, 5),
            "lon": round(metro["lon"] + lon_jitter, 5),
            "metro_area": metro["name"],
            "district_id": None,   # assigned below
            "region_id": None,
            "franchisee_id": franchisee_id,
            "format": "carryout_delivery",
            "unit_volume_bias": round(unit_volume_bias(), 4),
            "is_franchise": is_franchise,
            "status": "active",
        })
    return _assign_districts(units)
