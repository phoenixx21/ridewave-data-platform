# Block 2 - Data Generator
# WHY: Simulates RideWave's historical ride, driver, and payment data
#      that needs to be migrated to the new data platform
# HOW: Generates 5 CSV files with realistic data including
#      intentional nulls that Silver layer must handle

import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)  # ensures same data every run — important for testing

CITIES    = ["Mumbai","Delhi","Bengaluru","Chennai","Pune","Hyderabad"]
STATUSES  = ["completed","completed","completed","cancelled","no_show"]
VEHICLES  = ["Bike","Auto","Sedan","SUV","Mini"]
PAY_MODES = ["UPI","Card","Cash","Wallet"]
base_date = datetime(2024, 1, 1)

# ── 1. DRIVERS (100 rows) ─────────────────────────────────────────
drivers = []
for i in range(1, 101):
    drivers.append({
        "driver_id"   : f"DRV{i:03d}",
        "driver_name" : f"Driver_{i}",
        "city"        : random.choice(CITIES),
        "vehicle_type": random.choice(VEHICLES),
        "rating"      : round(random.uniform(3.5, 5.0), 1),
        "total_rides" : random.randint(50, 2000),
        "joined_date" : (base_date - timedelta(
                          days=random.randint(30,730))
                        ).strftime("%Y-%m-%d"),
        "is_active"   : random.choice(["Y","Y","Y","N"])
    })

with open("data/drivers.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=drivers[0].keys())
    w.writeheader(); w.writerows(drivers)

# ── 2. VEHICLES (30 rows) ─────────────────────────────────────────
vehicles = []
for i in range(1, 31):
    vehicles.append({
        "vehicle_id"  : f"VEH{i:03d}",
        "vehicle_type": random.choice(VEHICLES),
        "model"       : random.choice(["Honda Activa","TVS Jupiter",
                                       "Maruti Swift","Hyundai i20",
                                       "Toyota Innova"]),
        "year"        : random.randint(2018, 2024),
        "base_fare"   : round(random.uniform(30, 150), 2),
        "per_km_rate" : round(random.uniform(8, 25), 2)
    })

with open("data/vehicles.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=vehicles[0].keys())
    w.writeheader(); w.writerows(vehicles)

# ── 3. RIDES (500 rows) ───────────────────────────────────────────
# NOTE: Nulls are intentionally injected here
# Silver layer will handle these — do not remove them
rides = []
for i in range(1, 501):
    fare   = round(random.uniform(50, 800), 2)
    status = random.choice(STATUSES)
    if i % 20 == 0: fare   = None  # intentional null for Silver
    if i % 25 == 0: status = None  # intentional null for Silver

    rides.append({
        "ride_id"     : f"RID{i:04d}",
        "driver_id"   : f"DRV{random.randint(1,100):03d}",
        "customer_id" : f"CUST{random.randint(1,200):03d}",
        "vehicle_id"  : f"VEH{random.randint(1,30):03d}",
        "city"        : random.choice(CITIES),
        "fare_amount" : fare,
        "distance_km" : round(random.uniform(1.5, 35.0), 2),
        "ride_status" : status,
        "ride_date"   : (base_date + timedelta(
                          days=random.randint(0,89))
                        ).strftime("%Y-%m-%d"),
        "pickup_time" : f"{random.randint(6,23):02d}:{random.randint(0,59):02d}"
    })

with open("data/rides.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rides[0].keys())
    w.writeheader(); w.writerows(rides)

# ── 4. PAYMENTS (500 rows) ────────────────────────────────────────
payments = []
for i, ride in enumerate(rides, 1):
    payments.append({
        "payment_id"    : f"PAY{i:04d}",
        "ride_id"       : ride["ride_id"],
        "payment_mode"  : random.choice(PAY_MODES),
        "amount_paid"   : ride["fare_amount"],
        "payment_status": random.choice(["success","success",
                                         "success","failed","pending"]),
        "payment_date"  : ride["ride_date"]
    })

with open("data/payments.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=payments[0].keys())
    w.writeheader(); w.writerows(payments)

# ── 5. TRIPS (~600-900 rows) ──────────────────────────────────────
trips = []
for ride in rides[:300]:
    for stop in range(1, random.randint(2,4)):
        trips.append({
            "trip_id"    : str(uuid.uuid4())[:8].upper(),
            "ride_id"    : ride["ride_id"],
            "stop_number": stop,
            "location"   : random.choice(["Airport","Station","Mall",
                                          "Hospital","Office","Home"]),
            "wait_mins"  : random.randint(0, 15),
            "km_to_next" : round(random.uniform(1, 12), 2)
        })

with open("data/trips.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=trips[0].keys())
    w.writeheader(); w.writerows(trips)

print(f"drivers.csv  : {len(drivers)} rows")
print(f"vehicles.csv : {len(vehicles)} rows")
print(f"rides.csv    : {len(rides)} rows")
print(f"payments.csv : {len(payments)} rows")
print(f"trips.csv    : {len(trips)} rows")
print("All 5 CSV files generated in data/ folder")
