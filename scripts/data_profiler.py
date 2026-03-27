import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pandas as pd
from ridewave_utils import log_summary

FILES = [
    {"file": "data/drivers.csv",  "key": "driver_id"},
    {"file": "data/vehicles.csv", "key": "vehicle_id"},
    {"file": "data/rides.csv",    "key": "ride_id"},
    {"file": "data/payments.csv", "key": "payment_id"},
    {"file": "data/trips.csv",    "key": "trip_id"},
]

for item in FILES:
    df          = pd.read_csv(item["file"])
    rows, cols  = df.shape
    null_counts = {col: int(df[col].isnull().sum()) for col in df.columns}
    dup_count   = int(df.duplicated(subset=[item["key"]]).sum())

    null_report = {
        "column"    : item["key"],
        "null_count": null_counts.get(item["key"], 0),
        "valid"     : null_counts.get(item["key"], 0) == 0
    }

    print(f"\nFile      : {item['file']}")
    print(f"Rows      : {rows} | Columns: {cols}")
    print(f"Null counts: {null_counts}")
    print(f"Duplicate key ({item['key']}): {dup_count} duplicates")

    log_summary(item["file"].split("/")[-1], rows, null_report, dup_count)