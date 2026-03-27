# Block 5 - Kinesis Producer
# WHY: Simulates RideWave mobile app sending real-time ride events
#      In production this would be the actual app backend service
# HOW: Uses OOP class pattern — industry standard for producers
#      Sends 50 ride events to Kinesis stream with error handling
# RUN ON: AWS CloudShell (not laptop — no credentials needed)

import boto3
import csv
import json
import random
import time
import uuid
from datetime import datetime


class RideEventProducer:
    """
    Sends ride events to Kinesis Data Stream.
    Simulates real-time ride requests from RideWave app.
    """

    CONFIG = {
        "stream_name"  : "ridewave-rides-stream-YOUR_NAME",  # ← change YOUR_NAME
        "region"       : "us-east-1",
        "batch_size"   : 50,
        "delay_seconds": 0.1
    }

    def __init__(self):
        self.kinesis = boto3.client(
            "kinesis",
            region_name=self.CONFIG["region"]
        )
        self.sent   = 0
        self.failed = 0

    def build_event(self, row):
        """Adds event_timestamp to each ride record."""
        event = dict(row)
        event["event_timestamp"] = datetime.utcnow().isoformat()
        event["event_id"]        = str(uuid.uuid4())[:8].upper()
        return json.dumps(event)

    def send_event(self, event_json):
        """Sends one event to Kinesis. Handles errors gracefully."""
        try:
            self.kinesis.put_record(
                StreamName   = self.CONFIG["stream_name"],
                Data         = event_json,
                PartitionKey = str(uuid.uuid4())
            )
            self.sent += 1
        except Exception as e:
            self.failed += 1
            print(f"[RideWave] ERROR: {str(e)}")

    def run(self, csv_path):
        """Reads rides.csv and sends each row as a Kinesis event."""
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            count  = 0
            for row in reader:
                if count >= self.CONFIG["batch_size"]:
                    break
                self.send_event(self.build_event(row))
                count += 1
                time.sleep(self.CONFIG["delay_seconds"])

        print(f"\n[RideWave] Summary")
        print(f"  Sent   : {self.sent}")
        print(f"  Failed : {self.failed}")


if __name__ == "__main__":
    producer = RideEventProducer()
    producer.run("data/rides.csv")