# Block 5 - Lambda Auto Producer
# WHY: Automatically sends 10 ride events per minute to Kinesis
#      simulating continuous real-time data from RideWave app
# HOW: EventBridge triggers this Lambda every 1 minute
#      Lambda sends 10 events then sleeps until next trigger
# IMPORTANT: Disable EventBridge trigger after lab to avoid
#            flooding S3 and incurring unnecessary costs

import boto3, json, random, uuid
from datetime import datetime

kinesis  = boto3.client('kinesis', region_name='eu-central-1')

# CHANGE YOUR_NAME below
STREAM   = 'ridewave-rides-stream-ashreya'
CITIES   = ["Mumbai","Delhi","Bengaluru","Chennai","Pune","Hyderabad"]
STATUSES = ["completed","completed","confirmed","cancelled"]
VEHICLES = ["Bike","Auto","Sedan","SUV"]

def lambda_handler(event, context):
    sent = 0; failed = 0

    for i in range(10):
        try:
            ride = {
                "ride_id"    : str(uuid.uuid4())[:8].upper(),
                "driver_id"  : f"DRV{random.randint(1,100):03d}",
                "customer_id": f"CUST{random.randint(1,200):03d}",
                "city"       : random.choice(CITIES),
                "fare_amount": round(random.uniform(50,800),2),
                "distance_km": round(random.uniform(1.5,35.0),2),
                "ride_status": random.choice(STATUSES),
                "vehicle_type": random.choice(VEHICLES),
                "payment_mode": random.choice(["UPI","Card","Cash","Wallet"]),
                "event_time" : datetime.utcnow().isoformat(),
                "ingest_date": datetime.utcnow().strftime("%Y-%m-%d")
            }
            kinesis.put_record(
                StreamName=STREAM, Data=json.dumps(ride),
                PartitionKey=ride["ride_id"]
            )
            sent += 1
            print(f"Sent: {ride['ride_id']} | {ride['city']} | Rs{ride['fare_amount']}")
        except Exception as e:
            failed += 1
            print(f"Failed: {str(e)}")

    print(f"Summary: Sent={sent} | Failed={failed}")
    return {"statusCode": 200, "sent": sent, "failed": failed}