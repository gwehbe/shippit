import csv
import random
from datetime import datetime, timedelta

def generate_random_id(prefix):
    return f"{prefix}_{random.randint(100, 999)}"

def generate_csv():
    # 1:1 mapping - pre-generate unique shipment ids for each order
    order_ids = [f"ord_{i:03d}" for i in range(1, 35)]
    shipment_mapping = {oid: generate_random_id("shp") for oid in order_ids}
    
    event_types = ["order_created", "shipment_booked", "shipment_dispatched", "delivery_completed"]
    carriers = ["aus_post", "dhl", "couriers_please", "aramex"]
    services = ["express", "standard"]
    
    data = []
    used_event_ids = set()
    base_time = datetime(2026, 1, 12, 10, 0, 0)

    def get_unique_event_id():
        while True:
            eid = generate_random_id("evt")
            if eid not in used_event_ids:
                used_event_ids.add(eid)
                return eid

    for i, order_id in enumerate(order_ids):
        # create a logical sequence for each order
        order_start = base_time + timedelta(minutes=i * 10)
        
        for j, etype in enumerate(event_types):
            event_ts = order_start + timedelta(hours=j * 2)
            ingested_at = event_ts + timedelta(minutes=random.randint(1, 5))
            
            # constraints: unique event_id, shared order/shipment IDs
            event_id = get_unique_event_id()
            ship_id = shipment_mapping[order_id] if j > 0 else ""
            
            carrier = random.choice(carriers) if j > 0 else ""
            service = f'{{"service_level": "{random.choice(services)}"}}' if j > 0 else "{}"
            
            data.append([
                event_id, etype, order_id, ship_id, 
                event_ts.isoformat() + "Z", ingested_at.isoformat() + "Z", 
                carrier, service
            ])
    
    # --- inject deliberate "messy" data for integrity mechanisms ---

    # 1. add duplicates
    for _ in range(5):
        dup = data[random.randint(0, len(data)-1)].copy()
        # Slightly offset ingestion time to simulate a retry
        original_ingestion = datetime.fromisoformat(dup[5].replace("Z", ""))
        dup[5] = (original_ingestion + timedelta(seconds=30)).isoformat() + "Z"
        data.append(dup)

    # 2. add late arriving data
    data.append([
        get_unique_event_id(), "order_created", "ord_999", "", 
        "2026-01-11T09:00:00Z", "2026-01-12T23:59:59Z", "", ""
    ])

    # 3. add out-of-order delivery
    data.append([
        get_unique_event_id(), "delivery_completed", "ord_999", "shp_999", 
        "2026-01-11T15:00:00Z", "2026-01-12T23:50:00Z", "dhl", "express"
    ])

    # shuffle to simulate a real unsorted event stream
    random.shuffle(data)

    # write to seeds folder
    with open('seeds/shipment_lifecycle_events.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "event_type", "order_id", "shipment_id", "event_timestamp", "ingested_at", "carrier", "attributes"])
        writer.writerows(data)

    print(f"Generated {len(data)} records in seeds/shipment_events.csv")

if __name__ == "__main__":
    generate_csv()
