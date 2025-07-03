import json
import time
from faker import Faker
from google.cloud import pubsub_v1
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# --- GCP Pub/Sub Configuration ---
# *** IMPORTANT: Make sure this Project ID is exactly "airy-ceremony-463816-t4" ***
project_id = "airy-ceremony-463816-t4"
topic_id = "orders-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# --- Function to generate a series of events for a single order ---
def generate_order_events():
    order_id = fake.uuid4()
    patient_name = fake.name()
    drug_name = fake.word().capitalize() + " " + fake.word().capitalize() + " (" + fake.word().upper() + ")"
    quantity = fake.random_int(min=1, max=100) # This will be an INTEGER for BigQuery
    pharmacy_id = fake.numerify(text="PH#####")
    prescription_id = fake.bothify(text="PRES#####?#")
    total_cost = round(fake.random_number(digits=3) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
    insurance_provider = fake.random_element(elements=('Blue Cross', 'Aetna', 'Cigna', 'UnitedHealthcare', 'Medicaid'))
    delivery_address = {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.postcode()
    }

    # Start with the order being placed today or recently
    order_placed_time = fake.date_time_between(start_date='-30d', end_date='now')

    events = []

    # 1. Order Placed Event
    events.append({
        "event_id": fake.uuid4(),
        "order_id": order_id,
        "event_type": "Order_Placed",
        "event_timestamp": order_placed_time.isoformat(),
        "status_after_event": "Pending",
        "patient_name": patient_name,
        "drug_name": drug_name,
        "quantity": quantity,
        "pharmacy_id": pharmacy_id,
        "prescription_id": prescription_id,
        "total_cost": total_cost,
        "delivery_address_street": delivery_address["street"],
        "delivery_address_city": delivery_address["city"],
        "delivery_address_state": delivery_address["state"],
        "delivery_address_zip_code": delivery_address["zip_code"],
        "insurance_provider": insurance_provider,
        "claim_status": None, # Not applicable for this event type
        "tracking_number": None, # Not applicable for this event type
    })

    # 2. Insurance Validation Event (can be Approved, Denied, or Pending Review)
    insurance_decision_time = order_placed_time + timedelta(hours=random.randint(1, 24))
    claim_status = fake.random_element(elements=('Approved', 'Denied', 'Pending Review'))
    events.append({
        "event_id": fake.uuid4(),
        "order_id": order_id,
        "event_type": "Insurance_Validation",
        "event_timestamp": insurance_decision_time.isoformat(),
        "status_after_event": "Processing" if claim_status == 'Approved' else "On Hold",
        "patient_name": patient_name, # Include core order details with each event for easier analysis
        "drug_name": drug_name,
        "quantity": quantity,
        "pharmacy_id": pharmacy_id,
        "prescription_id": prescription_id,
        "total_cost": total_cost,
        "delivery_address_street": delivery_address["street"],
        "delivery_address_city": delivery_address["city"],
        "delivery_address_state": delivery_address["state"],
        "delivery_address_zip_code": delivery_address["zip_code"],
        "insurance_provider": insurance_provider,
        "claim_status": claim_status, # Relevant for this event
        "tracking_number": None,
    })

    # 3. Order Shipped Event (only if insurance approved)
    if claim_status == 'Approved' and random.random() < 0.8: # 80% chance to ship if approved
        shipped_time = insurance_decision_time + timedelta(hours=random.randint(12, 48))
        tracking_number = fake.bothify(text="ABC#####?#?#")
        events.append({
            "event_id": fake.uuid4(),
            "order_id": order_id,
            "event_type": "Order_Shipped",
            "event_timestamp": shipped_time.isoformat(),
            "status_after_event": "Shipped",
            "patient_name": patient_name,
            "drug_name": drug_name,
            "quantity": quantity,
            "pharmacy_id": pharmacy_id,
            "prescription_id": prescription_id,
            "total_cost": total_cost,
            "delivery_address_street": delivery_address["street"],
            "delivery_address_city": delivery_address["city"],
            "delivery_address_state": delivery_address["state"],
            "delivery_address_zip_code": delivery_address["zip_code"],
            "insurance_provider": insurance_provider,
            "claim_status": "Approved", # Should be approved if shipped
            "tracking_number": tracking_number, # Relevant for this event
        })

        # 4. Order Delivered Event (only if shipped)
        if random.random() < 0.9: # 90% chance to deliver if shipped
            delivered_time = shipped_time + timedelta(hours=random.randint(24, 72))
            events.append({
                "event_id": fake.uuid4(),
                "order_id": order_id,
                "event_type": "Order_Delivered",
                "event_timestamp": delivered_time.isoformat(),
                "status_after_event": "Delivered",
                "patient_name": patient_name,
                "drug_name": drug_name,
                "quantity": quantity,
                "pharmacy_id": pharmacy_id,
                "prescription_id": prescription_id,
                "total_cost": total_cost,
                "delivery_address_street": delivery_address["street"],
                "delivery_address_city": delivery_address["city"],
                "delivery_address_state": delivery_address["state"],
                "delivery_address_zip_code": delivery_address["zip_code"],
                "insurance_provider": insurance_provider,
                "claim_status": "Approved", # Should be approved if delivered
                "tracking_number": tracking_number,
            })
    
    # Sort events by timestamp to ensure chronological order
    events.sort(key=lambda x: x['event_timestamp'])

    return events

# --- Main loop to generate and publish orders ---
def publish_order_events(num_orders=10):
    print(f"Generating events for {num_orders} fake PBM orders and publishing to {topic_path}")
    total_events_published = 0
    for i in range(num_orders):
        order_events = generate_order_events()
        for event in order_events:
            data = json.dumps(event).encode("utf-8") # Pub/Sub messages must be bytes
            future = publisher.publish(topic_path, data)
            # print(f"Published event ID: {event['event_id']} for order {event['order_id']}") # Uncomment for verbose output
            future.result() # Wait for publish to complete
            total_events_published += 1
            time.sleep(0.05) # Small delay between events

    print(f"Finished publishing {total_events_published} events for {num_orders} orders.")

if __name__ == "__main__":
    print("--- Starting PBM Order Event Data Generation ---")
    print("Ensure you have authenticated with 'gcloud auth application-default login'")
    publish_order_events(num_orders=10000) # GENERATE EVENTS FOR 10,000 ORDERS
    print("--- PBM Order Event Data Generation Complete ---")