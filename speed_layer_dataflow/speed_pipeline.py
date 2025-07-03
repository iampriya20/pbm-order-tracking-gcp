import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from apache_beam.transforms.window import SlidingWindows
import json
from datetime import datetime

# --- Configuration ---
PROJECT_ID = "airy-ceremony-463816-t4"
# Corrected INPUT_TOPIC to match your existing Pub/Sub topic
INPUT_TOPIC = f"projects/{PROJECT_ID}/topics/orders-topic" 
OUTPUT_TABLE = f"{PROJECT_ID}:pbm_gold.live_kpis"
# Corrected GCS_TEMP_LOCATION to match your existing bucket
GCS_TEMP_LOCATION = f'gs://pbm_storage_account/temp'

def run():
    # Define pipeline options for a streaming job on Dataflow
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        runner='DataflowRunner',
        region='us-central1',
        temp_location=GCS_TEMP_LOCATION,
        # Staging location is typically a subfolder of temp_location
        staging_location=f'{GCS_TEMP_LOCATION}/staging'
    )

    with beam.Pipeline(options=options) as p:
        # 1. Read from Pub/Sub in a continuous stream and parse the JSON
        events = (
            p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
              | "ParseJSON" >> beam.Map(lambda x: json.loads(x))
        )

        # --- Metric 1: Orders in the Last 5 Minutes ---
        (
            events
            | "FilterPlacedEvents" >> beam.Filter(lambda e: e.get('event_type', '').lower() == 'order_placed')
            # Sliding window: 5 minutes (300 seconds) duration, slides every 1 minute (60 seconds)
            | "WindowPlacedInto5Min" >> beam.WindowInto(
                SlidingWindows(300, 60), 
                trigger=AfterWatermark(early=AfterProcessingTime(10)), # Trigger every 10s of processing time if no watermark progress
                accumulation_mode=AccumulationMode.DISCARDING # Each window output is independent
              ) 
            | "CountPlacedEvents" >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | "FormatPlacedOutput" >> beam.Map(lambda count: {
                'metric_name': 'orders_last_5_min',
                'metric_value': count,
                'last_updated': datetime.utcnow().isoformat()
            })
            | "WritePlacedToBQ" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Append new metrics
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER # Table must exist
            )
        )

        # --- Metric 2: Shipped in the Last 5 Minutes ---
        (
            events
            | "FilterShippedEvents" >> beam.Filter(lambda e: e.get('event_type', '').lower() == 'order_shipped')
            | "WindowShippedInto5Min" >> beam.WindowInto(
                SlidingWindows(300, 60),
                trigger=AfterWatermark(early=AfterProcessingTime(10)),
                accumulation_mode=AccumulationMode.DISCARDING
              )
            | "CountShippedEvents" >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | "FormatShippedOutput" >> beam.Map(lambda count: {
                'metric_name': 'shipped_last_5_min',
                'metric_value': count,
                'last_updated': datetime.utcnow().isoformat()
            })
            | "WriteShippedToBQ" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    print("Starting Speed Layer Dataflow deployment...")
    run()