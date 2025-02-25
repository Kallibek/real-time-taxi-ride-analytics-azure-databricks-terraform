#!/usr/bin/env python3
import os
import json
import time
import argparse
import pandas as pd
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

def load_historical_data(input_file):
    """Load the historical taxi data CSV."""
    print(f"Loading historical data from {input_file}...")
    df = pd.read_csv(input_file)

    # Convert datetime columns to pandas datetime objects.
    datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
        else:
            print(f"Warning: Column '{col}' not found in the CSV.")
    return df

def create_producer(conn_str_file):
    """
    Read the Azure Event Hub connection string from file
    and create an EventHubProducerClient.
    """
    if not os.path.exists(conn_str_file):
        raise FileNotFoundError(f"{conn_str_file} not found. Ensure the file is generated and accessible.")

    with open(conn_str_file, "r") as file:
        connection_string = file.read().strip()

    producer = EventHubProducerClient.from_connection_string(conn_str=connection_string)
    return producer

def generate_and_send_event(df, producer):
    """Randomly sample a row, adjust the timestamps, and send as an event."""
    # Randomly sample one row from the DataFrame
    sample = df.sample(n=1).iloc[0]

    # Calculate the original duration between dropoff and pickup.
    original_dropoff = sample["tpep_dropoff_datetime"]
    original_pickup = sample["tpep_pickup_datetime"]
    duration = original_dropoff - original_pickup

    # Use the current UTC time as the new dropoff timestamp
    new_dropoff = datetime.utcnow()
    # Preserve the original duration for pickup
    new_pickup = new_dropoff - duration

    # Convert the sampled row to a dictionary
    taxi_event = sample.to_dict()

    # Update the datetime fields to reflect new pickup/dropoff times
    taxi_event["tpep_dropoff_datetime"] = new_dropoff.strftime("%Y-%m-%d %H:%M:%S")
    taxi_event["tpep_pickup_datetime"] = new_pickup.strftime("%Y-%m-%d %H:%M:%S")

    # With 10% chance, set one of the datetime fields to null.
    if random.random() < 0.1:
        if random.choice([True, False]):
            taxi_event["tpep_pickup_datetime"] = None
            print("Introducing missing value: tpep_pickup_datetime set to null.")
        else:
            taxi_event["tpep_dropoff_datetime"] = None
            print("Introducing missing value: tpep_dropoff_datetime set to null.")

    # Convert the dictionary to JSON
    event_payload = json.dumps(taxi_event, default=str)

    try:
        # Create a batch and add the event
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(event_payload))
        producer.send_batch(event_data_batch)

        # Provide basic logging
        print(
            f"Sent event with VendorID={taxi_event.get('VendorID', 'N/A')} "
            f"| Dropoff={taxi_event.get('tpep_dropoff_datetime', 'N/A')} "
            f"| Pickup={taxi_event.get('tpep_pickup_datetime', 'N/A')}"
        )
    except Exception as e:
        print("Error while sending event:", e)

def main():
    parser = argparse.ArgumentParser(description="Generate real-time taxi data events by sampling historical data.")
    parser.add_argument(
        "--input-file",
        type=str,
        default="data/yellow_tripdata_2024-11.csv",
        help="Path to the historical CSV file"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1,
        help="Interval (in seconds) between each event (default: 2 seconds)"
    )
    parser.add_argument(
        "--conn-str-file",
        type=str,
        default="src/taxi_eventhub_connection_string.txt",
        help="Path to the file containing the Event Hub connection string"
    )
    args = parser.parse_args()

    # Load historical data
    df = load_historical_data(args.input_file)
    if df.empty:
        print("No data loaded. Exiting.")
        return

    # Create the Event Hub producer client
    producer = create_producer(args.conn_str_file)

    print("Starting real-time event generation. Press Ctrl+C to stop.")
    try:
        while True:
            generate_and_send_event(df, producer)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Event generation interrupted by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
