import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import os

def modify_datetime(original_dt, new_date):
    """
    Given a datetime object and a new_date (a date object), return a new datetime with new_date and original time.
    """
    return datetime.combine(new_date, original_dt.time())

def maybe_set_nulls(row):
    # With a 1% chance, set one of the datetime columns to null (pd.NaT)
    if np.random.rand() < 0.02:
        # Randomly decide which column to null out.
        if np.random.rand() < 0.5:
            row["tpep_pickup_datetime"] = pd.NaT
        else:
            row["tpep_dropoff_datetime"] = pd.NaT
    return row

def main():
    parser = argparse.ArgumentParser(
        description="Generate new taxi data for a configurable date range by sampling rows from an existing CSV file."
    )
    parser.add_argument("--input-file", type=str, default="data/yellow_tripdata_2024-11.csv",
                        help="Path to the input CSV file (default: data/yellow_tripdata_2024-11.csv)")
    parser.add_argument("--start-date", type=str, default='2025-02-01',
                        help="Start date for the new data in YYYY-MM-DD format (e.g. 2025-01-01)")
    parser.add_argument("--end-date", type=str, default='2025-02-24',
                        help="End date for the new data in YYYY-MM-DD format (e.g. 2025-01-31)")
    parser.add_argument("--rows-per-day", type=int, default=5000,
                        help="Number of rows to sample for each day (default: 5000)")
    parser.add_argument("--output-file", type=str, default=None,
                        help="Path to the output CSV file. If not provided, a default file name including start and end dates will be used.")
    
    args = parser.parse_args()

    # Parse the configured start and end dates.
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        print("Error: Start and end dates must be in YYYY-MM-DD format.")
        return

    if start_date > end_date:
        print("Error: Start date must be before or equal to end date.")
        return

    # Set default output file name if not provided
    if not args.output_file:
        args.output_file = f"data/generated_historical_taxi_data/yellow_tripdata_{args.start_date}_to_{args.end_date}.csv"

    # Load the historical taxi data.
    print(f"Loading data from {args.input_file}...")
    df = pd.read_csv(args.input_file)

    # Convert the datetime columns to pandas datetime objects.
    datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
        else:
            print(f"Warning: Column '{col}' not found in the CSV. Please adjust the column names as necessary.")

    # Prepare a list to hold daily data.
    daily_data = []
    current_date = start_date

    print("Processing daily samples...")
    while current_date <= end_date:
        # Sample rows for the current day.
        if len(df) >= args.rows_per_day:
            sample_df = df.sample(n=args.rows_per_day, replace=False, random_state=np.random.randint(0, 10000))
        else:
            sample_df = df.sample(n=args.rows_per_day, replace=True, random_state=np.random.randint(0, 10000))

        # Update the datetime columns to use the current day while preserving the original time.
        if "tpep_pickup_datetime" in sample_df.columns:
            sample_df["tpep_pickup_datetime"] = sample_df["tpep_pickup_datetime"].apply(lambda dt: modify_datetime(dt, current_date))
        if "tpep_dropoff_datetime" in sample_df.columns:
            sample_df["tpep_dropoff_datetime"] = sample_df["tpep_dropoff_datetime"].apply(lambda dt: modify_datetime(dt, current_date))

        # Apply a 1% chance to null out either the pickup or dropoff datetime.
        sample_df = sample_df.apply(maybe_set_nulls, axis=1)

        daily_data.append(sample_df)
        current_date += timedelta(days=1)

    # Combine all the daily samples into one DataFrame.
    result_df = pd.concat(daily_data, ignore_index=True)

    # Create output directory if it doesn't exist.
    output_dir = os.path.dirname(args.output_file)
    os.makedirs(output_dir, exist_ok=True)

    # Write the final DataFrame to the output CSV file.
    result_df.to_csv(args.output_file, index=False)
    print(f"Output saved to {args.output_file}")

if __name__ == "__main__":
    main()
