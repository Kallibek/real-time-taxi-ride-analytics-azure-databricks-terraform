import pandas as pd

def convert_parquet_to_csv(parquet_file, csv_file):
    """
    Converts a Parquet file to a CSV file.
    
    :param parquet_file: Path to the input Parquet file
    :param csv_file: Path to the output CSV file
    """
    try:
        # Read the Parquet file
        df = pd.read_parquet(parquet_file, engine='pyarrow')

        # Save as CSV
        df.to_csv(csv_file, index=False)
        
        print(f"Conversion successful! CSV file saved at: {csv_file}")
    except Exception as e:
        print(f"Error: {e}")

# Example usage
convert_parquet_to_csv("data\yellow_tripdata_2024-11.parquet", "data\yellow_tripdata_2024-11.csv")
