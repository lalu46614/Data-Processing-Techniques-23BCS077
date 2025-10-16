import pandas as pd
import os

# File paths
SOURCE_CDC_FILE = "sensor_data_cdc.csv"
PROCESSED_FILE = "processed_sensor_data.csv"

# Read CDC source
cdc_df = pd.read_csv(SOURCE_CDC_FILE)

# Check if processed file exists
if os.path.exists(PROCESSED_FILE):
    processed_df = pd.read_csv(PROCESSED_FILE)
    # Get the last processed timestamp
    last_timestamp = processed_df['timestamp'].max()
    # Filter only new rows
    incremental_df = cdc_df[cdc_df['timestamp'] > last_timestamp]
    if incremental_df.empty:
        print("No new data found. Exiting script.")
        exit(0)
    else:
        print(f"New incremental data found: {len(incremental_df)} rows")
else:
    print("No existing dataset found. Creating a new one.")
    incremental_df = cdc_df
    processed_df = pd.DataFrame()  # empty placeholder

# Feature engineering
incremental_df['temp_hum_ratio'] = incremental_df['temperature'] / incremental_df['humidity']

# Encode status column
status_mapping = {"OK": 0, "WARN": 1, "FAIL": 2}
incremental_df['status_code'] = incremental_df['status'].map(status_mapping)

# Combine with processed data
updated_df = pd.concat([processed_df, incremental_df], ignore_index=True)

# Save updated dataset
updated_df.to_csv(PROCESSED_FILE, index=False)
print(f"Updated dataset saved to '{PROCESSED_FILE}'.")

# Display processed data
print("Processed & Updated Data:")
print(updated_df)
