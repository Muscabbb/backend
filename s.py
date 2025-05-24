import pandas as pd
import random
from datetime import datetime, timedelta

def random_timestamp(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds())) # Ensure integer
    return start + timedelta(seconds=random_seconds)

# Define date range
start_date = datetime(2018, 1, 1)
end_date = datetime(2021, 12, 31, 23, 59, 59)

# Read the CSV into a Pandas DataFrame
try:
    df = pd.read_csv('products.csv')

    # Generate timestamps and apply them to a new 'timestamp' column
    # Using .apply() is efficient for row-wise operations in Pandas
    df['timestamp'] = df.apply(lambda row: random_timestamp(start_date, end_date).strftime('%Y-%m-%d %H:%M:%S'), axis=1)

    # Write the modified DataFrame to a new CSV file
    df.to_csv('products_with_timestamps.csv', index=False) # index=False prevents writing the DataFrame index as a column

    print("Successfully added timestamps and saved to 'products_with_timestamps.csv'")

except FileNotFoundError:
    print("Error: 'products.csv' not found. Please make sure the file exists.")
except Exception as e:
    print(f"An error occurred: {e}")