# This is a separate Python script you would run independently
# to set up your Elasticsearch index and load data.

from elasticsearch import Elasticsearch
import pandas as pd # Assuming you use pandas to load your CSV data


ELASTICSEARCH_URL = "https://390d86116dde42f0ae542ebb8f132b4c.us-central1.gcp.cloud.es.io:443"
ELASTICSEARCH_API_KEY = "ejV3S2ZwY0JpQ1JXSDdDUzZ4aWo6QWJUQk1pVVZTWUJmM2ppcVZUZ1Fadw=="
INDEX_NAME = "hekto"



PRODUCTS_CSV_PATH = "products.csv" # <-- Path to your product data CSV

def get_elasticsearch_client():
    try:
        client = Elasticsearch(
            ELASTICSEARCH_URL,
            api_key=ELASTICSEARCH_API_KEY
        )
        if not client.ping():
            raise ConnectionError("Failed to connect to Elasticsearch")
        return client
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return None

def create_index_with_mapping_and_index_data():
    client = get_elasticsearch_client()
    if not client:
        print("Failed to get Elasticsearch client. Exiting.")
        return

    # --- 1. DANGER: Delete the existing index if it exists ---
    # This is necessary to apply a new mapping if the field type was wrong.
    # ONLY RUN THIS IF YOU ARE PREPARED TO LOSE ALL CURRENT DATA IN 'hekto'
    if client.indices.exists(index=INDEX_NAME):
        client.indices.delete(index=INDEX_NAME)
        print(f"Index '{INDEX_NAME}' deleted successfully.")

    # --- 2. Define the Mapping for your Index ---
    # This is where you put {"timestamp": {"type": "date", ...}}
    index_mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "gender": {"type": "keyword"}, # For exact matching (e.g., "Men", "Women")
                "masterCat": {"type": "keyword"}, # For exact matching/faceting
                "subCat": {"type": "keyword"}, # For exact matching/faceting
                "articleType": {"type": "keyword"}, # For exact matching/faceting
                "baseColour": {"type": "keyword"}, # For exact color filtering
                "season": {"type": "keyword"},
                "year": {"type": "keyword"}, # If 'year' is like "2023", "2024" and not a numeric calculation
                "usage": {"type": "keyword"},
                "productDisplayName": {"type": "text"}, # For full-text search on product names
                "image": {"type": "keyword"}, # Storing image URLs/paths as exact strings
                "price": {"type": "float"}, # For range queries
                "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"} # For sorting by date
            }
        }
    }

    # --- 3. Create the Index with the Defined Mapping ---
    try:
        client.indices.create(index=INDEX_NAME, body=index_mapping)
        print(f"Index '{INDEX_NAME}' created with correct mapping for all fields.")
    except Exception as e:
        print(f"Error creating index '{INDEX_NAME}': {e}")
        return

    # --- 4. Load and Index Your Data ---
    try:
        df = pd.read_csv(PRODUCTS_CSV_PATH)

        # IMPORTANT: Ensure the 'timestamp' column is correctly handled for Elasticsearch
        if 'timestamp' not in df.columns:
            print(f"Warning: 'timestamp' column not found in {PRODUCTS_CSV_PATH}. Adding current time as a fallback.")
            # Assign current time if timestamp is missing. Use current time in Mogadishu, Somalia.
            now = pd.Timestamp.now(tz='Africa/Mogadishu') # Explicitly set timezone if applicable
            df['timestamp'] = now.isoformat() # This will include timezone offset
        else:
            # Ensure the timestamp column is parsed as datetime and then formatted for ES
            # Use 'coerce' to turn unparseable dates into NaT, then handle them
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
            # Fill NaT values with current time if any failed parsing
            if df['timestamp'].isnull().any():
                print("Warning: Some 'timestamp' values could not be parsed. Filling with current time.")
                now = pd.Timestamp.now(tz='Africa/Mogadishu')
                df['timestamp'] = df['timestamp'].fillna(now)
            df['timestamp'] = df['timestamp'].dt.isoformat() + 'Z' # Ensure ISO format with 'Z' for UTC

        records_to_index = df.to_dict(orient='records')

        print(f"Starting to index {len(records_to_index)} documents...")
        for i, record in enumerate(records_to_index):
            # Elasticsearch document IDs are typically strings, even if original 'id' is int
            es_id = str(record['id'])
            client.index(index=INDEX_NAME, id=es_id, document=record)
            if (i + 1) % 100 == 0: # Print progress
                print(f"Indexed {i + 1} documents.")
        print(f"Successfully indexed all {len(records_to_index)} documents into '{INDEX_NAME}'.")

    except FileNotFoundError:
        print(f"Error: {PRODUCTS_CSV_PATH} not found. Cannot index data.")
    except Exception as e:
        print(f"Error during data loading or indexing: {e}")

if __name__ == "__main__":
    create_index_with_mapping_and_index_data()