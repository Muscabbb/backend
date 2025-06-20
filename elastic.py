from elasticsearch import Elasticsearch, helpers, exceptions
import json
from datetime import datetime
import pandas as pd # Import pandas for robust date parsing, if you used it before

# Connect to Elasticsearch
client = Elasticsearch(
    "https://390d86116dde42f0ae542ebb8f132b4c.us-central1.gcp.cloud.es.io:443",
    api_key="ejV3S2ZwY0JpQ1JXSDdDUzZ4aWo6QWJUQk1pVVZTWUJmM2ppcVZUZ1Fadw=="
)


index_name = "hekto"
json_file_path = "C:/Users/hp/Downloads/Products/test.json"

# --- IMPORTANT: Ensure your 'hekto' index has the correct date mapping before running this ---
# If you haven't run the `create_index_with_mapping_and_index_data.py` script
# from the previous solution, you MUST run it first to set up the mapping for 'timestamp'
# as {"type": "date", "format": "strict_date_optional_time||epoch_millis"}

# Read JSON lines file
with open(json_file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Generator to yield valid documents with processed timestamp
def generate_actions():
    for i, line in enumerate(lines):
        try:
            doc = json.loads(line)

            if not isinstance(doc, dict):
                print(f"Skipping line {i} as it's not a valid dictionary: {line.strip()}")
                continue

            # --- *** CRITICAL CHANGE HERE: Pre-process the 'timestamp' field *** ---
            if 'timestamp' in doc and doc['timestamp']:
                # Try to parse the date string "YYYY-MM-DD HH:MM:SS"
                try:
                    # Parse the string into a datetime object
                    dt_object = datetime.strptime(str(doc['timestamp']), "%Y-%m-%d %H:%M:%S")
                    # Convert it to ISO 8601 format with 'Z' for UTC
                    doc['timestamp'] = dt_object.isoformat() + 'Z'
                except ValueError as ve:
                    print(f"Warning: Failed to parse timestamp '{doc['timestamp']}' on line {i}. Error: {ve}. Skipping document's timestamp.")
                    # Option 1: Remove the timestamp field if it's unparseable (might cause issues with sorting later)
                    # del doc['timestamp']
                    # Option 2: Set a default valid timestamp (e.g., current time or a fixed epoch)
                    doc['timestamp'] = datetime.now().isoformat() + 'Z' # Fallback to current UTC time
            else:
                # If 'timestamp' field is missing or empty, assign a default
                doc['timestamp'] = datetime.now().isoformat() + 'Z' # Assign current UTC time

            # Assign _id if you want to control it (e.g., from 'id' field in your doc)
            _id = None
            if 'id' in doc:
                _id = str(doc['id']) # Elasticsearch _id is a string

            yield {
                "_index": index_name,
                "_id": _id, # Use the 'id' field from your document as Elasticsearch _id
                "_source": doc
            }

        except json.JSONDecodeError as jde:
            print(f"Skipping line {i} due to JSON decoding error: {jde}. Line: {line.strip()}")
        except Exception as e:
            print(f"Skipping line {i} due to unexpected error: {e}. Line: {line.strip()}")

# Perform the bulk upload
try:
    success, errors = helpers.bulk(
        client,
        generate_actions(),
        raise_on_error=False,   # Allow partial success
        stats_only=False        # Show individual errors
    )

    print(f"\n--- Bulk Upload Results ---")
    print(f"{success} documents uploaded successfully.")
    if errors:
        print(f"{len(errors)} documents failed to index.")
        print(f"--- First 5 errors ---")
        for error in errors[:5]:  # Show first 5 errors
            print(json.dumps(error, indent=2))
        print(f"--- End of errors ---")

except exceptions.ElasticsearchException as e:
    print(f"Elasticsearch connection or bulk operation error: {e}")