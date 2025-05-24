import pandas as pd
import json

# Load CSV
df = pd.read_csv("./products.csv")

# Optional: Rename columns or clean data if needed
df = df.dropna(subset=["image", "id", "price", "timestamp"])  # Drop rows missing critical info
df = df.reset_index(drop=True)

# Convert DataFrame to list of dictionaries
products = df.to_dict(orient="records")

# Save as JSON Lines (one JSON object per line, ideal for bulk import)
with open("C:/Users/hp/Downloads/Products/test.json", "w", encoding="utf-8") as f:
    for product in products:
        json.dump(product, f)
        f.write("\n")
