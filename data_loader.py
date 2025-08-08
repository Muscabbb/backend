# dataloader.py
import pandas as pd
from elasticsearch_utils import get_elasticsearch_client, INDEX_NAME

def load_product_data_from_elasticsearch():
    """
    Loads product data from Elasticsearch index and extracts relevant lists.
    
    Returns:
        dict: A dictionary containing the loaded data from Elasticsearch.
    """
    client = get_elasticsearch_client()
    if not client:
        print("[DataLoader] Elasticsearch client not available, falling back to CSV")
        return load_product_data_from_csv()
    
    try:
        # Get all products from Elasticsearch
        query = {
            "query": {"match_all": {}},
            "size": 10000  # Adjust based on your data size
        }
        
        response = client.search(index=INDEX_NAME, body=query)
        hits = response.get('hits', {}).get('hits', [])
        
        if not hits:
            print("[DataLoader] No data found in Elasticsearch, falling back to CSV")
            return load_product_data_from_csv()
        
        # Convert Elasticsearch results to DataFrame
        products = [hit['_source'] for hit in hits]
        df = pd.DataFrame(products)
        
        print(f"\n[DataLoader] Loaded {len(df)} products from Elasticsearch")
        print(f"[DataLoader] Raw unique base colours from Elasticsearch: {df['baseColour'].unique().tolist()}")
        
        data = {
            'master_categories': df['masterCategory'].unique().tolist(),
            'sub_categories': df['subCategory'].unique().tolist(),
            'article_types': df['articleType'].unique().tolist(),
            'base_colors': df['baseColour'].unique().tolist(),
            'seasons': df['season'].unique().tolist(),
            'brands': extract_brands(df),
            'df': df
        }
        return data
        
    except Exception as e:
        print(f"[DataLoader] Error loading from Elasticsearch: {e}")
        print("[DataLoader] Falling back to CSV")
        return load_product_data_from_csv()

def load_product_data_from_csv(csv_file='products.csv'):
    """
    Loads product data from CSV file and extracts relevant lists.
    This is now a fallback method when Elasticsearch is not available.

    Returns:
        dict: A dictionary containing the loaded data.
    """
    df = pd.read_csv(csv_file)

    print(f"\n[DataLoader] Raw unique base colours from CSV (fallback): {df['baseColour'].unique().tolist()}")

    data = {
        'master_categories': df['masterCategory'].unique().tolist(),
        'sub_categories': df['subCategory'].unique().tolist(),
        'article_types': df['articleType'].unique().tolist(),
        'base_colors': df['baseColour'].unique().tolist(),
        'seasons': df['season'].unique().tolist(),
        'brands': extract_brands(df),
        'df': df
    }
    return data

def load_product_data(csv_file='products.csv'):
    """
    Main function to load product data. Tries Elasticsearch first, falls back to CSV.
    
    Returns:
        dict: A dictionary containing the loaded data.
    """
    return load_product_data_from_elasticsearch()

def extract_brands(df):
    """
    Extracts unique brand names from product display names.
    (Same implementation as before)
    """
    all_names = df['productDisplayName'].dropna().unique()
    brands = set()
    for name in all_names:
        words = name.split()
        for word in words:
            cleaned_word = word.strip().lower().title()
            if cleaned_word and not any(char.isdigit() for char in cleaned_word):
                brands.add(cleaned_word)
    return list(brands)

def check_exact_product_match(query, product_data=None):
    """
    Checks if the query matches any product exactly in the productDisplayName column.
    
    Args:
        query (str): The search query
        product_data (dict, optional): Product data dictionary. If None, loads fresh data.
    
    Returns:
        bool: True if exact match found, False otherwise
    """
    if product_data is None:
        product_data = load_product_data()
    
    df = product_data.get('df')
    if df is None or df.empty:
        return False
    
    # Check for exact match (case-insensitive)
    exact_matches = df[df['productDisplayName'].str.lower() == query.lower()]
    return len(exact_matches) > 0

# Language detection and validation functions removed - now handled directly in main.py endpoint

# Example usage (if you want to test it):
if __name__ == '__main__':
    product_data = load_product_data('products.csv')
    print(f"\n[DataLoader] 'base_colors' sent to parser: {product_data['base_colors']}")
    print(product_data['master_categories'])
    print(product_data['brands'][:10])