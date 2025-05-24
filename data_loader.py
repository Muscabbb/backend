# dataloader.py
import pandas as pd

def load_product_data(csv_file='products.csv'):
    """
    Loads product data and extracts relevant lists.
    The base_colors list is now hardcoded for completeness and consistency.

    Returns:
        dict: A dictionary containing the loaded data.
    """
    df = pd.read_csv(csv_file)

    print(f"\n[DataLoader] Raw unique base colours from CSV (for reference only): {df['baseColour'].unique().tolist()}")

    data = {
        'master_categories': df['masterCategory'].unique().tolist(),
        'sub_categories': df['subCategory'].unique().tolist(),
        'article_types': df['articleType'].unique().tolist(),
        # --- YOUR EXCELLENT SUGGESTION: HARDCODED COMPREHENSIVE COLORS ---
        'base_colors': [
            "black", "white", "grey", "red", "blue", "green", "yellow", "orange", "purple", "pink",
            "brown", "beige", "gold", "silver", "multicolour", "maroon", "navy blue", "olive", "peach",
            "teal", "turquoise", "violet", "cream", "charcoal", "lime green", "mustard", "off white",
            "rust", "tan", "sky blue", "dark blue", "light blue", "dark green", "light green",
            "dark red", "light red", "rose gold", "burgundy", "indigo", "cyan", "magenta"
        ],
        # --- END OF SUGGESTED CHANGE ---
        'seasons': df['season'].unique().tolist(), # Still dynamically loaded
        'brands': extract_brands(df), # Still dynamically loaded
        'df': df
    }
    return data

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

# Example usage (if you want to test it):
if __name__ == '__main__':
    product_data = load_product_data('products.csv')
    print(f"\n[DataLoader] 'base_colors' sent to parser: {product_data['base_colors']}")
    print(product_data['master_categories'])
    print(product_data['brands'][:10])