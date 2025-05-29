# create_parser_pickle.py
import pickle
import os
import sys

# IMPORTANT: This import is crucial. It tells Python that FastQueryParser
# comes from the 'queryParser' module.
from queryParser import FastQueryParser
from data_loader import load_product_data # Assuming this is correct

# Define the models directory
MODELS_DIR = "models"
os.makedirs(MODELS_DIR, exist_ok=True)
query_parser_pickle_path = os.path.join(MODELS_DIR, "query_parser.pkl")

print(f"--- Running {__file__} to create query_parser.pkl ---")

try:
    # 1. DELETE any existing problematic pickle
    if os.path.exists(query_parser_pickle_path):
        os.remove(query_parser_pickle_path)
        print(f"Removed existing pickle file: {query_parser_pickle_path}")

    print("Loading product data from 'products.csv'...")
    # Ensure 'products.csv' path is correct relative to where you run this script
    product_data = load_product_data('products.csv')
    print("Product data loaded.")

    print("Initializing FastQueryParser instance...")
    parser_instance = FastQueryParser(product_data)

    # THIS IS THE CRITICAL CHECK:
    # Verify that the class's module is correctly identified *before* pickling.
    # If this prints '__main__', there's a deeper issue with your module setup.
    print(f"DEBUG: Type of parser_instance: {type(parser_instance)}")
    print(f"DEBUG: Module of FastQueryParser: {parser_instance.__class__.__module__}")

    if parser_instance.__class__.__module__ != 'queryParser':
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!! WARNING: FastQueryParser is not correctly identified as !!")
        print(f"!!          coming from 'queryParser' module. It's: {parser_instance.__class__.__module__} !!")
        print("!!          Pickling it this way will likely cause the !!")
        print("!!          '__main__' unpickling error again. !!")
        print("!!          Ensure queryParser.py is a standard module. !!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        sys.exit(1) # Exit if the module path is wrong

    print(f"Pickling FastQueryParser to {query_parser_pickle_path}...")
    with open(query_parser_pickle_path, "wb") as f:
        pickle.dump(parser_instance, f)
    print("FastQueryParser pickled successfully!")

except FileNotFoundError:
    print(f"Error: products.csv not found. Please ensure '{os.path.abspath('products.csv')}' exists.")
except Exception as e:
    print(f"An unexpected error occurred during pickling: {e}")
    import traceback
    traceback.print_exc()

print("--- Finished pickling process ---")