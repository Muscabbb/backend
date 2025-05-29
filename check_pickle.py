# check_pickle.py
import pickle
import os

MODELS_DIR = "models"
query_parser_pickle_path = os.path.join(MODELS_DIR, "query_parser.pkl")

try:
    if not os.path.exists(query_parser_pickle_path):
        print(f"Error: Pickle file not found at {query_parser_pickle_path}")
    else:
        with open(query_parser_pickle_path, "rb") as f:
            # Try to load it, but wrap in try-except to catch the error
            try:
                obj = pickle.load(f)
                print(f"Successfully loaded object. Type: {type(obj)}")
                print(f"Module where class was defined: {obj.__class__.__module__}")
                print(f"Name of class: {obj.__class__.__name__}")
            except Exception as e:
                print(f"Failed to load pickle: {e}")
                print("\nThis usually means the class definition was not available or the module path in the pickle is wrong.")
                print("If the error mentions '__main__', the object was pickled from a top-level script.")

except Exception as e:
    print(f"An unexpected error occurred: {e}")