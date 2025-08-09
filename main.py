# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Union
import pickle
import os
from mangum import Mangum
try:
    from langdetect import detect, detect_langs
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    print("Warning: langdetect not installed. Falling back to simple validation.")

import re

def is_english_text(text):
    """Detect if text is in English using multiple strategies"""
    if not text or len(text.strip()) < 2:
        return True  # Default to English for very short text
    
    text_clean = text.strip().lower()
    
    # Strategy 1: Use langdetect with multiple attempts
    if LANGDETECT_AVAILABLE:
        try:
            # Try with original text
            detected_lang = detect(text_clean)
            if detected_lang != 'en':
                return False
                
            # Double-check with detect_langs for confidence
            lang_probs = detect_langs(text_clean)
            for lang_prob in lang_probs:
                if lang_prob.lang != 'en' and lang_prob.prob > 0.7:
                    return False
                    
        except Exception as e:
            print(f"Language detection failed: {e}. Using fallback methods.")
            # Continue to fallback strategies
    
    # Strategy 2: Check for non-English character patterns
    # Arabic script detection
    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]')
    if arabic_pattern.search(text_clean):
        return False
    
    # Somali and other African languages often use specific letter combinations
    # Common Somali patterns: 'dh', 'kh', 'sh', 'ay', 'oo', 'ii'
    somali_patterns = [
        r'\b(kalay|adi|waa|oo|iyo|ah|ku|ka|la)\b',  # Common Somali words
        r'[bcdfghjklmnpqrstvwxz]{3,}[aeiou]{2,}',   # Consonant clusters + vowel clusters
        r'\b\w*dh\w*\b',  # 'dh' sound common in Somali
        r'\b\w*kh\w*\b',  # 'kh' sound
        r'\b\w*[aeiou]{2,}\w*\b'  # Double vowels common in Somali
    ]
    
    somali_matches = 0
    for pattern in somali_patterns:
        if re.search(pattern, text_clean):
            somali_matches += 1
    
    if somali_matches >= 2:  # If multiple Somali patterns match
        return False
    
    # Strategy 3: Check for common non-English words from various languages
    non_english_words = [
        # Spanish
        'camisa', 'roja', 'azul', 'negro', 'blanco', 'verde', 'amarillo',
        'pantalones', 'vestido', 'zapatos', 'chaqueta',
        # French  
        'rouge', 'bleu', 'noir', 'blanc', 'vert', 'jaune', 'chemise',
        'pantalon', 'robe', 'chaussures', 'veste', 'bonjour', 'monde',
        # German
        'rot', 'blau', 'schwarz', 'weiß', 'grün', 'gelb', 'hemd',
        'hose', 'kleid', 'schuhe', 'jacke',
        # Somali
        'kalay', 'adi', 'waa', 'iyo', 'guduud', 'buluug', 'madow',
        'caddaan', 'jaalle', 'cashar', 'khamiis', 'surwaal',
        # Arabic (transliterated)
        'ahmar', 'azraq', 'aswad', 'abyad', 'akhdar', 'asfar',
        'qamis', 'bantalon', 'hiza'
    ]
    
    words = text_clean.split()
    for word in words:
        if word in non_english_words:
            return False
    
    # Strategy 4: Check character composition
    # If more than 15% non-ASCII characters, likely not English
    non_ascii_chars = sum(1 for char in text if ord(char) > 127)
    if len(text) > 0 and (non_ascii_chars / len(text)) > 0.15:
        return False
    
    return True




# Import modules
from elasticsearch_utils import get_elasticsearch_client, build_elasticsearch_query, execute_elasticsearch_query, INDEX_NAME
from queryParser import FastQueryParser # <--- KEEP THIS LINE! This is essential for unpickling.
from data_loader import load_product_data, check_exact_product_match
from recommender_model import load_data_from_elasticsearch, build_model, recommend_products_for_user # New imports

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace "*" with your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GLOBAL MODEL INITIALIZATION ---
# Create a directory for models if it doesn't exist
MODELS_DIR = "models"
os.makedirs(MODELS_DIR, exist_ok=True)

# 1. Initialize and Load Query Parser
# The type hint refers to the class you're importing, which is now correct.
parser: Union[FastQueryParser, None] = None
query_parser_pickle_path = os.path.join(MODELS_DIR, "query_parser.pkl")

try:
    # Always create a new parser instead of trying to load from pickle
    # This avoids pickle deserialization issues
    print("Initializing new Query Parser...")
    product_data = load_product_data() # Now loads from Elasticsearch with CSV fallback
    parser = FastQueryParser(product_data)
    print("Query Parser initialized successfully.")
except Exception as e:
    print(f"Error initializing Query Parser: {e}. Using a fallback DummyParser.")
    class DummyParser:
        def parse_query(self, query: str) -> Dict:
            print("WARNING: Using DummyParser.")
            return {"original_query": query, "keywords": query.lower().split()}
    parser = DummyParser()


# 2. Initialize and Load Recommendation Model
rec_model = None
rec_user_item_matrix = None

# Recommendation model should also be initialized once at startup
try:
    print("Loading data for Recommendation Model from Elasticsearch...")
    rec_df = load_data_from_elasticsearch()
    if not rec_df.empty:
        print("Building Recommendation Model...")
        rec_model, rec_user_item_matrix = build_model(rec_df)
        if rec_model and rec_user_item_matrix is not None:
            print("Recommendation Model built successfully.")
        else:
            print("Failed to build Recommendation Model due to empty data or other issues.")
    else:
        print("No interaction data found in Elasticsearch. Recommendation model will not be active.")
except Exception as e:
    print(f"Error initializing Recommendation Model from Elasticsearch: {e}. Recommendations will not be available.")


# 3. Get Elasticsearch Client
client = get_elasticsearch_client()
if not client:
    print("Elasticsearch client could not be initialized. Search API may not function correctly.")

# --- DATA MODELS ---
class QueryRequest(BaseModel):
    query: str

class RecommendRequest(BaseModel):
    user_id: str
    num_recommendations: int = 20

# --- API ENDPOINTS ---

@app.post("/parse") # Renamed for clarity
async def parse_and_search_endpoint(request: QueryRequest):
    try:
        if parser is None:
            raise HTTPException(status_code=500, detail="Query Parser is not initialized.")

        # Pre-validation: Enforce English-only queries
        query = request.query.strip()
        
        # Check if query is in English (strict validation)
        is_english = is_english_text(query)
        if is_english:
            validation_reason = "Query is in English"
            print(f"Query validation: {validation_reason}")
        else:
            validation_reason = "this language is not allowed pls use english"
            print(f"Query rejected: {validation_reason}")
            return {
                "status": "rejected", 
                "products": [], 
                "parsed_query": {},
                "reason": validation_reason
            }

        parsed_queries = parser.parse_query(request.query)
        print("PARSED QUERIES:", parsed_queries)

        products = []
        if parsed_queries:
            # Execute separate queries for each parsed statement
            products = execute_elasticsearch_query(parsed_queries)
            print(f"FOUND {len(products)} PRODUCTS")
        else:
            print("No parsed queries. Skipping search.")

        return {"status": "success", "products": products, "parsed_query": parsed_queries}

    except Exception as e:
        print(f"Error during search: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing search request: {e}")


@app.get("/getbyId/{product_id}") # Renamed path parameter for clarity
async def get_product_by_id(product_id: str):
    if not client:
        raise HTTPException(status_code=500, detail="Elasticsearch client not initialized.")

    try:
        response = client.search(index=INDEX_NAME, body={
            "query": {
                "term": {
                    "id": product_id # Use .keyword for exact string match on ProductID
                }
            }
        })

        hits = response["hits"]["hits"]
        if not hits:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found.")

        return {"status": "success", "product": hits[0]["_source"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving product: {e}")


@app.get("/getAll")
async def get_all_products():
    if not client:
        raise HTTPException(status_code=500, detail="Elasticsearch client not initialized.")

    try:
        response = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size":100})
        products = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"status": "success", "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching products: {e}")


@app.get("/latest-products")
async def get_latest_products(limit: int = 10):
    if not client:
        raise HTTPException(status_code=500, detail="Elasticsearch client not initialized.")

    try:
        es_query = {
            "query": {
                "match_all": {}
            },
            "sort": [
                {"year": {"order": "desc"}} # Assuming 'SearchTimestamp' or similar for latest
            ],
            "size": limit
        }

        response = client.search(index=INDEX_NAME, body=es_query)
        products = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"status": "success", "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest products: {e}")

# --- NEW RECOMMENDATION ENDPOINT ---
@app.post("/rec")
async def get_user_recommendations(request: RecommendRequest):
    if rec_model is None or rec_user_item_matrix is None:
        raise HTTPException(status_code=503, detail="Recommendation model not loaded or built. Check server logs.")

    try:
        recommendations = recommend_products_for_user(
            user_id=request.user_id,
            N=request.num_recommendations,
            model=rec_model,
            user_item_matrix=rec_user_item_matrix
        )
        
        if isinstance(recommendations, str): # Error message from recommender_model
            raise HTTPException(status_code=404, detail=recommendations)

        # Optionally, fetch full product details for recommended ProductIDs from Elasticsearch
        recommended_products_details = []
        if client and recommendations:
            # --- START OF FIX ---
            # 1. Convert recommended_product_ids from strings to integers
            # 2. Change the Elasticsearch field name to "id"
            recommended_int_ids = [int(p_id) for p_id in recommendations if p_id.isdigit()] # Convert to int, add safety check
            print(recommendations) # Debugging line to check IDs
            print(recommended_int_ids) # Debugging line to check IDs
            if recommended_int_ids: # Only query if there are valid int IDs
                es_reco_query = {
                    "query": {
                        "terms": {
                            "id": recommended_int_ids # Changed field name to "id", using int values
                        }
                    },
                    "size": len(recommended_int_ids) # Fetch all recommended products
                }
                # print(f"DEBUG: ES Recommendation Query: {es_reco_query}") # Good for debugging

                reco_response = client.search(index=INDEX_NAME, body=es_reco_query)
                recommended_products_details = [hit["_source"] for hit in reco_response["hits"]["hits"]]

                # You might want to sort these to match the order of recommendations list
                # Create a dictionary for quick lookup and then sort by original order
                # Use the original string IDs for mapping if your model uses strings for sorting,
                # but ensure the lookup key is consistent with the ES data (int 'id')
                details_map = {p['id']: p for p in recommended_products_details if 'id' in p}
                sorted_details = [details_map[int(prod_id)] for prod_id in recommendations if int(prod_id) in details_map]
                recommended_products_details = sorted_details
            # --- END OF FIX ---


        return {"status": "success", "recommended_product_ids": recommendations, "products": recommended_products_details}

    except HTTPException as he:
        raise he # Re-raise HTTP exceptions directly
    except Exception as e:
        print(f"Error generating recommendations: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating recommendations: {e}")








handler = Mangum(app)

#uvicorn main:app --reload
#uvicorn main:app --reload --host 0.0.0.0 --port 8000

