# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Union
import pickle
import os

# Import modules
from elasticsearch_utils import get_elasticsearch_client, build_elasticsearch_query, execute_elasticsearch_query
from queryParser import FastQueryParser # <--- KEEP THIS LINE! This is essential for unpickling.
from data_loader import load_product_data
from recommender_model import load_data_from_mongodb, build_model, recommend_products_for_user # New imports

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
    if os.path.exists(query_parser_pickle_path):
        print("Attempting to load pickled Query Parser...")
        with open(query_parser_pickle_path, "rb") as f:
            # This line will now correctly find queryParser.FastQueryParser
            # because you've imported FastQueryParser from queryParser.
            parser = pickle.load(f)
        print("Query Parser loaded from pickle.")
    else:
        # This 'else' block is for creating the parser if the pickle doesn't exist.
        # It relies on FastQueryParser being imported.
        print("Pickled Query Parser not found. Initializing new parser and pickling it...")
        product_data = load_product_data('products.csv') # Ensure correct path
        parser = FastQueryParser(product_data)
        with open(query_parser_pickle_path, "wb") as f:
            pickle.dump(parser, f)
        print("New Query Parser initialized and pickled.")
except Exception as e:
    print(f"Error initializing or loading Query Parser: {e}. Using a fallback DummyParser.")
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
    print("Loading data for Recommendation Model from MongoDB...")
    rec_df = load_data_from_mongodb()
    if not rec_df.empty:
        print("Building Recommendation Model...")
        rec_model, rec_user_item_matrix = build_model(rec_df)
        if rec_model and rec_user_item_matrix is not None:
            print("Recommendation Model built successfully.")
        else:
            print("Failed to build Recommendation Model due to empty data or other issues.")
    else:
        print("No interaction data found in MongoDB. Recommendation model will not be active.")
except Exception as e:
    print(f"Error initializing Recommendation Model from MongoDB: {e}. Recommendations will not be available.")


# 3. Get Elasticsearch Client
client = get_elasticsearch_client()
if not client:
    print("Elasticsearch client could not be initialized. Search API may not function correctly.")

# --- DATA MODELS ---
class QueryRequest(BaseModel):
    query: str

class RecommendRequest(BaseModel):
    user_id: str
    num_recommendations: int = 5

# --- API ENDPOINTS ---

@app.post("/parse") # Renamed for clarity
async def parse_and_search_endpoint(request: QueryRequest):
    try:
        if parser is None:
            raise HTTPException(status_code=500, detail="Query Parser is not initialized.")

        parsed_query = parser.parse_query(request.query)
        print("PARSED QUERY:", parsed_query)

        products = []
        if client:
            es_query = build_elasticsearch_query(parsed_query)
            print("ELASTICSEARCH QUERY:", es_query)
            products = execute_elasticsearch_query(client, es_query)
        else:
            print("Elasticsearch client not available. Skipping search.")

        return {"status": "success", "products": products, "parsed_query": parsed_query}

    except Exception as e:
        print(f"Error during search: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing search request: {e}")


@app.get("/getbyId/{product_id}") # Renamed path parameter for clarity
async def get_product_by_id(product_id: str):
    if not client:
        raise HTTPException(status_code=500, detail="Elasticsearch client not initialized.")

    try:
        response = client.search(index="hekto", body={
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
        response = client.search(index="hekto", body={"query": {"match_all": {}}, "size":100})
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
                {"timestamp": {"order": "desc"}} # Assuming 'SearchTimestamp' or similar for latest
            ],
            "size": limit
        }

        response = client.search(index="hekto", body=es_query)
        products = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"status": "success", "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest products: {e}")

# --- NEW RECOMMENDATION ENDPOINT ---
@app.post("/recommendations")
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
            # Construct a terms query for ProductIDs
            es_reco_query = {
                "query": {
                    "terms": {
                        "ProductID.keyword": recommendations # Assuming ProductID in ES matches recommender's ProductID
                    }
                },
                "size": len(recommendations) # Fetch all recommended products
            }
            reco_response = client.search(index="hekto", body=es_reco_query)
            recommended_products_details = [hit["_source"] for hit in reco_response["hits"]["hits"]]
            # You might want to sort these to match the order of recommendations list
            # Create a dictionary for quick lookup and then sort
            details_map = {p['ProductID']: p for p in recommended_products_details if 'ProductID' in p}
            sorted_details = [details_map[prod_id] for prod_id in recommendations if prod_id in details_map]
            recommended_products_details = sorted_details


        return {"status": "success", "recommended_product_ids": recommendations, "recommended_products_details": recommended_products_details}

    except HTTPException as he:
        raise he # Re-raise HTTP exceptions directly
    except Exception as e:
        print(f"Error generating recommendations: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating recommendations: {e}")