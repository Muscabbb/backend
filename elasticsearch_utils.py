# elasticsearch_utils.py
import os
from typing import Dict, List, Union
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY")
INDEX_NAME = os.getenv("INDEX_NAME")

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

def build_elasticsearch_query(parsed_query: Dict) -> Dict:
    # Color mapping
    color_mapping = {
        "navy": "Navy Blue",
        "blue": "Blue",
        "black": "Black",
        "white": "White",
        "grey": "Grey",
        "gray": "Grey",
        "green": "Green",
        "red": "Red",
        "purple": "Purple",
        "silver": "Silver",
        "beige": "Beige",
        "brown": "Brown"
    }
    
    # Season mapping
    season_mapping = {
        "fall": "Fall",
        "autumn": "Fall",
        "winter": "Winter",
        "spring": "Spring",
        "summer": "Summer"
    }
    
    colors = parsed_query.get("colors", [])
    article_types = parsed_query.get("articleTypes", [])
    seasons = parsed_query.get("seasons", [])
    brands = parsed_query.get("brands", [])
    
    # Map colors and seasons
    mapped_colors = [color_mapping.get(color.lower(), color.title()) for color in colors]
    mapped_seasons = [season_mapping.get(season.lower(), season.title()) for season in seasons]
    
    query = {"bool": {}}
    must_clauses = []
    filter_clauses = []
    
    # Brands
    if brands:
        if len(brands) == 1:
            must_clauses.append({"match": {"productDisplayName": brands[0]}})
        else:
            brand_should_clauses = [{"match": {"productDisplayName": brand}} for brand in brands]
            must_clauses.append({"bool": {"should": brand_should_clauses, "minimum_should_match": 1}})
    
    # Article Types
    if article_types:
        if len(article_types) == 1:
            filter_clauses.append({"term": {"articleType": article_types[0]}})
        else:
            filter_clauses.append({"terms": {"articleType": article_types}})
    
    # Colors
    if mapped_colors:
        if len(mapped_colors) == 1:
            filter_clauses.append({"term": {"baseColour": mapped_colors[0]}})
        else:
            filter_clauses.append({"terms": {"baseColour": mapped_colors}})
    
    # Seasons
    if mapped_seasons:
        if len(mapped_seasons) == 1:
            filter_clauses.append({"term": {"season": mapped_seasons[0]}})
        else:
            filter_clauses.append({"terms": {"season": mapped_seasons}})
    
    # Price Range
    price_range_data = parsed_query.get("price_range")
    if isinstance(price_range_data, dict):
        range_query = {}
        if "min" in price_range_data:
            range_query["gte"] = price_range_data["min"]
        if "max" in price_range_data:
            range_query["lte"] = price_range_data["max"]
        if range_query:
            filter_clauses.append({"range": {"price": range_query}})
    
    # Combine clauses
    if must_clauses:
        query["bool"]["must"] = must_clauses
    if filter_clauses:
        query["bool"]["filter"] = filter_clauses
    
    # Fallback
    if not must_clauses and not filter_clauses:
        query["bool"]["must"] = {"match": {"productDisplayName": parsed_query.get("query", "")}}

    final_query = {
        "query": query,
        "size": 50,
        "sort": [
            {"year": {"order": "desc"}},
            {"_score": {"order": "desc"}}
        ]
    }

    # You can add a print statement here to debug the final query being returned
    # print(f"DEBUG: Final ES Query built: {final_query}")

    return final_query

def create_individual_queries(parsed_query: Dict) -> List[Dict]:
    """
    Create individual queries for each complete statement/sentence
    Each query represents one complete product description
    """
    colors = parsed_query.get("colors", [])
    article_types = parsed_query.get("articleTypes", [])
    seasons = parsed_query.get("seasons", [])
    brands = parsed_query.get("brands", [])
    price_range = parsed_query.get("price_range")
    
    individual_queries = []
    
    # Create queries for each complete statement
    # Pair colors with article types in order (first color with first article type, etc.)
    if colors and article_types:
        max_items = max(len(colors), len(article_types))
        
        for i in range(max_items):
            # Use modulo to cycle through shorter lists
            color = colors[i % len(colors)] if colors else None
            article_type = article_types[i % len(article_types)] if article_types else None
            season = seasons[i % len(seasons)] if seasons else None
            brand = brands[i % len(brands)] if brands else None
            
            query = {
                "colors": [color] if color else [],
                "articleTypes": [article_type] if article_type else [],
                "seasons": [season] if season else seasons,
                "brands": [brand] if brand else brands,
                "price_range": price_range
            }
            individual_queries.append(query)
    else:
        # If we don't have both colors and article types, return the original query
        individual_queries.append(parsed_query)
    
    return individual_queries

def execute_elasticsearch_query(parsed_queries: List[Dict]) -> List[Dict]:
    """
    Execute separate Elasticsearch queries for each classified query and return 10 products per query
    """
    client = get_elasticsearch_client()
    if not client:
        return []

    query_results = []
    
    for index, parsed_query in enumerate(parsed_queries):
        query_products = []
        
        # Build the elasticsearch query for this classified query
        query = build_elasticsearch_query(parsed_query)
        # Limit to 10 products per query
        query["size"] = 10
        
        try:
            # Execute the search
            response = client.search(index=INDEX_NAME, body=query)
            
            # Extract products from response
            hits = response.get('hits', {}).get('hits', [])
            
            for hit in hits:
                source = hit['_source']
                product = {
                    "id": source.get('id'),
                    "gender": source.get('gender'),
                    "masterCategory": source.get('masterCategory'),
                    "subCategory": source.get('subCategory'),
                    "articleType": source.get('articleType'),
                    "baseColour": source.get('baseColour'),
                    "season": source.get('season'),
                    "year": source.get('year'),
                    "usage": source.get('usage'),
                    "productDisplayName": source.get('productDisplayName'),
                    "image": source.get('image'),
                    "price": source.get('price'),
                    "timestamp": source.get('timestamp')
                }
                query_products.append(product)
                
        except Exception as e:
            print(f"Error executing Elasticsearch query for index {index}: {e}")
            
        # Add the results for this query with its index
        query_result = {
            "query_index": index,
            "query_details": parsed_query,
            "products": query_products,
            "product_count": len(query_products)
        }
        query_results.append(query_result)
    
    return query_results