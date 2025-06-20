# elasticsearch_utils.py
from typing import Dict, List, Union
from elasticsearch import Elasticsearch

ELASTICSEARCH_URL = "https://390d86116dde42f0ae542ebb8f132b4c.us-central1.gcp.cloud.es.io:443"
ELASTICSEARCH_API_KEY = "ejV3S2ZwY0JpQ1JXSDdDUzZ4aWo6QWJUQk1pVVZTWUJmM2ppcVZUZ1Fadw=="
INDEX_NAME = "hekto"

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
    query = {"bool": {}}
    must_clauses = []
    filter_clauses = [] # Use filter for non-scoring exact matches, often for keywords/terms

    # 1. Brand: Match within productDisplayName for brand names
    if parsed_query.get("brand"):
        must_clauses.append({"match": {"productDisplayName": parsed_query["brand"]}})

    # 2. Article Type: Prefer matching on the dedicated 'articleType' field
    # Assuming 'articleType' is a keyword field in ES, use .keyword for exact match
    if parsed_query.get("articleType"):
        filter_clauses.append({"term": {"articleType": parsed_query["articleType"]}})

    # 3. Colors: Take ONLY the first element from the array and use a single 'term' query
    # Match against 'baseColour.keyword' as per your instruction
    if parsed_query.get("colors") and parsed_query["colors"]: # Ensure list exists and is not empty
        first_color = parsed_query["colors"][0]
        filter_clauses.append({"term": {"baseColour": first_color.title()}}) # Assuming colors are case-insensitive, use title case for consistency


    # 4. Seasons: Take ONLY the first element from the array and use a single 'term' query
    # Match against 'season.keyword' as per your instruction
    if parsed_query.get("seasons") and parsed_query["seasons"]: # Ensure list exists and is not empty
        first_season = parsed_query["seasons"][0]
        filter_clauses.append({"term": {"season": first_season.title()}}) # Assuming seasons are case-insensitive, use title case for consistency


    # 5. Price Range
    # Assuming price_range from parsed_query could be a dict like {"min": X, "max": Y}
    # Or None if not parsed
    price_range_data: Union[Dict, None] = parsed_query.get("price_range")
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

    # If no specific clauses are generated, use a broader match on the original query
    if not must_clauses and not filter_clauses:
        query["bool"]["must"] = {"match": {"productDisplayName": parsed_query["original_query"]}}

    final_query = {
        "query": query,
        "size": 10
    }

    # You can add a print statement here to debug the final query being returned
    # print(f"DEBUG: Final ES Query built: {final_query}")

    return final_query

def execute_elasticsearch_query(client: Elasticsearch, query: Dict) -> List[Dict]:
    try:
        response = client.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        products = [hit["_source"] for hit in hits]
        return products
    except Exception as e:
        print(f"Error executing Elasticsearch query: {e}")
        return []