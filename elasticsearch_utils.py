from typing import Dict, List
from elasticsearch import Elasticsearch

ELASTICSEARCH_URL = "https://my-elasticsearch-project-b5135b.es.us-east-1.aws.elastic.cloud:443"
ELASTICSEARCH_API_KEY = "ZEZDeTlwWUJUY296R0V3cmdPT1k6N2RucGJ6dkExaTFpclJUWW5jUElxdw=="
INDEX_NAME = "hekto"  # Assuming you've created an index named "products" with relevant mappings

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
    filter_clauses = []

    # 1. Brand
    if parsed_query.get("brand"):
        must_clauses.append({"match": {"productDisplayName": parsed_query["brand"]}})  # Search within productDisplayName

    # 2. Article Type
    if parsed_query.get("articleType"):
        must_clauses.append({"match": {"productDisplayName": parsed_query["articleType"]}})

    # 3. Colors
    if parsed_query.get("colors"):
        filter_clauses.append({"terms": {"baseColour": parsed_query["colors"]}})  # Assuming "baseColour" is the field

    # 4. Price Range
    if parsed_query.get("price_range"):
        range_query = {}
        if "min" in parsed_query["price_range"]:
            range_query["gte"] = parsed_query["price_range"]["min"]
        if "max" in parsed_query["price_range"]:
            range_query["lte"] = parsed_query["price_range"]["max"]
        if range_query:
            filter_clauses.append({"range": {"price": range_query}})  # Assuming "price" is the field

    # Combine clauses
    if must_clauses:
        query["bool"]["must"] = must_clauses
    if filter_clauses:
        query["bool"]["filter"] = filter_clauses

    final_query = {
        "query": query,
        "size": 10  # You can adjust the number of results
    }

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
