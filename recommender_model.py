import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from scipy.sparse import csr_matrix
from pymongo import MongoClient
from elasticsearch_utils import get_elasticsearch_client
from typing import Tuple, List, Union

# MongoDB connection details (kept for fallback)
MONGO_URI = "mongodb+srv://musab:lkLcVq0MOOxpLxxo@cluster0.9r682mt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "hekto" # Your database name
MONGO_COLLECTION_NAME = "Interactions" # <<< IMPORTANT: Confirm this is your actual collection name for interactions

# Elasticsearch index for interactions
INTERACTIONS_INDEX = "interactions"  # Elasticsearch index name for user interactions

# Load and preprocess data from Elasticsearch
def load_data_from_elasticsearch() -> pd.DataFrame:
    try:
        client = get_elasticsearch_client()
        if not client:
            print("Elasticsearch client not available, falling back to MongoDB")
            return load_data_from_mongodb()
        
        # Query to get all interactions from Elasticsearch
        query = {
            "query": {"match_all": {}},
            "size": 10000,  # Adjust based on your data size
            "_source": ["userId", "productId", "interactionType"]
        }
        
        response = client.search(index=INTERACTIONS_INDEX, body=query)
        hits = response.get('hits', {}).get('hits', [])
        
        if not hits:
            print(f"No interaction data found in Elasticsearch index: {INTERACTIONS_INDEX}. Falling back to MongoDB.")
            return load_data_from_mongodb()
        
        # Convert Elasticsearch results to DataFrame
        interactions = [hit['_source'] for hit in hits]
        df = pd.DataFrame(interactions)
        
        print(f"[RecommenderModel] Loaded {len(df)} interactions from Elasticsearch")
        
        # Map 'interactionType' to numerical values
        interaction_mapping = {
            'view': 1,
            'add_to_cart': 2,
            'purchase': 3
        }
        df['interactionType'] = df['interactionType'].map(interaction_mapping)
        
        # Drop rows where essential columns are missing after mapping
        df = df.dropna(subset=['userId', 'productId', 'interactionType'])
        
        # Ensure correct data types
        df['productId'] = df['productId'].astype(int).astype(str)
        df['userId'] = df['userId'].astype(str)
        df['interactionType'] = df['interactionType'].astype(int)
        
        # Aggregate duplicate user-product interactions
        df_aggregated = df.groupby(['userId', 'productId'])['interactionType'].max().reset_index()
        
        return df_aggregated
        
    except Exception as e:
        print(f"Error loading data from Elasticsearch: {e}")
        print("Falling back to MongoDB")
        return load_data_from_mongodb()

# Load and preprocess data from MongoDB (fallback)
def load_data_from_mongodb() -> pd.DataFrame:
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        # Fetch data - only userId, productId, and interactionType are needed
        mongo_data = list(collection.find({}, {'_id': 0, 'userId': 1, 'productId': 1, 'interactionType': 1}))

        if not mongo_data:
            print(f"No data found in MongoDB collection: {MONGO_COLLECTION_NAME}. Returning empty DataFrame.")
            return pd.DataFrame(columns=['userId', 'productId', 'interactionType'])

        df = pd.DataFrame(mongo_data)

        # Map 'interactionType' to numerical values
        interaction_mapping = {
            'view': 1,
            'add_to_cart': 2,
            'purchase': 3
        }
        df['interactionType'] = df['interactionType'].map(interaction_mapping)
        
        # Drop rows where essential columns are missing after mapping
        df = df.dropna(subset=['userId', 'productId', 'interactionType'])
        
        # Ensure integer type for interactionType
        df['productId'] = df['productId'].astype(int).astype(str)
        df['userId'] = df['userId'].astype(str)
        df['interactionType'] = df['interactionType'].astype(int)

        # --- IMPORTANT NEW STEP: Aggregate duplicate user-product interactions ---
        # Group by userId and productId, then take the maximum interactionType
        df_aggregated = df.groupby(['userId', 'productId'])['interactionType'].max().reset_index()
        # --- END NEW STEP ---

        client.close()
        return df_aggregated # Return the aggregated DataFrame
    except Exception as e:
        print(f"Error loading data from MongoDB: {e}")
        return pd.DataFrame(columns=['userId', 'productId', 'interactionType'])

# The rest of your recommender_model.py remains the same
# build_model and recommend_products_for_user will now receive the aggregated DataFrame

# Build and train model
def build_model(df: pd.DataFrame) -> Tuple[Union[NearestNeighbors, None], Union[pd.DataFrame, None]]:
    if df.empty:
        print("DataFrame is empty, cannot build recommendation model.")
        return None, None

    # Create user-item matrix
    user_item_matrix = df.pivot(
        index='userId',
        columns='productId',
        values='interactionType'
    ).fillna(0)

    # Convert to sparse matrix
    sparse_matrix = csr_matrix(user_item_matrix.values)

    # Train model with more neighbors for better recommendations
    # Increase n_neighbors to get more diverse similar users
    n_neighbors = min(20, len(user_item_matrix) - 1)  # Ensure we don't exceed available users
    model = NearestNeighbors(
        metric='cosine',
        algorithm='brute',
        n_neighbors=n_neighbors
    )
    model.fit(sparse_matrix)

    return model, user_item_matrix

# Enhanced recommendation function with better personalization
def recommend_products_for_user(user_id: str, N: int = 20,
                                  model: Union[NearestNeighbors, None] = None,
                                  user_item_matrix: Union[pd.DataFrame, None] = None) -> Union[List[str], str]:
    
    # If model or matrix not provided, load and build them from MongoDB
    if model is None or user_item_matrix is None:
        print("Recommendation model or matrix not provided, attempting to load from MongoDB.")
        df = load_data_from_mongodb()
        model, user_item_matrix = build_model(df)
        if model is None or user_item_matrix is None:
            return "Failed to initialize recommendation model. No recommendations can be made."

    # Handle cold start users (users not in the dataset)
    if user_id not in user_item_matrix.index:
        print(f"User '{user_id}' not found. Providing popular item recommendations.")
        return get_popular_items_recommendation(user_item_matrix, N)

    user_idx = user_item_matrix.index.get_loc(user_id)
    user_interactions = user_item_matrix.iloc[user_idx, :]
    
    # Get items the target user hasn't interacted with
    uninteracted_items = user_interactions[user_interactions == 0].index
    
    if len(uninteracted_items) == 0:
        print(f"User '{user_id}' has interacted with all products. Providing diverse popular recommendations.")
        return get_popular_items_recommendation(user_item_matrix, N)

    # Find similar users with dynamic neighbor count
    max_neighbors = min(15, len(user_item_matrix) - 1)
    distances, indices = model.kneighbors(
        user_item_matrix.iloc[user_idx, :].values.reshape(1, -1),
        n_neighbors=max_neighbors
    )

    # Get similar users' indices and distances (excluding the user themselves)
    similar_users_indices = indices.flatten()[1:]
    similarity_scores = 1 - distances.flatten()[1:]  # Convert distance to similarity
    
    if len(similar_users_indices) == 0:
        print("No similar users found. Providing popular item recommendations.")
        return get_popular_items_recommendation(user_item_matrix, N)

    # Calculate weighted predictions based on user similarity
    recommendations_scores = {}
    
    for i, similar_user_idx in enumerate(similar_users_indices):
        similarity = similarity_scores[i]
        if similarity <= 0:  # Skip users with no similarity
            continue
            
        similar_user_interactions = user_item_matrix.iloc[similar_user_idx, :]
        
        # For each uninteracted item, calculate weighted score
        for item in uninteracted_items:
            if similar_user_interactions[item] > 0:  # Similar user interacted with this item
                # Weight the interaction by similarity and interaction strength
                weighted_score = similarity * similar_user_interactions[item]
                
                if item in recommendations_scores:
                    recommendations_scores[item] += weighted_score
                else:
                    recommendations_scores[item] = weighted_score
    
    if not recommendations_scores:
        print("No suitable recommendations found from similar users. Providing popular items.")
        return get_popular_items_recommendation(user_item_matrix, N)
    
    # Sort recommendations by score
    sorted_recommendations = sorted(recommendations_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Extract top recommendations
    top_recommendations = [item for item, score in sorted_recommendations[:N]]
    
    # If we don't have enough recommendations, fill with popular items
    if len(top_recommendations) < N:
        popular_items = get_popular_items_recommendation(user_item_matrix, N - len(top_recommendations))
        # Add popular items that aren't already in recommendations
        for item in popular_items:
            if item not in top_recommendations:
                top_recommendations.append(item)
                if len(top_recommendations) >= N:
                    break
    
    return top_recommendations[:N]

# Helper function to get popular items for cold start users
def get_popular_items_recommendation(user_item_matrix: pd.DataFrame, N: int) -> List[str]:
    """
    Get popular items based on overall interaction frequency and strength.
    Used for cold start users or when collaborative filtering fails.
    """
    # Calculate popularity score for each item (sum of all interactions)
    item_popularity = user_item_matrix.sum(axis=0)
    
    # Get top N popular items
    popular_items = item_popularity.nlargest(N)
    
    return list(popular_items.index)