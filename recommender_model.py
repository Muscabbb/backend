import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from scipy.sparse import csr_matrix
from pymongo import MongoClient
from typing import Tuple, List, Union

# MongoDB connection details
MONGO_URI = "mongodb+srv://musab:lkLcVq0MOOxpLxxo@cluster0.9r682mt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "hekto" # Your database name
MONGO_COLLECTION_NAME = "Interactions" # <<< IMPORTANT: Confirm this is your actual collection name for interactions

# Load and preprocess data from MongoDB
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
        df['productId'] = df['productId'].astype(str)
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

    # Train model
    model = NearestNeighbors(
        metric='cosine',
        algorithm='brute',
        n_neighbors=20
    )
    model.fit(sparse_matrix)

    return model, user_item_matrix

# Recommendation function
def recommend_products_for_user(user_id: str, N: int = 5,
                                  model: Union[NearestNeighbors, None] = None,
                                  user_item_matrix: Union[pd.DataFrame, None] = None) -> Union[List[str], str]:
    
    # If model or matrix not provided, load and build them from MongoDB
    if model is None or user_item_matrix is None:
        print("Recommendation model or matrix not provided, attempting to load from MongoDB.")
        df = load_data_from_mongodb()
        model, user_item_matrix = build_model(df)
        if model is None or user_item_matrix is None:
            return "Failed to initialize recommendation model. No recommendations can be made."

    if user_id not in user_item_matrix.index:
        return f"User '{user_id}' not found in the interaction dataset. Cannot make personalized recommendations."

    user_idx = user_item_matrix.index.get_loc(user_id)

    # Find similar users
    distances, indices = model.kneighbors(
        user_item_matrix.iloc[user_idx, :].values.reshape(1, -1),
        n_neighbors=10 # Get 10 nearest neighbors
    )

    # Get similar users' indices (excluding the user themselves if present)
    similar_users_indices = indices.flatten()[1:]
    
    if len(similar_users_indices) == 0:
        return "No similar users found to make recommendations for this user."

    similar_users_interactions = user_item_matrix.iloc[similar_users_indices, :]

    # Get items the target user hasn't interacted with
    user_interactions = user_item_matrix.iloc[user_idx, :]
    uninteracted_items = user_interactions[user_interactions == 0].index

    if len(uninteracted_items) == 0:
        return f"User '{user_id}' has interacted with all available products, or no new un-interacted products can be recommended."

    # Calculate predicted interactions (average interaction of similar users for un-interacted items)
    # Filter similar_users_interactions to only include columns (products) that the current user hasn't interacted with
    predicted_interactions = similar_users_interactions[uninteracted_items].mean(axis=0)

    # Get top N recommendations (only products with a predicted interaction greater than 0)
    top_n = predicted_interactions[predicted_interactions > 0].nlargest(N)
    
    if top_n.empty:
        return "Could not find suitable recommendations based on similar users' interactions."

    return list(top_n.index)