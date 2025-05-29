# fast_query_parser.py
import re
import torch
from sentence_transformers import SentenceTransformer, util
from rapidfuzz import fuzz
from typing import List # Import List for type hinting

class FastQueryParser:
    def __init__(self, data: dict, category_model_name="all-MiniLM-L6-v2"):
        """
        Initializes the FastQueryParser.

        Args:
            data (dict): Dictionary containing loaded product data from dataloader.py.
                         This will still provide categories for semantic matching and brands.
            category_model_name (str): Name of the SentenceTransformer model.
        """

        # Fast semantic model for category matching
        self.category_model = SentenceTransformer(category_model_name)
        if torch.cuda.is_available():
            self.category_model = self.category_model.to('cuda')

        # Categories and simplified categories (populated from data_loader - STILL NEEDED)
        self.master_categories: List[str] = data.get('master_categories', [])
        self.sub_categories: List[str] = data.get('sub_categories', [])
        self.article_types: List[str] = data.get('article_types', [])
        self.categories: List[str] = list(set(self.sub_categories + self.article_types + self.master_categories))
        self.simplified_categories: List[str] = [cat.lower() for cat in self.categories if isinstance(cat, str)]

        # Encode the simplified categories ONCE in the constructor
        self.category_embeddings = self.encode_categories()

        # Known brands (populated from data_loader)
        # Note: Your current data_loader.py has extract_brands which processes productDisplayName
        # This list below is the one you already have static in the file. Keep it.
        brands = [
            "nike", "adidas", "puma", "reebok", "new balance", "asics", "vans", "converse", "skechers",
            "jordan", "yeezy", "fendi", "gucci", "prada", "versace", "burberry", "chanel", "dior", "armani",
            "calvin klein", "tommy hilfiger", "ralph lauren", "levis", "wrangler", "diesel", "gap", "old navy", "h&m", "zara",
            "forever 21", "mango", "uniqlo", "topshop", "asos", "boohoo", "missguided", "pretty little thing", "nasty gal", "revolve",
            "nordstrom", "macys", "bloomingdale's", "saks fifth avenue", "neiman marcus", "harrods", "selfridges", "net-a-porter", "mytheresa", "farfetch",
            "michael kors", "coach", "kate spade", "tory burch", "marc jacobs", "dooney & bourke", "fossil", "guess", "steve madden", "aldo",
            "nine west", "sam edelman", "clarks", "timberland", "ugg", "hunter", "sorel", "birkenstock", "teva", "chaco",
            "ray-ban", "oakley", "maui jim", "persol", "versace", "gucci", "prada", "fendi", "armani", "tom ford",
            "rolex", "omega", "tag heuer", "cartier", "tissot", "citizen", "seiko", "casio", "swatch", "michael kors",
            "fossil", "kate spade", "marc jacobs", "tory burch", "coach", "dooney & bourke", "guess", "steve madden", "aldo", "nine west",
            "sam edelman", "clarks", "timberland", "ugg", "hunter", "sorel", "birkenstock", "teva", "chaco", "patagonia",
            "north face", "columbia", "arcteryx", "marmot", "ll bean", "eddie bauer", "rei", "outdoor voices", "lululemon", "athleta",
            "spanx", "free people", "anthropologie", "urban outfitters", "madewell", "j crew", "banana republic", "ann taylor", "loft",
            "express", "bebe", "guess", "forever 21", "hollister", "american eagle", "abercrombie & fitch", "pacsun", "zumiez",
            "hot topic", "torrid", "lane bryant", "asos", "boohoo", "missguided", "pretty little thing", "nasty gal", "revolve",
            "nordstrom", "macys", "bloomingdale's", "saks fifth avenue", "neiman marcus", "harrods", "selfridges", "net-a-porter", "mytheresa", "farfetch",
            "balenciaga", "saint laurent", "celine", "givenchy", "valentino", "alexander mcqueen", "balmain", "off-white", "vetements",
            "supreme", "stussy", "carhartt wip", "obey", "thrasher", "palace", "bape", "kith", "ronnie fieg", "aime leon dore",
            "acne studios", "comme des garcons", "issey miyake", "yohji yamamoto", "rick owens", "dries van noten", "maison margiela", "jil sander", "the row", "bottega veneta"
        ]
        self.known_brands = [brand.lower() for brand in brands]
        self.normalized_brands = [re.sub(r'[^\w\s]', '', brand) for brand in self.known_brands]

        # --- MANUAL LISTS FOR COLORS AND SEASONS ---
        # These are now hardcoded and do NOT rely on data_loader for this info.
        self.color_keywords: List[str] = [
            "black", "white", "red", "blue", "green", "yellow", "orange", "purple", "pink",
            "brown", "grey", "gray", "silver", "gold", "navy", "maroon", "olive", "beige",
            "teal", "magenta", "cyan", "lime", "indigo", "violet", "turquoise", "khaki",
            "cream", "burgundy", "lavender", "peach", "tan", "charcoal", "forest green",
            "sky blue", "royal blue", "light blue", "dark blue", "light green", "dark green",
            "rose gold", "coral", "fuchsia"
        ]
        self.season_tags: List[str] = [
            "spring", "summer", "fall", "autumn", "winter" # "fall" and "autumn" for robustness
        ]
        # --- END MANUAL LISTS ---

        self.usage_keywords: List[str] = [u.lower() for u in data.get('usage', []) if isinstance(u, str)]

        # Compile regex for price extraction ONCE
        self.price_regex = re.compile(r"\$?\s*(\d{2,5})")

    def encode_categories(self) -> torch.Tensor:
        """Encodes the simplified categories using the category model."""
        if self.simplified_categories:
            embeddings = self.category_model.encode(self.simplified_categories, convert_to_tensor=True)
            if torch.cuda.is_available():
                embeddings = embeddings.to('cuda')
            return embeddings
        else:
            return torch.tensor([])

    def parse_query(self, query: str) -> dict:
        """Parses the input query to extract relevant information."""

        result = {
            "original_query": query,
            "masterCategory": None,
            "subCategory": None,
            "articleType": None,
            "brand": None,
            "price_range": None,
            "colors": [],
            "seasons": [],
            "usage": None
        }

        lowered_query = query.lower()
        normalized_query = re.sub(r'[^\w\s]', '', lowered_query)

        # --- Price Extraction ---
        prices = [int(m.group(1)) for m in self.price_regex.finditer(query)]
        if prices:
            if len(prices) == 1:
                result["price_range"] = {"max": prices[0]}
            else:
                result["price_range"] = {"min": min(prices), "max": max(prices)}

        # --- Fuzzy Brand Matching ---
        if self.known_brands:
            best_match = None
            best_score = 0
            for i, brand in enumerate(self.known_brands):
                score = fuzz.WRatio(normalized_query, self.normalized_brands[i]) # Use normalized brands
                if score > best_score:
                    best_score = score
                    best_match = self.known_brands[i]
                if best_score > 90:
                    break
            if best_match and best_score >= 80:
                result["brand"] = best_match

        # --- Semantic Category Matching (Master Category, Sub Category, Article Type) ---
        query_embedding = self.category_model.encode(query, convert_to_tensor=True)
        if torch.cuda.is_available():
            query_embedding = query_embedding.to('cuda')

        max_similarity = -1
        best_match = None
        category_type = None

        for i, cat in enumerate(self.categories):
            if isinstance(cat, str):
                cat_embedding = self.category_embeddings[i].unsqueeze(0)
                similarity = util.cos_sim(query_embedding, cat_embedding)[0][0]
                if similarity > max_similarity and similarity > 0.4:
                    max_similarity = similarity
                    best_match = cat
                    if best_match in self.sub_categories:
                        category_type = "subCategory"
                    elif best_match in self.article_types:
                        category_type = "articleType"
                    elif best_match in self.master_categories:
                        category_type = "masterCategory"

        if category_type == "subCategory" and best_match:
            result["subCategory"] = best_match
        elif category_type == "articleType" and best_match:
            result["articleType"] = best_match
        elif category_type == "masterCategory" and best_match:
            result["masterCategory"] = best_match

        # --- Color Matching (now using hardcoded list) ---
        matched_colors = {color for color in self.color_keywords if isinstance(color, str) and color in lowered_query}
        result["colors"] = list(matched_colors)

        # --- Season Tag Extraction (now using hardcoded list) ---
        matched_seasons = {tag for tag in self.season_tags if isinstance(tag, str) and tag in lowered_query}
        result["seasons"] = list(matched_seasons)

        # --- Usage Extraction (still dynamic from data_loader) ---
        result["usage"] = None
        for usage in self.usage_keywords:
            if isinstance(usage, str) and usage in lowered_query:
                result["usage"] = usage
                break

        return result