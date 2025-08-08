import re
import torch
from sentence_transformers import SentenceTransformer, util
from rapidfuzz import fuzz, process
from typing import List, Dict, Set, Tuple, Union, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastQueryParser")

class FastQueryParser:
    def __init__(self, data: dict, category_model_name: str = "all-MiniLM-L6-v2"):
        # Initialize model
        self.category_model = SentenceTransformer(category_model_name)
        if torch.cuda.is_available():
            self.category_model = self.category_model.to('cuda')
            logger.info(f"Using GPU for category model")
        else:
            logger.info(f"Using CPU for category model")

        # Category data with enhanced normalization
        self.master_categories = set(data.get('master_categories', []))
        self.sub_categories = set(data.get('sub_categories', []))
        self.article_types = set(data.get('article_types', []))
        
        # Create category mapping with normalized versions
        self.category_map = {}
        for cat in self.article_types:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "articleType")
        for cat in self.sub_categories:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "subCategory")
        for cat in self.master_categories:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "masterCategory")
        
        # Enhanced brand list with aliases
        self.brand_aliases = {
            "nike": ["nike", "nk", "nik"],
            "adidas": ["adidas", "adi", "adiddas", "addidas"],
            "puma": ["puma", "pum", "pma"],
            "zara": ["zara", "zra"],
            "wrangler": ["wrangler", "wranglr", "wrnglr"],
            "fila": ["fila", "fla", "fl"],
            "skagen": ["skagen", "skagn", "skg"],
            "titan": ["titan", "ttn", "ttan"],
            "fabindia": ["fabindia", "fabindya", "fabindia"],
            "jealous 21": ["jealous 21", "jealous21", "jl21", "j21", "jealous"],
            "peter england": ["peter england", "peterengland", "ptr england", "p england"],
            "fossil": ["fossil", "fosil", "fsl"],
            "police": ["police", "plc", "polce"],
            "john players": ["john players", "johnplayers", "jp", "j players"],
            # Add more as needed
        }
        # Create flat list of all brand aliases
        self.all_brand_aliases = []
        for aliases in self.brand_aliases.values():
            self.all_brand_aliases.extend(aliases)
        self.brand_list = list(self.brand_aliases.keys())
        self.normalized_brands = [re.sub(r'[^\w\s]', '', brand) for brand in self.all_brand_aliases]

        # Enhanced attribute lists
        self.color_keywords = [
            "black", "white", "red", "blue", "green", "yellow", "orange", "purple", "pink",
            "brown", "grey", "gray", "silver", "gold", "navy", "maroon", "olive", "beige",
            "teal", "magenta", "cyan", "lime", "indigo", "violet", "turquoise", "khaki",
            "cream", "burgundy", "lavender", "peach", "tan", "charcoal", "forest green",
            "sky blue", "royal blue", "light blue", "dark blue", "light green", "dark green",
            "rose gold", "coral", "fuchsia"
        ]
        self.season_tags = ["spring", "summer", "fall", "autumn", "winter"]
        self.usage_keywords = list(set([u.lower() for u in data.get('usage', []) if isinstance(u, str)]))

        # Improved price regex with context awareness
        self.price_regex = re.compile(
            r"(?<!\w)(?:under|over|less than|more than|below|above)?\s*"
            r"(?:\$|₹|€|£|Rs?\.?)\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d{2,5})\b"
            r"|\b(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:dollars|USD|INR|EUR|GBP|rs|rupees)\b",
            re.IGNORECASE
        )
        
        # Create n-grams for category matching
        self.category_ngrams = set()
        for phrase in self.category_map.keys():
            words = phrase.split()
            # Create n-grams from 1 to 3 words
            for n in range(1, min(4, len(words) + 1)):
                for i in range(len(words) - n + 1):
                    self.category_ngrams.add(' '.join(words[i:i+n]))
                    
        # ===== HIERARCHICAL CATEGORY MAPPING =====
        # Define hierarchical relationships between article types and higher-level categories
        self.hierarchical_map = {
            # Format: "normalized_article_type": ("subCategory", "masterCategory")
            "jeans": ("Bottomwear", "Apparel"),
            "slipper": ("Footwear", "Footwear"),
            "handbag": ("Bags", "Accessories"),
            "tshirt": ("Topwear", "Apparel"),
            "t shirt": ("Topwear", "Apparel"),
            "shirt": ("Topwear", "Apparel"),
            "top": ("Topwear", "Apparel"),
            "watch": ("Watches", "Accessories"),
            "sunglass": ("Eyewear", "Accessories"),
            "shoe": ("Footwear", "Footwear"),
            "dress": ("Dress", "Apparel"),
            "skirt": ("Bottomwear", "Apparel"),
            "jacket": ("Outerwear", "Apparel"),
            "coat": ("Outerwear", "Apparel"),
            "sweater": ("Topwear", "Apparel"),
            "hoodie": ("Topwear", "Apparel"),
            "pant": ("Bottomwear", "Apparel"),
            "short": ("Bottomwear", "Apparel"),
            "boot": ("Footwear", "Footwear"),
            "sandal": ("Footwear", "Footwear"),
            "sneaker": ("Footwear", "Footwear"),
            "jewelry": ("Jewelry", "Accessories"),
            "necklace": ("Jewelry", "Accessories"),
            "bracelet": ("Jewelry", "Accessories"),
            "ring": ("Jewelry", "Accessories"),
            "belt": ("Belts", "Accessories"),
            "scarf": ("Accessories", "Accessories"),
            "hat": ("Headwear", "Accessories"),
            "glove": ("Accessories", "Accessories"),
            "sock": ("Hosiery", "Apparel"),
            "underwear": ("Innerwear", "Apparel"),
            "bra": ("Innerwear", "Apparel"),
            "swimwear": ("Swimwear", "Apparel"),
            "suit": ("Suits", "Apparel"),
            "blazer": ("Suits", "Apparel"),
            "activewear": ("Activewear", "Apparel"),
            "legging": ("Activewear", "Apparel"),
            "purse": ("Bags", "Accessories"),
            "backpack": ("Bags", "Accessories"),
            "wallet": ("Wallets", "Accessories"),
            "eyewear": ("Eyewear", "Accessories"),
            "perfume": ("Fragrance", "Beauty"),
            "cosmetic": ("Makeup", "Beauty"),
            "skincare": ("Skincare", "Beauty"),
        }
        
        # Add plural forms automatically
        hierarchical_copy = self.hierarchical_map.copy()
        for key, value in hierarchical_copy.items():
            if key.endswith('y'):
                plural = key[:-1] + 'ies'
            else:
                plural = key + 's'
            if plural not in self.hierarchical_map:
                self.hierarchical_map[plural] = value

    def _split_query_into_segments(self, query: str) -> List[str]:
        """Split complex queries into individual product requests"""
        # Split by common connectors and punctuation
        segments = re.split(r'\.\s*|\;\s*|\s+also\s+|\s+additionally\s+|\s+and\s+a\s+|\s+and\s+also\s+|\s+next\s+|\s+then\s+', query)
        
        # Remove empty segments and short segments that are just connectors
        segments = [s.strip() for s in segments if s.strip() and len(s.split()) > 2]
        
        # Handle remaining connectors at start of segments
        cleaned_segments = []
        for seg in segments:
            # Remove leading connectors
            seg = re.sub(r'^(also|additionally|and|then|next|,)\s*', '', seg, flags=re.IGNORECASE)
            if seg:
                cleaned_segments.append(seg)
                
        return cleaned_segments if cleaned_segments else [query]

    def _infer_hierarchical_categories(self, result: dict) -> None:
        """Infer subCategory and masterCategory based on recognized articleTypes"""
        # Process each article type to infer higher categories
        for article in list(result["articleTypes"]):
            # Normalize the article type for matching
            norm_article = re.sub(r'[^\w\s]', '', article.lower())
            
            # Find matching hierarchical category
            if norm_article in self.hierarchical_map:
                sub_cat, master_cat = self.hierarchical_map[norm_article]
                
                # Add inferred categories if not already present
                if sub_cat and sub_cat not in result["subCategories"]:
                    result["subCategories"].append(sub_cat)
                if master_cat and master_cat not in result["masterCategories"]:
                    result["masterCategories"].append(master_cat)
                    
            # Handle special cases with multi-word article types
            elif ' ' in norm_article:
                # Try to match the last word (e.g., "casual shirt" → "shirt")
                last_word = norm_article.split()[-1]
                if last_word in self.hierarchical_map:
                    sub_cat, master_cat = self.hierarchical_map[last_word]
                    if sub_cat and sub_cat not in result["subCategories"]:
                        result["subCategories"].append(sub_cat)
                    if master_cat and master_cat not in result["masterCategories"]:
                        result["masterCategories"].append(master_cat)

    def _parse_segment(self, segment: str) -> dict:
        """Parse individual product request segment"""
        result = {
            "masterCategories": [],
            "subCategories": [],
            "articleTypes": [],
            "brands": [],
            "price_range": None,
            "colors": [],
            "seasons": [],
            "usage": []
        }

        lowered = segment.lower()
        normalized = re.sub(r'[^\w\s]', '', lowered)
        tokens = normalized.split()
        
        # --- Price Extraction with context awareness ---
        prices = []
        for match in self.price_regex.finditer(segment):
            # Use first non-empty group
            price_str = next((g for g in match.groups() if g is not None), None)
            if price_str:
                try:
                    price_val = int(float(price_str.replace(',', '')))
                    prices.append(price_val)
                except:
                    continue
        
        # Filter prices that are part of brand names
        valid_prices = []
        for price in prices:
            price_str = str(price)
            # Check if price appears in any brand aliases in this segment
            if not any(price_str in alias for alias in self.all_brand_aliases if price_str in alias):
                valid_prices.append(price)
        
        if valid_prices:
            result["price_range"] = {"min": min(valid_prices), "max": max(valid_prices)} if len(valid_prices) > 1 else {"max": valid_prices[0]}

        # --- Robust Brand Extraction with Fuzzy Matching ---
        # Find best brand matches with threshold
        brand_matches = process.extract(
            lowered, 
            self.all_brand_aliases, 
            scorer=fuzz.WRatio, 
            score_cutoff=80  # Slightly lower threshold to capture more brands
        )
        
        # Get unique brands (avoid duplicates)
        seen_brands = set()
        for match, score, _ in brand_matches:
            # Find canonical brand name
            for brand, aliases in self.brand_aliases.items():
                if match in aliases and brand not in seen_brands:
                    result["brands"].append(brand)
                    seen_brands.add(brand)
                    break

        # --- Enhanced Category Matching with N-grams ---
        # Create n-grams from tokens (1-3 words)
        ngrams = set()
        for n in range(1, 4):
            for i in range(len(tokens) - n + 1):
                ngram = ' '.join(tokens[i:i+n])
                ngrams.add(ngram)
        
        # Check all n-grams against category map
        matched_categories = set()
        for ngram in ngrams:
            if ngram in self.category_map:
                cat, cat_type = self.category_map[ngram]
                matched_categories.add((cat, cat_type))
        
        # Also check against all possible n-grams from category phrases
        for phrase in self.category_ngrams:
            if phrase in normalized:
                # Find the original category for this phrase
                if phrase in self.category_map:
                    cat, cat_type = self.category_map[phrase]
                    matched_categories.add((cat, cat_type))
        
        # Add matched categories to result
        for cat, cat_type in matched_categories:
            if cat_type == "articleType":
                result["articleTypes"].append(cat)
            elif cat_type == "subCategory":
                result["subCategories"].append(cat)
            elif cat_type == "masterCategory":
                result["masterCategories"].append(cat)

        # --- Semantic Fallback for Categories with Higher Threshold ---
        if not any([result["articleTypes"], result["subCategories"], result["masterCategories"]]):
            query_embedding = self.category_model.encode(segment, convert_to_tensor=True)
            if torch.cuda.is_available():
                query_embedding = query_embedding.to('cuda')
            
            # Get all category names
            all_categories = list(self.category_map.keys())
            if all_categories:
                embeddings = self.category_model.encode(all_categories, convert_to_tensor=True)
                if torch.cuda.is_available():
                    embeddings = embeddings.to('cuda')
                
                # Find top matches with higher threshold
                similarities = util.cos_sim(query_embedding, embeddings)[0]
                top_idx = similarities.argmax().item()
                if similarities[top_idx] > 0.5:  # Increased threshold for confidence
                    cat, cat_type = self.category_map[all_categories[top_idx]]
                    if cat_type == "articleType":
                        result["articleTypes"].append(cat)
                    elif cat_type == "subCategory":
                        result["subCategories"].append(cat)
                    elif cat_type == "masterCategory":
                        result["masterCategories"].append(cat)

        # --- Attribute Extraction ---
        # Colors with multi-word support
        color_matches = set()
        for color in self.color_keywords:
            if ' ' in color:
                if color in lowered:
                    color_matches.add(color)
            else:
                if f' {color} ' in f' {lowered} ':
                    color_matches.add(color)
        result["colors"] = list(color_matches)
        
        # Seasons
        result["seasons"] = [s for s in self.season_tags if s in lowered]
        
        # Usage with word boundaries
        result["usage"] = [u for u in self.usage_keywords if re.search(rf"\b{u}\b", lowered)]

        # ===== HIERARCHICAL CATEGORY INFERENCE =====
        # Infer higher-level categories based on recognized article types
        if result["articleTypes"]:
            self._infer_hierarchical_categories(result)

        # Cleanup empty lists
        for key in ["masterCategories", "subCategories", "articleTypes", "brands"]:
            if not result[key]:
                result[key] = []

        return result

    def parse_query(self, query: str) -> List[dict]:
        """Parse complex queries with multiple product requests"""
        segments = self._split_query_into_segments(query)
        results = []
        
        for segment in segments:
            if segment:  # Ensure segment is not empty
                result = self._parse_segment(segment)
                results.append({
                    "query": segment,
                    **result
                })
                
        return results if results else [{
            "query": query,
            "masterCategories": [],
            "subCategories": [],
            "articleTypes": [],
            "brands": [],
            "price_range": None,
            "colors": [],
            "seasons": [],
            "usage": []
        }]