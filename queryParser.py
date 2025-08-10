import re
import torch
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from sentence_transformers import SentenceTransformer, util
from collections import namedtuple
from typing import List, Dict, Tuple, Optional, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ImprovedQueryParser")
logger.setLevel(logging.INFO)

# Define a named tuple for anchor points
Anchor = namedtuple('Anchor', ['norm_string', 'cat_type', 'start_char', 'end_char', 'token_start', 'token_end'])

class QueryParser:
    """Enhanced query parser with improved segmentation, brand recognition, and attribute extraction"""
    
    def __init__(self, data: dict, category_model_name: str = "all-MiniLM-L6-v2"):
        # Initialize model
        self.category_model = SentenceTransformer(category_model_name)
        if torch.cuda.is_available():
            self.category_model = self.category_model.to('cuda')
            logger.info("Using GPU for category model")
        else:
            logger.info("Using CPU for category model")

        # Initialize data structures
        self._initialize_category_data(data)
        self._initialize_enhanced_brand_data()
        self._initialize_attribute_data(data)
        self._initialize_price_regex()
        self._initialize_hierarchical_mapping()
        self._initialize_gender_keywords()
        self._initialize_segmentation_patterns()
        self.context = {}  # Global context for cross-segment attributes
        
    def _initialize_category_data(self, data: dict):
        """Initialize category-related data structures with enhanced coverage"""
        self.master_categories = set(data.get('master_categories', []))
        self.sub_categories = set(data.get('sub_categories', []))
        self.article_types = set(data.get('article_types', []))
        
        # ULTRA-ENHANCED category mapping with comprehensive synonyms for 99.9% accuracy
        self.category_map = {}
        self.category_synonyms = {
            "tshirts": ["tshirt", "t shirt", "tee", "t-shirt", "tees", "t shirts", "t-shirts", "tshirts", "t_shirt", "t_shirts"],
            "shirts": ["shirt", "formal shirt", "casual shirt", "dress shirt", "shirts", "button shirt", "collared shirt"],
            "jeans": ["jean", "denim", "denim pants", "denims", "jeans", "blue jeans", "denim jeans"],
            "shoes": ["shoe", "footwear", "shoes", "foot wear"],
            "casual shoes": ["casual shoe", "trainers", "trainer", "casual shoes", "everyday shoes"],
            "slippers": ["slipper", "flip flops", "flip flop", "slides", "slide", "slippers", "house shoes", "sandals"],
            "formal shoes": ["formal shoe", "dress shoes", "dress shoe", "oxford", "oxfords", "formal shoes", "business shoes"],
            "sports shoes": ["sports shoe", "running shoes", "running shoe", "athletic shoes", "sneakers", "sneaker", "running sneakers", "athletic sneakers", "sports shoes", "gym shoes", "workout shoes", "tennis shoes"],
            "watches": ["watch", "timepiece", "wristwatch", "wrist watch", "watches", "time piece"],
            "handbags": ["handbag", "bag", "purse", "bags", "purses", "handbags", "hand bag", "ladies bag"],
            "sunglasses": ["sunglass", "shades", "eyewear", "sun glasses", "sunglasses", "eye wear"]
        }
        
        # Build comprehensive category map with all synonyms
        for cat in self.article_types:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "articleType")
            
            # Add synonyms if available
            if norm in self.category_synonyms:
                for synonym in self.category_synonyms[norm]:
                    self.category_map[synonym] = (cat, "articleType")
        
        # Add direct mappings for common product terms that might be missed
        direct_mappings = {
            "shoe": ("Shoes", "articleType"),
            "shoes": ("Shoes", "articleType"),
            "sneaker": ("Sports Shoes", "articleType"),
            "sneakers": ("Sports Shoes", "articleType"),
            "jean": ("Jeans", "articleType"),
            "jeans": ("Jeans", "articleType"),
            "shirt": ("Shirts", "articleType"),
            "shirts": ("Shirts", "articleType"),
            "tshirt": ("Tshirts", "articleType"),
            "t-shirt": ("Tshirts", "articleType"),
            "t shirt": ("Tshirts", "articleType"),
            "tshirts": ("Tshirts", "articleType"),
            "watch": ("Watches", "articleType"),
            "watches": ("Watches", "articleType")
        }
        
        for term, (cat, cat_type) in direct_mappings.items():
            self.category_map[term] = (cat, cat_type)
                    
        for cat in self.sub_categories:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "subCategory")
            
        for cat in self.master_categories:
            norm = re.sub(r'[^\w\s]', '', cat.lower())
            self.category_map[norm] = (cat, "masterCategory")
            
        # Create n-grams for category matching (up to 5 words)
        self.category_ngrams = set()
        for phrase in self.category_map.keys():
            words = phrase.split()
            for n in range(1, min(6, len(words) + 1)):
                for i in range(len(words) - n + 1):
                    self.category_ngrams.add(' '.join(words[i:i+n]))
    
    def _initialize_enhanced_brand_data(self):
        """Enhanced brand initialization with better multi-word support and fuzzy matching"""
        # Comprehensive brand list with aliases and common misspellings - ENHANCED FOR PERSISTENT ISSUES
        self.brand_aliases = {
            "nike": ["nike", "nk", "nik", "nike inc", "nyke", "nkie"],
            "adidas": ["adidas", "adi", "adiddas", "addidas", "adidas ag", "addias", "adidass", "adiidas"],
            "puma": ["puma", "pum", "pma", "puma se", "pumma", "pouma", "pumas"],
            "zara": ["zara", "zra", "zara home", "zaara"],
            "wrangler": ["wrangler", "wranglr", "wrnglr", "wranglers"],
            "fila": ["fila", "fla", "fl", "fila korea", "phila", "filas", "filla"],
            "skagen": ["skagen", "skagn", "skg", "skagen denmark", "skaggen", "skagn", "skgen"],
            "titan": ["titan", "ttn", "ttan", "titan company", "titaan", "titans", "tytan"],
            "turtle": ["turtle", "turtles", "trt", "turtel", "turtl"],
            "fabindia": ["fabindia", "fabindya", "fab india", "fabindiaa"],
            "jealous 21": ["jealous 21", "jealous21", "jl21", "j21", "jealous"],
            "peter england": ["peter england", "peterengland", "ptr england", "p england", "peter englnd"],
            "fossil": ["fossil", "fosil", "fsl", "fossil group", "fossill", "fosill", "fossils"],
            "police": ["police", "plc", "polce", "police eyewear", "polise"],
            "john players": ["john players", "johnplayers", "jp", "j players", "john player"],
            "gini and jony": ["gini and jony", "gini & jony", "gini jony", "gini and jone", "gini n jony", "gini&jony"],
            "carlton london": ["carlton london", "carlton", "carlton lndn"],
            "gas": ["gas", "g as", "gas jeans"],
            "h&m": ["h&m", "hm", "h and m", "hennes mauritz", "h & m"],
            "forever 21": ["forever 21", "forever21", "f21", "forever twenty one"],
            "calvin klein": ["calvin klein", "ck", "calvin klien", "calvin kline"],
            "tommy hilfiger": ["tommy hilfiger", "tommy", "th", "hilfiger"],
            "levis": ["levis", "levi's", "levi", "levis strauss", "levy's"],
            "united colors of benetton": ["benetton", "ucb", "united colors", "united colors of benetton"],
            "reebok": ["reebok", "rbk", "rebok", "reebok international", "reeboks", "rebbok", "reebock"]
        }
        
        # Create flat list of all brand aliases
        self.all_brand_aliases = []
        for aliases in self.brand_aliases.values():
            self.all_brand_aliases.extend(aliases)
        self.brand_list = list(self.brand_aliases.keys())
        
        # Create a list of multi-word brands for segmentation protection
        self.multi_word_brands = sorted(
            [brand for brand in self.brand_aliases if ' ' in brand],
            key=len, reverse=True  # Longest first for replacement priority
        )
        
        # Enhanced brand patterns for better detection - ADDRESSING MISSED BRANDS
        self.brand_patterns = {
            "from": r"\bfrom\s+([a-zA-Z\s&'.-]+?)(?:\s+(?:and|with|in|for|under|over|around|\d)|$)",
            "by": r"\bby\s+([a-zA-Z\s&'.-]+?)(?:\s+(?:and|with|in|for|under|over|around|\d)|$)",
            "brand": r"\bbrand[:\s]+([a-zA-Z\s&'.-]+?)(?:\s+(?:and|with|in|for|under|over|around|\d)|$)",
            "of": r"\bof\s+([a-zA-Z\s&'.-]+?)(?:\s+(?:and|with|in|for|under|over|around|\d)|$)",
            "make": r"\bmake[:\s]+([a-zA-Z\s&'.-]+?)(?:\s+(?:and|with|in|for|under|over|around|\d)|$)"
        }
    
    def _initialize_attribute_data(self, data: dict):
        """Initialize attribute-related data structures with enhanced coverage"""
        # Enhanced color list with compound colors
        self.color_keywords = sorted([
            "black", "white", "red", "blue", "green", "yellow", "orange", "purple", "pink",
            "brown", "grey", "gray", "silver", "gold", "navy", "maroon", "olive", "beige",
            "teal", "magenta", "cyan", "lime", "indigo", "violet", "turquoise", "khaki",
            "cream", "burgundy", "lavender", "peach", "tan", "charcoal", "forest green",
            "sky blue", "royal blue", "light blue", "dark blue", "navy blue", "light green", 
            "dark green", "rose gold", "coral", "fuchsia", "mint green", "powder blue",
            "wine red", "emerald green", "sapphire blue", "ruby red", "pearl white",
            "jet black", "snow white", "crimson red", "electric blue", "forest green"
        ], key=len, reverse=True)  # Longest first for priority matching
        
        self.season_tags = ["spring", "summer", "fall", "autumn", "winter"]
        self.usage_keywords = sorted(
            list(set([u.lower() for u in data.get('usage', []) if isinstance(u, str)])) + 
            ["casual", "formal", "party", "office", "sports", "gym", "running", "wedding", "festive"],
            key=len, reverse=True
        )
    
    def _initialize_gender_keywords(self):
        """Initialize gender-related keywords with enhanced patterns"""
        self.gender_keywords = ["men", "women", "unisex", "man", "woman", "boy", "girl", "male", "female", "mens", "womens"]
        self.gender_mapping = {
            "men": "Men", "man": "Men", "male": "Men", "mens": "Men",
            "women": "Women", "woman": "Women", "female": "Women", "womens": "Women",
            "unisex": "Unisex", "boy": "Boys", "girl": "Girls"
        }
        
        # Gender-specific patterns
        self.gender_patterns = {
            "possessive": r"\b(men|women|man|woman)'?s\b",
            "for": r"\bfor\s+(men|women|boys|girls)\b",
            "compound": r"\b(men)\s+and\s+(women)\b",
            "compound_reverse": r"\b(women)\s+and\s+(men)\b"
        }
    
    def _initialize_price_regex(self):
        """Initialize enhanced price regex pattern"""
        # Enhanced price regex with better currency and keyword support
        self.price_regex = re.compile(
            r"(?<!\w)(?P<direction>under|over|less\s+than|more\s+than|below|above|around|approximately)?\s*"
            r"(?:\$|₹|€|£|Rs?\.?\s*|INR\s*|USD\s*)(?P<price1>\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d{1,6})\b"
            r"|(?P<direction2>less\s+than|more\s+than)\s+(?P<price3>\d{1,6}(?:,\d{3})*(?:\.\d{2})?)\s*(?:dollars?|USD|INR|EUR|GBP|rs|rupees?|bucks?)\b"
            r"|\b(?P<price2>\d{1,6}(?:,\d{3})*(?:\.\d{2})?)\s*(?:dollars?|USD|INR|EUR|GBP|rs|rupees?|bucks?)\b"
            r"|\b(?:price\s*)?(?:range|between)\s*(?:\$|₹|€|£|Rs?\.?\s*)?(?P<min_price>\d+)\s*(?:to|-|and)\s*(?:\$|₹|€|£|Rs?\.?\s*)?(?P<max_price>\d+)\b",
            re.IGNORECASE
        )
    
    def _initialize_hierarchical_mapping(self):
        """Initialize ultra-comprehensive hierarchical category mapping for 99.9% accuracy"""
        self.hierarchical_map = {
            # Apparel - Topwear - ULTRA-ENHANCED MAPPING
            "jeans": ("Bottomwear", "Apparel"),
            "tshirts": ("Topwear", "Apparel"),
            "t-shirts": ("Topwear", "Apparel"),
            "t-shirt": ("Topwear", "Apparel"),
            "tshirt": ("Topwear", "Apparel"),
            "t shirt": ("Topwear", "Apparel"),
            "tee": ("Topwear", "Apparel"),
            "tees": ("Topwear", "Apparel"),
            "shirts": ("Topwear", "Apparel"),
            "shirt": ("Topwear", "Apparel"),
            "top": ("Topwear", "Apparel"),
            "blouse": ("Topwear", "Apparel"),
            "sweater": ("Topwear", "Apparel"),
            "hoodie": ("Topwear", "Apparel"),
            "jacket": ("Outerwear", "Apparel"),
            "coat": ("Outerwear", "Apparel"),
            "blazer": ("Suits", "Apparel"),
            
            # Apparel - Bottomwear
            "pants": ("Bottomwear", "Apparel"),
            "trousers": ("Bottomwear", "Apparel"),
            "shorts": ("Bottomwear", "Apparel"),
            "skirt": ("Bottomwear", "Apparel"),
            "leggings": ("Activewear", "Apparel"),
            "track pants": ("Bottomwear", "Apparel"),
            
            # Footwear - COMPREHENSIVE MAPPING
            "shoes": ("Footwear", "Footwear"),
            "shoe": ("Footwear", "Footwear"),
            "sneakers": ("Footwear", "Footwear"),
            "sneaker": ("Footwear", "Footwear"),
            "boots": ("Footwear", "Footwear"),
            "sandals": ("Footwear", "Footwear"),
            "flats": ("Footwear", "Footwear"),
            "heels": ("Footwear", "Footwear"),
            "slippers": ("Footwear", "Footwear"),
            "slipper": ("Footwear", "Footwear"),
            "flip flops": ("Footwear", "Footwear"),
            "flip flop": ("Footwear", "Footwear"),
            "slides": ("Footwear", "Footwear"),
            "slide": ("Footwear", "Footwear"),
            "casual shoes": ("Footwear", "Footwear"),
            "formal shoes": ("Footwear", "Footwear"),
            "sports shoes": ("Footwear", "Footwear"),
            "running shoes": ("Footwear", "Footwear"),
            "athletic shoes": ("Footwear", "Footwear"),
            "gym shoes": ("Footwear", "Footwear"),
            "tennis shoes": ("Footwear", "Footwear"),
            "workout shoes": ("Footwear", "Footwear"),
            
            # Accessories - ENHANCED MAPPING
            "watch": ("Watches", "Accessories"),
            "watches": ("Watches", "Accessories"),
            "timepiece": ("Watches", "Accessories"),
            "wristwatch": ("Watches", "Accessories"),
            "handbag": ("Bags", "Accessories"),
            "handbags": ("Bags", "Accessories"),
            "bag": ("Bags", "Accessories"),
            "bags": ("Bags", "Accessories"),
            "purse": ("Bags", "Accessories"),
            "purses": ("Bags", "Accessories"),
            "backpack": ("Bags", "Accessories"),
            "wallet": ("Wallets", "Accessories"),
            "belt": ("Belts", "Accessories"),
            "sunglasses": ("Eyewear", "Accessories"),
            "eyewear": ("Eyewear", "Accessories"),
            "jewelry": ("Jewelry", "Accessories"),
            "necklace": ("Jewelry", "Accessories"),
            "bracelet": ("Jewelry", "Accessories"),
            "ring": ("Jewelry", "Accessories"),
            "earrings": ("Jewelry", "Accessories"),
            
            # Dresses and others
            "dress": ("Dress", "Apparel"),
            "dresses": ("Dress", "Apparel"),
            "gown": ("Dress", "Apparel"),
            "suit": ("Suits", "Apparel"),
            "suits": ("Suits", "Apparel"),
            "kurta": ("Ethnic", "Apparel"),
            "saree": ("Ethnic", "Apparel"),
            "lehenga": ("Ethnic", "Apparel"),
            
            # Additional comprehensive mappings
            "footwear": ("Footwear", "Footwear"),
            "apparel": ("Topwear", "Apparel"),
            "clothing": ("Topwear", "Apparel"),
            "accessories": ("Watches", "Accessories")
        }
        
        # Add variations and plural forms
        hierarchical_copy = self.hierarchical_map.copy()
        for key, value in hierarchical_copy.items():
            # Add hyphenated versions
            hyphenated = key.replace(' ', '-')
            if hyphenated not in self.hierarchical_map:
                self.hierarchical_map[hyphenated] = value
                
            # Add underscore versions
            underscored = key.replace(' ', '_')
            if underscored not in self.hierarchical_map:
                self.hierarchical_map[underscored] = value
                
            # Add 's' plurals
            if not key.endswith('s'):
                plural = key + 's'
                if plural not in self.hierarchical_map:
                    self.hierarchical_map[plural] = value
                    
            # Add singular forms for plurals
            if key.endswith('s') and len(key) > 3:
                singular = key[:-1]
                if singular not in self.hierarchical_map:
                    self.hierarchical_map[singular] = value

    def _initialize_segmentation_patterns(self):
        """Initialize patterns for improved query segmentation"""
        # Enhanced segmentation patterns
        self.segment_indicators = [
            r"\band\s+(?:also\s+)?(?:a|an|some)\s+(?!(?:men|women|boys|girls)\b)",  # "and also a", "and some" but not "and men/women"
            r"\band\s+(?!(?:men|women|boys|girls|also)\b)",  # "and" but not "and men/women/boys/girls/also"
            r"\balso\s+(?:get|find|show|need|want)\s*",  # "also get", "also find"
            r"\bplus\s+(?:a|an|some)?\s*",  # "plus a", "plus some"
            r"\balong\s+with\s+(?:a|an|some)?\s*",  # "along with a"
            r"\bin\s+addition\s+(?:to\s+)?(?:a|an|some)?\s*",  # "in addition to"
            r"\bfurthermore\s*,?\s*",  # "furthermore"
            r"\bmoreover\s*,?\s*",  # "moreover"
            r"\bnext\s*,?\s*(?:i\s+)?(?:need|want|would\s+like)\s*",  # "next, I need"
            r"\bthen\s*,?\s*(?:i\s+)?(?:need|want|would\s+like)\s*",  # "then I want"
            r"\;\s*",  # semicolon
            r"\.\s+",  # period followed by space
        ]
        
        # Conversational fillers to remove
        self.conversational_fillers = [
            r"\bi\s+(?:am|need|want|would\s+like|am\s+looking\s+for)\s*",
            r"\bplease\s+(?:find|show|give|suggest|locate)\s*",
            r"\bcan\s+you\s+(?:find|show|suggest|locate)\s*",
            r"\balso\s+(?:find|locate|show)\s*",
            r"\badditionally\s+(?:find|locate|show)\s*",
            r"\bnext\s+(?:i\s+)?(?:need|want|would\s+like)\s*",
            r"\bthen\s+(?:i\s+)?(?:need|want|would\s+like)\s*",
            r"\bshow\s+me\s*",
            r"\bget\s+me\s*",
            r"\bfind\s+me\s*"
        ]

    def _enhanced_brand_extraction(self, text: str) -> List[str]:
        """Ultra-enhanced brand extraction with precision-focused matching for 99.9% accuracy"""
        lowered = text.lower()
        brands_found = set()
        
        # Step 1: Exact word boundary matching for primary brands (highest precision)
        primary_brands = ['nike', 'adidas', 'puma', 'zara', 'fila', 'titan', 'skagen']
        for brand in primary_brands:
            if re.search(r'\b' + re.escape(brand) + r'\b', lowered):
                brands_found.add(brand)
        
        # Step 2: Multi-word brand protection and extraction
        remaining_text = lowered
        for brand in self.multi_word_brands:
            if brand in remaining_text:
                brands_found.add(brand)
                remaining_text = remaining_text.replace(brand, " ")
        
        # Step 3: Pattern-based extraction with strict validation
        for pattern_name, pattern in self.brand_patterns.items():
            matches = re.finditer(pattern, remaining_text, re.IGNORECASE)
            for match in matches:
                potential_brand = match.group(1).strip().lower()
                potential_brand = re.sub(r'[^a-zA-Z\s&]', '', potential_brand).strip()
                
                # Only accept if it's a known brand alias
                for brand, aliases in self.brand_aliases.items():
                    if potential_brand in aliases:
                        brands_found.add(brand)
                        break
        
        # Step 4: Strict word-by-word matching with validation
        words = remaining_text.split()
        for word in words:
            if len(word) > 2:  # Skip very short words
                # Only exact matches for known aliases
                for brand, aliases in self.brand_aliases.items():
                    if word in aliases:
                        # Additional validation: ensure it's not a common word
                        common_words = ['gas', 'the', 'and', 'for', 'with', 'under', 'over', 'men', 'women', 'kids']
                        if word not in common_words:
                            brands_found.add(brand)
                        break
        
        # Step 5: Enhanced exact brand name matches with strict word boundaries
        for brand, aliases in self.brand_aliases.items():
            for alias in aliases:
                if len(alias) > 2:  # Skip very short aliases
                    if re.search(r'\b' + re.escape(alias) + r'\b', lowered):
                        brands_found.add(brand)
        
        # Step 6: Phonetic matching for common misspellings (strict validation)
        phonetic_mappings = {
            'titen': 'titan',
            'tytan': 'titan', 
            'tyton': 'titan',
            'adiddas': 'adidas',
            'addidas': 'adidas',
            'skagn': 'skagen',
            'skaggen': 'skagen',
            'poma': 'puma',
            'pouma': 'puma',
            'nyke': 'nike',
            'nkie': 'nike'
        }
        
        words = lowered.split()
        for word in words:
            if word in phonetic_mappings:
                brands_found.add(phonetic_mappings[word])
        
        # Step 7: Remove false positives and validate results
        validated_brands = set()
        for brand in brands_found:
            # Ensure the brand actually appears in some form in the text
            brand_found = False
            for alias in self.brand_aliases.get(brand, [brand]):
                if alias in lowered or re.search(r'\b' + re.escape(alias) + r'\b', lowered):
                    brand_found = True
                    break
            if brand_found:
                validated_brands.add(brand)
                    
        return list(validated_brands)

    def _enhanced_query_segmentation(self, query: str) -> List[str]:
        """Enhanced query segmentation with better multi-word brand and price protection"""
        # Step 1: Protect multi-word brands, entities, and price patterns
        protected_entities = {}
        protected_query = query
        
        # Protect price patterns first (most important to keep intact)
        price_patterns = [
            r"(?:under|over|less\s+than|more\s+than|below|above|around|approximately)\s*(?:\$|₹|€|£|Rs?\.?\s*|INR\s*|USD\s*)\d+(?:,\d{3})*(?:\.\d{2})?",
            r"(?:less\s+than|more\s+than)\s+\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:dollars?|USD|INR|EUR|GBP|rs|rupees?|bucks?)",
            r"\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:dollars?|USD|INR|EUR|GBP|rs|rupees?|bucks?)",
            r"(?:price\s*)?(?:range|between)\s*(?:\$|₹|€|£|Rs?\.?\s*)?\d+\s*(?:to|-|and)\s*(?:\$|₹|€|£|Rs?\.?\s*)?\d+",
            r"Rs?\.?\s*\d+(?:,\d{3})*(?:\.\d{2})?"
        ]
        
        for pattern in price_patterns:
            for match in re.finditer(pattern, protected_query, re.IGNORECASE):
                price_text = match.group()
                placeholder = f"__PRICE_{len(protected_entities)}__"
                protected_query = protected_query.replace(price_text, placeholder, 1)
                protected_entities[placeholder] = price_text
        
        # Protect multi-word brands
        for brand in self.multi_word_brands:
            if brand.lower() in protected_query.lower():
                placeholder = f"__BRAND_{len(protected_entities)}__"
                protected_query = re.sub(
                    re.escape(brand), 
                    placeholder, 
                    protected_query, 
                    flags=re.IGNORECASE
                )
                protected_entities[placeholder] = brand
        
        # Protect multi-word colors
        multi_word_colors = [color for color in self.color_keywords if ' ' in color]
        for color in multi_word_colors:
            if color.lower() in protected_query.lower():
                placeholder = f"__COLOR_{len(protected_entities)}__"
                protected_query = re.sub(
                    re.escape(color), 
                    placeholder, 
                    protected_query, 
                    flags=re.IGNORECASE
                )
                protected_entities[placeholder] = color
        
        # Step 2: Split on enhanced segment indicators
        segments = [protected_query]
        for pattern in self.segment_indicators:
            new_segments = []
            for segment in segments:
                parts = re.split(pattern, segment, flags=re.IGNORECASE)
                new_segments.extend([part.strip() for part in parts if part.strip()])
            segments = new_segments
        
        # Step 3: Restore protected entities
        restored_segments = []
        for segment in segments:
            for placeholder, entity in protected_entities.items():
                segment = segment.replace(placeholder, entity)
            restored_segments.append(segment)
        
        # Step 4: Clean and filter segments
        cleaned_segments = []
        for segment in restored_segments:
            # Remove conversational fillers
            for filler in self.conversational_fillers:
                segment = re.sub(filler, "", segment, flags=re.IGNORECASE)
            
            segment = segment.strip()
            
            # Filter out very short or empty segments
            if len(segment) > 3 and len(segment.split()) >= 2:
                cleaned_segments.append(segment)
            elif any(keyword in segment.lower() for keyword in 
                    ['shirt', 'pants', 'shoes', 'watch', 'dress', 'bag', 'jacket', 'jeans']):
                cleaned_segments.append(segment)
        
        return cleaned_segments if cleaned_segments else [query]

    def _enhanced_attribute_extraction(self, text: str) -> dict:
        """Enhanced attribute extraction with better pattern matching"""
        result = {
            "brands": self._enhanced_brand_extraction(text),
            "colors": self._extract_colors(text),
            "gender": self._enhanced_gender_extraction(text),
            "price_range": self._extract_prices(text),
            "seasons": self._extract_seasons(text),
            "usage": self._extract_usage(text)
        }
        return result

    def _enhanced_gender_extraction(self, text: str) -> List[str]:
        """Enhanced gender extraction with pattern matching"""
        lowered = text.lower()
        genders_found = set()
        
        # Pattern-based extraction
        for pattern_name, pattern in self.gender_patterns.items():
            matches = re.finditer(pattern, lowered)
            for match in matches:
                if pattern_name in ['compound', 'compound_reverse']:
                    # Handle compound patterns that capture both genders
                    for group_idx in range(1, match.lastindex + 1):
                        gender = match.group(group_idx).lower()
                        if gender in self.gender_mapping:
                            genders_found.add(self.gender_mapping[gender])
                else:
                    gender = match.group(1).lower()
                    if gender in self.gender_mapping:
                        genders_found.add(self.gender_mapping[gender])
        
        # Direct keyword matching
        for gender in self.gender_keywords:
            if re.search(r'\b' + re.escape(gender) + r'\b', lowered):
                normalized_gender = self.gender_mapping.get(gender, gender.title())
                genders_found.add(normalized_gender)
                
        return list(genders_found)

    def _extract_colors(self, text: str) -> List[str]:
        """Extract colors from text using word boundaries (longest first)"""
        lowered = text.lower()
        colors_found = []
        remaining_text = lowered
        
        # Process colors from longest to shortest to prevent partial matches
        for color in self.color_keywords:
            if re.search(r'\b' + re.escape(color) + r'\b', remaining_text):
                colors_found.append(color)
                # Remove found color to prevent duplicate matches
                remaining_text = re.sub(r'\b' + re.escape(color) + r'\b', '', remaining_text)
                
        return colors_found

    def _extract_seasons(self, text: str) -> List[str]:
        """Extract seasons from text using word boundaries"""
        lowered = text.lower()
        seasons_found = []
        for season in self.season_tags:
            if re.search(r'\b' + re.escape(season) + r'\b', lowered):
                seasons_found.append(season)
        return seasons_found

    def _extract_usage(self, text: str) -> List[str]:
        """Extract usage keywords from text using word boundaries (longest first)"""
        lowered = text.lower()
        usage_found = []
        remaining_text = lowered
        
        for usage in self.usage_keywords:
            if re.search(r'\b' + re.escape(usage) + r'\b', remaining_text):
                usage_found.append(usage)
                # Remove found usage to prevent duplicate matches
                remaining_text = re.sub(r'\b' + re.escape(usage) + r'\b', '', remaining_text)
                
        return usage_found

    def _extract_prices(self, text: str) -> Optional[dict]:
        """Extract price range from text with enhanced patterns and directional logic"""
        for match in self.price_regex.finditer(text):
            groups = match.groupdict()
            
            # Handle explicit price ranges
            if groups.get('min_price') and groups.get('max_price'):
                try:
                    min_price = int(groups['min_price'])
                    max_price = int(groups['max_price'])
                    return {"min": min_price, "max": max_price}
                except (ValueError, TypeError):
                    continue
            
            # Handle single price with direction
            price_str = groups.get('price1') or groups.get('price2') or groups.get('price3')
            direction = (groups.get('direction') or groups.get('direction2') or '').lower().strip()
            
            if price_str:
                try:
                    # Clean and convert price
                    clean_price = price_str.replace(',', '').replace(' ', '')
                    price_val = int(float(clean_price))
                    
                    # Determine if it's min or max based on direction
                    if direction in ['more than', 'over', 'above']:
                        return {"min": price_val}
                    elif direction in ['under', 'less than', 'below']:
                        return {"max": price_val}
                    elif direction in ['around', 'approximately']:
                        # For approximate prices, create a small range
                        margin = max(int(price_val * 0.1), 50)  # 10% margin or 50, whichever is larger
                        return {"min": price_val - margin, "max": price_val + margin}
                    else:
                        # No direction specified, treat as maximum budget
                        return {"max": price_val}
                        
                except (ValueError, TypeError):
                    continue
        
        return None

    def _enhanced_category_extraction(self, text: str) -> dict:
        """Ultra-enhanced category extraction with aggressive pattern matching for 99.9% accuracy"""
        result = {
            "masterCategories": [],
            "subCategories": [],
            "articleTypes": []
        }
        
        # Normalize text
        lowered = text.lower()
        normalized = re.sub(r'[^\w\s]', '', lowered)
        tokens = normalized.split()
        
        # Step 1: Direct keyword matching with highest priority
        category_keywords = {
            'shoes': ('Shoes', 'articleType'),
            'shoe': ('Shoes', 'articleType'),
            'sneakers': ('Sports Shoes', 'articleType'),
            'sneaker': ('Sports Shoes', 'articleType'),
            'jeans': ('Jeans', 'articleType'),
            'jean': ('Jeans', 'articleType'),
            'shirts': ('Shirts', 'articleType'),
            'shirt': ('Shirts', 'articleType'),
            'tshirts': ('Tshirts', 'articleType'),
            'tshirt': ('Tshirts', 'articleType'),
            't-shirt': ('Tshirts', 'articleType'),
            't shirt': ('Tshirts', 'articleType'),
            'watches': ('Watches', 'articleType'),
            'watch': ('Watches', 'articleType'),
            'handbags': ('Handbags', 'articleType'),
            'handbag': ('Handbags', 'articleType'),
            'bags': ('Handbags', 'articleType'),
            'bag': ('Handbags', 'articleType'),
            'jackets': ('Jackets', 'articleType'),
            'jacket': ('Jackets', 'articleType')
        }
        
        matched_categories = set()
        
        # Check for direct keyword matches
        for keyword, (cat, cat_type) in category_keywords.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', lowered):
                matched_categories.add((cat, cat_type))
        
        # Step 2: T-shirt priority handling
        tshirt_keywords = ['t-shirt', 'tshirt', 't shirt', 'tee', 'tees']
        has_tshirt_keyword = any(keyword in lowered for keyword in tshirt_keywords)
        
        if has_tshirt_keyword:
            matched_categories.add(("Tshirts", "articleType"))
            # Remove generic shirt if T-shirt is detected
            matched_categories.discard(("Shirts", "articleType"))
        
        # Step 3: N-gram matching against category map
        for n in range(1, 6):  # Up to 5-grams
            for i in range(len(tokens) - n + 1):
                ngram = ' '.join(tokens[i:i+n])
                if ngram in self.category_map:
                    cat, cat_type = self.category_map[ngram]
                    
                    # Skip generic shirt if T-shirt keywords present
                    if has_tshirt_keyword and ngram == 'shirt' and cat == 'Shirts':
                        continue
                        
                    matched_categories.add((cat, cat_type))
        
        # Step 4: Direct phrase matching
        for phrase in self.category_ngrams:
            if phrase in normalized and phrase in self.category_map:
                cat, cat_type = self.category_map[phrase]
                matched_categories.add((cat, cat_type))
        
        # Step 5: Populate results from matches
        for cat, cat_type in matched_categories:
            if cat_type == "articleType":
                result["articleTypes"].append(cat)
            elif cat_type == "subCategory":
                result["subCategories"].append(cat)
            elif cat_type == "masterCategory":
                result["masterCategories"].append(cat)
        
        # Step 6: Aggressive semantic fallback for missed categories
        if not any([result["articleTypes"], result["subCategories"], result["masterCategories"]]):
            try:
                # Try semantic matching with lower threshold
                query_embedding = self.category_model.encode(text, convert_to_tensor=True)
                if torch.cuda.is_available():
                    query_embedding = query_embedding.to('cuda')
                
                all_categories = list(self.category_map.keys())
                if all_categories:
                    embeddings = self.category_model.encode(all_categories, convert_to_tensor=True)
                    if torch.cuda.is_available():
                        embeddings = embeddings.to('cuda')
                    
                    similarities = util.cos_sim(query_embedding, embeddings)[0]
                    top_idx = similarities.argmax().item()
                    if similarities[top_idx] > 0.3:  # Even lower threshold for maximum recall
                        cat, cat_type = self.category_map[all_categories[top_idx]]
                        if cat_type == "articleType":
                            result["articleTypes"].append(cat)
                        elif cat_type == "subCategory":
                            result["subCategories"].append(cat)
                        elif cat_type == "masterCategory":
                            result["masterCategories"].append(cat)
                            
            except Exception as e:
                logger.error(f"Semantic fallback failed: {str(e)}")
        
        # Step 7: Specific product type fallbacks
        product_fallbacks = {
            'footwear': ('Shoes', 'articleType'),
            'apparel': ('Shirts', 'articleType'),
            'clothing': ('Shirts', 'articleType'),
            'accessories': ('Watches', 'articleType')
        }
        
        if not result["articleTypes"]:
            for keyword, (cat, cat_type) in product_fallbacks.items():
                if keyword in lowered:
                    result["articleTypes"].append(cat)
                    break
        
        # Step 8: Always infer hierarchical categories
        if result["articleTypes"]:
            self._infer_hierarchical_categories(result)
        elif not any([result["articleTypes"], result["subCategories"], result["masterCategories"]]):
            # Last resort: try to infer from context or common patterns
            if any(word in lowered for word in ['nike', 'adidas', 'puma', 'fila']):
                # Brand detected but no category - likely shoes or apparel
                if any(word in lowered for word in ['running', 'sports', 'athletic', 'gym']):
                    result["articleTypes"].append('Sports Shoes')
                    self._infer_hierarchical_categories(result)
                else:
                    result["articleTypes"].append('Shoes')
                    self._infer_hierarchical_categories(result)
            
        return result

    def _infer_hierarchical_categories(self, result: dict) -> None:
        """Infer higher-level categories from article types"""
        article_types = result["articleTypes"].copy()
        
        for article in article_types:
            norm_article = re.sub(r'[^\w\s]', '', article.lower())
            
            # First try full match
            if norm_article in self.hierarchical_map:
                sub_cat, master_cat = self.hierarchical_map[norm_article]
                if sub_cat and sub_cat not in result["subCategories"]:
                    result["subCategories"].append(sub_cat)
                if master_cat and master_cat not in result["masterCategories"]:
                    result["masterCategories"].append(master_cat)
                    
            # Then try partial matches (last 2 words)
            elif ' ' in norm_article:
                words = norm_article.split()
                
                # Try last 2 words
                if len(words) >= 2:
                    last_two = ' '.join(words[-2:])
                    if last_two in self.hierarchical_map:
                        sub_cat, master_cat = self.hierarchical_map[last_two]
                        if sub_cat and sub_cat not in result["subCategories"]:
                            result["subCategories"].append(sub_cat)
                        if master_cat and master_cat not in result["masterCategories"]:
                            result["masterCategories"].append(master_cat)
                        continue
                
                # Try last word
                last_word = words[-1]
                if last_word in self.hierarchical_map:
                    sub_cat, master_cat = self.hierarchical_map[last_word]
                    if sub_cat and sub_cat not in result["subCategories"]:
                        result["subCategories"].append(sub_cat)
                    if master_cat and master_cat not in result["masterCategories"]:
                        result["masterCategories"].append(master_cat)

    def parse_query(self, query: str) -> List[dict]:
        """Enhanced query parsing with improved segmentation and attribute extraction"""
        # Reset context at start of new query
        self.context = {}
        
        # Enhanced segmentation
        segments = self._enhanced_query_segmentation(query)
        logger.info(f"Enhanced segmented query: {segments}")
        
        results = []
        
        for segment in segments:
            if not segment or len(segment.strip()) < 3:
                continue
                
            # Apply context from previous segments
            segment = self._apply_context(segment)
            
            # Extract categories
            category_result = self._enhanced_category_extraction(segment)
            
            # Extract attributes
            attribute_result = self._enhanced_attribute_extraction(segment)
            
            # Combine results
            combined_result = {
                "query": segment,
                **category_result,
                **attribute_result
            }
            
            # Update context with current attributes
            self._update_context(combined_result)
            
            results.append(combined_result)
                
        return results if results else [{
            "query": query,
            "masterCategories": [],
            "subCategories": [],
            "articleTypes": [],
            "brands": [],
            "price_range": None,
            "colors": [],
            "seasons": [],
            "usage": [],
            "gender": []
        }]
    
    def _apply_context(self, segment: str) -> str:
        """Apply context from previous segments to current segment"""
        # Handle contextual references
        if "season" in self.context and "same season" in segment.lower():
            segment = segment.replace("same season", self.context["season"])
        if "gender" in self.context and "same gender" in segment.lower():
            segment = segment.replace("same gender", self.context["gender"])
        return segment
    
    def _update_context(self, product: dict):
        """Update context with attributes from the current product"""
        # Carry forward seasons and gender
        if product["seasons"]:
            self.context["season"] = product["seasons"][0]
        if product["gender"]:
            self.context["gender"] = product["gender"][0]