# 🔍 Parse Endpoint Workflow Documentation

This document provides a detailed step-by-step breakdown of what happens when the `/parse` endpoint is hit until the response is sent back to the client.

## 📋 Overview

The `/parse` endpoint processes natural language queries and converts them into structured Elasticsearch queries to search for products. Here's the complete workflow:

---

## 🚀 Step-by-Step Workflow

### 1️⃣ **Client Request Received**
```
🌐 Client → FastAPI Server
```
- **Endpoint**: `POST /parse`
- **Input**: JSON payload with `query` field containing natural language text
- **Example**: `{"query": "red nike shoes under 100"}`

⬇️

### 2️⃣ **Environment Variables Loading**
```
🔧 Load Configuration
```
- **File**: `elasticsearch_utils.py`
- **Action**: Load environment variables from `.env` file:
  - `ELASTICSEARCH_URL` - Elasticsearch server URL
  - `ELASTICSEARCH_API_KEY` - Authentication key
  - `INDEX_NAME` - Target index name (e.g., "hekto")

⬇️

### 3️⃣ **Query Parser Initialization Check**
```
🧠 Initialize/Load Query Parser
```
- **File**: `main.py` (lines 99-120)
- **Logic**:
  ```python
  if query_parser is None:
      try:
          # Try to load from pickle file
          with open('query_parser.pkl', 'rb') as f:
              query_parser = pickle.load(f)
      except FileNotFoundError:
          # Create new parser with product data
          data = load_data()
          query_parser = FastQueryParser(data)
  ```

⬇️

### 4️⃣ **Natural Language Query Parsing**
```
🔤 Parse Natural Language → Structured Data
```
- **File**: `queryParser.py`
- **Class**: `FastQueryParser`
- **Method**: `parse_query(query: str)`

#### 4.1 **Text Preprocessing**
- Convert query to lowercase
- Remove special characters: `re.sub(r'[^\w\s]', '', lowered_query)`

#### 4.2 **Price Extraction**
- Use regex to find price patterns
- Extract single price (max) or price range (min-max)

#### 4.3 **Brand Matching (Fuzzy)**
- Compare against 100+ known brands using `rapidfuzz.fuzz.WRatio`
- Threshold: 80% similarity score
- Brands include: Nike, Adidas, Puma, Gucci, etc.

#### 4.4 **Category Matching (Semantic)**
- Use SentenceTransformer model (`all-MiniLM-L6-v2`)
- Generate embeddings for query and categories
- Calculate cosine similarity
- Threshold: 0.4 similarity
- Categories: `masterCategory`, `subCategory`, `articleType`

#### 4.5 **Color & Season Extraction**
- Match against predefined color keywords
- Match against season tags

#### 4.6 **Usage Extraction**
- Match against usage keywords (sports, casual, formal, etc.)

**Output Structure**:
```json
{
  "original_query": "red nike shoes under 100",
  "masterCategory": null,
  "subCategory": null,
  "articleType": "shoes",
  "brand": "nike",
  "price_range": {"max": 100},
  "colors": ["red"],
  "seasons": [],
  "usage": null
}
```

⬇️

### 5️⃣ **Elasticsearch Client Connection**
```
🔌 Connect to Elasticsearch
```
- **File**: `elasticsearch_utils.py`
- **Function**: `get_elasticsearch_client()`
- **Process**:
  - Create Elasticsearch client with URL and API key
  - Handle connection errors
  - Return client instance or None

⬇️

### 6️⃣ **Elasticsearch Query Building**
```
🏗️ Build Elasticsearch Query
```
- **File**: `elasticsearch_utils.py`
- **Function**: `build_elasticsearch_query(parsed_query)`
- **Logic**:

#### 6.1 **Query Structure Creation**
```json
{
  "query": {
    "bool": {
      "must": [],
      "filter": []
    }
  }
}
```

#### 6.2 **Field Mapping**
- **Brand** → `productDisplayName` (match query)
- **Article Type** → `articleType.keyword` (term query)
- **Colors** → `baseColour.keyword` (term query, first color only)
- **Seasons** → `season.keyword` (term query, first season only)

#### 6.3 **Price Range Handling**
- Add range query for `price` field if price_range exists

⬇️

### 7️⃣ **Elasticsearch Query Execution**
```
🔍 Execute Search Query
```
- **File**: `elasticsearch_utils.py`
- **Function**: `execute_elasticsearch_query(client, query)`
- **Process**:
  - Execute search against `INDEX_NAME`
  - Handle search errors
  - Process hits into product dictionaries

⬇️

### 8️⃣ **Response Preparation**
```
📦 Prepare Final Response
```
- **File**: `main.py`
- **Structure**:
```json
{
  "parsed_query": {
    "original_query": "red nike shoes under 100",
    "brand": "nike",
    "articleType": "shoes",
    "colors": ["red"],
    "price_range": {"max": 100}
  },
  "elasticsearch_query": {
    "query": {
      "bool": {
        "must": [...],
        "filter": [...]
      }
    }
  },
  "results": [
    {
      "id": "12345",
      "productDisplayName": "Nike Air Max Red",
      "price": 89.99,
      "baseColour": "Red",
      "articleType": "Shoes"
    }
  ]
}
```

⬇️

### 9️⃣ **Error Handling**
```
⚠️ Handle Potential Errors
```
- **Parser Errors**: Return error message if query parsing fails
- **Elasticsearch Errors**: Return empty results with error info
- **Connection Errors**: Graceful degradation

⬇️

### 🔟 **Response Sent to Client**
```
📤 FastAPI Server → Client
```
- **HTTP Status**: 200 OK (success) or appropriate error code
- **Content-Type**: `application/json`
- **Response**: Complete JSON with parsed query, ES query, and results

---

## 🛠️ Technical Components

### **Key Files Involved**
- 📄 `main.py` - FastAPI endpoint handler
- 🧠 `queryParser.py` - Natural language processing
- 🔍 `elasticsearch_utils.py` - Elasticsearch operations
- ⚙️ `.env` - Environment configuration

### **External Dependencies**
- 🤖 **SentenceTransformer** - Semantic similarity
- ⚡ **RapidFuzz** - Fuzzy string matching
- 🔍 **Elasticsearch** - Search engine
- 🚀 **FastAPI** - Web framework
- 🐍 **PyTorch** - ML model backend

### **Environment Variables Required**
```bash
ELASTICSEARCH_URL=https://your-elasticsearch-url
ELASTICSEARCH_API_KEY=your-api-key
INDEX_NAME=hekto
```

---

## 📊 Performance Considerations

- **Query Parser Caching**: Parser is loaded once and reused
- **Model Optimization**: Uses GPU if available for embeddings
- **Fuzzy Matching**: Early termination at 90% similarity
- **Semantic Threshold**: 0.4 minimum similarity for categories

---

## 🔄 Flow Diagram

```
🌐 Client Request
    ⬇️
🔧 Load Environment Variables
    ⬇️
🧠 Initialize Query Parser
    ⬇️
🔤 Parse Natural Language
    ⬇️
🔌 Connect to Elasticsearch
    ⬇️
🏗️ Build ES Query
    ⬇️
🔍 Execute Search
    ⬇️
📦 Prepare Response
    ⬇️
📤 Send to Client
```

This comprehensive workflow ensures that natural language queries are efficiently processed and converted into meaningful search results! 🎯