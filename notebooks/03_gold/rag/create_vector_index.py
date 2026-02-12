# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Create Vector Search Index for RAG
# MAGIC 
# MAGIC **Purpose**: Generate embeddings and create a searchable vector index
# MAGIC 
# MAGIC **Components**:
# MAGIC 1. Prepare documents with combined text for embedding
# MAGIC 2. Create Databricks Vector Search endpoint
# MAGIC 3. Create Delta Sync index with managed embeddings
# MAGIC 
# MAGIC **Input**: `riskbricks.silver.rag_documents`
# MAGIC **Output**: `riskbricks.gold.rag_vector_index`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet

# COMMAND ----------

# Restart Python after pip install
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.vector_search.client import VectorSearchClient
import time

# Configuration
catalog = "riskbricks"

# Source table is now the CLEANED Silver layer
source_table = f"{catalog}.silver.rag_documents"

# Vector Search settings
vs_endpoint_name = "riskbricks_vs_endpoint"
vs_index_name = f"{catalog}.gold.rag_vector_index"

# Embedding model (Databricks Foundation Model)
embedding_model = "databricks-bge-large-en"

print(f"📊 Source table (Silver): {source_table}")
print(f"🔍 Vector index (Gold): {vs_index_name}")
print(f"🧠 Embedding model: {embedding_model}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Verify Source Data

# COMMAND ----------

# Check source table
record_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
print(f"📰 RAG corpus has {record_count:,} documents")

# Document type distribution
print("\n📊 Document Types:")
spark.sql(f"""
    SELECT doc_type, COUNT(*) as count
    FROM {source_table}
    GROUP BY doc_type
    ORDER BY count DESC
""").show()

# Sample data
print("\n📝 Sample Documents:")
spark.sql(f"""
    SELECT doc_id, symbol, doc_type, LEFT(title, 50) as title_preview
    FROM {source_table}
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Verify Silver Table Has Embedding Text
# MAGIC 
# MAGIC The Silver layer (`rag_documents`) already contains `text_for_embedding` column.

# COMMAND ----------

# Verify the Silver table has the embedding column
print("📝 Verifying Silver table structure...")

# Check for text_for_embedding column
columns = spark.sql(f"DESCRIBE {source_table}").collect()
column_names = [row[0] for row in columns]

if 'text_for_embedding' in column_names:
    print("✅ text_for_embedding column found in Silver table")
else:
    print("⚠️ text_for_embedding column not found - run notebooks/02_silver/rag/clean_to_silver.py first")

# Show sample
print("\n📝 Sample embedded text:")
spark.sql(f"""
    SELECT doc_id, symbol, doc_type, LEFT(text_for_embedding, 150) as text_preview 
    FROM {source_table}
    LIMIT 3
""").show(truncate=False)

# Verify record count
record_count = spark.sql(f"SELECT COUNT(*) FROM {source_table}").collect()[0][0]
print(f"\n📊 Total documents for indexing: {record_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Create Vector Search Endpoint

# COMMAND ----------

# Initialize Vector Search client
vsc = VectorSearchClient()

# Check if endpoint exists
try:
    endpoint = vsc.get_endpoint(vs_endpoint_name)
    print(f"✅ Using existing endpoint: {vs_endpoint_name}")
    print(f"   Status: {endpoint.get('endpoint_status', {}).get('state', 'UNKNOWN')}")
except Exception as e:
    print(f"📝 Creating new endpoint: {vs_endpoint_name}")
    try:
        vsc.create_endpoint(
            name=vs_endpoint_name,
            endpoint_type="STANDARD"
        )
        print(f"✅ Endpoint creation initiated: {vs_endpoint_name}")
        print("⏳ Waiting for endpoint to be ready (this may take 5-10 minutes)...")
        
        # Wait for endpoint to be ready
        for i in range(30):
            time.sleep(30)
            try:
                endpoint = vsc.get_endpoint(vs_endpoint_name)
                status = endpoint.get('endpoint_status', {}).get('state', '')
                print(f"   Status: {status}")
                if status == 'ONLINE':
                    print(f"✅ Endpoint is ONLINE!")
                    break
            except:
                pass
    except Exception as create_error:
        print(f"⚠️ Error creating endpoint: {str(create_error)}")
        print("\n📋 Create endpoint manually in Databricks UI:")
        print("   1. Go to Machine Learning > Vector Search")
        print(f"   2. Create endpoint: {vs_endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Create Vector Search Index

# COMMAND ----------

# Source table for vector index (using cleaned Silver layer)
embedding_source_table = source_table  # riskbricks.silver.rag_documents

# Check if index exists, create if not
try:
    existing_index = vsc.get_index(vs_endpoint_name, vs_index_name)
    print(f"✅ Index already exists: {vs_index_name}")
    
    # Get status
    status = existing_index.describe()
    sync_state = status.get('status', {}).get('detailed_state', 'UNKNOWN')
    print(f"   Status: {sync_state}")
    
except Exception as e:
    print(f"📝 Creating new vector index: {vs_index_name}")
    
    try:
        # Create Delta Sync Index with managed embeddings
        index = vsc.create_delta_sync_index(
            endpoint_name=vs_endpoint_name,
            index_name=vs_index_name,
            source_table_name=embedding_source_table,
            pipeline_type="TRIGGERED",  # Manual sync
            primary_key="doc_id",
            embedding_source_column="text_for_embedding",
            embedding_model_endpoint_name=embedding_model
        )
        
        print(f"✅ Vector index created: {vs_index_name}")
        print("⏳ Index is syncing... This may take several minutes.")
        print("   You can check status in the Databricks UI.")
        
    except Exception as create_error:
        error_msg = str(create_error)
        print(f"⚠️ Error creating index: {error_msg}")
        
        if "already exists" in error_msg.lower():
            print("   Index already exists - attempting to get it...")
        else:
            print("\n📋 Alternative: Create index manually in Databricks UI:")
            print(f"   1. Go to Catalog > {catalog} > silver > rag_for_embedding")
            print(f"   2. Click 'Create' > 'Vector Search Index'")
            print(f"   3. Endpoint: {vs_endpoint_name}")
            print(f"   4. Index name: {vs_index_name}")
            print(f"   5. Primary key: doc_id")
            print(f"   6. Embedding column: text_for_embedding")
            print(f"   7. Embedding model: {embedding_model}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Check and Trigger Sync

# COMMAND ----------

try:
    index = vsc.get_index(vs_endpoint_name, vs_index_name)
    
    # Check sync status - handle different response structures
    status = index.describe()
    
    # Extract state (can be in different locations)
    sync_state = (
        status.get('status', {}).get('detailed_state') or
        status.get('status', {}).get('state') or
        status.get('detailed_state') or
        status.get('state', 'UNKNOWN')
    )
    
    # Extract document count (can be in different locations)
    num_docs = (
        status.get('status', {}).get('num_rows') or
        status.get('status', {}).get('indexed_row_count') or
        status.get('num_rows') or
        status.get('indexed_row_count') or
        0
    )
    
    print(f"📊 Index Status:")
    print(f"   State: {sync_state}")
    print(f"   Documents indexed: {num_docs:,}")
    
    # If count is 0, try to get it from the source table
    if num_docs == 0:
        try:
            source_count = spark.sql(f"SELECT COUNT(*) FROM {embedding_source_table}").collect()[0][0]
            print(f"   (Source table has {source_count:,} documents)")
        except:
            pass
    
    if sync_state in ['OFFLINE', 'OFFLINE_FAILED']:
        print("\n🔄 Triggering sync...")
        index.sync()
        print("✅ Sync triggered! Check back in a few minutes.")
    elif sync_state in ['ONLINE_TRIGGERED_SYNC', 'PROVISIONING']:
        print("\n⏳ Sync in progress... Check Databricks UI for real-time status.")
    elif 'ONLINE' in str(sync_state):
        print("\n✅ Index is ready for queries!")
        
except Exception as e:
    print(f"⚠️ Could not check index: {str(e)}")
    print("   The index may still be initializing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Vector Search Queries

# COMMAND ----------

def test_search(query, num_results=5, doc_type_filter=None):
    """Test vector search with a query"""
    try:
        index = vsc.get_index(vs_endpoint_name, vs_index_name)
        
        # Build filters
        filters = None
        if doc_type_filter:
            filters = {"doc_type": doc_type_filter}
        
        results = index.similarity_search(
            query_text=query,
            columns=["doc_id", "symbol", "company_name", "doc_type", "title", "source", "published_date"],
            num_results=num_results,
            filters=filters
        )
        
        print(f"\n🔍 Query: '{query}'")
        if doc_type_filter:
            print(f"   Filter: doc_type = '{doc_type_filter}'")
        print(f"📊 Results:\n")
        
        for row in results.get('result', {}).get('data_array', []):
            doc_type = row[3]
            title = row[4][:60] + "..." if len(row[4]) > 60 else row[4]
            print(f"  📄 [{doc_type}] {title}")
            print(f"     {row[1]} ({row[2]}) | {row[5]} | {row[6]}")
            print()
        
        return results
        
    except Exception as e:
        print(f"⚠️ Search error: {str(e)}")
        print("   Index may still be syncing. Try again in a few minutes.")
        return None

# COMMAND ----------

# Test 1: General company query
test_search("What is Costco? Tell me about the company", num_results=5)

# COMMAND ----------

# Test 2: SEC filings query
test_search("Costco annual report 10-K business risks", num_results=3)

# COMMAND ----------

# Test 3: News query
test_search("Latest Apple stock news and earnings", num_results=5)

# COMMAND ----------

# Test 4: Filtered query - only SEC filings
test_search("Microsoft financial statements", num_results=3, doc_type_filter="sec_10k")

# COMMAND ----------

# Test 5: Wikipedia company info
test_search("Tesla company history and Elon Musk", num_results=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Vector Index Complete!

# COMMAND ----------

print(f"""
================================================================================
✅ VECTOR SEARCH INDEX COMPLETE!
================================================================================

📊 Configuration:
   - Endpoint: {vs_endpoint_name}
   - Index: {vs_index_name}
   - Embedding model: {embedding_model}
   - Source table: {embedding_source_table}
   
🔍 Index Capabilities:
   - Semantic search across all document types
   - Filter by doc_type (news, sec_10k, sec_10q, sec_8k, wiki_company, etc.)
   - Fast retrieval for RAG queries
   
📝 Document Types Indexed:
   - news: Yahoo Finance, Google News headlines
   - sec_10k: Annual reports
   - sec_10q: Quarterly reports  
   - sec_8k: Material events
   - wiki_company: Wikipedia company info
   - stock_context: Current price/volume data
   - earnings_event: High-volume trading analysis
   
🧪 Test Queries:
   - "What's in Amazon's 10-K?"
   - "Tell me about Microsoft's history"
   - "Latest Tesla news"
   - "Apple earnings report"
   
🔄 Next Step:
   Run 03_news_rag_agent.py to build the RAG agent

================================================================================
""")

dbutils.notebook.exit("success")
