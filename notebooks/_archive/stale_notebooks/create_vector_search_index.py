# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Create Vector Search Index for RAG Corpus
# MAGIC
# MAGIC Creates a Vector Search index on `riskbricks.gold.rag_corpus` for semantic retrieval.
# MAGIC
# MAGIC **What This Does:**
# MAGIC - Generates embeddings for news articles, SEC filings, and company info
# MAGIC - Creates a Vector Search index for similarity search
# MAGIC - Enables semantic retrieval (not just keyword matching)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `riskbricks.gold.rag_corpus` table exists and is populated
# MAGIC - Vector Search endpoint is enabled in workspace

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F
import time

catalog = "riskbricks"
schema = "gold"
table_name = f"{catalog}.{schema}.rag_corpus"
index_name = f"{catalog}.{schema}.rag_corpus_index"

print(f"📊 Source table: {table_name}")
print(f"🔍 Index name: {index_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Verify Source Data

# COMMAND ----------

# Check if table exists and has data
if not spark.catalog.tableExists(table_name):
    raise ValueError(f"Table {table_name} does not exist. Run create_rag_corpus notebook first.")

doc_count = spark.table(table_name).count()
print(f"✅ Found {doc_count:,} documents in RAG corpus")

if doc_count == 0:
    raise ValueError("RAG corpus is empty. Ingest data first.")

# Show sample
print("\n📄 Sample documents:")
spark.table(table_name).select("symbol", "published_date", "title", "source").show(5, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Check Vector Search Endpoint

# COMMAND ----------

# Initialize Vector Search client
try:
    vsc = VectorSearchClient()
    print("✅ Vector Search client initialized")
except Exception as e:
    print(f"❌ Error initializing Vector Search client: {e}")
    print("\nℹ️  Vector Search may not be enabled in this workspace.")
    print("To enable: Contact Databricks support or use serverless Vector Search.")
    dbutils.notebook.exit("Vector Search not available")

# COMMAND ----------

# List existing endpoints
try:
    endpoints = vsc.list_endpoints()
    print(f"✅ Found {len(endpoints)} Vector Search endpoint(s)")
    for ep in endpoints:
        print(f"  - {ep.get('name', 'unnamed')}: {ep.get('endpoint_status', 'unknown status')}")
except Exception as e:
    print(f"⚠️  Could not list endpoints: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create or Get Endpoint

# COMMAND ----------

endpoint_name = "riskbricks_vs_endpoint"

try:
    # Try to get existing endpoint
    endpoint = vsc.get_endpoint(endpoint_name)
    print(f"✅ Using existing endpoint: {endpoint_name}")
except Exception:
    # Create new endpoint
    print(f"Creating new Vector Search endpoint: {endpoint_name}")
    try:
        vsc.create_endpoint(
            name=endpoint_name,
            endpoint_type="STANDARD"  # or "SERVERLESS" if available
        )
        print(f"✅ Created endpoint: {endpoint_name}")
        print("⏳ Waiting for endpoint to be ready...")
        
        # Wait for endpoint to be ready
        max_wait = 600  # 10 minutes
        wait_time = 0
        while wait_time < max_wait:
            try:
                ep_status = vsc.get_endpoint(endpoint_name)
                status = ep_status.get('endpoint_status', {}).get('state', 'UNKNOWN')
                if status == 'ONLINE':
                    print(f"✅ Endpoint is online!")
                    break
                else:
                    print(f"  Status: {status}, waiting...")
                    time.sleep(30)
                    wait_time += 30
            except Exception as e:
                print(f"  Waiting... ({wait_time}s)")
                time.sleep(30)
                wait_time += 30
        
        if wait_time >= max_wait:
            print(f"⚠️  Endpoint creation timed out after {max_wait}s")
            dbutils.notebook.exit("Endpoint creation timeout")
    except Exception as e:
        print(f"❌ Error creating endpoint: {e}")
        dbutils.notebook.exit(f"Could not create endpoint: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Prepare Text Column for Embedding

# COMMAND ----------

# Create a combined text column for embedding
# This combines title, source, and any content/snippet
df = spark.table(table_name)

# Check if 'content' or 'snippet' column exists
columns = df.columns
has_content = 'content' in columns or 'text' in columns or 'snippet' in columns

if has_content:
    content_col = next((c for c in ['content', 'text', 'snippet'] if c in columns), 'title')
    df_with_text = df.withColumn(
        "embedding_text",
        F.concat_ws(" | ", 
                    F.col("source"), 
                    F.col("title"), 
                    F.coalesce(F.col(content_col), F.lit(""))
        )
    )
else:
    # Fallback: just use title and source
    df_with_text = df.withColumn(
        "embedding_text",
        F.concat_ws(" | ", F.col("source"), F.col("title"))
    )

# Add a unique ID if not present
if 'id' not in df_with_text.columns and 'doc_id' not in df_with_text.columns:
    df_with_text = df_with_text.withColumn("id", F.monotonically_increasing_id())

# Write to a staging table for Vector Search
staging_table = f"{table_name}_staging"
df_with_text.write.mode("overwrite").saveAsTable(staging_table)

print(f"✅ Created staging table: {staging_table}")
print(f"   Columns: {df_with_text.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Create Vector Search Index

# COMMAND ----------

# Define index parameters
primary_key = "id" if "id" in df_with_text.columns else "doc_id"
embedding_source_column = "embedding_text"
embedding_model_endpoint = "databricks-gte-large-en"  # Databricks-hosted embedding model

print(f"Creating index with:")
print(f"  Primary key: {primary_key}")
print(f"  Embedding source: {embedding_source_column}")
print(f"  Embedding model: {embedding_model_endpoint}")

try:
    # Check if index already exists
    try:
        existing_index = vsc.get_index(endpoint_name, index_name)
        print(f"ℹ️  Index already exists: {index_name}")
        print(f"   Status: {existing_index.get('status', {}).get('detailed_state', 'unknown')}")
        
        # Optionally delete and recreate
        recreate = False  # Set to True to force recreation
        if recreate:
            print("🗑️  Deleting existing index...")
            vsc.delete_index(endpoint_name, index_name)
            time.sleep(5)
        else:
            print("✅ Using existing index")
            dbutils.notebook.exit("Index already exists")
    except Exception:
        print(f"Creating new index: {index_name}")
    
    # Create index
    vsc.create_delta_sync_index(
        endpoint_name=endpoint_name,
        source_table_name=staging_table,
        index_name=index_name,
        pipeline_type="TRIGGERED",  # or "CONTINUOUS" for real-time updates
        primary_key=primary_key,
        embedding_source_column=embedding_source_column,
        embedding_model_endpoint_name=embedding_model_endpoint
    )
    
    print(f"✅ Index creation initiated: {index_name}")
    print("⏳ This may take several minutes for large datasets...")
    
    # Wait for index to be ready
    max_wait = 1800  # 30 minutes
    wait_time = 0
    while wait_time < max_wait:
        try:
            idx_status = vsc.get_index(endpoint_name, index_name)
            state = idx_status.get('status', {}).get('detailed_state', 'UNKNOWN')
            ready_state = idx_status.get('status', {}).get('ready', False)
            
            print(f"  [{wait_time}s] State: {state}, Ready: {ready_state}")
            
            if ready_state or state == 'ONLINE_INDEXED':
                print(f"✅ Index is ready!")
                break
            elif state in ['OFFLINE', 'ERROR']:
                print(f"❌ Index creation failed: {state}")
                dbutils.notebook.exit(f"Index creation failed: {state}")
            
            time.sleep(30)
            wait_time += 30
        except Exception as e:
            print(f"  Checking status... ({wait_time}s)")
            time.sleep(30)
            wait_time += 30
    
    if wait_time >= max_wait:
        print(f"⚠️  Index creation timeout after {max_wait}s")
        print("   Check index status in Databricks Vector Search UI")

except Exception as e:
    print(f"❌ Error creating index: {e}")
    print("\n📋 Troubleshooting:")
    print("  - Ensure Vector Search is enabled in workspace")
    print("  - Check endpoint status")
    print("  - Verify table has 'embedding_text' column")
    print(f"  - Error: {type(e).__name__}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Test Vector Search

# COMMAND ----------

# Test similarity search
test_query = "What happened with Apple stock recently?"

try:
    results = vsc.get_index(endpoint_name, index_name).similarity_search(
        query_text=test_query,
        columns=["symbol", "published_date", "title", "source", "url"],
        num_results=5
    )
    
    print(f"✅ Vector Search test successful!")
    print(f"\n🔍 Query: '{test_query}'")
    print(f"\n📄 Top {len(results.get('result', {}).get('data_array', []))} results:")
    
    for i, row in enumerate(results.get('result', {}).get('data_array', []), 1):
        print(f"\n{i}. {row}")

except Exception as e:
    print(f"⚠️  Could not test search: {e}")
    print("   Index may still be initializing. Try again in a few minutes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print("=" * 60)
print("🎉 Vector Search Setup Complete!")
print("=" * 60)
print()
print(f"📊 Indexed table: {staging_table}")
print(f"🔍 Index name: {index_name}")
print(f"🌐 Endpoint: {endpoint_name}")
print(f"📝 Documents indexed: {doc_count:,}")
print()
print("🔧 Next Steps:")
print("  1. Update retrieval agent to use vector search")
print("  2. Replace keyword filters with similarity_search()")
print("  3. Test semantic queries in AI agent chat")
print()
print("💡 Usage Example:")
print(f"""
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index("{endpoint_name}", "{index_name}")

results = index.similarity_search(
    query_text="news about Tesla earnings",
    columns=["symbol", "title", "source", "url"],
    num_results=10
)
""")
print("=" * 60)

# COMMAND ----------


