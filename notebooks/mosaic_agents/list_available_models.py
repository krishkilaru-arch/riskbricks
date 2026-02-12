# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 List Available Foundation Models
# MAGIC 
# MAGIC **Find which Foundation Models are available in your workspace**

# COMMAND ----------

import requests
import json

# Get authentication
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
api_url = ctx.apiUrl().get()

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

print(f"✅ Connected to: {api_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 List All Registered Models in system.ai

# COMMAND ----------

# Try to list models in system.ai catalog
print("🔍 Searching for Foundation Models...")
print("="*60)

# Method 1: Try listing registered models
try:
    list_url = f"{api_url}/api/2.1/unity-catalog/models"
    params = {"catalog_name": "system", "schema_name": "ai"}
    response = requests.get(list_url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        models = data.get("registered_models", [])
        
        print(f"\n✅ Found {len(models)} models in system.ai:")
        print("="*60)
        
        for model in models:
            name = model.get("name", "Unknown")
            full_name = model.get("full_name", "Unknown")
            print(f"\n📦 {name}")
            print(f"   Full name: {full_name}")
            
            # Check if it's a Llama model
            if "llama" in name.lower() or "llama" in full_name.lower():
                print(f"   🎯 LLAMA MODEL - Use this for agents!")
                
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ Error listing models: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Alternative: List Foundation Model Endpoints

# COMMAND ----------

print("\n🔍 Listing Foundation Model Endpoints...")
print("="*60)

try:
    # List existing serving endpoints to see what Foundation Models are already serving
    list_url = f"{api_url}/api/2.0/serving-endpoints"
    response = requests.get(list_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        endpoints = data.get("endpoints", [])
        
        foundation_models = []
        
        for ep in endpoints:
            # Check if it's a Foundation Model endpoint
            config = ep.get("config", {})
            served_entities = config.get("served_entities", [])
            
            for entity in served_entities:
                entity_name = entity.get("entity_name", "")
                if entity_name and ("system.ai" in entity_name or "databricks" in entity_name.lower()):
                    foundation_models.append({
                        "endpoint_name": ep.get("name"),
                        "entity_name": entity_name
                    })
        
        if foundation_models:
            print(f"\n✅ Found {len(foundation_models)} Foundation Model endpoints:")
            print("="*60)
            
            for fm in foundation_models:
                print(f"\n📦 Endpoint: {fm['endpoint_name']}")
                print(f"   Entity: {fm['entity_name']}")
                print(f"   🎯 You can use this entity name!")
        else:
            print("\n⚠️  No Foundation Model endpoints found")
            print("ℹ️  Foundation Models might not be enabled in your workspace")
            
    else:
        print(f"❌ Error: {response.status_code}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Common Foundation Model Names
# MAGIC 
# MAGIC **Try these entity names (one should work):**
# MAGIC 
# MAGIC ### Llama 3.3 70B
# MAGIC - `system.ai.meta_llama_3_3_70b_instruct`
# MAGIC - `system.ai.databricks_meta_llama_3_3_70b_instruct`
# MAGIC - `databricks-meta-llama-3-3-70b-instruct`
# MAGIC 
# MAGIC ### Llama 3.1 70B
# MAGIC - `system.ai.meta_llama_3_1_70b_instruct`
# MAGIC - `databricks-meta-llama-3-1-70b-instruct`
# MAGIC 
# MAGIC ### Llama 3.1 405B
# MAGIC - `system.ai.meta_llama_3_1_405b_instruct`
# MAGIC - `databricks-meta-llama-3-1-405b-instruct`
# MAGIC 
# MAGIC ### DBRX
# MAGIC - `system.ai.dbrx_instruct`
# MAGIC - `databricks-dbrx-instruct`
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Check Databricks docs or UI:**
# MAGIC - Go to **ML** → **Serving** → **Create Endpoint**
# MAGIC - Select "Foundation Model" 
# MAGIC - See which models are available in the dropdown

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test a Model Name

# COMMAND ----------

# Test if a specific model exists
test_model_names = [
    "system.ai.meta_llama_3_3_70b_instruct",
    "system.ai.databricks_meta_llama_3_3_70b_instruct",
    "system.ai.meta_llama_3_1_70b_instruct",
    "system.ai.dbrx_instruct",
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-meta-llama-3-1-70b-instruct"
]

print("🧪 Testing model names...")
print("="*60)

for model_name in test_model_names:
    test_endpoint_name = "test_model_check_temp"
    
    payload = {
        "name": test_endpoint_name,
        "config": {
            "served_entities": [
                {
                    "entity_name": model_name,
                    "scale_to_zero_enabled": True,
                    "workload_size": "Small"
                }
            ]
        }
    }
    
    try:
        create_url = f"{api_url}/api/2.0/serving-endpoints"
        response = requests.post(create_url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"✅ FOUND: {model_name}")
            
            # Delete the test endpoint immediately
            delete_url = f"{api_url}/api/2.0/serving-endpoints/{test_endpoint_name}"
            requests.delete(delete_url, headers=headers)
            
            print(f"   🎯 USE THIS MODEL NAME: {model_name}")
            break
        else:
            error_data = response.json()
            error_code = error_data.get("error_code", "")
            
            if "RESOURCE_DOES_NOT_EXIST" in error_code:
                print(f"❌ Not found: {model_name}")
            elif "RESOURCE_ALREADY_EXISTS" in error_code:
                print(f"⚠️  Test endpoint already exists, cleaning up...")
                delete_url = f"{api_url}/api/2.0/serving-endpoints/{test_endpoint_name}"
                requests.delete(delete_url, headers=headers)
            else:
                print(f"⚠️  {model_name}: {error_code}")
                
    except Exception as e:
        print(f"⚠️  Error testing {model_name}: {e}")

# COMMAND ----------

print("\n" + "="*60)
print("✅ Model discovery complete!")
print("="*60)
print("\nℹ️  If no model was found, check:")
print("   1. Foundation Models are enabled in your workspace")
print("   2. You have permission to use Foundation Models")
print("   3. Try creating an endpoint manually via UI to see available models")

# COMMAND ----------

dbutils.notebook.exit("success")
