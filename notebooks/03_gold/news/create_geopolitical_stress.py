# Databricks notebook source
# MAGIC %md
# MAGIC # 🌍 Geopolitical Risk & Dynamic Stress Tests
# MAGIC
# MAGIC **Purpose**: Identify geopolitical risks from news and create dynamic stress test scenarios
# MAGIC
# MAGIC **Input**: `riskbricks.silver.news_sentiment`
# MAGIC **Output**: `riskbricks.gold.geopolitical_risk_events`
# MAGIC
# MAGIC **What This Does:**
# MAGIC - Analyzes news for geopolitical events (Trump/Greenland, China/Taiwan, etc.)
# MAGIC - Creates stress test scenarios based on current events
# MAGIC - Updates portfolio risk calculations with new scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import requests

# Database setup
catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

print(f"✅ Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Get Databricks Token for LLM API

# COMMAND ----------

try:
    db_token = dbutils.secrets.get(scope="riskbricks", key="databricks-token")
except:
    db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
print(f"✅ Workspace: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Extract Geopolitical Events with LLM

# COMMAND ----------

def extract_geopolitical_events_from_news(news_articles):
    """
    Use LLM to extract geopolitical risk events from news headlines
    """
    
    # Prepare news summary
    news_summary = "\n".join([
        f"- {article['title']} (Risk: {article['risk_level']}, Sentiment: {article['sentiment_score']:.2f})"
        for article in news_articles[:50]  # Limit to 50 articles
    ])
    
    prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are a geopolitical risk analyst for portfolio management. Identify significant geopolitical events that could impact financial markets.

<|eot_id|><|start_header_id|>user<|end_header_id|>

Analyze these recent financial news headlines and identify MAJOR geopolitical risk events:

HEADLINES:
{news_summary}

For each SIGNIFICANT geopolitical event, provide analysis in this EXACT JSON format:

{{
  "events": [
    {{
      "event_name": "<short name like 'US-Greenland Tensions'>",
      "event_category": "<one of: trade_policy, military_conflict, diplomatic_crisis, energy_crisis, regulatory_change, leadership_change>",
      "severity": <1-10, where 10 is most severe>,
      "description": "<one sentence description>",
      "affected_sectors": ["<sector names>"],
      "estimated_market_impact_pct": <number, e.g. -5.0 for 5% decline>,
      "duration_estimate": "<short_term|medium_term|long_term>",
      "confidence": <0.0-1.0>
    }}
  ]
}}

Only include events with severity >= 5. Return ONLY valid JSON.

<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
    
    # Call LLM
    api_url = f"https://{workspace_url}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations"
    
    headers = {
        "Authorization": f"Bearer {db_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {"role": "system", "content": "You are a geopolitical analyst. Always respond with valid JSON only."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.4,
        "max_tokens": 1000
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            content = result.get('choices', [{}])[0].get('message', {}).get('content', '{}')
            
            # Parse JSON
            try:
                if '{' in content and '}' in content:
                    json_start = content.index('{')
                    json_end = content.rindex('}') + 1
                    json_str = content[json_start:json_end]
                    events_data = json.loads(json_str)
                    return events_data.get('events', [])
                else:
                    return []
            except json.JSONDecodeError as e:
                print(f"⚠️  JSON parse error: {str(e)}")
                return []
        else:
            print(f"❌ API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Get Recent High-Risk News

# COMMAND ----------

# Get recent news with high risk or strong sentiment
recent_news = spark.sql("""
    SELECT 
        title,
        sentiment,
        sentiment_score,
        risk_level,
        affected_sectors,
        portfolio_impact,
        key_topics,
        published_at
    FROM riskbricks.silver.news_sentiment
    WHERE published_at >= CURRENT_DATE() - INTERVAL 7 DAYS
        AND (risk_level IN ('high', 'medium') OR ABS(sentiment_score) > 0.3)
    ORDER BY published_at DESC, ABS(sentiment_score) DESC
    LIMIT 100
""").collect()

news_list = [row.asDict() for row in recent_news]
print(f"📊 Analyzing {len(news_list)} high-risk/high-sentiment news articles...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌍 Extract Geopolitical Events

# COMMAND ----------

if len(news_list) == 0:
    print("⚠️  No high-risk news found. Using predefined scenarios.")
    geopolitical_events = [
        {
            "event_name": "Baseline Market Volatility",
            "event_category": "market_conditions",
            "severity": 5,
            "description": "Normal market volatility with no major geopolitical events",
            "affected_sectors": ["All"],
            "estimated_market_impact_pct": -3.0,
            "duration_estimate": "short_term",
            "confidence": 0.9
        }
    ]
else:
    print("🤖 Using LLM to extract geopolitical events...")
    geopolitical_events = extract_geopolitical_events_from_news(news_list)
    
    if len(geopolitical_events) == 0:
        print("⚠️  LLM didn't identify any major events. Using predefined scenarios.")
        geopolitical_events = [
            {
                "event_name": "Market Uncertainty",
                "event_category": "market_conditions",
                "severity": 6,
                "description": "Elevated market uncertainty from multiple news factors",
                "affected_sectors": ["Technology", "Financials"],
                "estimated_market_impact_pct": -4.0,
                "duration_estimate": "short_term",
                "confidence": 0.7
            }
        ]

print(f"✅ Identified {len(geopolitical_events)} geopolitical risk events")

# COMMAND ----------

# Display identified events
for event in geopolitical_events:
    print(f"""
🌍 {event['event_name']}
   Category: {event['event_category']}
   Severity: {event['severity']}/10
   Impact: {event['estimated_market_impact_pct']}%
   Sectors: {', '.join(event['affected_sectors'])}
   {event['description']}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Geopolitical Events to Gold Layer

# COMMAND ----------

if len(geopolitical_events) > 0:
    # Add metadata
    for event in geopolitical_events:
        event['event_id'] = f"{event['event_name'].replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}"
        event['event_date'] = datetime.now()
        event['is_active'] = True
        event['created_at'] = datetime.now()
    
    # Create DataFrame
    events_df = spark.createDataFrame(geopolitical_events)
    
    # Create table
    table_name = f"{catalog}.gold.geopolitical_risk_events"
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            event_name STRING,
            event_category STRING,
            severity INT,
            description STRING,
            affected_sectors ARRAY<STRING>,
            estimated_market_impact_pct DOUBLE,
            duration_estimate STRING,
            confidence DOUBLE,
            event_date TIMESTAMP,
            is_active BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Geopolitical risk events identified from news analysis'
    """)
    
    # Merge (upsert) events
    events_df.createOrReplaceTempView("geo_events_temp")
    
    spark.sql(f"""
        MERGE INTO {table_name} as target
        USING geo_events_temp as source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    total_events = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
    print(f"✅ Saved {len(geopolitical_events)} events to {table_name}")
    print(f"   Total events in table: {total_events}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Create Dynamic Stress Test Scenarios

# COMMAND ----------

# Generate stress test scenarios from geopolitical events
stress_scenarios = []

for event in geopolitical_events:
    scenario = {
        'scenario_id': f"stress_{event['event_id']}",
        'scenario_name': f"Stress Test: {event['event_name']}",
        'scenario_description': event['description'],
        'event_id': event['event_id'],
        'market_shock_pct': event['estimated_market_impact_pct'],
        'affected_sectors': event['affected_sectors'],
        'severity': event['severity'],
        'created_at': datetime.now()
    }
    stress_scenarios.append(scenario)

print(f"📊 Created {len(stress_scenarios)} stress test scenarios from geopolitical events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Stress Test Scenarios

# COMMAND ----------

if len(stress_scenarios) > 0:
    scenarios_df = spark.createDataFrame(stress_scenarios)
    
    table_name = f"{catalog}.gold.dynamic_stress_scenarios"
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            scenario_id STRING,
            scenario_name STRING,
            scenario_description STRING,
            event_id STRING,
            market_shock_pct DOUBLE,
            affected_sectors ARRAY<STRING>,
            severity INT,
            created_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Dynamic stress test scenarios based on current geopolitical events'
    """)
    
    scenarios_df.write.mode("append").saveAsTable(table_name)
    
    print(f"✅ Saved stress test scenarios to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Geopolitical Risk Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current active geopolitical events
# MAGIC SELECT 
# MAGIC   event_name,
# MAGIC   event_category,
# MAGIC   severity,
# MAGIC   estimated_market_impact_pct as impact_pct,
# MAGIC   affected_sectors,
# MAGIC   description,
# MAGIC   event_date
# MAGIC FROM riskbricks.gold.geopolitical_risk_events
# MAGIC WHERE is_active = true
# MAGIC ORDER BY severity DESC, event_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event category distribution
# MAGIC SELECT 
# MAGIC   event_category,
# MAGIC   COUNT(*) as event_count,
# MAGIC   AVG(severity) as avg_severity,
# MAGIC   AVG(estimated_market_impact_pct) as avg_impact
# MAGIC FROM riskbricks.gold.geopolitical_risk_events
# MAGIC WHERE is_active = true
# MAGIC GROUP BY event_category
# MAGIC ORDER BY avg_severity DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most affected sectors by geopolitical risks
# MAGIC SELECT 
# MAGIC   sector,
# MAGIC   COUNT(DISTINCT event_id) as num_events,
# MAGIC   AVG(severity) as avg_severity,
# MAGIC   SUM(estimated_market_impact_pct) as cumulative_impact_pct
# MAGIC FROM riskbricks.gold.geopolitical_risk_events
# MAGIC LATERAL VIEW explode(affected_sectors) t as sector
# MAGIC WHERE is_active = true
# MAGIC GROUP BY sector
# MAGIC ORDER BY num_events DESC, cumulative_impact_pct ASC
# MAGIC LIMIT 10;

# COMMAND ----------

print("""
================================================================================
✅ GEOPOLITICAL RISK ANALYSIS COMPLETE!
================================================================================

🌍 Geopolitical Events Identified: {events}
📊 Stress Test Scenarios Created: {scenarios}

💾 Tables Created:
   - riskbricks.gold.geopolitical_risk_events
   - riskbricks.gold.dynamic_stress_scenarios

📋 Next Steps:
   1. Review geopolitical events above
   2. Create UC functions for querying risks
   3. Integrate with AI agent for contextual responses
   4. Add to Risk Dashboard for visualization
   
🎯 Use Cases:
   - "What are the current geopolitical risks?"
   - "How would the Greenland situation impact portfolios?"
   - "Run stress test for [event name]"
   - "Which sectors are most exposed to geopolitical risk?"

================================================================================
""".format(events=len(geopolitical_events), scenarios=len(stress_scenarios)))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'events_identified': len(geopolitical_events),
    'scenarios_created': len(stress_scenarios)
}))
