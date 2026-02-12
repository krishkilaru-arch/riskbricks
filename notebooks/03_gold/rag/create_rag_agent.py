# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Comprehensive RAG Agent
# MAGIC 
# MAGIC **Purpose**: Answer questions using multiple data sources via RAG
# MAGIC 
# MAGIC **Data Sources Available**:
# MAGIC - 📰 News (Yahoo Finance, Google News)
# MAGIC - 📄 SEC Filings (10-K, 10-Q, 8-K)
# MAGIC - 📚 Wikipedia (Company background)
# MAGIC - 📊 Stock Data (Price, volume context)
# MAGIC 
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC User Query → Vector Search → Retrieve Documents → LLM Synthesis → Answer
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch databricks-sdk --quiet

# COMMAND ----------

# Restart Python after pip install
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
import json

# Configuration
catalog = "riskbricks"
vs_endpoint_name = "riskbricks_vs_endpoint"
vs_index_name = f"{catalog}.gold.rag_vector_index"  # Vector index in Gold layer
llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

print(f"🔍 Vector Search (Gold): {vs_index_name}")
print(f"🧠 LLM: {llm_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 RAG Agent Class

# COMMAND ----------

class RiskBricksRAGAgent:
    """
    Comprehensive RAG Agent for financial intelligence
    
    Supports queries about:
    - Stock news and events
    - SEC filings (10-K, 10-Q, 8-K)
    - Company background (Wikipedia)
    - Stock price context
    
    Usage:
        agent = RiskBricksRAGAgent()
        answer = agent.query("What's in Costco's 10-K annual report?")
    """
    
    def __init__(self, vs_endpoint=vs_endpoint_name, vs_index=vs_index_name, llm=llm_endpoint):
        self.vs_endpoint = vs_endpoint
        self.vs_index = vs_index
        self.llm_endpoint = llm
        self.vsc = VectorSearchClient()
        self.workspace_client = WorkspaceClient()
        
        # Document type descriptions for better context
        self.doc_type_labels = {
            'news': '📰 News Article',
            'sec_10k': '📄 SEC 10-K Annual Report',
            'sec_10q': '📄 SEC 10-Q Quarterly Report',
            'sec_8k': '📄 SEC 8-K Material Event',
            'wiki_company': '📚 Wikipedia Company Info',
            'stock_context': '📊 Stock Price Data',
            'earnings_event': '📈 Earnings/Trading Event'
        }
        
        # System prompt
        self.system_prompt = """You are a financial research assistant with access to multiple data sources:
- News articles from Yahoo Finance and Google News
- SEC filings (10-K annual reports, 10-Q quarterly reports, 8-K material events)
- Wikipedia company information
- Stock price and trading data

INSTRUCTIONS:
1. Answer questions based ONLY on the provided context documents
2. Cite your sources clearly, mentioning the document type and date
3. If asked about SEC filings, explain what type of filing it is (10-K = annual, 10-Q = quarterly, 8-K = material events)
4. If the context doesn't contain relevant information, say so honestly
5. Be concise but informative
6. For stock price questions, reference the stock context data

FORMAT:
- Start with a direct answer
- Provide supporting details from the documents
- End with source citations in [brackets]"""

    def detect_doc_type_filter(self, query):
        """Detect if user wants a specific document type"""
        query_lower = query.lower()
        
        if any(x in query_lower for x in ['10-k', '10k', 'annual report', 'yearly report']):
            return 'sec_10k'
        elif any(x in query_lower for x in ['10-q', '10q', 'quarterly report', 'quarter report']):
            return 'sec_10q'
        elif any(x in query_lower for x in ['8-k', '8k', 'material event', 'filing event']):
            return 'sec_8k'
        elif any(x in query_lower for x in ['wikipedia', 'history', 'background', 'founded', 'company info']):
            return 'wiki_company'
        elif any(x in query_lower for x in ['stock price', 'current price', 'trading', 'volume']):
            return 'stock_context'
        elif any(x in query_lower for x in ['news', 'headline', 'latest', 'recent', 'today']):
            return 'news'
        
        return None  # No filter, search all types
    
    def retrieve_context(self, query, num_results=7, doc_type_filter=None):
        """Retrieve relevant documents using vector search"""
        try:
            index = self.vsc.get_index(self.vs_endpoint, self.vs_index)
            
            # Build filters
            filters = None
            if doc_type_filter:
                filters = {"doc_type": doc_type_filter}
            
            results = index.similarity_search(
                query_text=query,
                columns=["doc_id", "symbol", "company_name", "doc_type", "title", "content", "source", "published_date", "url"],
                num_results=num_results,
                filters=filters
            )
            
            documents = []
            for row in results.get('result', {}).get('data_array', []):
                documents.append({
                    'doc_id': row[0],
                    'symbol': row[1],
                    'company_name': row[2],
                    'doc_type': row[3],
                    'title': row[4],
                    'content': row[5],
                    'source': row[6],
                    'date': row[7],
                    'url': row[8]
                })
            
            return documents
            
        except Exception as e:
            print(f"⚠️ Retrieval error: {str(e)}")
            return []
    
    def format_context(self, documents):
        """Format retrieved documents for LLM context"""
        if not documents:
            return "No relevant documents found in the database."
        
        context_parts = ["Here are the relevant documents:\n"]
        
        for i, doc in enumerate(documents, 1):
            doc_label = self.doc_type_labels.get(doc['doc_type'], doc['doc_type'])
            
            context_parts.append(f"""
---
Document {i}: {doc_label}
Company: {doc['company_name']} ({doc['symbol']})
Title: {doc['title']}
Source: {doc['source']}
Date: {doc['date']}

Content:
{doc['content'][:1500]}
---""")
        
        return "\n".join(context_parts)
    
    def generate_answer(self, query, context, documents):
        """Generate answer using LLM"""
        try:
            prompt = f"""Based on the following documents, answer the user's question.

{context}

USER QUESTION: {query}

Provide a clear, well-structured answer based on the documents above. Cite sources."""

            response = self.workspace_client.serving_endpoints.query(
                name=self.llm_endpoint,
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=self.system_prompt),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt)
                ],
                max_tokens=600,
                temperature=0.3
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"⚠️ LLM error: {str(e)}")
            return self._fallback_response(documents)
    
    def _fallback_response(self, documents):
        """Generate fallback response if LLM fails"""
        if not documents:
            return "I couldn't find relevant documents for your query."
        
        response = "Here are the relevant documents I found:\n\n"
        for doc in documents[:5]:
            doc_label = self.doc_type_labels.get(doc['doc_type'], doc['doc_type'])
            response += f"**{doc_label}**: {doc['title']}\n"
            response += f"- Company: {doc['company_name']} ({doc['symbol']})\n"
            response += f"- Source: {doc['source']}, {doc['date']}\n"
            response += f"- Preview: {doc['content'][:200]}...\n\n"
        
        return response
    
    def query(self, question, num_results=7):
        """
        Main method to answer questions
        
        Args:
            question: User's question
            num_results: Number of documents to retrieve
        
        Returns:
            Dict with answer, sources, and metadata
        """
        print(f"🔍 Processing: {question}")
        
        # Step 1: Detect if user wants specific doc type
        doc_type_filter = self.detect_doc_type_filter(question)
        if doc_type_filter:
            print(f"  📁 Filtering by: {doc_type_filter}")
        
        # Step 2: Retrieve relevant documents
        print("  📰 Retrieving documents...")
        documents = self.retrieve_context(question, num_results, doc_type_filter)
        print(f"  ✅ Found {len(documents)} documents")
        
        # Step 3: Format context
        context = self.format_context(documents)
        
        # Step 4: Generate answer
        print("  🧠 Generating answer...")
        answer = self.generate_answer(question, context, documents)
        
        return {
            'question': question,
            'answer': answer,
            'sources': documents,
            'num_sources': len(documents),
            'doc_type_filter': doc_type_filter
        }
    
    def query_simple(self, question):
        """Simple query that returns just the answer string"""
        result = self.query(question)
        return result['answer']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test the RAG Agent

# COMMAND ----------

# Initialize agent
agent = RiskBricksRAGAgent()
print("✅ RAG Agent initialized")

# COMMAND ----------

# Test 1: General news query
print("=" * 80)
result = agent.query("What recent news is there about Costco?")
print("\n📝 ANSWER:")
print(result['answer'])
print("\n📚 SOURCES:")
for src in result['sources'][:3]:
    print(f"  • [{src['doc_type']}] {src['title'][:60]}...")

# COMMAND ----------

# Test 2: SEC 10-K query
print("=" * 80)
result = agent.query("What's in Apple's 10-K annual report? What are the main business risks?")
print("\n📝 ANSWER:")
print(result['answer'])

# COMMAND ----------

# Test 3: Wikipedia/history query
print("=" * 80)
result = agent.query("Tell me about Tesla's history and background")
print("\n📝 ANSWER:")
print(result['answer'])

# COMMAND ----------

# Test 4: Stock context query
print("=" * 80)
result = agent.query("What is Microsoft's current stock price and trading volume?")
print("\n📝 ANSWER:")
print(result['answer'])

# COMMAND ----------

# Test 5: SEC 8-K material events
print("=" * 80)
result = agent.query("Any recent material events or 8-K filings for Amazon?")
print("\n📝 ANSWER:")
print(result['answer'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Create UC Function for RAG Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop existing function if it exists (clean slate)
# MAGIC DROP FUNCTION IF EXISTS riskbricks.functions.query_rag;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create SQL-based RAG query function (compatible with UC runtime)
# MAGIC -- Uses direct LIKE matching on question text against company names
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.functions.query_rag(
# MAGIC     question STRING COMMENT 'Question about stocks, news, filings, or company info'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'RAG agent that answers questions using news, SEC filings, Wikipedia, and stock data'
# MAGIC RETURN (
# MAGIC   SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT(
# MAGIC       'You are a financial research assistant. Answer the question based ONLY on these documents. Be concise and cite sources. If no relevant documents, say so.\n\n',
# MAGIC       'DOCUMENTS:\n',
# MAGIC       COALESCE(
# MAGIC         (SELECT CONCAT_WS('\n\n', COLLECT_LIST(doc_text))
# MAGIC         FROM (
# MAGIC           SELECT CONCAT('[', doc_type, '] ', title, ' (', symbol, ', ', source, ', ', published_date, ')\n', LEFT(content, 600)) as doc_text
# MAGIC           FROM riskbricks.silver.rag_documents
# MAGIC           WHERE 
# MAGIC             -- Direct match: question contains the stock symbol
# MAGIC             LOWER(question) LIKE CONCAT('%', LOWER(symbol), '%')
# MAGIC             -- Direct match: question contains first word of company name
# MAGIC             OR LOWER(question) LIKE CONCAT('%', LOWER(SPLIT(company_name, ' ')[0]), '%')
# MAGIC             -- Direct match: question contains company name variations
# MAGIC             OR (LOWER(question) LIKE '%costco%' AND symbol = 'COST')
# MAGIC             OR (LOWER(question) LIKE '%apple%' AND symbol = 'AAPL')
# MAGIC             OR (LOWER(question) LIKE '%microsoft%' AND symbol = 'MSFT')
# MAGIC             OR (LOWER(question) LIKE '%amazon%' AND symbol = 'AMZN')
# MAGIC             OR (LOWER(question) LIKE '%google%' AND symbol = 'GOOGL')
# MAGIC             OR (LOWER(question) LIKE '%nvidia%' AND symbol = 'NVDA')
# MAGIC             OR (LOWER(question) LIKE '%netflix%' AND symbol = 'NFLX')
# MAGIC             OR (LOWER(question) LIKE '%meta%' AND symbol = 'META')
# MAGIC             OR (LOWER(question) LIKE '%tesla%' AND symbol = 'TSLA')
# MAGIC             OR (LOWER(question) LIKE '%jpmorgan%' AND symbol = 'JPM')
# MAGIC             OR (LOWER(question) LIKE '%goldman%' AND symbol = 'GS')
# MAGIC             OR (LOWER(question) LIKE '%disney%' AND symbol = 'DIS')
# MAGIC             OR (LOWER(question) LIKE '%walmart%' AND symbol = 'WMT')
# MAGIC             OR (LOWER(question) LIKE '%johnson%' AND symbol = 'JNJ')
# MAGIC             OR (LOWER(question) LIKE '%pfizer%' AND symbol = 'PFE')
# MAGIC             OR (LOWER(question) LIKE '%exxon%' AND symbol = 'XOM')
# MAGIC             OR (LOWER(question) LIKE '%chevron%' AND symbol = 'CVX')
# MAGIC             OR (LOWER(question) LIKE '%visa%' AND symbol = 'V')
# MAGIC             OR (LOWER(question) LIKE '%mastercard%' AND symbol = 'MA')
# MAGIC             OR (LOWER(question) LIKE '%unitedhealth%' AND symbol = 'UNH')
# MAGIC           ORDER BY published_date DESC
# MAGIC           LIMIT 5
# MAGIC         )),
# MAGIC         'No relevant documents found for this query.'
# MAGIC       ),
# MAGIC       '\n\nQUESTION: ', question,
# MAGIC       '\n\nANSWER (cite sources):'
# MAGIC     )
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test UC Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test general query
# MAGIC SELECT riskbricks.functions.query_rag('What recent news is there about Costco?') as answer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test SEC filing query
# MAGIC SELECT riskbricks.functions.query_rag("What's in Apple's 10-K annual report?") as answer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test company history query
# MAGIC SELECT riskbricks.functions.query_rag('Tell me about Amazon company history') as answer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ RAG Agent Complete!

# COMMAND ----------

print("""
================================================================================
✅ COMPREHENSIVE RAG AGENT COMPLETE!
================================================================================

🤖 Components Created:
   1. RiskBricksRAGAgent Python class - Full RAG implementation
   2. UC Function: riskbricks.functions.query_rag

📊 Data Sources Available:
   ┌─────────────────┬────────────────────────────────────────┐
   │ Document Type   │ What It Answers                        │
   ├─────────────────┼────────────────────────────────────────┤
   │ news            │ "What happened to Costco today?"       │
   │ sec_10k         │ "What's in Apple's annual report?"     │
   │ sec_10q         │ "Microsoft's quarterly financials?"    │
   │ sec_8k          │ "Any material events at Amazon?"       │
   │ wiki_company    │ "Tell me about Tesla's history"        │
   │ stock_context   │ "What's Nvidia's current price?"       │
   │ earnings_event  │ "Any significant trading activity?"    │
   └─────────────────┴────────────────────────────────────────┘

🎯 Example Queries:
   - "What recent news is there about Costco?"
   - "What's in Apple's 10-K annual report?"
   - "Tell me about Tesla's history and background"
   - "Any SEC 8-K filings for Microsoft?"
   - "What's Amazon's current stock price?"

💻 Usage:

   Python:
       agent = RiskBricksRAGAgent()
       result = agent.query("What happened to Apple stock?")
       print(result['answer'])
   
   SQL:
       SELECT riskbricks.functions.query_rag('Apple 10-K report?')

🔗 Integration:
   - Streamlit AI Chat page updated to use this
   - Multi-Agent Supervisor can call query_rag function
   - Direct SQL queries from any notebook

================================================================================
""")

dbutils.notebook.exit("success")
