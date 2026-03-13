# 📊 Retail Insights Assistant  
### GenAI-Powered Conversational Analytics Platform

A production-style, multi-agent GenAI system that translates natural-language business questions into validated analytical insights over structured retail data.



---

## 🚀 What This Project Does

Retail teams work with large, complex sales datasets. Business users want fast answers without writing SQL.

This system enables users to:

- Ask business questions in plain English  
- Automatically convert them into safe analytical queries  
- Validate results with guardrails  
- Receive business-ready insights through a conversational interface  

The platform bridges **LLMs and analytical systems** in a controlled, auditable, and scalable way.

---


## 🧠 Core Capabilities

### Conversational Analytics

Ask questions like:

- Which category generated the highest revenue last quarter?  
- Compare domestic vs international revenue.  
- How did revenue trend over the last 3 months?  

The system interprets intent, generates SQL, executes analytics, validates outputs, and summarizes results.

---

### Multi-Agent Intelligence Layer

The system is implemented using a modular multi-agent architecture:

| Agent | Responsibility |
|------|----------------|
| Intent Agent | Extracts structured analytical intent |
| Scope Guard | Blocks out-of-domain / unsafe questions |
| Router Agent | Determines data sources, joins, execution mode |
| Data Agent | Generates safe SQL and runs DuckDB queries |
| Validation Agent | Performs data quality and sanity checks |
| Insight Agent | Produces business-level insights |
| Memory Layer | Manages follow-ups and conversational context |

This design enforces **separation of concerns**, **traceability**, and **debug-friendly workflows**.

---

## 🏗️ High-Level Architecture

```
User
  ↓
Intent Agent
  ↓
Scope Guard (Hallucination Control)
  ↓
Router Agent
  ↓
Data Agent (DuckDB / SQL)
  ↓
Validation Agent
  ↓
Insight Agent (LLM)
  ↓
Streamlit UI
```

Out-of-scope questions never reach the data or generation layers.

---

## 🛠️ Technology Stack

### GenAI & Orchestration
- OpenAI API  
- LangChain  
- LangGraph  

### Data & Analytics
- DuckDB  
- Parquet  
- Pandas  

### Application Layer
- Streamlit  
- Python  

---

## 🗄️ Data Layer Design

- Raw CSV ingestion  
- Standardization & normalization  
- Canonical fact and dimension tables  
- Columnar storage using Parquet  
- Analytical querying via DuckDB  

### Why DuckDB

- Optimized for analytical workloads  
- Native Parquet execution  
- SQL interface  
- Seamlessly scales from local to cloud environments  

---

## 📈 Scalability & Production Thinking

Although implemented on sample-scale data, the system is designed for large-scale deployments.

### Data Engineering
- Batch ingestion using Spark / Dask  
- Partitioned Parquet datasets  
- Incremental processing pipelines  

### Storage & Compute
- Object storage (S3 / ADLS / GCS)  
- Delta Lake formats  
- DuckDB, Snowflake, or BigQuery as analytical engines  

### Retrieval & Performance
- Partition pruning  
- Metadata-driven filtering  
- Optional vector retrieval for RAG-based summaries  

### Model Orchestration
- Deterministic prompt templates  
- Intent caching  
- LLM call minimization  

### Monitoring & Evaluation
- Validation severity levels  
- Query failure detection  
- Confidence scoring  
- Cost and latency observability (future)  

---

## 🛡️ Hallucination & Safety Controls

The system explicitly blocks:

- General knowledge queries  
- Political or unrelated questions  
- Non-analytical prompts  

Blocked queries:

- Do not generate SQL  
- Do not access data  
- Do not produce fabricated answers  

The UI guides users back to supported analytical use-cases.

---

## 💻 Streamlit Application

The Streamlit UI provides:

- Conversational querying  
- Validation feedback  
- Business-ready insight summaries  
- Optional result inspection  

Run locally:

```bash
streamlit run app/streamlit_app.py
```

---

## ✅ Example Supported Questions

- Which category generated the highest revenue last quarter?  
- How did total revenue change over the last 3 months?  
- Compare domestic and international revenue for the last quarter.

---

## 📂 Project Structure

```
retail_insights_assistant/
├── agents/              # Agent implementations
├── orchestration/       # LangGraph workflows
├── storage/             # DuckDB + parquet layer
├── llm/                 # LLM client and prompts
├── app/
│   └── streamlit_app.py
├── schema/              # Table and metric registry
├── data/                # Raw, staging, processed datasets
├── README.md
└── requirements.txt
```

---

## ⚠️ Assumptions & Current Limits

- Sample-scale datasets  
- Simplified temporal filters  
- Vector retrieval disabled  
- Inline configuration  

---

## 🔮 Planned Improvements

- Externalized config & prompt registry  
- Automated testing layer  
- Vector-based summarization  
- Cost and latency monitoring  
- API-first deployment  

---

## 🏁 Conclusion

This project demonstrates:

- Strong data engineering foundations  
- Correct multi-agent GenAI system design  
- Safe LLM integration  
- Analytics-first architecture  
- Production-grade thinking  

It satisfies the functional, technical, and architectural requirements of the Blend360 GenAI assignment.


## ⚙️ Setup & Execution Guide

This guide explains how to set up the environment, prepare the data layer, and run the Retail Insights Assistant locally.

---

### 🔹 Prerequisites

Ensure the following are installed:

- Python **3.10+**
- Git
- pip or conda
- OpenAI API key

Verify Python version:

python --version

### 🔹 Project Setup

1. Clone the repository

git clone <your-repo-url>
cd retail_insights_assistant

2. Create and activate virtual environment

python -m venv .venv
.venv\Scripts\activate

3. Install dependencies

pip install --upgrade pip
pip install -r requirements.txt

4. Get Open AI API Key

###  🔹 Data Pipeline Execution

python pipelines/run_pipeline.py

###  🔹 Start the Application

streamlit run app/streamlit_app.py

The UI will be available at:

http://localhost:8501

### 🔹 Docker Deployment

1. **Build the Docker Image**

```bash
docker build -t retail-insights-assistant .
```

2. **Run the Docker Container**

   Since the application requires an OpenAI API key, pass it as an environment variable (without quotes):

```bash
docker run -p 8501:8501 -e OPENAI_API_KEY=your-api-key-here retail-insights-assistant
```

   Access the application at `http://localhost:8501`.

###  🔹 Example Queries

- Which category generated the highest revenue last quarter?

- How did total revenue change over the last 3 months?

- Compare domestic and international revenue for the last quarter.
