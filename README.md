<div align="center">
  <h1>🚀 HPE Agentic Bug Triage & Routing System</h1>
  <p><b>An autonomous, multi-agent AI pipeline designed to intelligently enrich, synthesize, and route software bugs across diverse enterprise tracking systems.</b></p>
</div>

---

## 📖 Overview

In modern enterprise software environments, bug reports are often fragmented across multiple tracking systems such as Jira, GitHub, Bugzilla, and customer support portals. Engineers must manually navigate these disconnected platforms to gather context, identify related issues, and determine appropriate actions.

The **Agentic Bug Triage & Routing System** is a unified intelligence layer that aggregates bugs from multiple sources and executes a multi-agent AI pipeline to automate investigation. The system retrieves complete bug context, identifies related issues across connected systems, enriches results with relevant knowledge-base content, and generates structured AI-assisted triage recommendations in real time.

---

## ✨ Key Features

* **🌐 Unified Bug Dashboard**

  * Aggregates a near real-time, read-only view of issues across Jira, GitHub, Bugzilla, and support portals.

* **🤖 Four-Agent AI Pipeline**

  * ContextFetchAgent
  * CrossSystemFetchAgent
  * EnrichmentAgent
  * AISynthesisAgent

* **⚡ Progressive Results Streaming**

  * Results are streamed panel-by-panel through WebSockets as each agent completes, eliminating the need to wait for the entire pipeline to finish.

* **🎯 Automated Triage & Routing**

  * Generates structured severity classifications (P0–P3), root-cause hypotheses, confidence scores, affected components, and recommended actions.

* **📊 Cross-System Correlation**

  * Identifies duplicate and related issues across disconnected bug-tracking systems using semantic search and similarity scoring.

* **📚 Knowledge Base Enrichment**

  * Searches connected knowledge sources and documentation repositories to provide additional investigation context.

* **🔌 Dynamic Connector Registry**

  * New source systems can be connected through configuration without modifying pipeline logic.

* **🛡️ Reliable Event-Driven Architecture**

  * Uses Kafka for asynchronous event processing, Redis for caching and WebSocket bridging, and PostgreSQL for workflow persistence and recovery.


---

## 🛠️ Tech Stack

### Backend & Infrastructure

* **Framework:** FastAPI (Python)
* **Database:** PostgreSQL + SQLAlchemy 2.0
* **Event Processing:** Apache Kafka
* **Caching & Pub/Sub:** Redis
* **Containerization:** Docker & Docker Compose

### AI & Agents

* **LLM Engine:** Groq (Llama 3.3 70B)
* **Validation:** Pydantic for strict schema validation and structured outputs
* **Architecture:** Multi-agent orchestration pipeline

### Frontend

* **Framework:** React + Vite
* **Communication:** REST APIs + WebSockets
* **Styling:** Modern responsive UI

---

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have the following installed:
- Python 3.10+
- Node.js 18+
- Docker & Docker Compose

### 2. Environment Setup
Create a `.env` file in the root directory and configure the following variables:

```env
JWT_SECRET=your_jwt_secret_here
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=120
POSTGRES_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/hpe_bugtriage
REDIS_URL=redis://localhost:6379/0
REDIS_TTL_TICKET_SECONDS=300
REDIS_TTL_BUGLIST_SECONDS=120
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TRIAGE_REQUESTS=triage.requests
KAFKA_CONSUMER_GROUP=bugtriage-orchestrator
ENABLE_LOCAL_PIPELINE_FALLBACK=true
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=llama-3.3-70b-versatile
GROQ_TEMPERATURE=0.0
LOG_FORMAT=console

# Connector tokens — env var names must match auth_secret_ref in source_registry
APACHE_SPARK_GITHUB_TOKEN=your_github_token_here
APACHE_KAFKA_GITHUB_TOKEN=your_github_token_here
APACHE_SPARK_JIRA_TOKEN=
APACHE_KAFKA_JIRA_TOKEN=
MOZILLA_FIREFOX_BUGZILLA_TOKEN=

# KNOWLEDGE BASE
CONFLUENCE_BASE_URL=https://your-domain.atlassian.net/wiki
CONFLUENCE_EMAIL=your_confluence_email_here
CONFLUENCE_API_TOKEN=your_confluence_token_here
CONFLUENCE_SPACE_KEY=HPEKB
```

### 3. Spin up Infrastructure
Start the required background services (Postgres, Redis, Kafka) using Docker:
```bash
docker-compose -f docker-compose.dev.yml up -d
```

### 4. Install Dependencies & Initialize Database
Set up your Python virtual environment and run the database migrations and seeders:
```bash
python -m venv venv
source venv/Scripts/activate  # (or venv/bin/activate on Mac/Linux)

pip install -r requirements.txt

# Initialize tables
python -m scripts.init_db

# Seed test data and knowledge base
python scripts/seed_kb_articles.py
```

### 5. Run the Application
**Start the Backend (FastAPI):**
```bash
uvicorn api_gateway.main:app --reload --host 0.0.0.0 --port 8000
```

**Start the Frontend (React):**
```bash
cd frontend
npm install
npm run dev
```

### 6. Access the Application
- **Frontend Dashboard:** Navigate to `http://localhost:5173` to view the live dashboard.
- **Backend API Docs:** Navigate to `http://localhost:8000/docs` to explore the interactive FastAPI Swagger documentation.

---

## 👥 Team Contributions

### Phase 1 – Collaborative Design

The project architecture was designed collaboratively by the entire team.

All team members contributed to:

* High-Level Design (HLD)
* Software Design Document (SDD)
* Agent workflow design
* Database design
* Connector architecture
* Sequence diagrams
* Technology evaluation
* Design reviews and brainstorming sessions

### Phase 2 – Primary Implementation & Review Areas
   - **Core Backend & API Gateway:** [Pulkit Jain](https://github.com/pulkitjn3010)
   - **Infrastructure & Database:** [Shivansh Gaur](https://github.com/sh1vanshgaur)
   - **AI Orchestration & Synthesis:** [Disha Jain](https://github.com/DishaJn2)
   - **AI Enrichment & Correlation:** [Anuj Modani](https://github.com/animus08)
   - **Data Integrations & Connectors:** [Om Jain](https://github.com/Omjain27112005)