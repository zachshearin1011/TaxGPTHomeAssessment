# TaxGPT — AI-Powered Tax & Financial Chatbot

A Django-based chatbot that answers financial and tax questions using hybrid retrieval (vector search + knowledge graph + structured data queries), backed by PostgreSQL, Django REST Framework, and GraphQL.

## Architecture

Attached as TaxGPT.png

## Design Rationale

### Why Django + DRF + GraphQL?

- **Django** provides a mature, batteries-included framework with ORM, migrations, admin interface, authentication, and security best practices — aligned with production requirements for scalable backend systems.
- **Django REST Framework** delivers standardized RESTful APIs with serialization, pagination, and viewsets for clean, well-documented endpoints.
- **GraphQL (Graphene-Django)** offers a flexible query interface alongside REST, enabling clients to request exactly the data they need — particularly useful for conversation history traversal.
- **PostgreSQL** serves as the primary relational database for conversation history, message tracking, and ingested file metadata. SQLite is supported as a development fallback.

### Hybrid Retrieval Architecture

The system combines three retrieval strategies:

1. **Vector Search (ChromaDB + Sentence-Transformers)** — Semantic similarity search across all ingested documents using `all-MiniLM-L6-v2` embeddings. Handles open-ended questions about tax concepts, regulations, and financial topics.

2. **Knowledge Graph (NetworkX)** — Entity-relationship graph capturing connections between taxpayer types, states, income sources, deductions, and tax concepts. Enables multi-hop reasoning about entity relationships.

3. **Structured Query Engine (Pandas)** — Direct DataFrame operations for aggregation queries (totals, averages, counts, comparisons). Delivers precise numerical answers without LLM hallucination risk.

A query classifier routes incoming questions to the appropriate retrieval combination, and the HybridRetriever merges results into a unified context for the LLM.

### Separation of Concerns

- `core/` contains framework-agnostic ML/AI logic that can be tested independently
- `chat/` wraps the core logic in Django views, models, and serializers
- `config/` holds Django project-level configuration

## Setup

### Prerequisites

- Python 3.9+
- PostgreSQL (optional — SQLite works for development)
- OpenAI API key

### Installation

```bash
cd TaxGPTHomeAssessment

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

### Configuration

Copy the example environment file and set your API key:

```bash
cp .env.example .env
```

Edit `.env` and set:

```
OPENAI_API_KEY=sk-your-key-here
```

For PostgreSQL (production):

```
DB_ENGINE=postgresql
DB_NAME=taxgpt
DB_USER=postgres
DB_PASSWORD=your-password
DB_HOST=localhost
DB_PORT=5432
```

For SQLite (development):

```
DB_ENGINE=sqlite3
```

### Database Setup

```bash
python manage.py migrate
python manage.py createsuperuser  # optional
```

### Data Ingestion

Place your datasets in `data/raw/`, then run:

```bash
python manage.py ingest
```

Or use the API endpoint:

```bash
curl -X POST http://localhost:8000/api/ingest
```

### Run the Server

```bash
python manage.py runserver
```

The chat UI is available at http://localhost:8000

## API Reference

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Chat web UI |
| `POST` | `/api/chat` | Send a message |
| `POST` | `/api/ingest` | Trigger data ingestion |
| `GET` | `/api/stats` | System statistics |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/conversations/` | List conversations (paginated) |
| `GET` | `/api/conversations/{id}/` | Get conversation with messages |

### Chat Request

```json
{
  "message": "What is the average tax owed by individuals in CA?",
  "conversation_id": "optional-uuid",
  "reset": false
}
```

### Chat Response

```json
{
  "answer": "The average tax owed by individuals in CA is $26,400.00.",
  "sources": ["tax_data.csv"],
  "query_type": "structured",
  "conversation_id": "uuid",
  "metadata": {
    "vector_results": 8,
    "graph_entities": ["individual", "CA"]
  }
}
```

### GraphQL

Available at `/graphql/` with GraphiQL interface.

**Query conversations:**

```graphql
query {
  conversations(limit: 10) {
    id
    title
    createdAt
    messages {
      role
      content
      sources
    }
  }
}
```

**Send a message:**

```graphql
mutation {
  sendMessage(message: "What is the standard deduction for 2023?") {
    answer
    sources
    queryType
    conversationId
  }
}
```

### Admin Interface

Django admin is available at `/admin/` for managing conversations, messages, and ingested files.

## Testing

```bash
pytest tests/ -v
```

## Evaluation

Start the server, then run:

```bash
python scripts/evaluate.py
```

Results are saved to `eval/evaluation_results.json`.

## Technology Stack

| Component | Technology |
|-----------|------------|
| Backend Framework | Django 4.2+ |
| REST API | Django REST Framework |
| GraphQL | Graphene-Django |
| Database | PostgreSQL / SQLite |
| Vector Store | ChromaDB |
| Embeddings | Sentence-Transformers (all-MiniLM-L6-v2) |
| Knowledge Graph | NetworkX |
| LLM | OpenAI GPT-4o-mini |
| Data Processing | Pandas, PyMuPDF, python-pptx |
| Frontend | Vanilla HTML/CSS/JavaScript |