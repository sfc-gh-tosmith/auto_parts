# Auto Parts: Customer Support Intelligence with Snowflake Cortex AI

End-to-end demo that turns raw support tickets and customer surveys into AI-enriched, queryable intelligence — using only SQL and Snowflake-native services.

## What's Inside

| File | Description |
|------|-------------|
| `setup.sql` | Creates the full pipeline: Dynamic Tables, Search Services, Semantic Views, Cortex Agent, and Streamlit deployment |
| `streamlit_app.py` | Interactive dashboard with ticket/survey analytics and a live AI Playground |

## Architecture

```
Raw Tables ──► Dynamic Tables (AI enrichment) ──► Search Services (semantic search)
                                                ──► Semantic Views (text-to-SQL)
                                                          │
                                                    Cortex Agent
                                                (conversational Q&A)
```

**Cortex AI functions used in Dynamic Tables:**
- `CLASSIFY_TEXT` — categorize tickets/surveys and assess urgency
- `SENTIMENT` — score customer sentiment (-1 to +1)
- `SUMMARIZE` — generate concise summaries
- `TRANSLATE` — auto-detect language and translate to English

**Dynamic Tables** refresh every 1 minute, so new data is automatically enriched with no orchestration needed.

**Cortex Search Services** enable semantic search — find tickets about "brake problems" even if the text says "stopping distance issues."

**Semantic Views** define business metrics (avg sentiment, ticket counts, NPS proxy) so Cortex Analyst can translate natural language into accurate SQL.

**Cortex Agent** combines all of the above into a single conversational interface. Ask things like:
- *"What are customers most upset about?"*
- *"Which product category has the lowest sentiment?"*
- *"Find tickets about warranty issues for BMW"*

## Getting Started

1. Load your support ticket and survey data into base tables
2. Run `setup.sql` (adjust warehouse/database names as needed)
3. Upload `streamlit_app.py` to the stage and open the Streamlit dashboard

## Documentation

- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Cortex AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-functions)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic)
- [Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agent)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
