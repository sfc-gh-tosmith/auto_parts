-- =============================================================================
-- Auto Parts: Customer Support & Survey Intelligence with Snowflake Cortex AI
-- =============================================================================
-- This script demonstrates how to build a complete AI-powered customer support
-- intelligence pipeline using Snowflake Cortex AI functions and Dynamic Tables.
--
-- What you'll build:
--   1. Dynamic Tables that automatically classify, score, and summarize tickets
--   2. Dynamic Tables that analyze survey responses with AI
--   3. Cortex Search Services for semantic search over tickets and surveys
--   4. Semantic Views for natural-language querying via Cortex Analyst
--   5. A Cortex Agent that ties everything together for conversational Q&A
--
-- Cortex AI functions used:
--   - CLASSIFY_TEXT  : Categorize tickets and surveys into predefined labels
--   - SENTIMENT      : Score customer sentiment from -1 (negative) to +1 (positive)
--   - SUMMARIZE      : Generate concise summaries of ticket text
--   - TRANSLATE      : Auto-detect language and translate to English
--
-- Prerequisites:
--   - A Snowflake account with Cortex AI functions enabled
--   - SYSADMIN role (or a role with CREATE TABLE, CREATE DYNAMIC TABLE, etc.)
--   - A warehouse (we use AI_DEMO_WH below — adjust to yours)
--   - The base tables loaded with support tickets and survey data
--     (see the synthetic data generation script separately, or bring your own)
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE AI_DEMOS;
USE SCHEMA PUBLIC;
USE WAREHOUSE AI_DEMO_WH;


-- =============================================================================
-- STEP 1: Dynamic Table — AI-Enriched Support Tickets
-- =============================================================================
-- This Dynamic Table reads raw support tickets and applies four Cortex AI
-- functions to each row. Because it's a Dynamic Table with TARGET_LAG of
-- 1 minute, any new rows inserted into the base table are automatically
-- enriched within ~60 seconds — no scheduled jobs or orchestration needed.
--
-- Columns added by AI:
--   CATEGORY        : Classified issue type (e.g., Warranty/Returns, Wrong Fitment)
--   URGENCY         : Classified priority level (Low, Medium, High, Critical)
--   SENTIMENT_SCORE : Numeric sentiment from -1 to +1
--   SUMMARY         : One-sentence AI summary of the ticket
--   TICKET_TEXT_EN  : English translation (for non-English tickets)
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE AUTO_PARTS_TICKETS_ENRICHED
  TARGET_LAG = '1 minute'
  WAREHOUSE = AI_DEMO_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
AS
SELECT
    TICKET_ID,
    CUSTOMER_NAME,
    CUSTOMER_EMAIL,
    TICKET_TEXT,
    VEHICLE_MAKE,
    VEHICLE_MODEL,
    VEHICLE_YEAR,
    ORDER_NUMBER,
    SUBMITTED_AT,
    CHANNEL,

    -- CLASSIFY_TEXT: Automatically categorize the ticket into one of 8 categories.
    -- The task_description parameter gives the model context about the domain.
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TICKET_TEXT,
        ['Warranty/Returns', 'Shipping Issue', 'Wrong Fitment', 'Product Quality',
         'Pricing', 'Technical Question', 'General Inquiry', 'Positive Feedback'],
        {'task_description': 'Classify an auto parts customer support ticket into the most relevant category'}
    ):label::VARCHAR AS CATEGORY,

    -- SENTIMENT: Score how positive or negative the customer feels.
    -- Returns a float from -1.0 (very negative) to +1.0 (very positive).
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(TICKET_TEXT), 4) AS SENTIMENT_SCORE,

    -- CLASSIFY_TEXT (again): Determine urgency based on safety risk,
    -- time sensitivity, and level of customer frustration.
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TICKET_TEXT,
        ['Low', 'Medium', 'High', 'Critical'],
        {'task_description': 'Classify urgency of this customer support ticket based on safety risk, time sensitivity, and frustration level'}
    ):label::VARCHAR AS URGENCY,

    -- SUMMARIZE: Generate a concise one-sentence summary.
    SNOWFLAKE.CORTEX.SUMMARIZE(TICKET_TEXT) AS SUMMARY,

    -- TRANSLATE: Auto-detect source language and translate to English.
    -- The empty string '' means "auto-detect the source language."
    SNOWFLAKE.CORTEX.TRANSLATE(TICKET_TEXT, '', 'en') AS TICKET_TEXT_EN

FROM AUTO_PARTS_SUPPORT_TICKETS;


-- =============================================================================
-- STEP 2: Dynamic Table — AI-Analyzed Customer Surveys
-- =============================================================================
-- Same pattern: reads raw survey responses and enriches them with AI.
-- KEY_THEMES uses COMPLETE (via SUMMARIZE) to extract the main themes.
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE AUTO_PARTS_SURVEYS_ANALYZED
  TARGET_LAG = '1 minute'
  WAREHOUSE = AI_DEMO_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
AS
SELECT
    SURVEY_ID,
    CUSTOMER_NAME,
    SURVEY_TEXT,
    RATING,
    SUBMITTED_AT,
    SURVEY_TYPE,

    -- CLASSIFY_TEXT: Categorize the survey feedback topic
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        SURVEY_TEXT,
        ['Product Quality', 'Shipping Speed', 'Website Experience',
         'Customer Service', 'Warranty Experience', 'Pricing/Value'],
        {'task_description': 'Classify a customer survey response for an auto parts retailer'}
    ):label::VARCHAR AS CATEGORY,

    -- SENTIMENT: Score the survey response sentiment
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(SURVEY_TEXT), 4) AS SENTIMENT_SCORE,

    -- SUMMARIZE: Extract key themes from the survey text
    SNOWFLAKE.CORTEX.SUMMARIZE(SURVEY_TEXT) AS KEY_THEMES

FROM AUTO_PARTS_SURVEYS;


-- =============================================================================
-- STEP 3: Cortex Search Services
-- =============================================================================
-- Cortex Search enables semantic (meaning-based) search over text columns.
-- This lets the Cortex Agent find relevant tickets or surveys even when the
-- user's question doesn't match exact keywords.
--
-- For example, searching "brake problems" will also find tickets about
-- "stopping distance issues" or "worn pads."
-- =============================================================================

-- Search service for support tickets: search over the English-translated text
-- with filters on category, urgency, vehicle make, and channel.
CREATE OR REPLACE CORTEX SEARCH SERVICE AUTO_PARTS_TICKET_SEARCH
  ON TICKET_TEXT_EN
  ATTRIBUTES CATEGORY, URGENCY, VEHICLE_MAKE, CHANNEL
  WAREHOUSE = AI_DEMO_WH
  TARGET_LAG = '1 minute'
  AS (
    SELECT
        TICKET_ID::VARCHAR AS TICKET_ID,
        CUSTOMER_NAME,
        TICKET_TEXT_EN,
        CATEGORY,
        URGENCY,
        VEHICLE_MAKE,
        VEHICLE_MODEL,
        SENTIMENT_SCORE::VARCHAR AS SENTIMENT_SCORE,
        SUMMARY,
        CHANNEL,
        SUBMITTED_AT::VARCHAR AS SUBMITTED_AT
    FROM AUTO_PARTS_TICKETS_ENRICHED
  );

-- Search service for surveys: search over the survey text
-- with filters on category and survey type.
CREATE OR REPLACE CORTEX SEARCH SERVICE AUTO_PARTS_SURVEY_SEARCH
  ON SURVEY_TEXT
  ATTRIBUTES CATEGORY, SURVEY_TYPE
  WAREHOUSE = AI_DEMO_WH
  TARGET_LAG = '1 minute'
  AS (
    SELECT
        SURVEY_ID::VARCHAR AS SURVEY_ID,
        CUSTOMER_NAME,
        SURVEY_TEXT,
        CATEGORY,
        SURVEY_TYPE,
        RATING::VARCHAR AS RATING,
        SENTIMENT_SCORE::VARCHAR AS SENTIMENT_SCORE,
        KEY_THEMES,
        SUBMITTED_AT::VARCHAR AS SUBMITTED_AT
    FROM AUTO_PARTS_SURVEYS_ANALYZED
  );


-- =============================================================================
-- STEP 4: Semantic Views for Cortex Analyst (Text-to-SQL)
-- =============================================================================
-- Semantic Views define the business meaning of your data — dimensions, facts,
-- and metrics — so Cortex Analyst can translate natural language questions
-- into accurate SQL. The AI_SQL_GENERATION instruction gives the model
-- domain-specific context for better query generation.
-- =============================================================================

-- Semantic view for support tickets
CREATE OR REPLACE SEMANTIC VIEW AUTO_PARTS_TICKETS_SV
  TABLES (
    tickets AS AI_DEMOS.PUBLIC.AUTO_PARTS_TICKETS_ENRICHED
      PRIMARY KEY (TICKET_ID)
  )
  FACTS (
    tickets.ticket_text AS TICKET_TEXT
      COMMENT = 'Original customer support ticket text',
    tickets.summary AS SUMMARY
      COMMENT = 'AI-generated summary of the ticket',
    tickets.ticket_text_en AS TICKET_TEXT_EN
      COMMENT = 'English translation of the ticket text',
    tickets.order_number AS ORDER_NUMBER
      COMMENT = 'Order number referenced in the ticket',
    tickets.customer_email AS CUSTOMER_EMAIL
      COMMENT = 'Customer email address'
  )
  DIMENSIONS (
    tickets.customer_name AS CUSTOMER_NAME
      COMMENT = 'Name of the customer who submitted the ticket',
    tickets.vehicle_make AS VEHICLE_MAKE
      COMMENT = 'Vehicle manufacturer: BMW, Volvo, Audi, VW, Mercedes, Porsche, Land Rover, Jaguar',
    tickets.vehicle_model AS VEHICLE_MODEL
      COMMENT = 'Model of the vehicle',
    tickets.vehicle_year AS VEHICLE_YEAR
      COMMENT = 'Year of the vehicle',
    tickets.category AS CATEGORY
      COMMENT = 'AI-classified ticket category: Warranty/Returns, Shipping Issue, Wrong Fitment, Product Quality, Pricing, Technical Question, General Inquiry, Positive Feedback',
    tickets.urgency AS URGENCY
      COMMENT = 'AI-classified urgency level: Low, Medium, High, Critical',
    tickets.channel AS CHANNEL
      COMMENT = 'Support channel: Email, Phone, Chat, Web Form',
    tickets.submitted_date AS SUBMITTED_AT::DATE
      COMMENT = 'Date the ticket was submitted'
  )
  METRICS (
    tickets.ticket_count AS COUNT(TICKET_ID)
      COMMENT = 'Total number of support tickets',
    tickets.avg_sentiment AS AVG(SENTIMENT_SCORE)
      COMMENT = 'Average sentiment score from -1 very negative to +1 very positive',
    tickets.min_sentiment AS MIN(SENTIMENT_SCORE)
      COMMENT = 'Lowest (most negative) sentiment score',
    tickets.max_sentiment AS MAX(SENTIMENT_SCORE)
      COMMENT = 'Highest (most positive) sentiment score',
    tickets.high_urgency_count AS COUNT_IF(URGENCY IN ('High', 'Critical'))
      COMMENT = 'Number of High or Critical urgency tickets',
    tickets.negative_ticket_count AS COUNT_IF(SENTIMENT_SCORE < -0.1)
      COMMENT = 'Number of tickets with negative sentiment'
  )
  COMMENT = 'Semantic view over AI-enriched support tickets for auto parts'
  AI_SQL_GENERATION 'This data contains support tickets for an auto parts retailer specializing in European vehicles. Each ticket has been enriched by Cortex AI with category, urgency, sentiment score (-1 to +1), and summary. Use SENTIMENT_SCORE < 0 for unhappy/upset customers. When asked what customers are most upset about, group by category and order by avg_sentiment ascending. Use submitted_date for time-based analysis.';

-- Semantic view for customer surveys
CREATE OR REPLACE SEMANTIC VIEW AUTO_PARTS_SURVEYS_SV
  TABLES (
    surveys AS AI_DEMOS.PUBLIC.AUTO_PARTS_SURVEYS_ANALYZED
      PRIMARY KEY (SURVEY_ID)
  )
  FACTS (
    surveys.survey_text AS SURVEY_TEXT
      COMMENT = 'Original customer survey response text',
    surveys.key_themes AS KEY_THEMES
      COMMENT = 'AI-extracted key themes from the survey response'
  )
  DIMENSIONS (
    surveys.customer_name AS CUSTOMER_NAME
      COMMENT = 'Name of the customer who submitted the survey',
    surveys.survey_type AS SURVEY_TYPE
      COMMENT = 'Type of survey: Post-Purchase, Post-Support, General Feedback',
    surveys.category AS CATEGORY
      COMMENT = 'AI-classified survey category: Product Quality, Shipping Speed, Website Experience, Customer Service, Warranty Experience, Pricing/Value',
    surveys.rating AS RATING
      COMMENT = 'Customer rating from 1 (worst) to 5 (best) stars',
    surveys.submitted_date AS SUBMITTED_AT::DATE
      COMMENT = 'Date the survey was submitted'
  )
  METRICS (
    surveys.survey_count AS COUNT(SURVEY_ID)
      COMMENT = 'Total number of surveys',
    surveys.avg_rating AS AVG(RATING)
      COMMENT = 'Average customer rating (1-5 stars)',
    surveys.avg_sentiment AS AVG(SENTIMENT_SCORE)
      COMMENT = 'Average sentiment score from -1 very negative to +1 very positive',
    surveys.low_rating_count AS COUNT_IF(RATING <= 2)
      COMMENT = 'Number of dissatisfied surveys (1-2 stars)',
    surveys.high_rating_count AS COUNT_IF(RATING >= 4)
      COMMENT = 'Number of satisfied surveys (4-5 stars)',
    surveys.negative_survey_count AS COUNT_IF(SENTIMENT_SCORE < -0.1)
      COMMENT = 'Number of surveys with negative sentiment',
    surveys.nps_proxy AS (COUNT_IF(RATING >= 4) - COUNT_IF(RATING <= 2)) * 100.0 / NULLIF(COUNT(SURVEY_ID), 0)
      COMMENT = 'NPS proxy: promoters minus detractors as a percentage'
  )
  COMMENT = 'Semantic view over AI-analyzed customer surveys for auto parts'
  AI_SQL_GENERATION 'This data contains customer surveys for an auto parts retailer. Each survey is AI-analyzed with category, sentiment score (-1 to +1), and key themes. Ratings are 1-5 stars. When asked about satisfaction, use both RATING and avg_sentiment. When asked what needs improvement, look at low ratings or negative sentiment grouped by category.';


-- =============================================================================
-- STEP 5: Cortex Agent
-- =============================================================================
-- The Agent ties together the semantic views (for structured queries) and the
-- search services (for semantic text search) into a single conversational
-- interface. Users can ask questions like:
--
--   "What are customers most upset about right now?"
--   "Which product category has the lowest sentiment?"
--   "Find tickets about brake pad warranty issues"
--   "What do surveys say about shipping speed?"
--
-- The agent automatically picks the right tool for each question:
--   - Semantic views for aggregate/analytical questions (counts, averages, trends)
--   - Search services for finding specific tickets or survey responses by topic
-- =============================================================================

CREATE OR REPLACE AGENT AUTO_PARTS_SUPPORT_AGENT
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 120,
      "tokens": 200000
    }
  },
  "instructions": {
    "orchestration": "You are a customer support intelligence assistant for an auto parts retailer that specializes in European vehicles (BMW, Volvo, Audi, VW, Mercedes, Porsche, Land Rover, Jaguar). You help support managers and analysts understand customer sentiment, identify trending issues, and find specific tickets or survey responses.\n\nTool Selection Guidelines:\n- For ANALYTICAL questions (counts, averages, trends, comparisons, rankings): Use the semantic view tools (query_tickets or query_surveys). These generate SQL queries against the enriched data.\n- For SEARCH questions (find specific tickets, look up examples, search by topic): Use the search tools (search_tickets or search_surveys). These perform semantic text search.\n- When asked about 'customer sentiment' or 'what customers are upset about': Use query_tickets with avg_sentiment metric grouped by category.\n- When asked about 'survey satisfaction' or 'ratings': Use query_surveys with avg_rating metric.\n- If a question could span both tickets and surveys, query both tools and combine the insights.\n\nData freshness: Dynamic Tables refresh every 1 minute, so data is near-real-time.",
    "response": "Be concise and data-driven. Lead with the direct answer, then provide supporting details. Use tables for multi-row results. When showing sentiment scores, note that -1 is very negative and +1 is very positive. Always mention the number of records behind any aggregate to give context."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "query_tickets",
        "description": "Query AI-enriched support ticket data. Use for analytical questions about ticket counts, sentiment trends, category breakdowns, urgency distribution, and vehicle-specific issues. Data includes: ticket category (Warranty/Returns, Shipping Issue, Wrong Fitment, Product Quality, Pricing, Technical Question, General Inquiry, Positive Feedback), urgency (Low/Medium/High/Critical), sentiment score (-1 to +1), vehicle make/model/year, channel, and submission date. Use this tool when the user asks aggregate questions like 'what are customers most upset about' or 'which category has the most tickets'."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "query_surveys",
        "description": "Query AI-analyzed customer survey data. Use for analytical questions about survey ratings, satisfaction scores, NPS, sentiment by category, and feedback trends. Data includes: survey category (Product Quality, Shipping Speed, Website Experience, Customer Service, Warranty Experience, Pricing/Value), rating (1-5 stars), sentiment score (-1 to +1), survey type (Post-Purchase, Post-Support, General Feedback), and key themes. Use this tool when the user asks about customer satisfaction, ratings, or survey feedback patterns."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "search_tickets",
        "description": "Semantic search over support ticket text. Use to find specific tickets by topic, keyword, or issue description. Returns matching tickets with their AI-enriched metadata (category, urgency, sentiment, summary). Use this when the user asks to 'find tickets about X' or 'show me examples of Y complaints'."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "search_surveys",
        "description": "Semantic search over customer survey responses. Use to find specific survey feedback by topic or theme. Returns matching surveys with rating, sentiment, category, and key themes. Use this when the user asks to 'find surveys about X' or 'what do customers say about Y'."
      }
    }
  ],
  "tool_resources": {
    "query_tickets": {
      "execution_environment": {
        "query_timeout": 120,
        "type": "warehouse",
        "warehouse": "AI_DEMO_WH"
      },
      "semantic_view": "AI_DEMOS.PUBLIC.AUTO_PARTS_TICKETS_SV"
    },
    "query_surveys": {
      "execution_environment": {
        "query_timeout": 120,
        "type": "warehouse",
        "warehouse": "AI_DEMO_WH"
      },
      "semantic_view": "AI_DEMOS.PUBLIC.AUTO_PARTS_SURVEYS_SV"
    },
    "search_tickets": {
      "execution_environment": {
        "query_timeout": 120,
        "type": "warehouse",
        "warehouse": "AI_DEMO_WH"
      },
      "search_service": "AI_DEMOS.PUBLIC.AUTO_PARTS_TICKET_SEARCH"
    },
    "search_surveys": {
      "execution_environment": {
        "query_timeout": 120,
        "type": "warehouse",
        "warehouse": "AI_DEMO_WH"
      },
      "search_service": "AI_DEMOS.PUBLIC.AUTO_PARTS_SURVEY_SEARCH"
    }
  }
}
$$;


-- =============================================================================
-- STEP 6: Deploy the Streamlit Dashboard (Optional)
-- =============================================================================
-- The Streamlit app (streamlit_app.py) provides a visual dashboard with:
--   Tab 1: Support ticket KPIs, charts by category/urgency/channel/sentiment
--   Tab 2: Survey analysis with NPS proxy, rating distribution, sentiment
--   Tab 3: Vehicle-specific insights (issues by make, sentiment by make)
--   Tab 4: Live AI Playground — paste any text and run Cortex AI in real time
--
-- To deploy, upload streamlit_app.py to a stage and create the Streamlit object:
-- =============================================================================

CREATE STAGE IF NOT EXISTS AUTO_PARTS_SUPPORT_STAGE;

-- Upload the file: PUT file://streamlit_app.py @AUTO_PARTS_SUPPORT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CREATE OR REPLACE STREAMLIT AUTO_PARTS_SUPPORT_DASHBOARD
  ROOT_LOCATION = '@AI_DEMOS.PUBLIC.AUTO_PARTS_SUPPORT_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = AI_DEMO_WH
  TITLE = 'Auto Parts Support & Survey Intelligence';


-- =============================================================================
-- STEP 7: Verify Everything Works
-- =============================================================================

-- Check row counts
SELECT 'TICKETS_ENRICHED' AS TABLE_NAME, COUNT(*) AS ROWS FROM AUTO_PARTS_TICKETS_ENRICHED
UNION ALL
SELECT 'SURVEYS_ANALYZED', COUNT(*) FROM AUTO_PARTS_SURVEYS_ANALYZED;

-- Check AI enrichment quality on a sample ticket
SELECT TICKET_ID, CATEGORY, URGENCY, ROUND(SENTIMENT_SCORE, 3) AS SENTIMENT,
       LEFT(SUMMARY, 100) AS SUMMARY_PREVIEW
FROM AUTO_PARTS_TICKETS_ENRICHED
LIMIT 5;

-- Check survey analysis quality
SELECT SURVEY_ID, CATEGORY, RATING, ROUND(SENTIMENT_SCORE, 3) AS SENTIMENT,
       LEFT(KEY_THEMES, 100) AS THEMES_PREVIEW
FROM AUTO_PARTS_SURVEYS_ANALYZED
LIMIT 5;

-- Verify semantic views exist
SHOW SEMANTIC VIEWS LIKE 'AUTO_PARTS%' IN SCHEMA AI_DEMOS.PUBLIC;

-- Verify search services exist
SHOW CORTEX SEARCH SERVICES LIKE 'AUTO_PARTS%' IN SCHEMA AI_DEMOS.PUBLIC;

-- Verify the agent exists
SHOW AGENTS LIKE 'AUTO_PARTS%' IN SCHEMA AI_DEMOS.PUBLIC;
