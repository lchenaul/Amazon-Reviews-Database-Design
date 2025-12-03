# Phase 2 – Elasticsearch Application (Group B)

This folder contains the code and description for **Phase 2** of the Amazon Reviews final project.

- **Requirement 1:** Group B fuzzy/exact-search queries (seven total).
- **Requirement 2:** Justification for choosing **Elasticsearch** as the database and key benchmark metrics.
- **Requirement 3:** Code to create indices and load the Amazon Appliances Reviews and Metadata datasets into Elasticsearch.
- **Requirement 4–5:** Elasticsearch queries for all seven Group B questions and example executions.

## Files

- `es_index_setup.py` – Creates the `amazon_reviews` and `amazon_metadata` indices and bulk-loads the CSV data into Elasticsearch.
- `es_groupB_queries.py` – Implements the seven Group B fuzzy/exact-search queries and prints sample results.
- `docs/Chenault_Final_Project_Phase2.html` *(optional)* – Exported notebook/HTML with narrative, screenshots, and full outputs.

## How to run (local environment)

1. Start Elasticsearch (for example via Docker):
   ```bash
   docker compose up -d
pip install elasticsearch pandas
python es_index_setup.py
python es_groupB_queries.py

