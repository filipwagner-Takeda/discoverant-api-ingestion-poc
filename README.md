# RestDataSource – High-Level Overview

`RestDataSource` is a Spark Data Source V2 implementation that provides a **standardized, unified, and reusable** way to ingest data from REST APIs across our organization.  
Its purpose is to replace one-off ingestion scripts with a consistent framework that:

- Handles common API interaction patterns  
- Reduces duplicated logic  
- Improves reliability and maintainability  
- Enables scalable, production-ready ingestion pipelines

## What It Does

### 1. Standardized API Ingestion

The DataSource enables Spark to read data directly from REST API endpoints.  
It abstracts the complexity of authentication, pagination, and request handling so users can ingest data by simply configuring options—no custom ingestion code required.

### 2. Automatic Schema Handling

The system supports two approaches:

- **Schema inference** — probing the API and generating a schema dynamically  
- **Static schema loading** — using a predefined JSON schema file

### 3. Pagination & Partitioning

We have two defined partitioning strategies to distribute API requests across cluster. To efficiently ingest large volumes of API data, the reader supports:

- **Page-based pagination**  
- **Parameter-based partitioning** (ID lists, metadata keys, etc.)  
- **Non-paginated modes**

### 4. Error Handling, Retries & Throttling

Built-in mechanisms to help ingestion succeed even when upstream APIs are slow or unstable.
To ensure resilience when interacting with external services, the ingestion pipeline includes:

- Automatic retries with **exponential backoff**  
- **Throttling** to avoid hitting API rate limits  
- Robust structured **logging of errors and responses**

### 5. JSON Flattening & Transformation

Helper methods that ensure consistent DataFrame output across different API formats.
API responses frequently contain nested or irregular JSON. The DataSource includes utilities for:

- Extracting nested values via **JSONPath**  
- Flattening JSON into Spark-compatible tabular rows  
- Handling flexible or inconsistent payload structures


### 6. Distributed Logging

Each Spark driver and executor initializes its own log handler to provide:

- Visibility into which executor processed which pages or parameters  
- Simplified debugging of distributed API calls  
- Standardized logs stored in a shared volume path  

## Why We Built It

The goal of this initiative is to provide a **unified, standardized, and scalable** approach to REST API ingestion across the organization.  
Instead of every team implementing their own logic, `RestDataSource` offers:

- A **single reusable ingestion mechanism**  
- Consistent configuration across all pipelines  
- Built-in resilience, logging, and error recovery  
- Easier long-term maintenance  
- A pluggable architecture aligned with Spark best practices  
