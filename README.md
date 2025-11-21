# API Ingestion Spark Data Source

A modular **PySpark Data Source V2 connector** for ingesting data directly from REST APIs into Spark DataFrames.  
This project provides robust JSON parsing, schema inference, and retry/pagination logic for large-scale API ingestion pipelines.

---

## 📘 Overview

This library consists of two main components:

### 1. **`JsonUtils`**
A stateless utility module for working with JSON objects, designed to:
- Flatten deeply nested JSON objects into dotted key-value pairs.
- Traverse JSON data using simple dot-notation paths.
- Load and infer Spark-compatible schemas from JSON definitions.
- Safely convert arbitrary Python values to Spark SQL-compatible data types.

### 2. **`RestDataSource`**
A fully functional **Spark Data Source V2 implementation** for REST APIs.  
It supports:
- Schema inference from live API responses.
- JSON path extraction (`json_path`) to target nested objects or arrays.
- Paging and parameter-based partitioning strategies for scalable ingestion.
- Configurable retry, throttling, and backoff for resilient API access.


---

## Features

- **Automatic Schema Discovery**  
  Probe REST endpoints and infer Spark `StructType` schemas directly from JSON payloads.

- **Schema loading from files**  
  Load JSON schema file and create Spark `StructType` schema.

- **Pagination & Parameterized Fetching**  
  Supports both `page`-based and `param`-based partitioning for distributed ingestion.

- **Robust Retry Logic**  
  Includes throttling and exponential backoff with configurable constants.

- **Dot-Path JSON Navigation**  
  Access deeply nested data with simple path expressions like:

