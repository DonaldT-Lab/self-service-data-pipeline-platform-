# Self-Service Data Pipeline Platform
# Overview

This project is a metadata-driven data pipeline platform that allows users to define and run data pipelines using simple YAML configuration files.

Instead of writing custom code for every dataset, pipelines are created declaratively. The platform automatically handles validation, ingestion, transformation, logging, and monitoring.

The goal is to reduce repeated engineering work and provide a scalable, standardized way to onboard data.

### Key Features 
# Configuration-Driven Pipelines
    Pipelines are defined using YAML files
    No need to write pipeline-specific code
    Easily reusable and scalable
# Data Ingestion
    CSV ingestion fully implemented
    API ingestion framework included
    Data loaded into DuckDB warehouse
# Data Quality Checks
    Validation before loading data
    Prevents bad data from entering the warehouse
    Supported checks:
        Not null
        Greater than (numeric validation)
        Timestamp format (framework ready)
# Transformation Layer
    SQL-based transformations
    Converts raw data into analytics-ready tables
    Raw → Analytics architecture
# Metadata Management
    Tracks all pipelines in a central registry:
        pipeline name
        owner
        source type
        destination table
        schedule
        last run status
# Execution Logging & Observability
Each pipeline run is logged with:
    run status (SUCCESS / FAILED)
    rows extracted
    rows loaded
    execution time
    error messages
# Orchestration Support
Automatically generates Airflow DAG stubs
Ready for scheduling and automation

# Architecture
YAML Config
   ↓
Validation Layer
   ↓
Extraction Layer (CSV / API)
   ↓
Data Quality Checks
   ↓
Raw Data Load (DuckDB)
   ↓
Transformation Layer (SQL)
   ↓
Analytics Tables
   ↓
Metadata & Logging

# Project Structure
self-service-platform/
│
├── configs/                  # YAML pipeline configs
├── data/                     # Source CSV files
├── transforms/               # SQL transformation files
│
├── pipeline_platform/
│   ├── config_parser.py
│   ├── validator.py
│   ├── pipeline_generator.py
│   ├── sources/
│   ├── warehouse/
│   ├── metadata/
│   ├── transforms/
│   ├── data_quality/
│
├── main.py                   # CLI entry point
├── requirements.txt