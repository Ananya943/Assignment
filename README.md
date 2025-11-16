# E-commerce Top-Seller Items Recommendation System

This project implements a comprehensive recommendation system for sellers on an e-commerce platform using Apache Spark, Apache Hudi, and Docker. The system analyzes internal sales and competitor data to recommend top-selling items that each seller currently does not have in their catalog.

## 🏗️ Architecture

The system follows a medallion architecture with the following components:

- **Bronze Layer**: Raw data ingestion from CSV files
- **Silver Layer**: Data cleaning, validation, and quarantine handling
- **Gold Layer**: Business logic and recommendation generation
- **Quarantine Zone**: Bad data isolation with failure reasons

## 📁 Project Structure

```
2025EM1100043/ecommerce_seller_recommendation/local/
│
├── configs/
│   └── ecomm_prod.yml         # Configuration file with input/output paths
├── src/
│   ├── etl_seller_catalog.py  # ETL pipeline for seller catalog data
│   ├── etl_company_sales.py   # ETL pipeline for company sales data
│   ├── etl_competitor_sales.py # ETL pipeline for competitor sales data
│   └── consumption_recommendation.py # Recommendation generation logic
├── scripts/
│   ├── etl_seller_catalog_spark_submit.sh
│   ├── etl_company_sales_spark_submit.sh
│   ├── etl_competitor_sales_spark_submit.sh
│   └── consumption_recommendation_spark_submit.sh
├── input_data_sets/
│   ├── clean/                 # Clean input data files
│   └── dirty/                 # Dirty input data files (for testing)
├── data/
│   ├── processed/             # Hudi tables and final CSV output
│   └── quarantine/            # Bad data with failure reasons
├── Dockerfile                 # Docker container configuration
├── docker-compose.yml         # Docker Compose configuration
├── requirements.txt           # Python dependencies
├── run_all_pipelines.sh       # Master script to run all pipelines
└── README.md                  # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for the container

### Running the Complete Pipeline

1. **Clone and navigate to the project directory:**
   ```bash
   cd Assignment
   ```

2. **Build and run using Docker Compose:**
   ```bash
   docker-compose up --build
   ```

3. **Alternative: Build and run manually:**
   ```bash
   # Build the Docker image
   docker build -t ecommerce-recommendation .
   
   # Run the container
   docker run -v $(pwd)/data:/app/data -v $(pwd)/logs:/app/logs ecommerce-recommendation
   ```

### Running Individual Pipelines

If you want to run pipelines individually:

```bash
# Build the image
docker build -t ecommerce-recommendation .

# Run individual ETL pipelines
docker run -v $(pwd)/data:/app/data ecommerce-recommendation bash scripts/etl_seller_catalog_spark_submit.sh
docker run -v $(pwd)/data:/app/data ecommerce-recommendation bash scripts/etl_company_sales_spark_submit.sh
docker run -v $(pwd)/data:/app/data ecommerce-recommendation bash scripts/etl_competitor_sales_spark_submit.sh

# Run recommendation generation
docker run -v $(pwd)/data:/app/data ecommerce-recommendation bash scripts/consumption_recommendation_spark_submit.sh
```

## 📊 Data Processing Pipeline

### 1. ETL Pipelines (15 Marks)

#### Seller Catalog ETL
- **Input**: `input_data_sets/clean/seller_catalog_clean.csv`
- **Cleaning**: Trim whitespace, normalize casing, convert data types
- **DQ Checks**: Validate seller_id, item_id, price >= 0, stock >= 0
- **Output**: Hudi table at `data/processed/seller_catalog_hudi/`

#### Company Sales ETL
- **Input**: `input_data_sets/clean/company_sales_clean.csv`
- **Cleaning**: Convert data types, standardize date format
- **DQ Checks**: Validate item_id, units_sold >= 0, revenue >= 0, valid dates
- **Output**: Hudi table at `data/processed/company_sales_hudi/`

#### Competitor Sales ETL
- **Input**: `input_data_sets/clean/competitor_sales_clean.csv`
- **Cleaning**: Trim strings, convert numeric types, standardize dates
- **DQ Checks**: Validate all required fields and business rules
- **Output**: Hudi table at `data/processed/competitor_sales_hudi/`

### 2. Consumption Layer (5 Marks)

#### Recommendation Generation
- **Inputs**: All three Hudi tables from ETL pipelines
- **Processing**:
  - Identify top 10 selling items per category from company data
  - Identify top 10 selling items per category from competitor data
  - For each seller, find missing items from their catalog
  - Calculate expected revenue: `expected_units_sold * marketplace_price`
- **Output**: CSV file at `data/processed/recommendations_csv/`

## 🔧 Configuration

The system uses a YAML configuration file (`configs/ecomm_prod.yml`) to specify input and output paths:

```yaml
seller_catalog:
  input_path: "/app/input_data_sets/clean/seller_catalog_clean.csv"
  hudi_output_path: "/app/data/processed/seller_catalog_hudi/"

company_sales:
  input_path: "/app/input_data_sets/clean/company_sales_clean.csv"
  hudi_output_path: "/app/data/processed/company_sales_hudi/"

competitor_sales:
  input_path: "/app/input_data_sets/clean/competitor_sales_clean.csv"
  hudi_output_path: "/app/data/processed/competitor_sales_hudi/"

recommendation:
  seller_catalog_hudi: "/app/data/processed/seller_catalog_hudi/"
  company_sales_hudi: "/app/data/processed/company_sales_hudi/"
  competitor_sales_hudi: "/app/data/processed/competitor_sales_hudi/"
  output_csv: "/app/data/processed/recommendations_csv/seller_recommend_data.csv"
```

## 🛠️ Technology Stack

- **Apache Spark 3.5.0**: Distributed data processing
- **Apache Hudi 0.15.0**: Data lake storage with ACID transactions
- **Python 3**: Programming language
- **Docker**: Containerization
- **YAML**: Configuration management

## 📈 Data Quality & Quarantine

The system implements comprehensive data quality checks:

### Seller Catalog DQ Rules
- Seller ID exists (not null)
- Item ID exists (not null)
- Price valid (>= 0)
- Stock valid (>= 0)
- Item name present (not null)
- Category present (not null)

### Company Sales DQ Rules
- Item ID exists (not null)
- Units sold valid (>= 0)
- Revenue valid (>= 0)
- Sale date valid (not null and <= current_date)

### Competitor Sales DQ Rules
- Item ID exists (not null)
- Seller ID exists (not null)
- Units sold valid (>= 0)
- Revenue valid (>= 0)
- Marketplace price valid (>= 0)
- Sale date valid (not null and <= current_date)

Failed records are moved to quarantine with detailed failure reasons.

## 📋 Output Schema

The final recommendation CSV contains:

| Column | Type | Description |
|--------|------|-------------|
| seller_id | String | Unique identifier of the seller |
| item_id | String | Unique identifier of the recommended item |
| item_name | String | Name of the recommended item |
| category | String | Category of the item |
| market_price | Double | Current market price of the item |
| expected_units_sold | Double | Expected units to be sold |
| expected_revenue | Double | Expected revenue (units * price) |

## 🔍 Monitoring & Logs

- Application logs are stored in `/app/logs/`
- Quarantine data is stored in `/app/data/quarantine/`
- Each quarantine record includes the original data and failure reason

## 🚨 Troubleshooting

### Common Issues

1. **Out of Memory**: Increase Docker memory allocation to at least 4GB
2. **Permission Denied**: Ensure Docker has access to the project directory
3. **Spark Submit Fails**: Check if all required packages are downloaded

### Debugging

To debug issues, run the container interactively:

```bash
docker run -it -v $(pwd)/data:/app/data ecommerce-recommendation bash
```

Then run individual commands to isolate the problem.

## 📝 Assignment Compliance

This implementation fulfills all assignment requirements:

✅ **ETL Ingestion (15 Marks)**
- YAML-configurable pipeline
- Apache Hudi for schema evolution and incremental upserts
- Data cleaning and DQ checks
- Quarantine zone handling
- Medallion architecture
- 3 different ETL pipelines
- Hudi tables with overwrite mode

✅ **Consumption Layer (5 Marks)**
- Medallion architecture
- Reads from 3 Hudi tables
- Data transformations and aggregations
- Recommendation calculations
- CSV output with overwrite mode

✅ **Project Structure**
- Follows specified directory structure
- Single configuration file
- Spark submit commands as specified
- Uses PySpark as tech stack

## 👥 Contributors

- Roll Number: 2025EM1100043
- Assignment: E-commerce Top-Seller Items Recommendation System

---

**Note**: This system is designed to run completely in Docker, requiring no additional installations on the host machine. All dependencies, including Spark and Hudi, are containerized for easy deployment and review.
