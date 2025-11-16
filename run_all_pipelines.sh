#!/bin/bash

echo "Starting E-commerce Recommendation Pipeline..."

# Set working directory
cd /app

# Run ETL pipelines
echo "Running ETL pipelines..."

echo "1. Processing Seller Catalog..."
bash scripts/etl_seller_catalog_spark_submit.sh
if [ $? -ne 0 ]; then
    echo "Error: Seller catalog ETL failed"
    exit 1
fi

echo "2. Processing Company Sales..."
bash scripts/etl_company_sales_spark_submit.sh
if [ $? -ne 0 ]; then
    echo "Error: Company sales ETL failed"
    exit 1
fi

echo "3. Processing Competitor Sales..."
bash scripts/etl_competitor_sales_spark_submit.sh
if [ $? -ne 0 ]; then
    echo "Error: Competitor sales ETL failed"
    exit 1
fi

echo "4. Generating Recommendations..."
bash scripts/consumption_recommendation_spark_submit.sh
if [ $? -ne 0 ]; then
    echo "Error: Recommendation generation failed"
    exit 1
fi

echo "Pipeline completed successfully!"
echo "Check /app/data/processed/recommendations_csv/ for final recommendations"