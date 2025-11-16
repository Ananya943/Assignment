import sys
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clean_seller_catalog(df):
    """Clean seller catalog data according to requirements"""
    # Trim whitespace in all string columns
    df_cleaned = df.select(
        trim(col("seller_id")).alias("seller_id"),
        trim(col("item_id")).alias("item_id"),
        trim(col("item_name")).alias("item_name"),
        trim(col("category")).alias("category"),
        col("marketplace_price"),
        col("stock_qty")
    )
    
    # Normalize casing
    df_cleaned = df_cleaned.select(
        col("seller_id"),
        col("item_id"),
        initcap(col("item_name")).alias("item_name"),  # Title Case
        when(upper(col("category")) == "ELECTRONICS", "Electronics")
        .when(upper(col("category")) == "FOOTWEAR", "Footwear")
        .when(upper(col("category")) == "APPAREL", "Apparel")
        .when(upper(col("category")) == "HOME", "Home")
        .when(upper(col("category")) == "BOOKS", "Books")
        .otherwise(initcap(col("category"))).alias("category"),
        col("marketplace_price").cast(DoubleType()).alias("marketplace_price"),
        col("stock_qty").cast(IntegerType()).alias("stock_qty")
    )
    
    # Fill missing stock_qty with 0
    df_cleaned = df_cleaned.fillna({"stock_qty": 0})
    
    # Remove duplicates based on key (seller_id + item_id)
    df_cleaned = df_cleaned.dropDuplicates(["seller_id", "item_id"])
    
    return df_cleaned

def apply_dq_checks(df):
    """Apply data quality checks and separate good/bad records"""
    # Define DQ conditions
    dq_conditions = [
        (col("seller_id").isNotNull(), "seller_id_null"),
        (col("item_id").isNotNull(), "item_id_null"),
        (col("marketplace_price") >= 0, "price_negative"),
        (col("stock_qty") >= 0, "stock_negative"),
        (col("item_name").isNotNull(), "item_name_null"),
        (col("category").isNotNull(), "category_null")
    ]
    
    # Create combined condition for good records
    good_condition = dq_conditions[0][0]
    for condition, _ in dq_conditions[1:]:
        good_condition = good_condition & condition
    
    # Separate good and bad records
    good_df = df.filter(good_condition)
    
    # Create bad records with failure reasons
    bad_records = []
    for condition, reason in dq_conditions:
        bad_df = df.filter(~condition).withColumn("dq_failure_reason", lit(reason))
        bad_records.append(bad_df)
    
    # Union all bad records
    if bad_records:
        quarantine_df = bad_records[0]
        for bad_df in bad_records[1:]:
            quarantine_df = quarantine_df.union(bad_df)
        quarantine_df = quarantine_df.dropDuplicates()
    else:
        quarantine_df = df.limit(0).withColumn("dq_failure_reason", lit(""))
    
    return good_df, quarantine_df

def write_to_hudi(df, output_path, table_name):
    """Write DataFrame to Hudi table"""
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'seller_id,item_id',
        'hoodie.datasource.write.partitionpath.field': '',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'item_name',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }
    
    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(output_path)

def main(config_path):
    # 1. Load YAML config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    input_path = config['seller_catalog']['input_path']
    output_path = config['seller_catalog']['hudi_output_path']

    # 2. Create SparkSession
    spark = get_spark_session("ETL_SellerCatalog")

    try:
        # 3. Read raw data
        df = spark.read.option("header", True).csv(input_path)
        print("Sample input rows:")
        df.show(5, truncate=False)
        print(f"Total input records: {df.count()}")

        # 4. Clean data
        df_cleaned = clean_seller_catalog(df)
        print("Sample cleaned rows:")
        df_cleaned.show(5, truncate=False)

        # 5. Apply DQ checks
        good_df, quarantine_df = apply_dq_checks(df_cleaned)
        print(f"Good records: {good_df.count()}")
        print(f"Quarantine records: {quarantine_df.count()}")

        # 6. Write quarantine data if any
        if quarantine_df.count() > 0:
            quarantine_path = "/app/data/quarantine/seller_catalog_quarantine"
            os.makedirs(quarantine_path, exist_ok=True)
            quarantine_df.coalesce(1).write.mode("overwrite").csv(quarantine_path, header=True)
            print(f"Quarantine data written to: {quarantine_path}")

        # 7. Write good data to Hudi
        if good_df.count() > 0:
            os.makedirs(output_path, exist_ok=True)
            write_to_hudi(good_df, output_path, "seller_catalog")
            print(f"Good data written to Hudi table: {output_path}")
        else:
            print("No good records to write!")

    except Exception as e:
        print(f"Error in ETL pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    main(args.config)
