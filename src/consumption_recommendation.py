import sys
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
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

def read_hudi_table(spark, path):
    """Read Hudi table from given path"""
    return spark.read.format("hudi").load(path)

def get_company_top_sellers(company_sales_df, seller_catalog_df, top_n=10):
    """Get top selling items from company sales data by category"""
    # Join company sales with seller catalog to get category information
    company_with_catalog = company_sales_df.join(
        seller_catalog_df.select("item_id", "category", "item_name").dropDuplicates(["item_id"]),
        "item_id",
        "inner"
    )
    
    # Calculate top sellers by category
    window_spec = Window.partitionBy("category").orderBy(desc("units_sold"))
    
    top_sellers = company_with_catalog.select(
        "item_id",
        "item_name", 
        "category",
        "units_sold",
        "revenue",
        row_number().over(window_spec).alias("rank")
    ).filter(col("rank") <= top_n)
    
    return top_sellers

def get_competitor_top_sellers(competitor_sales_df, seller_catalog_df, top_n=10):
    """Get top selling items from competitor sales data"""
    # Aggregate competitor sales by item
    competitor_agg = competitor_sales_df.groupBy("item_id").agg(
        sum("units_sold").alias("total_units_sold"),
        sum("revenue").alias("total_revenue"),
        avg("marketplace_price").alias("avg_marketplace_price"),
        count("seller_id").alias("num_sellers")
    )
    
    # Join with catalog to get item details
    competitor_with_catalog = competitor_agg.join(
        seller_catalog_df.select("item_id", "category", "item_name").dropDuplicates(["item_id"]),
        "item_id",
        "left"
    )
    
    # Get top sellers by category
    window_spec = Window.partitionBy("category").orderBy(desc("total_units_sold"))
    
    top_sellers = competitor_with_catalog.select(
        "item_id",
        "item_name",
        "category", 
        "total_units_sold",
        "total_revenue",
        "avg_marketplace_price",
        "num_sellers",
        row_number().over(window_spec).alias("rank")
    ).filter(col("rank") <= top_n)
    
    return top_sellers

def generate_recommendations(seller_catalog_df, company_top_sellers, competitor_top_sellers):
    """Generate recommendations for each seller using more efficient approach"""
    
    # Get all seller items for anti-join
    all_seller_items = seller_catalog_df.select("seller_id", "item_id").distinct()
    
    # Find missing items from company top sellers for all sellers
    company_missing = company_top_sellers.crossJoin(
        seller_catalog_df.select("seller_id").distinct()
    ).join(
        all_seller_items, ["seller_id", "item_id"], "left_anti"
    ).select(
        "seller_id",
        "item_id",
        "item_name", 
        "category",
        "units_sold",
        "revenue",
        lit("company").alias("source")
    )
    
    # Find missing items from competitor top sellers for all sellers
    competitor_missing = competitor_top_sellers.crossJoin(
        seller_catalog_df.select("seller_id").distinct()
    ).join(
        all_seller_items, ["seller_id", "item_id"], "left_anti"
    ).select(
        "seller_id",
        "item_id",
        "item_name",
        "category",
        "total_units_sold",
        "avg_marketplace_price",
        "num_sellers",
        lit("competitor").alias("source")
    )
    
    return company_missing, competitor_missing

def calculate_final_recommendations(recommendations, spark):
    """Calculate final recommendations with business metrics"""
    all_recommendations = []
    
    for company_missing, competitor_missing in recommendations:
        # Process company recommendations
        if company_missing.count() > 0:
            company_recs = company_missing.select(
                "seller_id",
                "item_id", 
                "item_name",
                "category",
                col("revenue").alias("market_price"),
                col("units_sold").alias("expected_units_sold"),
                (col("units_sold") * col("revenue")).alias("expected_revenue")
            )
            all_recommendations.append(company_recs)
        
        # Process competitor recommendations  
        if competitor_missing.count() > 0:
            competitor_recs = competitor_missing.select(
                "seller_id",
                "item_id",
                "item_name", 
                "category",
                col("avg_marketplace_price").alias("market_price"),
                (col("total_units_sold") / col("num_sellers")).alias("expected_units_sold"),
                ((col("total_units_sold") / col("num_sellers")) * col("avg_marketplace_price")).alias("expected_revenue")
            )
            all_recommendations.append(competitor_recs)
    
    # Union all recommendations
    if all_recommendations:
        final_df = all_recommendations[0]
        for rec_df in all_recommendations[1:]:
            final_df = final_df.union(rec_df)
        
        # Rank by expected revenue and take top 10 per seller
        window_spec = Window.partitionBy("seller_id").orderBy(desc("expected_revenue"))
        final_df = final_df.select(
            "*",
            row_number().over(window_spec).alias("rank")
        ).filter(col("rank") <= 10).drop("rank")
        
        return final_df
    else:
        # Return empty DataFrame with correct schema
        schema = StructType([
            StructField("seller_id", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("market_price", DoubleType(), True),
            StructField("expected_units_sold", DoubleType(), True),
            StructField("expected_revenue", DoubleType(), True)
        ])
        return spark.createDataFrame([], schema)

def main(config_path):
    # 1. Load YAML config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    seller_catalog_path = config['recommendation']['seller_catalog_hudi']
    company_sales_path = config['recommendation']['company_sales_hudi']
    competitor_sales_path = config['recommendation']['competitor_sales_hudi']
    output_csv_path = config['recommendation']['output_csv']

    # 2. Create SparkSession
    spark = get_spark_session("Consumption_Recommendation")

    try:
        # 3. Read Hudi tables
        print("Reading Hudi tables...")
        seller_catalog_df = read_hudi_table(spark, seller_catalog_path)
        company_sales_df = read_hudi_table(spark, company_sales_path)
        competitor_sales_df = read_hudi_table(spark, competitor_sales_path)
        
        print(f"Seller catalog records: {seller_catalog_df.count()}")
        print(f"Company sales records: {company_sales_df.count()}")
        print(f"Competitor sales records: {competitor_sales_df.count()}")

        # 4. Get top selling items
        print("Calculating top selling items...")
        company_top_sellers = get_company_top_sellers(company_sales_df, seller_catalog_df)
        competitor_top_sellers = get_competitor_top_sellers(competitor_sales_df, seller_catalog_df)
        
        print(f"Company top sellers: {company_top_sellers.count()}")
        print(f"Competitor top sellers: {competitor_top_sellers.count()}")

        # 5. Generate recommendations
        print("Generating recommendations...")
        company_missing, competitor_missing = generate_recommendations(seller_catalog_df, company_top_sellers, competitor_top_sellers)
        
        # 6. Calculate final recommendations with business metrics
        final_recommendations = calculate_final_recommendations([(company_missing, competitor_missing)], spark)
        
        print(f"Final recommendations: {final_recommendations.count()}")
        
        if final_recommendations.count() > 0:
            print("Sample recommendations:")
            final_recommendations.show(10, truncate=False)
            
            # 7. Write to CSV
            output_dir = os.path.dirname(output_csv_path)
            os.makedirs(output_dir, exist_ok=True)
            
            final_recommendations.coalesce(1).write.mode("overwrite").csv(output_dir, header=True)
            print(f"Recommendations written to: {output_dir}")
        else:
            print("No recommendations generated!")

    except Exception as e:
        print(f"Error in consumption pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    main(args.config)
