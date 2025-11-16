#!/bin/bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=4g \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.driver.maxResultSize=2g \
  src/consumption_recommendation.py \
  --config configs/ecomm_prod.yml
