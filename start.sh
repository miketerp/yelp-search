#!/bin/bash
sbt package

# Test locally, but use --args to run on CLDR cluster
spark-submit \
    --verbose \
    # --master yarn \
    # --deploy-mode cluster \
    --jars misc/mysql-connector-java-8.0.18.jar \
    target/scala-2.11/yelp-search_2.11-0.1.jar \
    "datasets/yelp_academic_dataset_business.json" "datasets/"
