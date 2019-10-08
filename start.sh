#!/bin/bash
sbt package

spark-submit target/scala-2.11/yelp-search_2.11-0.1.jar "datasets/yelp_academic_dataset_business.json" "datasets/"