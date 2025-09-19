#!/bin/sh

spark-submit \
  --master local \
  --py-files dist/guidedcapstone-1.0-py3.8.egg \
  --jars jars/postgresql-42.2.14.jar,jars/hadoop-azure.jar,jars/azure-storage.jar \
  src/run_data_ingestion.py \
  config/config.ini
