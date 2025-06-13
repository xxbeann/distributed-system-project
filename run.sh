#!/bin/bash
../spark-3.4.4-bin-hadoop3/bin/spark-submit \
  --class BarrierFreeAnalysis \
  --master local \
  --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" \
  build/BarrierFreeAnalysis
