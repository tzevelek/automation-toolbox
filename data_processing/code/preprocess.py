from __future__ import print_function
from __future__ import unicode_literals

import argparse
import csv
import os
import shutil
import sys
import time

import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum as _sum


def main():
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument("--s3_input_bucket", type=str, help="s3 input bucket")
    parser.add_argument("--s3_input_key_prefix", type=str, help="s3 input key prefix")
    parser.add_argument("--s3_output_bucket", type=str, help="s3 output bucket")
    parser.add_argument("--s3_output_key_prefix", type=str, help="s3 output key prefix")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CSVSum").getOrCreate()

    customSchema = StructType([
        StructField("income", IntegerType(), True)
    ])

    df = spark.read.format("csv").schema(customSchema).load("s3://" + os.path.join(args.s3_input_bucket, args.s3_input_key_prefix)).select("*", "_metadata.file_name")

    sum_df = df.groupBy('file_name').agg(_sum('income').alias('income_sum'))
    processed_rdd = sum_df.rdd
    processed_rdd.coalesce(1).saveAsTextFile("s3://" + os.path.join(args.s3_output_bucket, args.s3_output_key_prefix, "processed"))

if __name__ == "__main__":
    main()
