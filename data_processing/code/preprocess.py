from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkConf
from pyspark.sql import SparkSession

def transform(spark, s3_input_data,s3_output_train_data):
    print('Processing {} => {}'.format(s3_input_data, s3_output_train_data))

    # customSchema = StructType([
    #     StructField("income", IntegerType(), True)
    # ])

    # df_csv = spark.read.csv(path=s3_input_data,
    #                             sep='\t',
    #                             schema=customSchema,
    #                             header=True,
    #                             quote=None)
    # df_csv.show()

    rdd = spark.sparkContext.wholeTextFiles("s3_input_data")
    sum_rdd = rdd.map(lambda x: sum(int(y) for y in x[1].split("\n")))

    row = Row("val") # Or some other column name
    df = sum_rdd.map(row).toDF()  
    df.show()
    
    #train_df.write.option("header",True).csv("/opt/ml/processing/output/train").save(path=s3_output_train_data)
    #train_df.write.format('csv').save(path=s3_output_train_data)
    
    print('Saving to output file {}'.format(s3_output_train_data))
    df.write.format('csv').option('header','true').save(f'{s3_output_train_data}/output.csv',mode='overwrite')

    print('Wrote to output file:  {}'.format(s3_output_train_data))



def main():
    spark = SparkSession.builder.appName("pyspark-demo").getOrCreate()

    # Convert command line args into a map of args
    args_iter = iter(sys.argv[1:])
    args = dict(zip(args_iter, args_iter))
    print(args.keys())
    # Retrieve the args and replace 's3://' with 's3a://' (used by Spark)
    s3_input_data = args['s3_input_data'].replace('s3://', 's3a://')
    print(s3_input_data)

    s3_output_data = args['s3_output_data'].replace('s3://', 's3a://')
    print(s3_output_data)
    
    transform(spark,s3_input_data, s3_output_data)
    

if __name__ == "__main__":
    main()
    
