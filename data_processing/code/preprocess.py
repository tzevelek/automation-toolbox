from __future__ import print_function
from __future__ import unicode_literals

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

def transform(spark, s3_input_data,s3_output_train_data):
    print('Processing {} => {}'.format(s3_input_data, s3_output_train_data))

    rdd = spark.sparkContext.wholeTextFiles(s3_input_data, 9)
    sum_rdd = rdd.map(lambda x: sum(int(y) for y in x[1].split("\n")))

    row = Row("sum")
    df = sum_rdd.map(row).toDF()  
    df.show()

    print('Saving to output file {}'.format(s3_output_train_data))
    df.write.format('csv').option('header','true').save(f'{s3_output_train_data}/output.csv',mode='overwrite')

    print('Wrote to output file:  {}'.format(s3_output_train_data))



def main():
    spark = SparkSession.builder.appName("pyspark-demo").getOrCreate()

    args_iter = iter(sys.argv[1:])
    args = dict(zip(args_iter, args_iter))
    print(args.keys())
    # Retrieve the args and replace 's3://' with 's3a://'
    s3_input_data = args['s3_input_data'].replace('s3://', 's3a://')
    print(s3_input_data)

    s3_output_data = args['s3_output_data'].replace('s3://', 's3a://')
    print(s3_output_data)
    
    transform(spark,s3_input_data, s3_output_data)
    

if __name__ == "__main__":
    main()
    
