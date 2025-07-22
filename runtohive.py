from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

S3_INPUT_PATH = 's3://your-bucket/path/to/your/file'
S3_OUTPUT_PATH = 's3://your-bucket/path/to/output-folder'

def main():
    spark = SparkSession.builder\
            .appName('super store')\
            .enableHiveSupport()\
            .config('spark.sql.warehouse.dir', S3_OUTPUT_PATH)\
            .config('spark.sql.catalogImplementation', 'hive')\
            .config('hive.metastore.client.factory.class',
                'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory')\
            .getOrCreate()
    
    sss_schema = StructType([
        StructField('row_id', StringType(), True),
        StructField('order_id', StringType(), True),
        StructField('order_date', DateType(), True),
        StructField('ship_date', DateType(), True),
        StructField('ship_mode', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('customer_name', StringType(), True),
        StructField('segment', StringType(), True),
        StructField('country', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('postal_code', LongType(), True),
        StructField('region', StringType(), True),
        StructField('product_id', StringType(), True),
        StructField('category', StringType(), True),
        StructField('sub_category', StringType(), True),
        StructField('product_name', StringType(), True),
        StructField('sales', DoubleType(), True),
        StructField('quantity', LongType(), True),
        StructField('discount', DoubleType(), True),
        StructField('profit', DoubleType(), True)
    ])
    
    df = spark.read.format('csv')\
                    .schema(sss_schema)\
                    .option('header',False)\
                    .load(S3_INPUT_PATH)

    # then clean some data
    df = df.drop('row_id')

    spark.sql("CREATE DATABASE IF NOT EXISTS sss_silver;")
    spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS sss_silver.transaction(\
                order_id VARCHAR(100),\
                ship_date DATE,\
                ship_mode VARCHAR(100),\
                customer_id VARCHAR(100),\
                customer_name VARCHAR(250),\
                segment VARCHAR(150),\
                country VARCHAR(150),\
                city VARCHAR(100),\
                state VARCHAR(100),\
                region VARCHAR(100),\
                postal_code INT,\
                product_id VARCHAR(100),\
                catagory VARCHAR(100),\
                sub_catagory VARCHAR(100),\
                product_name VARCHAR(250)\
                sales DECIMAL(10,2),\
                quantity INT,\
                discount DECIMAL(10,2),\
                profit DECIMAL(10,2)
                )\
                PARTITION BY (order_date DATE)\
                STORED AS PARGUET\
                LOCATION '{S3_OUTPUT_PATH}'\
                ")

    df.write.mode('overwrite').partitionBy(col('order_date')).insertInto('sss_silver.transaction')
    
main()
