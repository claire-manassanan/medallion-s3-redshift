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

    df = spark.read.format('csv')\
                    .option('header',True)\
                    .option('inferSchema',True)\
                    .load(S3_INPUT_PATH)

    fact = df.select(col('Order ID')\
                    ,col('Order Date').cast(DateType()), col('Ship Date').cast(DateType())\
                    ,col('Ship Mode'), col('Customer ID')\
                    ,col('Postal Code'), col('Product ID')\
                    ,col('Sales').cast('float'), col('Quantity').cast('int'), col('Discount').cast('float')
                    )
  
    dim_cus = df.select(col('Customer ID'), col('Customer Name')\
                    ,col('Segment')
                    )\
                    .dropDuplicates()

    dim_country = df.select(col('Postal Code'), col('Country')\
                    ,col('City'), col('State'), col('Region')
                    )\
                    .dropDuplicates()

    dim_product = df.select(col('Product ID'), col('Category')\
                    ,col('Sub-Category'), col('Product Name')
                    )\
                    .dropDuplicates()

    spark.sql("CREATE DATABASE IF NOT EXISTS sss;")
    spark.sql(f"CREATE TABLE IF NOT EXISTS fact_order(\
                order_id VARCHAR(100),\
                order_date DATE,\
                ship_date DATE,\
                ship_mode VARCHAR(100),\
                customer_id VARCHAR(100),\
                postal_code INT,\
                product_id VARCHAR(100),\
                sales DECIMAL(10,2),\
                quantity INT,\
                discount DECIMAL(10,2)\
                )\
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\
                STORED AS TEXTFILE;\
                LOCATION '{S3_OUTPUT_PATH}'
                ")
            
    fact.createOrReplaceTempView('fact_tmp')
    spark.sql('INSERT INTO sss.fact_order SELECT * FROM fact_tmp;')

    spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS dim_cus(\
                customer_id VARCHAR(100),\
                customer_name VARCHAR(250),\
                segment VARCHAR(100)\
                )\
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\
                STORED AS TEXTFILE;\
                LOCATION '{S3_OUTPUT_PATH}'\
                ")
            
    dim_cus.createOrReplaceTempView('dim_cus_tmp')
    spark.sql('INSERT INTO sss.dim_cus SELECT * FROM dim_cus_tmp;')

    spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS dim_country(\
                postal_code INT,\
                country VARCHAR(250),\
                city VARCHAR(100),\
                state VARCHAR(100),\
                region VARCHAR(100)\
                )\
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\
                STORED AS TEXTFILE;\
                LOCATION '{S3_OUTPUT_PATH}'\
                ")
            
    dim_country.createOrReplaceTempView('dim_country_tmp')
    spark.sql('INSERT INTO sss.dim_country SELECT * FROM dim_country_tmp;')

    spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS dim_product(\
                product_id VARCHAR(100),\
                catagory VARCHAR(100),\
                sub_catagory VARCHAR(100),\
                product_name VARCHAR(250)\
                )\
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\
                STORED AS TEXTFILE;\
                LOCATION '{S3_OUTPUT_PATH}'\
                ")
            
    dim_product.createOrReplaceTempView('dim_product_tmp')
    spark.sql('INSERT INTO sss.dim_product SELECT * FROM dim_product_tmp;')
    
main()
