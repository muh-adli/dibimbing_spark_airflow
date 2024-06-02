import pyspark
import os

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import col, sum as _sum, to_date, sum, format_number

# ## load .env
# dotenv_path = Path('/.env')
# load_dotenv(dotenv_path=dotenv_path)
# print('dotenv done')

# ## get postgres properties
# postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
# postgres_dw_db = os.getenv('POSTGRES_DW_DB')
# postgres_user = os.getenv('POSTGRES_USER')
# postgres_password = os.getenv('POSTGRES_PASSWORD')

postgres_host = "dataeng-postgres"
postgres_dw_db = "warehouse"
postgres_user = "user"
postgres_password = "password"

print('postgres prop done')

## initialize spark
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Assignment_Day15')
        .setMaster('local')
        .set("spark.jars", "/opt/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())
print('spark initialization done')

## JDBC conn
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
table_name = 'public.retail'
prop = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}
print('JDBC conn done')

## Load table /w jdbc
retail_df = spark.read.jdbc(
    jdbc_url,
    table_name,
    properties=prop
)

retail_df.printSchema()
retail_df.show(10)

## Convert invoicedate to date type
DateInvoice_df = retail_df.withColumn('invoicedate', to_date(col('invoicedate')))

## Aggregate data by customerid and invoicedate
Date_analysis = DateInvoice_df.groupBy('invoicedate').agg(
    _sum('quantity').alias('Total_Item_Purchased'),
    _sum('unitprice').alias('Total_Customer_Spent')
)
## Format the total_quantity and total_unitprice columns to 2 decimal places
Date_analysis = Date_analysis.withColumn('Total_Item_Purchased', format_number('Total_Item_Purchased', 2)) \
                             .withColumn('Total_Customer_Spent', format_number('Total_Customer_Spent', 2))

## Show the result
Date_analysis.show()

Date_analysis.write.jdbc(
    url=jdbc_url,
    table="StockSpent_analysis_airflow",
    mode="append",
    properties=prop
)