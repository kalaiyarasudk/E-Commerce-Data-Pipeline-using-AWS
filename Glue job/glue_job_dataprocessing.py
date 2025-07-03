from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = {
    'JOB_NAME': 'ecommerce_etl_job',
    'input_path': 's3://ecommerce-project-aws/ecommerce-data-raw',
    'output_path': 's3://ecommerce-project-aws/ecommerce-data-processed'
}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_orders_data():
    orders_df = glueContext.create_dynamic_frame.from_options(
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        connection_options={"paths": [f"{args['input_path']}/orders/"], "recurse": True},
        transformation_ctx="orders_df"
    )
    df = orders_df.toDF()
    cleaned = df.filter(
        (F.col("order_id").isNotNull()) &
        (F.col("customer_id").isNotNull()) &
        (F.col("order_total") > 0)
    ).withColumn(
        "order_date", F.to_date(F.col("order_timestamp"))
    ).withColumn(
        "order_year", F.year("order_date")
    ).withColumn(
        "order_month", F.month("order_date")
    ).withColumn(
        "order_day", F.dayofmonth("order_date")
    ).withColumn(
        "is_weekend", F.dayofweek("order_date").isin([1, 7])
    ).withColumn(
        "order_hour", F.hour("order_timestamp")
    ).withColumn(
        "order_category",
        F.when(F.col("order_total") < 50, "Low Value")
         .when(F.col("order_total") < 200, "Medium Value")
         .otherwise("High Value")
    )
    return DynamicFrame.fromDF(cleaned, glueContext, "orders_processed")

def process_customers_data():
    customers_df = glueContext.create_dynamic_frame.from_options(
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        connection_options={"paths": [f"{args['input_path']}/customers/"], "recurse": True},
        transformation_ctx="customers_df"
    )
    df = customers_df.toDF()
    processed = df.withColumn(
        "customer_segment",
        F.when(F.col("total_orders") >= 10, "VIP")
         .when(F.col("total_orders") >= 5, "Regular")
         .otherwise("New")
    ).withColumn(
        "registration_year", F.year("registration_date")
    )
    return DynamicFrame.fromDF(processed, glueContext, "customers_processed")

def process_products_data():
    products_df = glueContext.create_dynamic_frame.from_options(
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        connection_options={"paths": [f"{args['input_path']}/products/"], "recurse": True},
        transformation_ctx="products_df"
    )
    df = products_df.toDF()
    processed = df.withColumn(
        "price_category",
        F.when(F.col("price") < 25, "Budget")
         .when(F.col("price") < 100, "Mid-range")
         .otherwise("Premium")
    ).withColumn(
        "is_in_stock", F.col("inventory_count") > 0
    )
    return DynamicFrame.fromDF(processed, glueContext, "products_processed")


try: 
    print("Starting ETL job...")

    orders_processed = process_orders_data()
    customers_processed = process_customers_data()
    products_processed = process_products_data()

    glueContext.write_dynamic_frame.from_options(
        frame=orders_processed,
        connection_type="s3",
        connection_options={"path": f"{args['output_path']}/orders/", "partitionKeys": ["order_year", "order_month"]},
        format="parquet",
        transformation_ctx="write_orders"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=customers_processed,
        connection_type="s3",
        connection_options={"path": f"{args['output_path']}/customers/"},
        format="parquet",
        transformation_ctx="write_customers"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=products_processed,
        connection_type="s3",
        connection_options={"path": f"{args['output_path']}/products/"},
        format="parquet",
        transformation_ctx="write_products"
    )

    print("ETL job completed successfully!")

except Exception as e:
    print(f"ETL job failed: {str(e)}")
    raise

finally:
    job.commit()
