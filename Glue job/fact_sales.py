from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

args = {
    'JOB_NAME': 'create_fact_sales',
    'output_path': 's3://ecommerce-project-aws/ecommerce-data-processed/fact_sales/'
}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    print("Reading processed data from Glue Catalog...")

    orders = glueContext.create_dynamic_frame.from_catalog(
        database="ecommerce_database", table_name="orders_orders"
    ).toDF()

    customers = glueContext.create_dynamic_frame.from_catalog(
        database="ecommerce_database", table_name="customers_customers"
    ).toDF()

    products = glueContext.create_dynamic_frame.from_catalog(
        database="ecommerce_database", table_name="products_products"
    ).toDF()

    print("Joining datasets to create fact_sales...")

    fact_df = orders.alias("o") \
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id")) \
        .join(products.alias("p"), F.col("o.product_id") == F.col("p.product_id")) \
        .withColumn("order_year", F.year("order_date")) \
        .withColumn("order_month", F.month("order_date")) \
        .select(
            "o.order_id", "o.customer_id", "o.product_id", "o.order_date",
            "o.order_total", "o.quantity", "o.order_category",
            "c.customer_segment", "p.category", "p.price_category",
            "order_year", "order_month"
        )

    fact_sales = DynamicFrame.fromDF(fact_df, glueContext, "fact_sales")

    glueContext.write_dynamic_frame.from_options(
        frame=fact_sales,
        connection_type="s3",
        connection_options={
            "path": args["output_path"],
            "partitionKeys": ["order_year", "order_month"]
        },
        format="parquet",
        transformation_ctx="write_fact_sales"
    )

    print("Fact table written successfully to S3.")

except Exception as e:
    print(f"Job failed: {str(e)}")
    raise

finally:
    job.commit()
