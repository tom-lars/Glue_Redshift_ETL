import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame

# ---------------------- Logging Setup ---------------------- #
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)

logger.info('Logger setup complete. Starting Glue job.')

# ---------------------- Glue Job Initialization ---------------------- #
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------- Read from Glue Catalog ---------------------- #
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mydatabase", table_name="product", transformation_ctx="S3bucket_node1"
)

logger.info('Schema of S3bucket_node1:')
S3bucket_node1.printSchema()

count = S3bucket_node1.count()
logger.info(f'Row count for S3bucket_node1: {count}')

# ---------------------- Apply Mapping ---------------------- #
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("marketplace", "string", "new_marketplace", "string"),
        ("customer_id", "long", "new_customer_id", "long"),
        ("product_id", "string", "new_product_id", "string"),
        ("seller_id", "string", "new_seller_id", "string"),
        ("sell_date", "string", "new_sell_date", "string"),
        ("quantity", "long", "new_quantity", "long"),
        ("year", "string", "new_year", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# ---------------------- ResolveChoice (cast string to long) ---------------------- #
ResolveChoice_node = ApplyMapping_node2.resolveChoice(
    specs=[('new_seller_id','cast:long')],
    transformation_ctx="ResolveChoice_node"
)

logger.info('Schema after ResolveChoice_node:')
ResolveChoice_node.printSchema()

# ---------------------- DataFrame Transformations ---------------------- #
spark_data_frame = ResolveChoice_node.toDF()

logger.info('Filtering rows where new_seller_id is NOT NULL...')
spark_data_frame_filter = spark_data_frame.where("new_seller_id is NOT NULL")

logger.info('Adding new_status column with value "Active"...')
spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))

logger.info('Showing filtered and transformed data:')
spark_data_frame_filter.show()

# ---------------------- Spark SQL Aggregation ---------------------- #
logger.info('Creating temp view: product_view...')
spark_data_frame_filter.createOrReplaceTempView("product_view")

logger.info('Running SQL aggregation on new_year...')
product_sql_df = spark.sql("""
    SELECT new_year,
           COUNT(new_customer_id) AS cnt,
           SUM(new_quantity) AS qty
    FROM product_view
    GROUP BY new_year
""")

logger.info('Displaying aggregated result:')
product_sql_df.show()

# ---------------------- Convert Back to DynamicFrame ---------------------- #
logger.info('Converting spark dataframe to dynamic frame...')
dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")

# ---------------------- Write to S3 ---------------------- #
logger.info('Writing output to S3 in parquet format at s3://mygluebucket897729108218/output/newproduct/')

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://mygluebucket897729108218/output/newproduct/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

logger.info('ETL job completed successfully.')

# ---------------------- Job Commit ---------------------- #
job.commit()
