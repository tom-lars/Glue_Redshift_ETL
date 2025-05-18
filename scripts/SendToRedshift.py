import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# ---------------------- Glue Job Initialization ---------------------- #
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------- Read from S3 (Parquet) ---------------------- #
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://mygluebucket897729108218/output/newproduct/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# ---------------------- Apply Mapping ---------------------- #
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("new_year", "string", "year", "string"),
        ("cnt", "long", "noofcustomer", "BIGINT"),
        ("qty", "long", "quantity", "BIGINT"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# ---------------------- Write to Amazon Redshift ---------------------- #
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://mygluebucket897729108218/temp/",
        "useConnectionProperties": "true",
        "dbtable": "public.product_tab_def",
        "connectionName": "redshift-glue-connection",
        "preactions": """
            CREATE TABLE IF NOT EXISTS public.product_tab_def (
                year VARCHAR,
                noofcustomer BIGINT,
                quantity BIGINT
            );
        """,
    },
    transformation_ctx="AmazonRedshift_node3",
)

# ---------------------- Job Commit ---------------------- #
job.commit()
