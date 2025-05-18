# AWS Glue ETL Project: Product Data Pipeline

## 📁 S3 Bucket Structure

s3://your-bucket/
│
├── input/
│ └── product/
│ ├── year=2021-dummy-csv/
│ └── year=2023-dummy-csv-2023/
│
├── output/
│ └── newproduct/
│
├── scripts/
│ └── fetch_from_s3.py
│ └── send_to_redshift.py
│
└── temp/


---

## 🔧 Glue Crawler Setup

### 1. Crawler: `FetchFromS3`
- **Source**: `s3://your-bucket/input/product/`
- **Database**: `mydatabase`
- **Table Name**: `product`
- **Format**: CSV
- **Schedule**: As needed

### 2. Crawler: `FetchFromRedshift`
- **Source**: Amazon Redshift table
- **Glue connection**: `redshift-glue-conn`
- **Database**: `mydatabase`
- **Table Name**: `dev_public_product_tab_def`
- **Redshift table path**: `"dev"."public"."product_tab_def"`

---

## ⚙️ Glue ETL Job Setup

### 1. Job: `FetchFromS3`
- **Source**: `mydatabase.product`
- **Transformations**:
  - Remove invalid `seller_id` formats (e.g., NULL, empty, malformed)
  - Drop entries with undesirable seller IDs
- **Target**: Write output in **Parquet** to `s3://your-bucket/output/newproduct/`

### 2. Job: `SendToRedshift`
- **Source**: `s3://your-bucket/output/newproduct/`
- **Target**: Append to Redshift table `dev.public.product_tab_def`
- **Connection**: `redshift-glue-conn`
- **Write Strategy**: Append
- **Data Format**: Parquet

---

## 🧱 Redshift Table Creation

Log in to Redshift and run:

```sql
CREATE TABLE public.product_tab_def (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    seller_id VARCHAR(50),
    price DECIMAL(10,2),
    created_at TIMESTAMP
);

🔌 Create Glue Connection (CLI)

Use AWS CLI to create the Redshift connection:

aws glue create-connection --name redshift-glue-conn \
  --connection-input '{
    "Name": "redshift-glue-conn",
    "ConnectionType": "JDBC",
    "ConnectionProperties": {
      "USERNAME": "your_redshift_user",
      "PASSWORD": "your_redshift_password",
      "JDBC_CONNECTION_URL": "jdbc:redshift://your-cluster-url:5439/dev"
    },
    "PhysicalConnectionRequirements": {
      "SubnetId": "subnet-xxxxxx",
      "SecurityGroupIdList": ["sg-xxxxxx"],
      "AvailabilityZone": "us-east-1a"
    }
  }'

Make sure your Glue job role has permission to use the connection and access the Redshift cluster.
❗ Potential Errors and Troubleshooting
Issue	Cause	Fix
Crawler fails to read S3 path	Incorrect prefix or object format	Ensure folder is partitioned correctly (year=xxxx)
ETL job fails with schema mismatch	Redshift table schema differs from transformed data	Adjust ETL mapping or alter Redshift table
Redshift connection timeout	Subnet or SG issues	Ensure Redshift is in a public subnet or has VPC peering/VPN with Glue
"Access Denied" to S3 bucket	IAM role doesn't have S3 permissions	Add s3:* on bucket or path in IAM policy
Parquet to Redshift mismatch	Glue can only load Parquet with matching Redshift types	Ensure data types match exactly (e.g., VARCHAR, DECIMAL)
Seller ID filter not working	Wrong transformation logic	Use Glue’s DynamicFrame filters properly or Spark SQL expression
✅ Final Notes

    Make sure to run both crawlers before launching jobs.

    Set up job bookmarks to avoid duplicate processing if needed.

    Use job-bookmark-option: job-bookmark-enable in job parameters if processing incremental data.

🧠 Tips

    Validate intermediate data by exporting a small sample from S3 and checking Parquet schema using tools like AWS Athena or parquet-tools.

    Monitor jobs via CloudWatch Logs for debugging.