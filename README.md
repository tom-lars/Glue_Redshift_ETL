# AWS Glue ETL Project: Product Data Pipeline

## S3 Bucket Overview

We're working with a straightforward S3 storage structure:

```
s3://your-bucket/
│
├── input/
│   └── product/
│       ├── year=2021-dummy-csv/
│       └── year=2023-dummy-csv-2023/
│
├── output/
│   └── newproduct/
│
├── scripts/
│   ├── fetch_from_s3.py
│   └── send_to_redshift.py
│
└── temp/
```

## Glue Crawler Configuration 

### Crawler 1: FetchFromS3
This crawler will scan our raw product data from S3:
- **Source**: `s3://your-bucket/input/product/`
- **Target Database**: `mydatabase`
- **Generated Table**: `product`
- **Data Format**: CSV
- **Run Frequency**: Manual/as needed

### Crawler 2: FetchFromRedshift
This crawler builds metadata for our Redshift table:
- **Source Connection**: Redshift table via `redshift-glue-conn`
- **Target Database**: `mydatabase`
- **Generated Table**: `dev_public_product_tab_def`
- **Redshift Path**: `"dev"."public"."product_tab_def"`

## ETL Job Setup

### Job 1: FetchFromS3
This job handles the initial data processing:
- **Source Data**: `mydatabase.product`
- **Processing Logic**:
  - Filter out records with invalid `seller_id` values (null, empty, incorrectly formatted)
  - Remove entries containing blacklisted seller IDs
- **Output**: Parquet files written to `s3://your-bucket/output/newproduct/`

### Job 2: SendToRedshift
This job loads the processed data into our data warehouse:
- **Source**: Processed Parquet files from `s3://your-bucket/output/newproduct/`
- **Destination**: Redshift table `dev.public.product_tab_def`
- **Connection**: Uses `redshift-glue-conn`
- **Write Method**: Append (adds new records without overwriting)
- **Format**: Parquet (optimized for columnar analytics)

## Redshift Table Structure

Connect to your Redshift cluster and execute:

```sql
CREATE TABLE public.product_tab_def (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    seller_id VARCHAR(50),
    price DECIMAL(10,2),
    created_at TIMESTAMP
);
```

## Setting Up the Redshift Connection

Run this AWS CLI command to establish the Glue-Redshift connection:

```bash
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
```

Remember to ensure your Glue job role has the necessary permissions to use this connection and access Redshift.

## Troubleshooting Common Issues

| Problem | Likely Cause | Solution |
|---------|-------------|----------|
| Crawler fails on S3 path | Path structure/format issues | Verify folder partitioning format (year=xxxx) |
| Schema mismatch errors | Data types don't align between Glue and Redshift | Adjust ETL mapping or modify Redshift table definition |
| Redshift connection timeout | Network configuration problem | Ensure proper subnet/security group setup or VPC connectivity |
| "Access Denied" errors | Insufficient IAM permissions | Grant appropriate S3 permissions to the IAM role |
| Parquet-to-Redshift issues | Type incompatibility | Ensure data types match between Parquet schema and Redshift table |
| Seller ID filtering not working | Transformation logic error | Review filter implementation in DynamicFrame or SQL expressions |

## Best Practices

- Always run the crawlers before launching your ETL jobs to ensure metadata is current
- Consider enabling job bookmarks to prevent duplicate processing: 
  - Add `job-bookmark-option: job-bookmark-enable` to job parameters for incremental loads
- Validate your transformations by:
  - Sampling output data from S3
  - Using Athena to query the Parquet files directly
  - Checking output schema with tools like parquet-tools
- Monitor job execution via CloudWatch Logs for easier debugging
