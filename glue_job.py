import sys
import io
import uuid

from urllib.parse import urlparse

import boto3
import pandas as pd

from awsglue.utils import getResolvedOptions


# Read arguments passed by the Glue job configuration
args = getResolvedOptions(
    sys.argv,
    [
        "INPUT_PATH",
        "OUTPUT_PATH"
    ]
)

input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"]


# Python shell jobs have no Spark context, so S3 is read and written with boto3
s3 = boto3.client("s3")


def split_s3_uri(uri):
    # Split an s3://bucket/key URI into its bucket and key parts
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def list_input_keys(bucket, key):
    # Support either a single file or every CSV under a folder prefix
    if key and not key.endswith("/"):
        return [key]

    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for obj in page.get("Contents", []):
            # Skip folder placeholder objects
            if obj["Key"].endswith("/"):
                continue
            keys.append(obj["Key"])

    return keys


def read_csv(bucket, key):
    # Read one CSV object from S3 into a DataFrame
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_csv(io.BytesIO(body))


# Resolve the input into the list of CSV objects to process
input_bucket, input_key = split_s3_uri(input_path)
input_keys = list_input_keys(input_bucket, input_key)

if not input_keys:
    raise Exception(f"No input files found at {input_path}")


# Read and union every matched CSV file into one DataFrame
df = pd.concat(
    [read_csv(input_bucket, key) for key in input_keys],
    ignore_index=True
)


# --- Transformation ---

# Ensure salary is numeric before calculating bonus
df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

# Drop rows where salary could not be parsed
df = df.dropna(subset=["salary"])

# Add a new column: bonus = 10% of salary, rounded to 2 decimals
df["bonus"] = (df["salary"] * 0.10).round(2)


# --- Write output ---

output_bucket, output_prefix = split_s3_uri(output_path)

if output_prefix and not output_prefix.endswith("/"):
    output_prefix += "/"

output_key = f"{output_prefix}part-{uuid.uuid4().hex}.csv"

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=True)

s3.put_object(
    Bucket=output_bucket,
    Key=output_key,
    Body=buffer.getvalue().encode("utf-8")
)

print(f"Read {len(input_keys)} input file(s) from {input_path}")
print(f"Wrote {len(df)} rows to s3://{output_bucket}/{output_key}")
