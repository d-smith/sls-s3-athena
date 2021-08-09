# sls-s3-athena

Sample to fire a lambda on s3 object creation, which translates the data to parquet format and writes it to another part of the bucket.

Use [sls-kinesis-firehose-s3](https://github.com/d-smith/sls-kinesis-firehose-s3) for the data ingest side.

Next experiment:

* Create table via jupyter notebook
* Create scheduled crawler by hand
* Try queries as new partitions created