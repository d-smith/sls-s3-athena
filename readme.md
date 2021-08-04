# sls-s3-athena

Sample to fire a lambda on s3 object creation, which translates the data to parquet format and writes it to another part of the bucket.

Use [sls-kinesis-firehose-s3](https://github.com/d-smith/sls-kinesis-firehose-s3) for the data ingest side.

State

* Set up basic structure to receieve event - done
* Transform event and write to parquet area - pending
* DB table set up - pending
* Query examples - pending
* Event notification on query results - interesting/pending