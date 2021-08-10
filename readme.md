# sls-s3-athena

Sample to fire a lambda on s3 object creation, which translates the data to parquet format and writes it to another part of the bucket.

Use [sls-kinesis-firehose-s3](https://github.com/d-smith/sls-kinesis-firehose-s3) for the data ingest side.


### Experiment - create athena table via hive ddl, update with glue

#### Set up

* Create table via jupyter notebook
* Create scheduled crawler by hand
* Try queries as new partitions created

#### Observations

* Glue crawler created a new table, did not update the existing table.
* Cannot force the naming of the table by adding a catalog target:


```
InvalidInputException: An error occurred (InvalidInputException) when calling the CreateCrawler operation: The crawler cannot have catalog targets mixed with other target types. Specify crawler targets that include only catalog, or only non-catalog targets
```


### Experiment

* Delete the existing table
* Run a glue crawler autoscheduled for 15 minutes

#### Observations

* Table created when crawler ran
* Queries executed successfully
* New partitions picked up, queryable

### Experiment

* Determine if we can add table partitions in advance of data landing in them...