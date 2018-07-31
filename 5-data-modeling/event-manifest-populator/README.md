# Event Manifest Populator

## Introduction

This is an [Apache Spark][spark] job to backpopulate a Snowplow event manifest in DynamoDB with the metadata of some or all enriched events from your archive in S3. 
This one-off job solves the "cold start" problem for identifying cross-batch natural deduplicates in Snowplow's [Hadoop Shred step][shredding].

## Usage

In order to use Event Manifest Populator, you need to have [boto2][boto]
installed:

```
$ pip install boto
```

Now you can run Event Manifest Populator with a single command (inside
event-manifest-populator directory):

```
$ python run.py run_emr $ENRICHED_ARCHIVE_S3_PATH $STORAGE_CONFIG_PATH $IGLU_RESOLVER_PATH
```

Task has three required arguments: 

1. Path to enriched events archive. It can be found in `aws.s3.buckets.enriched.archive` setting in your [config.yml][config]
2. Local path to [Duplicate storage][dynamodb-config] configuration JSON
3. Local path to [Iglu resolver][resolver] configuration JSON

Optionally, you can also pass following options:

* `--since` to reduce amount of data to be stored in DynamodDB. 
  If this option was passed Manifest Populator will process enriched events only after specified date.
  Input date supports two formats: `YYYY-MM-dd` and `YYYY-MM-dd-HH-mm-ss`.
* `--log-path` to store EMR job logs on S3. Normally, Manifest Populator does not
  produce any logs or output, but if some error occured you'll be able to
  inspect it in EMR logs stored in this path.
* `--profile` to specify AWS profile to create this EMR job.
* `--jar` to specify S3 path to custom JAR


## Building

Assuming [SBT](https://www.scala-sbt.org/) installed:

```bash
$ cd 5-data-modeling/event-manifest-populator
$ sbt assembly
```

## Copyright and License

Copyright 2017 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[spark]: http://spark.apache.org/

[boto]: http://boto.cloudhackers.com/en/latest/

[config]: https://github.com/snowplow/snowplow/blob/master/3-enrich/emr-etl-runner/config/config.yml.sample
[resolver]: https://github.com/snowplow/iglu/wiki/Iglu-client-configuration
[shredding]: https://github.com/snowplow/snowplow/wiki/Shredding

[dynamodb-config]: https://github.com/snowplow/snowplow/wiki/Configuring-storage-targets#dynamodb

[license]: http://www.apache.org/licenses/LICENSE-2.0
