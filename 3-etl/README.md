# ETL

![architecture] [architecture-image]

## Overview

The **ETL** (extract, transform, load) process takes raw SnowPlow events logged by a [Collector] [collectors], cleans them up, enriches them and puts them into [Storage] [storage].

## Available ETL

| ETL                             | Description                                                  | Status           | Read more                    |
|---------------------------------|--------------------------------------------------------------|------------------|------------------------------|
| [hive-etl] [e1] (1)             | An ETL process built using Apache Hive                       | Production-ready | Wiki                         |
| [hadoop-etl] [e2] (2)           | An ETL process built using Scalding for Apache Hadoop        | Pre-alpha        | Coming soon                  | 
| [emr-etl-runner] [e3]           | A RubyGem for running (1) or (2) on Amazon Elastic MapReduce | Beta             | Wiki                         |  

## Contributing

We welcome community contributions to the existing ETL processes or all-new ETL processes! We have a guide to contributing ETL code coming soon on the Wiki. 

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/3-etl/3-etl.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: ./3-etl/hive-etl/
[e2]: ./3-etl/hadoop-etl/
[e3]: ./3-etl/emr-etl-runner/