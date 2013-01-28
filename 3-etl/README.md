# ETL

![architecture] [architecture-image]

## Overview

The **ETL** (extract, transform, load) process takes raw SnowPlow events logged by a [Collector] [collectors], cleans them up, enriches them and puts them into [Storage] [storage].

## Available ETL

| ETL                             | Description                                                  | Status           | Read more                    |
|---------------------------------|--------------------------------------------------------------|------------------|------------------------------|
| [hive-etl] [e1] (1)             | An ETL process built using Apache Hive                       | Production-ready | Wiki                         |
| hadoop-etl (2)           | An ETL process built using Scalding for Apache Hadoop        | Pre-alpha        | Coming soon                  | 
| [emr-etl-runner] [e3]           | A RubyGem for running (1) or (2) on Amazon Elastic MapReduce | Production-ready | Wiki                         |  

## Documentation

1. [Guide to choosing an ETL module] [which-etl] discusses the merits of the different ETL modules.
2. [Setup guide] [setup] provides step-by-step instructions on setting up and running the [emr-etl-runner] [e3].

Both the above are published on the [wiki] [wiki].

## Contributing

We welcome community contributions to the existing ETL processes or all-new ETL processes! We have a guide to contributing ETL code coming soon on the Wiki. 

![Tracker](https://collector.snplow.com/i?&e=pv&page=3%20ETL%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/3-etl/3-etl.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: ./hive-etl/
[e2]: ./hadoop-etl/
[e3]: ./emr-etl-runner/
[setup]: https://github.com/snowplow/snowplow/wiki/deploying-emretlrunner
[which-etl]: https://github.com/snowplow/snowplow/wiki/choosing-an-etl-module
[wiki]: https://github.com/snowplow/snowplow/wiki
