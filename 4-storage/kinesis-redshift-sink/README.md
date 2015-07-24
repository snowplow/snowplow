# Kinesis Redshift Sink

## Introduction

The Kinesis Redshift Sink consumes records from an [Amazon Kinesis][kinesis] stream, extracts all custom JSON schemas, and inserts them into corresponding DB tables.
It does it on-the-fly, drip-feeding the database with new records in a near real-time fashion. It writes both into core events table, as well as custom schema tables.

## Prerequisites

- It is designed to work with Redshift though you could drip-feed into any database provided it can handle the amount of data and throughput

## Building

Assuming you already have [SBT 0.13.0] [sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 4-storage/kinesis-redshift-sink
    $ sbt compile

## Usage

The Kinesis Redshift Sink has the following command-line interface:

```
kinesis-redsihft-sink: Version 0.1.0. Copyright (c) 2015, Digdeep Digital Pty Ltd


Usage: kinesis-redsihft-sink [OPTIONS]

OPTIONS
--config filename
                   Configuration file.
```

## Configuration file

Create your own config file:

    $ cp src/main/resources/config.hocon.sample my.conf

Edit it and update the AWS credentials:

```js
aws {
  access-key: "default"
  secret-key: "default"
}
```

### Redshift section

There is a set of setting that you need to specify, the critical ones are the connection parameters for Redshift:

```js
  redshift {   
    # Connection parameters
    url: "jdbc:postgresql://...../snowplow"
    username: ""
    password: ""
    table: ""

    # Used when no specific DB schema is associated with json
    defaultSchema: ""

   # Allows to define the mapping between appId and DB schemas, so that the events get
   # inserted into different tables depending on which application they belong to
    appIdToSchema {
      test: "test"
    }
  }
```

The ```url```, ```username```, ```password``` parameters are just the location and credentials for Redshift database.
```table``` is the table into which the master events will go (typically ```events```).

```defaultSchema``` is the name of the schema that is going to be used by default for all tables. The shredder has the ability to insert records into multiple schemas, 
using the rules outlined below. Typically the default schema in Snowplow is ```atomic```.

### Schema mappings

The shredder has the ability to insert records into multiple schemas. 2 rules apply:

- you can specify a mapping from appId in the event to a schema using the ```appIdToSchema``` section
- you can use the schema name in the JSON schema URL

```appIdToSchema``` section has the following format: it is a collection of key-value pairs. The key is the appId value, the value is the schema name. 
For each event, appId is read from the event, and unless explicitly specified, all attached JSON schemas (custom contexts, unstruct etc.) will be stored in that schema
 
In the JSON schema URL, traditionally you specify the schema name as a single word, e.g. "page". If you add schema to it, separated by "." 
it will be taken into account when inserting JSON events with such schema.  

### Running

Next, start the sink, making sure to specify your new config file:

    $ sbt "run --config my.conf"
    
### Performance

The shredder implements drip-feeding approach, where the small batches of records are continuously inserted into the database.
It works roughly as follows:

1. It reads a group of records from Kinesis (using KCL). KCL controls how many records are retrieved, and how frequently
2. It stores the records in batches of 200 records without any time buffering (but using JDBC Batch mechanism)
3. Repeat

Bad records are discarded, with error logging, and exceptions are propagated to KCL for its own bad record handling.

### Caution

While developing this project, it was discovered that for contexts with multiple custom JSONs, if one of them fails validation, an overarching failure is 
generated preventing valid JSONs to be processed. Due to lack of time, a quick workaround was applied which changes validation logic. As the result of that, 
 events with such errors will not produce errors in the Kinesis Error stream but will only produce errors in the error log of the application.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

## Copyright and license

Copyright 2015 Digdeep Digital Pty Ltd.

Copyright 2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[hadoop-lzo]: https://github.com/twitter/hadoop-lzo
[protobufs]: https://github.com/google/protobuf/
[elephant-bird]: https://github.com/twitter/elephant-bird/
[s3]: http://aws.amazon.com/s3/
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[setup]: https://github.com/snowplow/snowplow/wiki/kinesis-lzo-s3-sink-setup
[techdocs]: https://github.com/snowplow/snowplow/wiki/kinesis-lzo-s3-sink

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[license]: http://www.apache.org/licenses/LICENSE-2.0
