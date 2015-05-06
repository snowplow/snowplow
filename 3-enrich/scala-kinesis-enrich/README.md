# Scala Kinesis Enrich

## Introduction

Scala Kinesis Enrich processes raw [Snowplow][snowplow] events from an input
[Amazon Kinesis][kinesis] stream and stores enriched events
into output Kinesis streams.
Events are enriched using the [scala-common-enrich][common-enrich] library.

## Building

Assuming you already have [SBT 0.13.0] [sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-enrich/scala-kinesis-enrich
    $ sbt compile
    
## Usage

Scala Kinesis Enrich has the following command-line interface:

```
snowplow-kinesis-enrich: Version 0.2.0. Copyright (c) 2013, Snowplow Analytics
Ltd.

Usage: snowplow-kinesis-enrich [OPTIONS]

OPTIONS
--config filename
                   Configuration file.

--enrichments filename
                   Directory of enrichment configuration JSONs
```

## Running

Create your own config file:

    $ cp src/main/resources/default.conf my.conf

Edit it and update the AWS credentials:

```js
aws {
  access-key: "cpf"
  secret-key: "cpf"
}
```

If you set the fields to "env", the access key and secret key will be taken from the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

Next, start the enricher, making sure to specify your new config file:

    $ sbt "run --config my.conf"

If you want to use customizable enrichments, create a directory of enrichment JSONs as described in the [Configuring Enrichments][configuring-enrichments] wiki page and pass its filepath using the --enrichments option:

    $ sbt "run --config my.conf --enrichments path/to/enrichmentsdirectory"

## Copyright and license

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
[common-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-common-enrich
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[configuring-enrichments]: https://github.com/snowplow/snowplow/wiki/5-Configuring-enrichments
[iglu-client-configuration]: https://github.com/snowplow/iglu/wiki/Iglu-client-configuration

[license]: http://www.apache.org/licenses/LICENSE-2.0
