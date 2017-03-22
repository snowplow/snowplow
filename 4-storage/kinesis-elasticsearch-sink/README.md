# Kinesis Elasticsearch Sink

## Introduction

The Kinesis Elasticsearch Sink consumes Snowplow enriched events or failed events from an [Amazon Kinesis][kinesis] stream, transforms them to JSON, and writes them to [Elasticsearch][elasticsearch]. Events which cannot be transformed or which are rejected by Elasticsearch are written to a separate Kinesis stream.

## Building

Assuming you already have [SBT 0.13.0][sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 4-storage/kinesis-elasticsearch-sink
    $ sbt compile
    
## Usage

The Kinesis Elasticsearch Sink has the following command-line interface:

```
snowplow-elasticsearch-sink: Version 0.1.0. Copyright (c) 2014, Snowplow Analytics
Ltd.

Usage: snowplow-elasticsearch-sink [OPTIONS]

OPTIONS
--config filename
                   Configuration file.
```

## Running

Create your own config file:

    $ cp src/main/resources/application.conf.example my.conf

Edit it and update the AWS credentials:

```js
aws {
  access-key: "default"
  secret-key: "default"
}
```

Next, start the sink, making sure to specify your new config file:

    $ sbt "run --config my.conf"

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

## Copyright and license

Copyright 2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[elasticsearch]: http://www.elasticsearch.org/
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[setup]: https://github.com/snowplow/snowplow/wiki/kinesis-elasticsearch-sink-setup
[techdocs]: https://github.com/snowplow/snowplow/wiki/kinesis-elasticsearch-sink

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[license]: http://www.apache.org/licenses/LICENSE-2.0
