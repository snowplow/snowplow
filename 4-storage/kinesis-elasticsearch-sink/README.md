# Kinesis Elasticsearch Sink

## Introduction

The Kinesis Elasticsearch Sink consumes Snowplow enriched events or failed events from an [Amazon Kinesis][kinesis] stream, transforms them to JSON, and writes them to [Elasticsearch][elasticsearch]. Events which cannot be transformed or which are rejected by Elasticsearch are written to a separate Kinesis stream.

## Building

Assuming you already have [SBT 0.13.8] [sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 4-storage/kinesis-elasticsearch-sink
    $ sbt assembly

The application compiles to `target/scala-2.10/snowplow-elasticsearch-sink-0.4.1`.

## Usage

The Kinesis Elasticsearch Sink has the following command-line interface:

```
Usage: snowplow-elasticsearch-sink [OPTIONS]

OPTIONS
--config filename
                   Configuration file
```

## Configuring

Create your own config file:

    $ cp src/main/resources/config.hocon.sample my.conf

Edit it and provide the necessary values.

| Key Name                            | Description |
|-------------------------------------|-------------|
| `kinesis.app-name`                  | Used to maintain stream state |
| `kinesis.in.stream-name`            | Kinesis stream name |
| `kinesis.in.stream-type`            | "good" for successfully enriched events or "bad" for rejected events |
| `kinesis.out.stream-name`           | Stream for enriched events which are rejected by Elasticsearch |
| `elasticsearch.cluster-name`        | Elasticsearch cluster name |
| `elasticsearch.endpoint`            | Elasticsearch endpoint URI |
| `elasticsearch.port`                | Elasticsearch port |
| `elasticsearch.max-timeout`         | Elasticsearch max timeout |
| `elasticsearch.index`               | Elasticsearch index name |
| `elasticsearch.type`                | Elasticsearch type name |
| `buffer.byte-limit`                 | Threshold for bytes in buffer |
| `buffer.record-limit`               | Threshold for records in buffer |
| `buffer.time-limit`                 | Threshold for time in buffer |
| `monitoring.snowplow.collector-uri` | Snowplow collector URI |
| `monitoring.snowplow.app-id`        | Snowplow app id |

Then, start the sink, making sure to specify your new config file:

    $ snowplow-elasticsearch-sink --config my.conf

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

## Copyright and license

Copyright 2015 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[elasticsearch]: http://www.elasticsearch.org/
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.8/sbt-launch.jar

[setup]: https://github.com/snowplow/snowplow/wiki/kinesis-elasticsearch-sink-setup
[techdocs]: https://github.com/snowplow/snowplow/wiki/kinesis-elasticsearch-sink

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[license]: http://www.apache.org/licenses/LICENSE-2.0
