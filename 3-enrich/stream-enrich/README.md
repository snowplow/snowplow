# Stream Enrich

## Introduction

Stream Enrich processes raw [Snowplow][snowplow] events from an input
[Amazon Kinesis][kinesis] stream and stores enriched events
into output Kinesis streams.
Events are enriched using the [scala-common-enrich][common-enrich] library.

## Building

Assuming you already have [SBT 0.13.0][sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-enrich/stream-enrich
    $ sbt compile
    
## Usage

Stream Enrich has the following command-line interface:

```
snowplow-stream-enrich: Version 0.9.0.
Usage: snowplow-stream-enrich [options]

--config <filename>
--resolver <resolver uri>
                         Iglu resolver file, 'file:[filename]' or 'dynamodb:[region/table/key]'
--enrichments <enrichments directory uri>
                         Direcotry of enrichment configuration JSONs, 'file:[filename]' or 'dynamodb:[region/table/key]'
```

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
[common-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-common-enrich
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[configuring-enrichments]: https://github.com/snowplow/snowplow/wiki/5-Configuring-enrichments
[iglu-client-configuration]: https://github.com/snowplow/iglu/wiki/Iglu-client-configuration

[license]: http://www.apache.org/licenses/LICENSE-2.0
