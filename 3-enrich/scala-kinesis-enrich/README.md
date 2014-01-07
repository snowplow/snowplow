# Scala Kinesis Enrich

## Introduction

TODO

## Building

Assuming you already have [SBT 0.13.0] [sbt] installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-enrich/scala-kinesis-enrich
    $ sbt compile
    
## Usage

Scala Kinesi Enrich has the following command-line interface:

```
TODO
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

Next, start the enricher, making sure to specify your new config file:

    $ sbt "run --config my.conf"

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
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[kinesis-ui]: https://console.aws.amazon.com/kinesis/?

[license]: http://www.apache.org/licenses/LICENSE-2.0
