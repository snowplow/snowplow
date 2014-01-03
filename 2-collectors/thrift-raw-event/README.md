# Snowplow Raw Event Thrift Schema

## Introduction

This is a [Thrift] [thrift] Schema for [SnowPlow] [snowplow] raw events.
It is contained within a Scala project to produce Java sources
and publish to Snowplow's maven repository.

## Using with Scala/sbt

1. Add the Snowplow maven repository as a [resolver][dependencies]:
   `"Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/"`
2. Check for the [newest version][versions]
3. Add this project as a [dependency][dependencies]:
   `"com.snowplowanalytics" % "snowplow-thrift-raw-event" % "0.2.0"`

## Copyright and license

The Snowplow Raw Event Thrift Schema is copyright 2013-2014
Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: http://snowplowanalytics.com
[thrift]: http://thrift.apache.org

[dependencies]: http://www.scala-sbt.org/release/docs/Getting-Started/Library-Dependencies.html
[versions]: http://maven.snplow.com/releases/com/snowplowanalytics/snowplow-thrift-raw-event/

[license]: http://www.apache.org/licenses/LICENSE-2.0
