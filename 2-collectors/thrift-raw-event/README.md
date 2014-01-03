# Thrift Raw Event

## Introduction

This is a [Thrift] [thrift] schema for [Snowplow] [snowplow] raw events.

For the Thrift IDL please see [snowplow-raw-event.thrift] [thrift-idl]. The Thrift IDL is defined within a Scala/SBT project to produce Java sources and publish to Snowplow's maven repository. Tests are written using ScalaCheck.

## Using with Scala/SBT

1. Add the Snowplow maven repository as a [resolver][dependencies]:
   `"Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/"`
2. Check for the [newest version][versions]
3. Add this project as a [dependency][dependencies]:
   `"com.snowplowanalytics" % "snowplow-thrift-raw-event" % "0.2.0"`

## Copyright and license

The Thrift Raw Event is copyright 2013-2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: http://snowplowanalytics.com
[thrift]: http://thrift.apache.org

[thrift-idl]: https://github.com/snowplow/snowplow/blob/feature/scala-rt-coll/2-collectors/thrift-raw-event/src/main/thrift/snowplow-raw-event.thrift
[dependencies]: http://www.scala-sbt.org/release/docs/Getting-Started/Library-Dependencies.html
[versions]: http://maven.snplow.com/releases/com/snowplowanalytics/snowplow-thrift-raw-event/

[license]: http://www.apache.org/licenses/LICENSE-2.0
