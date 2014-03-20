# S3 Kinesis Sink for Snowplow

## Building

To build this into a fat JAR, you need to first install the [hadoop-lzo](https://github.com/twitter/hadoop-lzo) native libraries.

Once built, move the resulting jar into the `lib/` directory, then run `sbt assembly`.

You can then run this jar using `java -jar target/scala_VVVV/kinesis-s3-sink.jar --config /path/to/config.conf`
