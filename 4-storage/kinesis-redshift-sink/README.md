# Java Kinesis Redshift Sink for SnowPlow

## Introduction

Java Kinesis Redshift Sink processes Snowplow events, in the [Canonical
Output][canonical-output] format, from an input [Amazon Kinesis][kinesis]
stream and stores them into a Redshift database.

This work derives from the sample Kinesis Applications and connectors library
included in the [amazon-kinesis-connectors][connectors] project.

Java Kinesis Redshift Sink two different ways to upload your events to
Redshift. The Kinesis Redshift Basic Sink, reads events from an input Kinesis
stream and pushes these into a local buffer; as soon as the buffer will get
enough records (configured via the `bufferRecordCountLimit` and
`bufferSizeByteLimit` settings), data are copied to an S3 bucket to be imported
into the Redshift database. 

The Kinesis Redshift Manifest Sink works almost like the Basic one but it reduces the calls
to Redshift by buffering more data to S3 and importing them into Redshift all
together via a [Manifest][redshift-manifest] file; once the Manifest is
written to S3, the Manifest Sink will publish the name of the file to the
output Kinesis stream, where it will then be read and sent to Redshift.

## Usage

Java Kinesis Redshift Sink expects its configurations to be defined in the
`src/main/resources` directory. There you can find three different files, one
for each Kinesis Application:

- `RedshiftBasic.properties`: here you configure the input Kinesis stream and the
  S3 bucket used to feed data into Redshift
- `RedshiftManifest.properties`: here you configure the input Kinesis
  stream and the S3 bucket. Note: the input stream for this Kinesis app should
  be the output stream from the one configured via the `S3Manifest.properties`
  app.
- `S3Manifest.properties`: configure the S3 bucket to store events data and
  manifest files, and the input and output Kinesis streams. The output stream
  will be used to publish the name of the Manifest file.

Notice that, you need to configure your settings before building the Java
Kinesis Sink apps, since these are included in the built jar.

## Building

Assuming you already have [Maven][mvn] installed:

```bash
$ mvn clean compile assembly:single
```

## Running

```bash
$ java -jar target/kinesis-redshift-sink-0.0.1-jar-with-dependencies.jar
```

## Copyright and license

See the [LICENSE](LICENSE.txt) file.

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[redshift-manifest]:
http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
[canonical-output]:
https://github.com/snowplow/snowplow/wiki/canonical-event-model
[connectors]: https://github.com/awslabs/amazon-kinesis-connectors
[mvn]: http://maven.apache.org/
