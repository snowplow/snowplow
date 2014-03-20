# S3 Kinesis Sink for Snowplow

## Building

To build this into a fat JAR, you need to first install the [hadoop-lzo](https://github.com/twitter/hadoop-lzo) native libraries.

Once built, move the resulting jar into the `lib/` directory, then run `sbt assembly`.

You can then run this jar using `java -jar target/scala_VVVV/kinesis-s3-sink.jar --config /path/to/config.conf`

## Troubleshooting

If you see the message `WARNING: java.lang.UnsatisfiedLinkError: Cannot load liblzo2.so.2 (liblzo2.so.2: cannot open shared object file: No such
 file or directory)!` this means you need to install `lzop` and `lzop-dev`. In ubuntu `sudo apt-get install lzop liblzo2-dev`.
