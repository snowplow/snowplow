ADD JAR s3://${JARBUCKET}/snowplow-log-deserializers-0.4.4.jar ;

CREATE EXTERNAL TABLE `extracted_logs_${RUN_DATE}`
ROW FORMAT SERDE 'com.snowplowanalytics.snowplow.hadoop.hive.SnowPlowEventDeserializer'
LOCATION '${CLOUDFRONT_LOGS}' ;
