package com.snowplowanalytics.snowplow.sinks

import scala.collection.JavaConverters._

// Java libs
import java.io.{
  OutputStream,
  DataOutputStream,
  ByteArrayInputStream,
  ByteArrayOutputStream,
  IOException
}
import java.util.Calendar
import java.text.SimpleDateFormat

// Java lzo
import org.apache.hadoop.conf.Configuration
import com.hadoop.compression.lzo.LzopCodec

// Elephant bird
import com.twitter.elephantbird.mapreduce.io.{
  ThriftBlockWriter
}

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

// Logging
import org.apache.commons.logging.{Log,LogFactory}

// AWS libs
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata

// AWS Kinesis connector libs
import com.amazonaws.services.kinesis.connectors.{
  UnmodifiableBuffer,
  KinesisConnectorConfiguration
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class S3Emitter(config: KinesisConnectorConfiguration) extends IEmitter[ SnowplowRawEvent ] {
  val bucket = config.S3_BUCKET
  val log = LogFactory.getLog(classOf[S3Emitter])
  val client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER)
  client.setEndpoint(config.S3_ENDPOINT)

  val lzoCodec = new LzopCodec()
  val conf = new Configuration()
  conf.set("io.compression.codecs", classOf[LzopCodec].getName)
  lzoCodec.setConf(conf)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  /**
   * Determines the filename in S3, which is the corresponding
   * Kinesis sequence range of records in the file.
   */
  protected def getFileName(firstSeq: String, lastSeq: String): String = {
    dateFormat.format(Calendar.getInstance().getTime()) +
      "-" + firstSeq + "-" + lastSeq + lzoCodec.getDefaultExtension()
  }

  /**
   * Reads items from a buffer and saves them to s3.
   *
   * This method is expected to return a List of items that
   * failed to be written out to S3, under the assumption that
   * the operation will be retried at some point later.
   */
  override def emit(buffer: UnmodifiableBuffer[ SnowplowRawEvent ]): java.util.List[ SnowplowRawEvent ] = {

    log.info("Flushing buffer with " + buffer.getRecords.size + " records.")

    val records = buffer.getRecords().asScala

    val indexOutputStream = new ByteArrayOutputStream()
    val outputStream = new ByteArrayOutputStream()

    // This writes to the underlying outputstream and indexoutput stream
    val lzoOutputStream = lzoCodec.createIndexedOutputStream(outputStream, new DataOutputStream(indexOutputStream))

    // This writes to the underlying lzo stream
    val thriftBlockWriter = new ThriftBlockWriter[SnowplowRawEvent](lzoOutputStream, classOf[SnowplowRawEvent], buffer.getRecords().size)

    // Popular the output stream with records
    records.foreach({ record =>
      try {
        thriftBlockWriter.write(record)
      } catch {
        case e: IOException => {
          log.error(e)
          buffer.getRecords
        }
      }
    })

    thriftBlockWriter.close

    val filename = getFileName(buffer.getFirstSequenceNumber, buffer.getLastSequenceNumber)
    val indexFilename = filename + ".index"
    val obj = new ByteArrayInputStream(outputStream.toByteArray)
    val indexObj = new ByteArrayInputStream(indexOutputStream.toByteArray)
    val objMeta = new ObjectMetadata()
    val indexObjMeta = new ObjectMetadata()

    objMeta.setContentLength(outputStream.size)
    indexObjMeta.setContentLength(indexOutputStream.size)

    try {
      client.putObject(bucket, filename, obj, objMeta)
      client.putObject(bucket, indexFilename, indexObj, indexObjMeta)
      log.info("Successfully emitted " + buffer.getRecords.size + " records to S3 in s3://" + bucket + "/" + filename + " with index " + indexFilename)

      // Success means we return an empty list i.e. there are no failed items to retry
      java.util.Collections.emptyList().asInstanceOf[ java.util.List[ SnowplowRawEvent ] ]
    } catch {
      case e: AmazonServiceException => {
        log.error(e)
        // This is a failure case, return the buffer items so that we can retry
        buffer.getRecords
      }
    }

  }

  override def shutdown() {
    client.shutdown
  }

  override def fail(records: java.util.List[ SnowplowRawEvent ]) {
    records.asScala.foreach { record =>
      log.error("Record failed: " + record)
    }
  }

}

