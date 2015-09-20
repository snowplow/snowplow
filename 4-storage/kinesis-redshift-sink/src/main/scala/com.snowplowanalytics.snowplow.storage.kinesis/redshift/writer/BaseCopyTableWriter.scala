package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}
import java.sql.Connection
import java.util.Properties
import java.util.zip.GZIPOutputStream
import javax.sql.DataSource

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.util.StringInputStream
import org.apache.commons.logging.LogFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

/**
 * Created by denismo on 18/09/15.
 */
abstract class BaseCopyTableWriter(dataSource:DataSource, table: String)(implicit config: KinesisConnectorConfiguration, props: Properties) extends CopyTableWriter {
  private val log = LogFactory.getLog(classOf[BaseCopyTableWriter])
  lazy val s3Manifest: String = createManifest
  lazy val bufferFileInterm: File = File.createTempFile(table + "_interm", ".tsv.gz")
  lazy val bufferFile: File = File.createTempFile(table, ".tsv.gz")
  var batchCount: Int = 0
  @volatile var pending: Future[Unit] = null

  var bufferFileStream: PrintWriter = null
  def write(values: Array[String]): Unit = {
    write(values.map(f => if (f == null) "" else f).mkString("\t"))
  }
  def write(value: String): Unit = {
    // 1. Write record to a file (create if necessary)
    // 2. Check if flush is necessary
    // 2.1 If yes, execute Redshift flush, truncate the file
    val file = getBufferFile
    file.println(value)
    batchCount += 1
    if (isFlushRequired && pending == null) {
      log.info("Flushing at " + System.currentTimeMillis())
      flush()
    }
  }
  def requiresJsonParsing: Boolean = false

  def beforeFlushToRedshift():Int = {
//    log.info(s"Before flush to redshift on $table")
    if (bufferFileStream != null) {
      bufferFileStream.flush()
      bufferFileStream.close()
      bufferFileStream = null
    }
    if (bufferFile != null && bufferFile.exists()) bufferFile.delete()
    if (bufferFileInterm != null && bufferFile != null) bufferFileInterm.renameTo(bufferFile)
    val res = batchCount
    batchCount = 0
    res
  }

  def flush(): Unit = {
    if (pending != null) {
      log.warn(s"Ignoring flush on $table - there is a flush in progress")
      return
    }
    val flushCount = beforeFlushToRedshift()
    pending = Future {
      try {
        onFlushToRedshift(flushCount, None)
      }
      catch {
        case e:Throwable =>
          log.error(s"Exception flushing $table", e)
      }
    }
    pending onComplete {
      case _ =>
        try {
          afterFlushToRedshift()
        }
        catch {
          case e:Throwable =>
            log.error(s"Exception after flush on $table", e)
        }
        pending = null
    }
  }

  def afterFlushToRedshift() = {

  }
  def onFlushToRedshift(flushCount: Int, con: Option[Connection])

  def isFlushRequired: Boolean = false

  def getBufferFile: PrintWriter = {
    if (bufferFileStream == null) {
      bufferFileStream = new PrintWriter(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(bufferFileInterm, false))))
    }
    bufferFileStream
  }
  def close() = {
    waitForPending()

    log.info(s"Closing writer for $table")
    if (bufferFileStream != null) {
      waitForPending()
      flush()
    }
    waitForPending()

    if (bufferFileStream != null) {
      bufferFileStream.close()
    }
    if (!props.containsKey("dontDeleteTempFiles")) {
      if (bufferFile != null && bufferFile.exists()) {
        bufferFile.delete()
      }
      if (bufferFileInterm != null && bufferFileInterm.exists()) {
        bufferFileInterm.delete()
      }
    }
  }

  protected def waitForPending(): Unit = {
    if (pending != null) {
      log.info("Waiting for pending flush to complete")
      Await.result(pending, 30 seconds)
    }
  }

  def createManifest: String = {
    val s3client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER)
    s3client.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_2))
    val endPoint = props.getProperty("sshEndpoint")
    val username = props.getProperty("sshUsername")
    val s3Folder = props.getProperty("sshS3Folder")
    val manifest = "{ \n\n    \"entries\": [ \n\t    {\"endpoint\":\"" + endPoint + "\", \n           " +
      "\"command\": \"cat " + bufferFile.getAbsolutePath + "\",\n           \"mandatory\":true, \n           \"username\": \"" + username + "\"} \n     ] \n}"
    val stream: StringInputStream = new StringInputStream(manifest)
    val metadata: ObjectMetadata = new ObjectMetadata()
    metadata.setContentLength(stream.available())
    s3client.putObject(s3Folder, table, stream, metadata)
    s"s3://$s3Folder/$table"
  }
}
