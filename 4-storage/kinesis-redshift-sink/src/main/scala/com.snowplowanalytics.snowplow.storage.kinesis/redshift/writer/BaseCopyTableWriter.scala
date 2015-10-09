package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}
import java.sql.{SQLException, Connection}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.GZIPOutputStream
import javax.sql.DataSource

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model.{StandardUnit, MetricDatum, PutMetricDataRequest}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.util.StringInputStream
import com.digdeep.util.concurrent.ThreadOnce
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.TableWriter
import org.apache.commons.logging.LogFactory
import scaldi.Injector

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps
import scala.util.Try
import scaldi.{Injector, Injectable}
import Injectable._



abstract class BaseCopyTableWriter(dataSource:DataSource, table: String)(implicit injector: Injector) extends CopyTableWriter {
  private val log = LogFactory.getLog(classOf[BaseCopyTableWriter])
  lazy val s3Manifest: String = createManifest
  lazy val bufferFileInterm: File = File.createTempFile(table + "_interm", ".tsv.gz")
  lazy val bufferFile: File = File.createTempFile(table, ".tsv.gz")
  private val props = inject[Properties]
  var batchCount: Int = 0
  var pending: Future[Unit] = null
  val updateInProgress = new AtomicBoolean()
  val threadOnce = new ThreadOnce()

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
    if (isFlushRequired) {
      flush()
    }
  }
  def requiresJsonParsing: Boolean = false

  def beforeFlushToRedshift():Int = {
    if (bufferFileStream != null) {
      bufferFileStream.flush()
      bufferFileStream.close()
      bufferFileStream = null
    }
    if (bufferFile != null && bufferFile.exists()) bufferFile.delete()
    if (bufferFileInterm != null && bufferFileInterm.exists() && bufferFile != null) bufferFileInterm.renameTo(bufferFile)
    val res = batchCount
    batchCount = 0
    if (bufferFile.exists()) {
      res
    } else {
      0
    }
  }

  def isUpdateInProgress(takeLock: Boolean = false): Boolean = !updateInProgress.compareAndSet(false, takeLock)

  def flush(): Unit = {
    if (isUpdateInProgress(true)) { // Note: this sets the flag to true essentially taking a lock
      threadOnce.callOnce(log.warn(s"Ignoring flush on $table - there is a flush in progress"))
      return
    }
    threadOnce.reset()
    log.info("Flushing events")
    val flushCount = beforeFlushToRedshift()
    val start = System.currentTimeMillis()
    pending = Future {
      var retryCount = 0
      var connection: Connection = null
      while (retryCount < 5) {
        try {
          connection = TableWriter.getConnection(dataSource)
          onFlushToRedshift(flushCount, start, Some(connection))
          retryCount = 5
        }
        catch {
          case se: SQLException =>
            log.error(s"SQLException flushing $table - retry $retryCount", se)
            retryCount += 1
            Thread.sleep(Math.round(1000 * Math.pow(4, retryCount)))
          case e:Throwable =>
            log.error(s"Non-database exception flushing $table", e)
            retryCount = 5 // break
        } finally {
          try {
            if (connection != null) connection.close()
          } catch {
            case t: Throwable =>
              log.error("Exception closing connection", t)
          }
        }
      }
    }
    pending onComplete {
      case _ =>
        pending = null
        updateInProgress.set(false)
        updateInProgress.synchronized {
          updateInProgress.notifyAll()
        }
    }
  }

  def onFlushToRedshift(flushCount: Int, start:Long, con: Option[Connection])

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

  protected def waitForPending(msg: Option[String] = None): Unit = {
    updateInProgress.synchronized {
      while (isUpdateInProgress()) {
        log.info(msg.getOrElse("Waiting for pending flush to complete"))
        updateInProgress.wait(60000)
      }
    }
  }

  def createManifest: String = {
    val s3client = new AmazonS3Client(inject[AWSCredentialsProvider])
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
