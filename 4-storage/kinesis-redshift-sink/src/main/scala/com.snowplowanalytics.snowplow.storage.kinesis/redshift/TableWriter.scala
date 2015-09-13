package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.io._
import java.lang.RuntimeException
import java.sql.{Types, Timestamp, PreparedStatement, SQLException, BatchUpdateException}
import java.util.Properties
import java.util.zip.GZIPOutputStream
import javax.sql.DataSource
import java.sql.Connection

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.util.StringInputStream
import com.snowplowanalytics.iglu.client.SchemaKey
import org.apache.commons.logging.LogFactory
import org.postgresql.ds.PGPoolingDataSource
import scala.concurrent.Future
import scala.language.implicitConversions

object SQLConverters {
  val log = LogFactory.getLog(classOf[TableWriter])
  class RedshiftOps(s: String) {
    def rsBoolean: Boolean = {
      if (s == null) throw new IllegalArgumentException("boolean string is null")
      s.toLowerCase match {
        case "1" | "true" | "yes" => true
        case _ => false
      }
    }
  }
  implicit def redshiftOps(s: String): RedshiftOps = new RedshiftOps(s)
  import java.math.{BigDecimal => BD}

  def setString(value: String, stat: PreparedStatement, index: Int, size: Int) =
    stat.setString(index, if (value == null || size > value.length) value else value.substring(0, size))
  def setTimestamp(value: String, stat: PreparedStatement, index: Int, size: Int) =
    stat.setTimestamp(index, if (isNull(value)) null else Timestamp.valueOf(value))

  def isNull(value: String): Boolean = {
    value == null || "".equals(value) || "undefined".equals(value) || "null".equals(value)
  }

  def setBoolean(value: String, stat: PreparedStatement, index: Int, size: Int) =
    if (isNull(value)) stat.setNull(index, Types.BOOLEAN) else stat.setBoolean(index, value.rsBoolean)
  def setInteger(value: String, stat: PreparedStatement, index: Int, size: Int) =
    if (isNull(value)) stat.setNull(index, Types.INTEGER) else stat.setInt(index, value.toInt)
  def setDouble(value: String, stat: PreparedStatement, index: Int, size: Int) =
    if (isNull(value)) stat.setNull(index, Types.DOUBLE) else stat.setDouble(index, value.toDouble)
  def setDecimal(value: String, stat: PreparedStatement, index: Int, size: Int) =
    try {
      if (isNull(value)) stat.setNull(index, Types.DECIMAL) else stat.setBigDecimal(index, new BD(value))
    }
    catch {
      case t:Throwable =>
        log.error(s"Invalid decimal: <$value>")
        throw t
    }
}

object TableWriter {
  def flush() = {
    writers.values.foreach(_.foreach(_.finished()))
  }

  val log = LogFactory.getLog(classOf[TableWriter])
  val writers = scala.collection.mutable.Map[String, Option[TableWriter]]()

  def tableExists(tableName: String)(implicit dataSource: DataSource) : Boolean = {
    val conn = dataSource.getConnection
    try {
      val res = conn.getMetaData.getTables(null, tableName.substring(0, tableName.indexOf('.')), tableName.substring(tableName.indexOf('.') + 1), Array("TABLE"))
      try {
        res.next()
      }
      finally {
        res.close()
      }
    }
    finally {
      conn.close()
    }
  }

  def tableForWriter(props:Properties, tableName: String): String = {
    if (tableName.contains(".")) return tableName
    props.getProperty("defaultSchema") + "." + tableName
  }

  def writerByName(schemaName: String, vendor: Option[String], version: Option[String], appId: String)
                  (implicit props: Properties, dataSource:DataSource): Option[TableWriter] = {
    val (dbSchema, dbTable) = if (schemaName.contains(".")) {
      (schemaName.substring(0, schemaName.indexOf(".")), schemaName.substring(schemaName.indexOf(".") + 1))
    } else {
      if (props.containsKey(appId + "_schema")) {
        (props.getProperty(appId + "_schema"), schemaName)
      } else {
        (props.getProperty("defaultSchema"), schemaName)
      }
    }
    val tableName = (vendor, version) match {
      case (Some(_vendor), Some(_version)) =>
        s"$dbSchema." + s"${_vendor}_${dbTable}_${_version}".replaceAllLiterally(".", "_")
      case (None, Some(_version)) =>
        s"$dbSchema." + s"${dbTable}_${_version}".replaceAllLiterally(".", "_")
      case (None, None) =>
        s"$dbSchema." + s"$dbTable".replaceAllLiterally(".", "_")
      case (Some(_vendor), None) =>
        s"$dbSchema." + s"${vendor}_$dbTable".replaceAllLiterally(".", "_")
    }
    synchronized {
      if (!writers.contains(tableName)) {
        if (tableExists(tableName)) {
          log.info(s"Creating writer for $tableName, appId $appId")
          writers += tableName -> Some(new TableWriter(dataSource, tableName))
        } else {
          log.error(s"Table does not exist for $tableName")
          writers += tableName -> None
        }
      }
      writers(tableName)
    }
  }
  def getConnection(dataSource:DataSource): Connection = {
    var progressiveDelay: Int = 5
    var progressiveCount: Int = 0
    var res: Connection = null
    while (res == null) {
      try {
        res = dataSource.getConnection
        res.setAutoCommit(false)
        progressiveCount = 0
        progressiveDelay = 5
      }
      catch {
        case e: SQLException =>
          log.error("Exception getting connection", e)
          if (e.getMessage().toLowerCase().contains("connection")) {
            try {
              log.error(s"Unable to get DB connection - sleeping for ${progressiveDelay}secs")
              Thread.sleep(progressiveDelay * 1000)
              if (progressiveCount < 10) {
                progressiveDelay *= 2
                progressiveCount += 1
              }
            }
            catch {
              case i: InterruptedException =>
                throw new RuntimeException(i)
            }
          }
      }
    }
    res
  }

}

trait FlushLimiter {
  def isFlushRequired: Boolean
  def flushed(writeTime: Long)
  def onRecord(values: Array[String])
}

class SizeFlushLimiter(batchSize: Int) extends FlushLimiter {
  var batchCount: Int = 0
  override def isFlushRequired: Boolean = {
    batchCount > batchSize
  }
  override def flushed(writeTime: Long) = {
    batchCount = 0
  }

  override def onRecord(values: Array[String]) = {
    batchCount += 1
  }
}

class RatioFlushLimiter(numerator: Int, denominator: Int, minWriteTime: Long) extends FlushLimiter {
  var writeTime: Long = 0
  var lastFlushTime: Long = System.currentTimeMillis()
  override def isFlushRequired: Boolean = {
    if (writeTime == 0) {
      System.currentTimeMillis() - lastFlushTime > minWriteTime
    } else {
      (System.currentTimeMillis() - lastFlushTime) * denominator > numerator * writeTime
    }
  }
  override def flushed(writeTime: Long) = {
    this.writeTime = writeTime
  }
  override def onRecord(values: Array[String]) = {}
}

trait CopyTableWriter {
  def flushToRedshift()
}

abstract class BaseCopyTableWriter(dataSource:DataSource, table: String)(implicit config: KinesisConnectorConfiguration, props: Properties) extends CopyTableWriter {
  private val log = LogFactory.getLog(classOf[BaseCopyTableWriter])
  lazy val s3Manifest: String = createManifest
  lazy val bufferFileInterm: File = File.createTempFile(table, ".tsv.gz")
  lazy val bufferFile: File = File.createTempFile(table + "_interm", ".tsv.gz")
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
    if (isFlushRequired && (pending == null || pending.isCompleted)) {
      flushToRedshift()
    }
  }

  def beforeFlushToRedshift() = {
    bufferFileStream.close()
    bufferFileStream = null
    if (bufferFile.exists()) bufferFile.delete()
    bufferFileInterm.renameTo(bufferFile)
  }

  def flushToRedshift() = {
    beforeFlushToRedshift()
    pending = Future {
      onFlushToRedshift()
    }
    pending onComplete {
      case _ =>
        afterFlushToRedshift()
        pending = null
    }
  }

  def afterFlushToRedshift() = {

  }
  abstract def onFlushToRedshift()

  def isFlushRequired: Boolean = false

  def getBufferFile: PrintWriter = {
    if (bufferFileStream == null) {
      bufferFileStream = new PrintWriter(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(bufferFileInterm, false))))
    }
    bufferFileStream
  }
  def close() = {
    if (bufferFileStream != null) {
      flushToRedshift()
    }
    if (bufferFileStream != null) {
      bufferFileStream.close()
    }
    if (bufferFile != null && bufferFile.exists()) {
      bufferFile.delete()
    }
    if (bufferFileInterm != null && bufferFileInterm.exists()) {
      bufferFileInterm.delete()
    }
  }
  def createManifest: String = {
    val s3client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER)
    val endPoint = props.getProperty("sshEndpoint")
    val username = props.getProperty("sshUsername")
    val s3Folder = props.getProperty("sshS3Folder")
    val manifest = s"{ \n\n    \"entries\": [ \n\t    {\"endpoint\":\"$endPoint\", \n           " +
      "\"command\": \"cat " + bufferFile.getAbsolutePath + "\",\n           \"mandatory\":true, \n           \"username\": \"$username\"} \n     ] \n}"
    val stream: StringInputStream = new StringInputStream(manifest)
    val metadata: ObjectMetadata = new ObjectMetadata()
    metadata.setContentLength(stream.available())
    s3client.putObject(s3Folder, table, stream, metadata)
    s"s3://$s3Folder/$table"
  }
}

class SchemaTableWriter(dataSource:DataSource, schema: SchemaKey, table: String)(implicit props:Properties) extends BaseCopyTableWriter(dataSource, table) {
  val jsonPaths = {
    val propJsonPaths = props.getProperty("jsonPaths")
    val (major, _, _) = schema.getModelRevisionAddition.get
    val newName = schema.name.replaceAllLiterally(".","_").replaceAllLiterally("-","_")
    val vendor = schema.vendor.replaceAllLiterally(".","_").replaceAllLiterally("-", "_")
    s"$propJsonPaths/$vendor/${newName}_$major.json"
  }
  override def onFlushToRedshift() = {
    Runtime.getRuntime.exec(s"scp ${bufferFile.getAbsolutePath} Launcher:${bufferFile.getAbsolutePath}").waitFor()
    val con = TableWriter.getConnection(dataSource)
    val stat = con.createStatement()
    try {
      try {
        val accessKey = props.getProperty("s3AccessKey")
        val secretKey = props.getProperty("s3SecretKey")
        stat.execute(s"COPY $table FROM '$s3Manifest' " +
          s"CREDENTIALS 'aws_access_key_id=$accessKey;aws_secret_access_key=$secretKey' " +
          s"json '$jsonPaths' " +
          "DELIMITER '\\t' MAXERROR 1000 EMPTYASNULL FILLRECORD TRUNCATECOLUMNS  TIMEFORMAT 'auto' ACCEPTINVCHARS GZIP COMPUPDATE OFF SSH;")
      }
      finally {
        stat.close()
      }
    }
    finally {
      con.close()
    }
  }
}

class EventsTableWriter(dataSource:DataSource, table: String)(implicit props:Properties) extends BaseCopyTableWriter(dataSource, table) {
  private val log = LogFactory.getLog(classOf[EventsTableWriter])
  var dependents: List[CopyTableWriter] = List()

  // TODO Properties
  val limiter: FlushLimiter = {
    if (props.containsKey("batchSize")) {
      new SizeFlushLimiter(Integer.parseInt(props.getProperty("batchSize")))
    } else if (props.containsKey("denominator") && props.containsKey("numerator") && props.containsKey("minWriteTime")) {
      new RatioFlushLimiter(Integer.parseInt(props.getProperty("numerator")),
        Integer.parseInt(props.getProperty("denominator")), java.lang.Long.parseLong(props.getProperty("minWriteTime")))
    } else {
      throw new scala.RuntimeException("Need to specify either batchSize or numerator/denominator/minWriteTime for flush limiter")
    }
  }

  override def isFlushRequired: Boolean = limiter.isFlushRequired

  override def onFlushToRedshift() = {
    val start = System.currentTimeMillis()
    Runtime.getRuntime.exec(s"scp ${bufferFile.getAbsolutePath} Launcher:${bufferFile.getAbsolutePath}").waitFor()
    val con = TableWriter.getConnection(dataSource)
    val stat = con.createStatement()
    try {
      try {
        val accessKey = props.getProperty("s3AccessKey")
        val secretKey = props.getProperty("s3SecretKey")
        stat.execute(s"COPY $table FROM '$s3Manifest' " +
          s"CREDENTIALS 'aws_access_key_id=$accessKey;aws_secret_access_key=$secretKey' " +
          "DELIMITER '\\t' MAXERROR 1000 EMPTYASNULL FILLRECORD TRUNCATECOLUMNS  TIMEFORMAT 'auto' ACCEPTINVCHARS GZIP COMPUPDATE OFF SSH;")
      }
      finally {
        stat.close()
      }
      dependents.foreach(_.flushToRedshift())
      limiter.flushed(System.currentTimeMillis() - start)
    }
    finally {
      con.close()
    }
  }

  def addDependent(dependent: CopyTableWriter) = {
    dependents ::= dependent
  }
}

class TableWriter(dataSource:DataSource, table: String)(implicit props:Properties) {
  val log = LogFactory.getLog(classOf[TableWriter])

  type SQLApplier = (String, PreparedStatement, Int, Int) => Unit

  case class ColumnInfo(typ: Int, size: Int)

  var names = Array[String]()
  var types = Array[ColumnInfo]()
  var stat: PreparedStatement = null
  var placeholders: String = ""
  var batchCount: Int = 0
  // [1, 2, 4, 5, 8, 93, -7, 12]
  // 4 Integer
  // 8 Double
  // 1 char
  // 2 numeric
  // 5 smallint
  // 93 timestamp
  // -7 bit
  // 12 varchar
  var converters: Map[Int, SQLApplier] =
    Map(1 -> SQLConverters.setString,
      2 -> SQLConverters.setDecimal,
      4 -> SQLConverters.setInteger,
      5 -> SQLConverters.setInteger,
      8 -> SQLConverters.setDouble,
      93 -> SQLConverters.setTimestamp,
      -7 -> SQLConverters.setBoolean,
      12 -> SQLConverters.setString
    )
  readMetadata()

  private def readMetadata() = {
    val conn = TableWriter.getConnection(dataSource)
    try {
      val dbMetadata = conn.getMetaData
      val tableMetadata = dbMetadata.getColumns(null, table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), null)
      while (tableMetadata.next()) {
        names = names ++ Array(tableMetadata.getString(4))
        types = types ++ Array(ColumnInfo(tableMetadata.getInt(5), tableMetadata.getInt(7)))
      }
      log.info(s"Table $table has ${names.length} names " + names.zip(types).map(pair => pair._1 + ":" + pair._2.typ).mkString(","))
      placeholders = "?" + (",?" * (names.length - 1))
      tableMetadata.close()
    }
    finally {
      conn.close()
    }
  }


  def write(values: Array[String]) = {
    if (values.length != names.length) throw new SQLException(s"Number of values does not match number of fields: ${values.length} != ${names.length}")
    if (stat == null) {
      val conn = TableWriter.getConnection(dataSource)
      stat = conn.prepareStatement(s"insert into $table values ($placeholders)")
    }
    try {
      for (index <- values.indices) {
        converters(types(index).typ)(values(index), stat, index + 1, types(index).size)
      }
      stat.addBatch()
      batchCount += 1
    }
    catch {
      case t:Throwable =>
        log.error("Skipping due to conversion errors: " +
          values.zipWithIndex.map(pair => names(pair._2) + ":" +  pair._1).mkString(","))
      // Don't clear batch - just skip this record
    }
    try {
      if (batchCount >= Integer.parseInt(props.getProperty("batchSize"))) {
        val count = stat.executeBatch().sum
        stat.clearBatch()
        batchCount = 0
        log.info(s"Inserted $count records into Redshift table $table")
      }
    }
    catch {
      case s:BatchUpdateException =>
        log.error(values.zipWithIndex.map(pair => names(pair._2) + ":" +  pair._1).mkString(","))
        batchCount = 0
        stat.clearBatch()
        log.error("Exception updating batch", s)
        log.error("Nested exception", s.getNextException)
//        throw s
      case e:Throwable =>
        log.error(values.zipWithIndex.map(pair => names(pair._2) + ":" +  pair._1).mkString(","))
        batchCount = 0
        stat.clearBatch()
        log.error("Exception updating batch", e)
        throw e
    }
  }

  def finished() = {
    if (stat != null) {
      try {
        val count = stat.executeBatch().sum
        stat.clearBatch()
        batchCount = 0
        log.info(s"Inserted $count records into Redshift table $table")
      }
      catch {
        case s:BatchUpdateException =>
          log.error("Exception updating batch", s)
          log.error("Nested exception", s.getNextException)
          throw s
        case e:Throwable =>
          log.error("Exception updating batch", e)
          throw e
      }
      finally {
        val conn = stat.getConnection
        conn.commit()
        stat.close()
        conn.close()
        stat = null
        batchCount = 0
      }
    }
  }
}
