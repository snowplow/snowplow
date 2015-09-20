package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.io._
import java.lang.RuntimeException
import java.sql.{Types, Timestamp, PreparedStatement, SQLException, BatchUpdateException}
import java.util.Properties
import java.util.zip.GZIPOutputStream
import javax.sql.DataSource
import java.sql.Connection

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.s3.model.{ObjectMetadata}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.util.StringInputStream
import com.snowplowanalytics.iglu.client.SchemaKey
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter.{RatioFlushLimiter, SizeFlushLimiter, FlushLimiter}
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer.{EventsTableWriter, SchemaTableWriter, BaseCopyTableWriter, CopyTableWriter}
import org.apache.commons.logging.LogFactory
import org.postgresql.ds.PGPoolingDataSource
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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

  def setBigint(value: String, stat: PreparedStatement, index: Int, size: Int) =
     if (value == null) stat.setNull(index, Types.BIGINT) else stat.setLong(index, value.toLong)
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
  def close() = {
    writers.values.foreach {
      case Some(ew: EventsTableWriter) => ew.close()
      case _ => ()
    }
  }

  def flush() = {
    writers.values.foreach {
      case Some(ew: EventsTableWriter) => ew.flush()
      case _ => ()
    }
  }

  val log = LogFactory.getLog(classOf[TableWriter])
  val writers = scala.collection.mutable.Map[String, Option[CopyTableWriter]]()

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

  def writerByName(schemaName: String, vendor: Option[String], version: Option[String], key: Option[SchemaKey], appId: String)
                  (implicit config: KinesisConnectorConfiguration, props: Properties, dataSource:DataSource): Option[CopyTableWriter] = {
    val (dbSchema, dbTable) = if (schemaName.contains(".")) {
      (schemaName.substring(0, schemaName.indexOf(".")), schemaName.substring(schemaName.indexOf(".") + 1))
    } else {
      if (props.containsKey(appId + "_schema")) {
        (props.getProperty(appId + "_schema"), schemaName)
      } else {
        (props.getProperty("defaultSchema"), schemaName)
      }
    }
    val tableName = s"$dbSchema." + ((vendor, version) match {
      case (Some(_vendor), Some(_version)) =>
        s"${_vendor}_${dbTable}_${_version}"
      case (None, Some(_version)) =>
        s"${dbTable}_${_version}"
      case (None, None) =>
        s"$dbTable"
      case (Some(_vendor), None) =>
         s"${vendor}_$dbTable"
    }).replaceAllLiterally(".", "_").replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase

    synchronized {
      if (!writers.contains(tableName)) {
        if (tableExists(tableName)) {
          log.info(s"Creating writer for $tableName, appId $appId")

          if (dbTable.startsWith("events")) {
            writers += tableName -> Some(new EventsTableWriter(dataSource, tableName))
          } else {
            writers += tableName -> Some(new SchemaTableWriter(dataSource, key.get, tableName))
          }
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
      12 -> SQLConverters.setString,
      -5 -> SQLConverters.setBigint
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
