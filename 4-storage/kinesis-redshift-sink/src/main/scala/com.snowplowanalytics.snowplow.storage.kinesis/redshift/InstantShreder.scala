package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.net.URL
import javax.sql.DataSource

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.fge.jackson.JsonLoader
import com.jayway.jsonpath.JsonPath
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey, JsonSchemaPair}
import com.snowplowanalytics.snowplow.enrich.hadoop._
import com.twitter.scalding._
import org.apache.commons.logging.LogFactory

import scalaz.Success

/**
 * Created by denismo on 17/07/15.
 */
class InstantShreder(dataSource : DataSource)(implicit resolver: Resolver) {

  val writers = scala.collection.mutable.Map[String, Option[TableWriter]]()
  val jsonPaths = scala.collection.mutable.Map[String, Option[Array[String]]]()
  implicit val _dataSource: DataSource = dataSource
  val log = LogFactory.getLog(classOf[InstantShreder])

  def shred(fields: Array[String]) = {
    val validatedEvents = ShredJob.loadAndShred(fields.mkString("\t"))
    val events = for {
      validated <- validatedEvents match {
        case Success(nel @ _ :: _) => Some(nel) // (Non-empty) List -> Some(List) of JsonSchemaPairs
        case _                     => None      // Discard
      }
    } yield for {
      pairs <- validated
    } yield pairs
    for (pairs <- events) {
      for (pair <- pairs) {
        store(pair._1, pair._2.toString)
      }
    }
  }

  def writerByKey(key: SchemaKey) : Option[TableWriter] = {
    val version = key.getModelRevisionAddition match {
      case Some((x:Int, y:Int, z:Int)) => x
      case _ => 1
    }
    val tableName = s"${key.vendor}_${key.name}_$version"
    if (!writers.contains(tableName)) {
      // TODO: Schema configuration?
      if (TableWriter.tableExists("atomic." + tableName)) {
        writers += tableName -> Some(new TableWriter(dataSource, "atomic." + tableName))
      } else {
        log.error(s"Table does not exist for $tableName")
        writers += tableName -> None
      }
    }
    writers(tableName)
  }

  def fieldsMapped(key: SchemaKey, json: String) : Option[Array[String]] = {
    val jsonPaths = getJsonPaths(key)
    if (jsonPaths.isDefined) {
      val fields = scala.collection.mutable.ArrayBuffer[String]()
      val jsonObj = JsonPath.parse(json)
      jsonPaths.get.foreach { path =>
        val value: List[String] = jsonObj.read(path)
        fields += value.head
      }
      Some(fields.toArray)
    } else None
  }

  def getJsonPaths(key: SchemaKey) : Option[Array[String]] = {
    val version = key.getModelRevisionAddition match {
      case Some((x:Int, y:Int, z:Int)) => x
      case _ => 1
    }
    val mapKey = s"${key.vendor}/${key.name}_$version"
    if (!jsonPaths.contains(mapKey)) {
      // TODO: Configuration?
      val jsonPath = s"https://s3-ap-southeast-2.amazonaws.com/digdeep-snowplow-hosted-assets/jsonpaths/${key.vendor}/${key.name}_$version.json"
      try {
        val arrayNode: ArrayNode = JsonLoader.fromURL(new URL(jsonPath)).get("jsonpaths").asInstanceOf[ArrayNode]
        val fields = scala.collection.mutable.ArrayBuffer[String]()
        for (index <- 0 until arrayNode.size()) {
          fields += arrayNode.get(index).asText
        }
        jsonPaths += mapKey -> Some(fields.toArray)
      } catch {
        case t:Throwable =>
          log.error(s"Unable to retrieve JSON paths at $jsonPath with map key $mapKey", t)
          jsonPaths += mapKey -> None
      }
    }
    jsonPaths(mapKey)
  }

  def store(key: SchemaKey, json: String): Unit = {
    val writer = writerByKey(key)
    val fields = fieldsMapped(key, json)
    if (writer.isDefined && fields.isDefined) {
      writer.get.write(fields.get)
    }
  }
}
