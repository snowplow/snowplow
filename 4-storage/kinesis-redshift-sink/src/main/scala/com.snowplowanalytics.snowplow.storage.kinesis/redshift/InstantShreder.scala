package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.net.URL
import java.util.Properties
import javax.sql.DataSource

import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.fge.jackson.JsonLoader
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.snowplowanalytics.iglu.client.repositories.HttpRepositoryRef
import com.snowplowanalytics.iglu.client.{RepositoryRefs, Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.hadoop._
import net.minidev.json.JSONArray
import org.apache.commons.logging.LogFactory

import scala.annotation.tailrec
import scalaz.{Failure, Success}

/**
 * Created by denismo on 17/07/15.
 */
class InstantShreder(dataSource : DataSource)(implicit resolver: Resolver, props: Properties) {


  val writers = scala.collection.mutable.Map[String, Option[TableWriter]]()
  val jsonPaths = scala.collection.mutable.Map[String, Option[Array[String]]]()
  implicit val _dataSource: DataSource = dataSource
  val log = LogFactory.getLog(classOf[InstantShreder])

  def shred(fields: Array[String]) = {
    val validatedEvents = ShredJob.loadAndShred(fields.mkString("\t"))
    val events = for {
      validated <- validatedEvents match {
        case Success(nel @ _ :: _) =>
          Some(nel) // (Non-empty) List -> Some(List) of JsonSchemaPairs
        case Failure(nel)          =>
          log.error(nel.toString())
          None      // Discard
        case _ => None
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

  private def writerByKey(key: SchemaKey) : Option[TableWriter] = {
    val version = key.getModelRevisionAddition match {
      case Some((x:Int, y:Int, z:Int)) => x
      case _ => 1
    }
    val (dbSchema, dbTable) = if (key.name.contains(".")) {
      (key.name.substring(0, key.name.indexOf(".")), key.name.substring(key.name.indexOf(".") + 1))
    } else {
      (props.getProperty("defaultSchema"), key.name)
    }
    val tableName = s"$dbSchema." + s"${key.vendor}_${dbTable}_$version".replaceAllLiterally(".","_")
    if (!writers.contains(tableName)) {
      if (TableWriter.tableExists(tableName)) {
        writers += tableName -> Some(new TableWriter(dataSource, tableName))
      } else {
        log.error(s"Table does not exist for $tableName")
        writers += tableName -> None
      }
    }
    writers(tableName)
  }

  private def fieldsMapped(key: SchemaKey, json: String) : Option[Array[String]] = {
    val jsonPaths = getJsonPaths(key)
    if (jsonPaths.isDefined) {
      val fields = scala.collection.mutable.ArrayBuffer[String]()
      val jsonObj = JsonPath.using(Configuration.defaultConfiguration().addOptions(com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL)).parse(json)
      jsonPaths.get.foreach { path =>
        val value: Object = jsonObj.read(path)
        var strValue: String = null
        if (value == null) {
          strValue = null
        } else value match {
          case strValue1: String =>
            strValue = strValue1
          case array: JSONArray =>
            strValue = array.get(0).asInstanceOf[String]
          case _ =>
        }

        fields += (if (value == null) null else value.toString)
      }
      Some(fields.toArray)
    } else None
  }

  private def lookupRepoUrl(schemaKey: SchemaKey, allRepos: RepositoryRefs): String = {

    @tailrec def recurse(schemaKey: SchemaKey, tried: RepositoryRefs, remaining: RepositoryRefs): String = {
      remaining match {
        case Nil => null
        case repo :: repos =>
          repo.lookupSchema(schemaKey) match {
            case Success(Some(schema)) => repo.asInstanceOf[HttpRepositoryRef].uri.toString
            case Success(None)         => recurse(schemaKey, tried.::(repo), repos)
            case Failure(e)            => recurse(schemaKey, tried.::(repo), repos)
          }
      }
    }
    def prioritizeRepos(schemaKey: SchemaKey): RepositoryRefs =
      allRepos.sortBy(r =>
        (!r.vendorMatched(schemaKey), r.classPriority, r.config.instancePriority)
      )

    recurse(schemaKey, Nil, prioritizeRepos(schemaKey))
  }

  private def getJsonPaths(key: SchemaKey) : Option[Array[String]] = {
    val version = key.getModelRevisionAddition match {
      case Some((x:Int, y:Int, z:Int)) => x
      case _ => 1
    }
    val mapKey = s"${key.vendor}/${key.name}_$version"
    if (!jsonPaths.contains(mapKey)) {
      val rootRepoURL = lookupRepoUrl(key, resolver.repos)
      if (rootRepoURL == null) {
        log.warn(s"Unable to determine repo URI for $key")
        return None
      }
      val jsonPath = s"$rootRepoURL/jsonpaths/${key.vendor}/${key.name}_$version.json"
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

  private def store(key: SchemaKey, json: String): Unit = {
    log.info(s"Store into $key")
    val writer = writerByKey(key)
    val fields = fieldsMapped(key, json)
    if (writer.isDefined && fields.isDefined) {
      val fieldString: String = fields.get.mkString(",")
      log.info(s"Storing ($fieldString) in ${key.toPath}")
      writer.get.write(fields.get)
    } else {
      if (writer.isEmpty) log.warn(s"Writer is not defined for $key")
      if (fields.isEmpty) log.warn(s"Could not parse fields off $json")
    }
  }
  def finished() = {
    writers.values.foreach(_.foreach(_.finished()))
  }
}
