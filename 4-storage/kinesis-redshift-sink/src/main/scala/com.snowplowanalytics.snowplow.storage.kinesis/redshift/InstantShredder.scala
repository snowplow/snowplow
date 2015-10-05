package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.io.{FileWriter, FileOutputStream}
import java.net.URL
import java.util.Properties
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.databind.node.{ObjectNode, ArrayNode}
import com.github.fge.jackson.JsonLoader
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.snowplowanalytics.iglu.client.repositories.HttpRepositoryRef
import com.snowplowanalytics.iglu.client.{RepositoryRefs, Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.hadoop._
import com.snowplowanalytics.snowplow.enrich.hadoop.shredder.Shredder
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer.{EventsTableWriter, SchemaTableWriter, CopyTableWriter}
import net.minidev.json.JSONArray
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._

import scala.annotation.tailrec
import scala.language.postfixOps
import scalaz.{Failure, Success}
import scaldi.{Injector, Injectable}
import Injectable._

class InstantShredder(implicit injector: Injector) {
  val jsonPaths = scala.collection.mutable.Map[String, Option[Array[String]]]()
  val log = LogFactory.getLog(classOf[InstantShredder])
  private lazy val Mapper = new ObjectMapper
  private val props = inject[Properties]
  var file = if (props.containsKey("logFile")) new FileWriter("/tmp/shredder.txt") else null

  def shred(fields: Array[String]) = {
    if (fields.length >= 108) {
      if (file != null) {
        file.write(fields.map(f => if (f == null) "" else f).mkString("\t")+"\n")
      }
      if (log.isDebugEnabled) log.debug("Shredding " + fields.map(f => if (f == null) "" else f).mkString(","))
      val appId = fields(FieldIndexes.appId)
      val validatedEvents = ShredJob.loadAndShred2(fields.map(f => if (f == null) "" else f).mkString("\t"))(inject[Resolver])
      val eventsWriter: Option[CopyTableWriter] = TableWriter.writerByName(props.getProperty("redshift_table"), None, None, None, appId)
      eventsWriter.foreach { writer =>
        val allStored = for {
          shredded <- validatedEvents._1
          pair <- shredded
          stored = store(appId, pair._1, pair._2.toString, writer)
        } yield stored

        // Erase the fields after they've been extracted by shredding
        fields(FieldIndexes.contexts) = produceCombinedContext(validatedEvents._2)
        fields(FieldIndexes.unstructEvent) = null
        if (fields.length >= FieldIndexes.derived_contexts) {
          fields(FieldIndexes.derived_contexts) = null
        }
        try {
          writer.write(fields)
        }
        catch {
          case e:Throwable =>
            log.error("Excepting writing event", e)
        }
      }
    } else {
      log.warn("Broken line: " + fields.mkString(","))
    }
  }

  private def produceCombinedContext(nodes: scala.Iterable[Option[JsonNode]]) : String = {
    val converted = (nodes flatten).toList
    if (converted.nonEmpty) {
      val res = Mapper.createObjectNode()
      res.put("schema", "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1")
      res.put("data", Mapper.createArrayNode().addAll(converted.asJavaCollection))
      res.toString
    } else {
      null
    }
  }

  private def writerByKey(key: SchemaKey, appId: String) : Option[CopyTableWriter] = {
    val version = key.getModelRevisionAddition match {
      case Some((x:Int, y:Int, z:Int)) => x
      case _ => 1
    }
    TableWriter.writerByName(key.name, Some(key.vendor), Some(version.toString), Some(key), appId)
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
            strValue = array.toString
          case _ => ()
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
      val rootRepoURL = props.getProperty("jsonPaths")
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

  private def store(appId: String, key: SchemaKey, json: String, eventsWriter: CopyTableWriter): String = {
    try {
      if (log.isDebugEnabled) log.debug(s"Store into $key")
      val writer = writerByKey(key, appId)
      if (writer.isDefined) {
        if (writer.get.requiresJsonParsing) {
          eventsWriter.asInstanceOf[EventsTableWriter].addDependent(writer.get.asInstanceOf[SchemaTableWriter])
          val fields = fieldsMapped(key, json)
          if (fields.isDefined) {
            val fieldString: String = fields.get.mkString(",")
            if (log.isDebugEnabled) log.debug(s"Storing ($fieldString) in ${key.toPath}")
            writer.get.write(fields.get)
            "1"
          } else {
            if (fields.isEmpty) log.warn(s"Could not parse fields off $json")
            "0"
          }
        } else {
          eventsWriter.asInstanceOf[EventsTableWriter].addDependent(writer.get.asInstanceOf[SchemaTableWriter])
          writer.get.write(json)
          "1"
        }
      } else {
        log.warn(s"Writer is not defined for $key")
        "0"
      }
    }
    catch {
      case t:Throwable =>
        log.error(s"Problem storing data into $key: $json", t)
        "0"
    }
  }
  def flush() = {
    if (file != null) file.flush()
    TableWriter.flush()
  }

  def close() = {
    flush()
    if (file != null) file.close()
    TableWriter.close()
  }
}
