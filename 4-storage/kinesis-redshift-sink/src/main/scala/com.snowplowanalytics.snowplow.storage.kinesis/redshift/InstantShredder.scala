package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.io.{FileWriter, FileOutputStream}
import java.net.URL
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.digdeep.util.concurrent.ThreadOnce
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.databind.node.{ObjectNode, ArrayNode}
import com.github.fge.jackson.JsonLoader
import com.google.common.util.concurrent.RateLimiter
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
import scala.collection
import scala.collection.JavaConverters._

import scala.annotation.tailrec
import scala.collection.parallel.mutable
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

  val rateLimiters = new ConcurrentHashMap[String, RateLimiter]()
  props.keys().asScala.toList.asInstanceOf[Seq[String]].find(p => p.endsWith("_throttle_rate"))
    .foreach { p =>
      rateLimiters.put(p.substring(0, p.length-"_throttle_rate".length), RateLimiter.create(props.getProperty(p).toInt))
    }

  def isToBeDiscarded(fields: Array[String]): Boolean = {
    if (fields.length < 108) return true
    val appId = fields(FieldIndexes.appId)
    if (props.containsKey(appId + "_schema") && props.getProperty(appId + "_schema") == "discard") {
      true
    } else {
      if (!props.contains(appId + "_throttle_rate")) {
        false
      } else {
        // Discard if cannot acquire the limiter
        !rateLimiters.get(appId).tryAcquire(1)
      }
    }
  }

  def shred(fields: Array[String]) = {
    if (!isToBeDiscarded(fields)) {
      if (file != null) {
        file.write(fields.map(f => if (f == null) "" else f).mkString("\t")+"\n")
      }
      if (log.isDebugEnabled) log.debug("Shredding " + fields.map(f => if (f == null) "" else f).mkString(","))
      val appId = fields(FieldIndexes.appId)

      val validatedEvents = ShredJob.loadAndShred2(fields.map(f => if (f == null) "" else f).mkString("\t"))(inject[Resolver])
      val eventsWriter: Option[CopyTableWriter] = TableWriter.writerByName(props.getProperty("redshift_table"), None, None, None, appId)
      eventsWriter.foreach { writer =>
        val unstored = collection.mutable.MutableList[Option[String]]()
        for (shredded <- validatedEvents._1) {
          for ((key, node) <- shredded) {
            unstored += store(appId, key, node.toString, writer)
          }
        }

        // Erase the fields after they've been extracted by shredding
        fields(FieldIndexes.contexts) = produceCombinedContext(validatedEvents._2, unstored)
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
    }
  }

  private def produceCombinedContext(nodes: scala.Iterable[Option[JsonNode]], unstored: collection.mutable.MutableList[Option[String]]) : String = {
    val converted = (nodes flatten) ++ (unstored flatten).map(Mapper.readTree).toList
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

  val threadOnce = new ThreadOnce()
  private def store(appId: String, key: SchemaKey, json: String, eventsWriter: CopyTableWriter): Option[String] = {
    try {
      val writer = writerByKey(key, appId)
      if (writer.isDefined) {
        if (writer.get.requiresJsonParsing) {
          eventsWriter.asInstanceOf[EventsTableWriter].addDependent(writer.get.asInstanceOf[SchemaTableWriter])
          val fields = fieldsMapped(key, json)
          if (fields.isDefined) {
            val fieldString: String = fields.get.mkString(",")
            if (log.isDebugEnabled) log.debug(s"Storing ($fieldString) in ${key.toPath}")
            writer.get.write(fields.get)
            None
          } else {
            if (fields.isEmpty) log.warn(s"Could not parse fields off $json")
            None
          }
        } else {
          eventsWriter.asInstanceOf[EventsTableWriter].addDependent(writer.get.asInstanceOf[SchemaTableWriter])
          writer.get.write(json)
          None
        }
      } else {
         threadOnce.callOnce(log.warn(s"Writer is not defined for $key"))
        Some(json)
      }
    }
    catch {
      case t:Throwable =>
        log.error(s"Problem storing data into $key: $json", t)
        Some(json)
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
