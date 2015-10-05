package com.digdeep.util.iglu.client

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.cache.CacheBuilder
import com.snowplowanalytics.iglu.client._
import org.apache.commons.logging.LogFactory

import scalaz.{Success, Failure}
import collection.JavaConverters._

/**
 * Created by denismo on 5/10/15.
 */
class ErrorCachingResolver(cacheSize: Int = 500,
                           repos: RepositoryRefs, delegate: Resolver) extends Resolver(cacheSize, repos)
{
  val log = LogFactory.getLog(classOf[ErrorCachingResolver])

  val errorCache = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.MINUTES).build[SchemaKey, ValidatedNel[JsonNode]]()
  def cacheError(schemaKey: SchemaKey, res: ValidatedNel[JsonNode]): ValidatedNel[JsonNode] = {
    errorCache.put(schemaKey, res)
    res
  }

  override def lookupSchema(schemaKey: SchemaKey): ValidatedNel[JsonNode] = {
    val stored = errorCache.getIfPresent(schemaKey)
    if (stored != null) {
      return stored
    }
    val res = delegate.lookupSchema(schemaKey)
    res match {
      case Failure(e) =>
        if (e.head.getMessage.startsWith("Could not find schema with key ")) {
          cacheError(schemaKey, res)
        } else {
          res
        }
      case Success(s) => res
    }
  }
}
