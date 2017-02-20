/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import cats.Functor
import cats.implicits._
import cats.free.Free

import com.snowplowanalytics.iglu.core.SchemaKey

// This project
import LoaderError._
import config.Semver
import utils.Common.toSnakeCase


/**
 * Container for S3 folder with shredded JSONs ready to load
 * Usually it represents self-describing event or custom/derived context
 *
 * @param prefix full S3 path, where folder with shredded JSONs resides
 * @param vendor self-describing type's vendor
 * @param name self-describing type's name
 * @param model self-describing type's SchemaVer model
 */
case class ShreddedType(prefix: S3.Folder, vendor: String, name: String, model: Int, jsonPaths: S3.Key) {
  /**
   * Get S3 prefix which Redshift should LOAD FROM
   */
  def getObjectPath: String =
    s"$prefix$vendor/$name/jsonschema/$model-"
}

/**
 * Companion object for `ShreddedType` containing discovering functions
 */
object ShreddedType {
  
  case class ShreddedTypeInfo(prefix: S3.Folder, vendor: String, name: String, model: Int)

  /**
   * Basis for Snowplow hosted assets bucket.
   * Can be modified to match specific region
   */
  val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

  /**
   * Default JSONPaths path
   */
  val JsonpathsPath = "4-storage/redshift-storage/jsonpaths/"

  /**
   * Regex to extract `SchemaKey` from `shredded/good`
   */
  val ShreddedSubpathPattern =
    ("""vendor=(?<vendor>[a-zA-Z0-9-_.]+)""" +
     """/name=(?<name>[a-zA-Z0-9-_]+)""" +
     """/format=(?<format>[a-zA-Z0-9-_]+)""" +
     """/version=(?<schemaver>[1-9][0-9]*(?:-(?:0|[1-9][0-9]*)){2})$""").r

  /**
   * Version of legacy Shred job, where old path pattern was used
   * `com.acme/event/jsonschema/1-0-0`
   */
  val ShredJobBeforeSparkVersion = Semver(0,11,0)

  /**
   * vendor + name + format + version + filename
   */
  private val MinShreddedPathLength = 5

  /**
   * Successfully fetched JSONPaths
   * Key: "vendor/filename_1.json";
   * Value: "s3://my-jsonpaths/redshift/vendor/filename_1.json"
   */
  private val cache = collection.mutable.HashMap.empty[String, S3.Key]

  private val DiscoveryErr = Functor[List].compose[DiscoveryStep]
  private val DiscoveryErrAction = Functor[Action].compose[DiscoveryStep]

  /**
   * Searches S3 for all files that can contain shredded types
   * (not `atomic-events` and not containing special symbols `$`)
   *
   * @param shreddedGood path to S3 bucket with shredded types
   * @param shredJob version of shred job to decide what path format we're discovering
   * @param region AWS region to look for JSONPaths
   * @return sorted set (only unique values) of discovered shredded type results,
   *         where result can be either shredded type, or discovery error
   */
  def discoverShreddedTypes(
    shreddedGood: S3.Folder,
    shredJob: Semver,
    region: String,
    assets: Option[S3.Folder]
  ): Action[List[DiscoveryStep[ShreddedType]]] = {

    val keys = LoaderA.listS3(shreddedGood)
    keys.map(keys => getShreddedTypes(keys.toList, shredJob, region, assets)).flatten
  }

  /**
   * Check where JSONPaths file for particular shredded type exists:
   * in cache, in custom `s3.buckets.jsonpath_assets` S3 path or in Snowplow hosted assets bucket
   * and return full JSONPaths S3 path
   *
   * @param shreddedType some shredded type (self-describing event or context)
   * @return full valid s3 path (with `s3://` prefix)
   */
  def discoverJsonPath(region: String, jsonpathAssets: Option[S3.Folder], shreddedType: ShreddedTypeInfo): DiscoveryAction[S3.Key] = {
    val filename = s"""${toSnakeCase(shreddedType.name)}_${shreddedType.model}.json"""
    val key = s"${shreddedType.vendor}/$filename"

    cache.get(key) match {
      case Some(jsonPath) =>
        Free.pure(jsonPath.asRight)
      case None =>
        jsonpathAssets match {
          case Some(assets) =>
            val path = S3.Folder.append(assets, shreddedType.vendor)
            val s3Key = S3.Key.coerce(path + filename)
            LoaderA.keyExists(s3Key).flatMap {
              case true =>
                cache.put(key, s3Key)
                Free.pure(s3Key.asRight)
              case false =>
                getSnowplowJsonPath(region, shreddedType.vendor, filename)
            }
          case None =>
            getSnowplowJsonPath(region, shreddedType.vendor, filename)
        }
    }
  }

  /**
   * Transform list of S3 keys into list of ShreddedTypes, with their JSONPath files,
   * or errors (either occurred on ShreddedType-transformation or JSONPath-discovery)
   *
   * @param paths list of all found S3 keys
   * @param shredJob version of shred job to decide what path format we're discovering
   *
   * @return list (with only unique values) of discovered shredded type results,
   *         where result can be either shredded type, or discovery error
   */
  def getShreddedTypes(paths: List[S3.Key], shredJob: Semver, region: String, assets: Option[S3.Folder]): Action[List[Either[DiscoveryFailure, ShreddedType]]] = {
    val keys = paths.filterNot(inAtomicEvents).filterNot(specialFile)
    val infos = keys.map(transformPath(_: S3.Key, shredJob)).distinct

    val shreddedTypes = DiscoveryErr.map(infos) { info =>
      val jsonPath = discoverJsonPath(region, assets, info)
      DiscoveryErrAction.map(jsonPath)(ShreddedType(info.prefix, info.vendor, info.name, info.model, _))
    }

    flattenEffects(shreddedTypes)
  }

  /**
   * Build valid table name for some shredded type
   *
   * @param shreddedType shredded type for self-describing event or context
   * @return valid table name
   */
  def getTableName(shreddedType: ShreddedType): String =
    s"${toSnakeCase(shreddedType.vendor)}_${toSnakeCase(shreddedType.name)}_${shreddedType.model}"

  /**
   * Check that JSONPaths file exists in Snowplow hosted assets bucket
   *
   * @param s3Region hosted assets region
   * @param vendor self-describing's type vendor
   * @param filename JSONPaths filename (without prefixes)
   * @return full S3 key if file exists, discovery error otherwise
   */
  def getSnowplowJsonPath(s3Region: String, vendor: String, filename: String): DiscoveryAction[S3.Key] = {
    val hostedAssetsBucket = getHostedAssetsBucket(s3Region)
    val folder = S3.Folder.append(hostedAssetsBucket, s"$JsonpathsPath$vendor")
    val key = S3.Key.coerce(folder + filename)
    LoaderA.keyExists(key).map {
      case true => key.asRight
      case false => JsonpathDiscoveryFailure(vendor + "/" + filename).asLeft
    }
  }

  /**
   * Get Snowplow hosted assets S3 bucket for specific region
   *
   * @param region valid AWS region
   * @return AWS S3 path such as `s3://snowplow-hosted-assets-us-west-2/`
   */
  def getHostedAssetsBucket(region: String): S3.Folder = {
    val suffix = if (region == "eu-west-1") "" else s"-$region"
    S3.Folder.coerce(s"$SnowplowHostedAssetsRoot$suffix")
  }

  /**
   * Parse S3 key path into shredded type
   *
   * @param key valid S3 key
   * @param shredJob version of shred job to decide what path format should be present
   * @return either discovery failure
   */
  def transformPath(key: S3.Key, shredJob: Semver): Either[DiscoveryFailure, ShreddedTypeInfo] = {
    val (bucket, path) = S3.splitS3Key(key)
    val (subpath, shredpath) = splitFilpath(path)
    extractSchemaKey(shredpath, shredJob) match {
      case Some(schemaKey) =>
        val prefix = S3.Folder.coerce("s3://" + bucket + "/" + subpath)
        val result = ShreddedTypeInfo(prefix, schemaKey.vendor, schemaKey.name, schemaKey.version.model)
        result.asRight
      case None =>
        ShreddedTypeDiscoveryFailure(key).asLeft
    }
  }

  /**
   * Extract `SchemaKey` from subpath, which can be
   * legacy-style (pre 1.5.0) com.acme/schema-name/jsonschema/1-0-0 or
   * modern-style (post-1.5.0) vendor=com.acme/name=schema-name/format=jsonschema/version=1-0-0
   * This function transforms any of above valid paths to `SchemaKey`
   *
   * @param subpath S3 subpath of four `SchemaKey` elements
   * @param shredJob shred job version to decide what format should be present
   * @return valid schema key if found
   */
  def extractSchemaKey(subpath: String, shredJob: Semver): Option[SchemaKey] =
    if (shredJob <= ShredJobBeforeSparkVersion) SchemaKey.fromPath(subpath)
    else subpath match {
      case ShreddedSubpathPattern(vendor, name, format, version) =>
        val igluPath = s"$vendor/$name/$format/$version"
        SchemaKey.fromPath(igluPath)
      case _ => None
    }

  /**
   * Predicate to check if S3 key is in atomic-events folder
   *
   * @param key full S3 path
   * @return true if path contains `atomic-events`
   */
  def inAtomicEvents(key: String): Boolean =
    key.split("/").contains("atomic-events")

  /**
   * Predicate to check if S3 key is special file like `$folder$`
   *
   * @param key full S3 path
   * @return true if path contains `atomic-events`
   */
  def specialFile(key: String): Boolean =
    key.contains("$")

  /**
   * Split S3 filepath (without bucket name) into subpath and shreddedpath
   * Works both for legacy and modern format. Omits file
   *
   * `path/to/shredded/good/run=2017-05-02-12-30-00/vendor=com.acme/name=event/format=jsonschema/version=1-0-0/part-0001`
   * ->
   * `(path/to/shredded/good/run=2017-05-02-12-30-00/, vendor=com.acme/name=event/format=jsonschema/version=1-0-0)`
   *
   * @param path S3 key without bucket name
   * @return pair of subpath and shredpath
   */
  private def splitFilpath(path: String): (String, String) = {
    path.split("/").reverse.splitAt(MinShreddedPathLength) match {
      case (reverseSchema, reversePath) =>
        (reversePath.reverse.mkString("/"), reverseSchema.tail.reverse.mkString("/"))
    }
  }

  /**
   * Flatten deeply nested effects of discovery actions
   */
  def flattenEffects(types: List[DiscoveryStep[Action[DiscoveryStep[ShreddedType]]]]): Action[List[DiscoveryStep[ShreddedType]]] = {
    val flattenEithers: List[Action[DiscoveryStep[ShreddedType]]] =
      types.map(err => err.sequence.map(_.flatten))
    flattenEithers.sequence
  }
}