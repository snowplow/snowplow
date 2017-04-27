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
package com.snowplowanalytics.snowplow.enrich.hadoop

// Scalaz
import scalaz._
import Scalaz._

// joda-time
import org.joda.time.{ DateTime, DateTimeZone }
import org.joda.time.format.DateTimeFormat

// Scala
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

// AWS
import com.amazonaws.auth.{ BasicAWSCredentials, AWSStaticCredentialsProvider }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.amazonaws.services.dynamodbv2.document.Table

// JSON
import org.json4s.{ JValue, DefaultFormats }
import org.json4s.jackson.JsonMethods.fromJsonNode
import com.fasterxml.jackson.databind.JsonNode

// Iglu
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._

// SCE
import com.snowplowanalytics.snowplow.enrich.common.ValidatedMessage

/**
  * Common trait for duplicate storages, storing triple of event attributes,
  * allowing to identify duplicated events across batches.
  * Currently, single implementation is `DynamoDbStorage`.
  */
trait DuplicateStorage {

  /**
    * Try to store parts of the event into duplicates table
    *
    * @param eventId snowplow event id (UUID)
    * @param eventFingerprint enriched event's fingerprint
    * @param etlTstamp timestamp of enrichment job
    * @return true if event is successfully stored in table,
    *         false if condition is failed - both eventId and fingerprint are already in table
    */
  def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean
}


/**
  * Companion object holding ADT for possible configurations and concrete implementations
  */
object DuplicateStorage {

  /**
    * Default format (etl_tstamp, dvce_sent_tstamp, dvce_created_tstamp)
    * @see `com.snowplowanalytics.snowplow.enrich.hadoop.inputs.EnrichedEventLoader`
    */
  val RedshiftTstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)

  /**
    * ADT to hold all possible types for duplicate storage configurations
    */
  sealed trait DuplicateStorageConfig

  /**
    * Configuration required to use duplicate-storage in dynamodb
    * This instance supposed to correspond to `iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/1-0-*`
    * Class supposed to used only for extracting and carrying configuration.
    * For actual interaction `DuplicateStorage` should be used
    *
    * @param name arbitrary human-readable name for storage target
    * @param accessKeyId AWS access key id
    * @param secretAccessKey AWS secret access key
    * @param awsRegion AWS region
    * @param dynamodbTable AWS DynamoDB to store duplicates
    */
  case class DynamoDbConfig(name: String, accessKeyId: String, secretAccessKey: String, awsRegion: String, dynamodbTable: String) extends DuplicateStorageConfig

  object DynamoDbConfig {

    implicit val formats = DefaultFormats

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON,
      * just extracted from CLI arguments (therefore wrapped in `Validation[Option]`
      * Lifted version of `extractFromBase64`
      *
      * @param base64Config base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param igluResolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extract(base64Config: ValidatedMessage[Option[String]], igluResolver: ValidatedNel[Resolver]): ValidatedNel[Option[DynamoDbConfig]] = {
      val nestedValidation = (base64Config.toValidationNel |@| igluResolver) { (config: Option[String], resolver: Resolver) =>
        config match {
          case Some(encodedConfig) =>
            for { config <- extractFromBase64(encodedConfig, resolver) } yield config.some
          case None => none.successNel
        }
      }

      nestedValidation.flatMap(identity)
    }

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON.
      * Also checks that JSON is valid
      *
      * @param base64 base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param resolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extractFromBase64(base64: String, resolver: Resolver): ValidatedNel[DynamoDbConfig] = {
      ShredJobConfig.base64ToJsonNode(base64, ShredJobConfig.DuplicateStorageConfigArg)  // Decode
        .toValidationNel                                                                 // Fix container type
        .flatMap { node: JsonNode => node.validate(dataOnly = true)(resolver) }          // Validate against schema
        .map(fromJsonNode)                                                               // Transform to JValue
        .flatMap { json: JValue =>                                                       // Extract
          Validation.fromTryCatch(json.extract[DynamoDbConfig]).leftMap(e => toProcMsg(e.getMessage)).toValidationNel
        }
    }
  }

  /**
    * Initialize storage object from configuration.
    * This supposed to be a universal constructor for `DuplicateStorage`,
    *
    * @param config all configuration required to instantiate storage
    * @return valid storage if no exceptions were thrown
    */
  def initStorage(config: DuplicateStorageConfig): Validated[DuplicateStorage] =
    config match {
      case DynamoDbConfig(_, accessKeyId, secretAccessKey, awsRegion, tableName) =>
        try {
          val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
          val client = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(awsRegion)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build()
          val table = DynamoDbStorage.getOrCreateTable(client, tableName)
          new DynamoDbStorage(client, table).success
        } catch {
          case NonFatal(e) =>
            toProcMsg("Cannot initialize duplicate storage:\n" + Option(e.getMessage).getOrElse("")).failure
        }
    }

  /**
    * Primary duplicate storage object, supposed to handle interactions with DynamoDB table
    * These objects encapsulate DynamoDB client, which contains lot of mutable state,
    * references and unserializable objects - therefore it should be constructed on last step -
    * straight inside `ShredJob`. To initialize use `initStorage`
    *
    * @param client AWS DynamoDB client object
    * @param table DynamodDB table name with duplicates
    */
  class DynamoDbStorage private[hadoop](client: AmazonDynamoDB, table: String) extends DuplicateStorage {
    import DynamoDbStorage._

    def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean = {
      val etl: Int = (DateTime.parse(etlTstamp, RedshiftTstampFormat).getMillis / 1000).toInt
      val ttl = etl + 15552000   // etl plus 180 days
      val putRequest = putRequestDummy
        .withExpressionAttributeValues(Map(":tst" -> new AttributeValue().withN(etl.toString)).asJava)
        .withItem(attributeValues(List(
          eventIdColumn -> eventId,
          fingerprintColumn -> eventFingerprint,
          etlTstampColumn -> etl,
          timeToLiveColumn -> ttl)
        ))

      try {
        client.putItem(putRequest)
        true
      } catch {
        case _: ConditionalCheckFailedException => false
      }
    }

    /**
      * Dummy request required with conditional write to set all attributes and `tst` value attribute
      * According to condition, write will pass if both eventId AND fingerprint are not present in table,
      * effectively meaning that only non-dupes will pass
      * Dupes can pass iff event's etl timestamp matches, effectively meaning that previous shred was
      * interrupted and we're overriding same event
      */
    private val putRequestDummy: PutItemRequest = new PutItemRequest()
      .withTableName(table)
      .withConditionExpression(s"(attribute_not_exists($eventIdColumn) AND attribute_not_exists($fingerprintColumn)) OR $etlTstampColumn = :tst")
  }

  object DynamoDbStorage {

    val eventIdColumn = "eventId"
    val fingerprintColumn = "fingerprint"
    val etlTstampColumn = "etlTime"
    val timeToLiveColumn = "ttl"

    /**
      * Indempotent action to create duplicates table
      * If table with name already exists - do nothing and just return name
      * If table doesn't exist - create table with predefined structure and return name
      * This is blocking operation as opposed to `DynamoDB#createTable`
      *
      * @param client AWS DynamoDB client with established connection
      * @param name DynamoDB table name
      * @return same table name or throw exception
      */
    def getOrCreateTable(client: AmazonDynamoDB, name: String): String = {
      val request = new DescribeTableRequest().withTableName(name)
      val result = try {
        Option(client.describeTable(request).getTable)
      } catch {
        case _: ResourceNotFoundException => None
      }
      result match {
        case Some(description) =>
          waitForActive(client, name, description)
          name
        case None => createTable(client, name).getTableName
      }
    }

    /**
      * Create DynamoDB table with indexes designed to store event duplicate information
      * This is blocking operation as opposed to `DynamoDB#createTable`
      *
      * @param client AWS DynamoDB client with established connection
      * @param name DynamoDB table name
      * @return table description object
      */
    private[hadoop] def createTable(client: AmazonDynamoDB, name: String): TableDescription = {
      val pks = List(
        new AttributeDefinition(eventIdColumn, ScalarAttributeType.S),
        new AttributeDefinition(fingerprintColumn, ScalarAttributeType.S))
      val schema = List(
        new KeySchemaElement(eventIdColumn, KeyType.HASH),
        new KeySchemaElement(fingerprintColumn, KeyType.RANGE))
      
      val request = new CreateTableRequest()
        .withTableName(name)
        .withAttributeDefinitions(pks.asJava)
        .withKeySchema(schema.asJava)
        .withProvisionedThroughput(new ProvisionedThroughput(100L, 100L))
      val response = client.createTable(request)

      val description = waitForActive(client, name, response.getTableDescription)
      val ttlSpecification = new TimeToLiveSpecification()
        .withAttributeName(timeToLiveColumn)
        .withEnabled(true)
      val ttlRequest = new UpdateTimeToLiveRequest()
        .withTableName(name)
        .withTimeToLiveSpecification(ttlSpecification)
      client.updateTimeToLive(ttlRequest) // Update when table is active

      description
    }

    /**
     * Blocking method to reassure that table is available for read
     */
    @throws[ResourceNotFoundException]
    private def waitForActive(client: AmazonDynamoDB, name: String, description: TableDescription) = {
      new Table(client, name, description).waitForActive()
    }

    /**
     * Helper method to transform list arguments into DynamoDB-compatible item with its
     * attributes
     *
     * @param attributes list of pairs of names and values, where values are string
     *                    and integers only
     * @return Java-compatible Hash-map
     */
    private[hadoop] def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, AttributeValue] =
      attributes.toMap.mapValues(asAttributeValue).asJava

    /**
     * Convert **only** string and integer to DynamoDB-compatible object
     */
    private def asAttributeValue(v: Any): AttributeValue = {
      val value = new AttributeValue
      v match {
        case s: String => value.withS(s)
        case n: java.lang.Number => value.withN(n.toString)
        case _ => null
      }
    }
  }
}
