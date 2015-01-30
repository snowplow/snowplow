 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Java
import java.io.File
import java.util.Properties
import java.nio.ByteBuffer

// Scala
import collection.JavaConversions._

// Config
import com.typesafe.config.{Config,ConfigFactory}

// Argot
import org.clapper.argot.ArgotParser

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase
import com.amazonaws.services.kinesis.model.{
  CreateStreamRequest,
  ListStreamsRequest,
  DeleteStreamRequest,
  DescribeStreamRequest,
  Record,
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordRequest,
  PutRecordResult,
  Shard,
  GetShardIteratorRequest,
  GetRecordsRequest,
  GetRecordsResult,
  ResourceNotFoundException
}

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._


object KinesisUpDown {

  def notmain (args : Array[String]) {

    val myStreamName = if (args.length == 1){
      args(0)
    } else {
      "bigquery_enriched"
    }

    val endpoint = "kinesis.us-west-2.amazonaws.com"
    val serviceName = "AmazonKinesis"   
    val regionId = "us-west-2"
    val client = new AmazonKinesisClient
    client.setEndpoint(endpoint, serviceName, regionId)

    val myStreamSize = 1

    def checkStatus(){

      val describeStreamRequest = new DescribeStreamRequest
      describeStreamRequest.setStreamName( myStreamName )

      val startTime = System.currentTimeMillis
      val endTime = startTime + 10*60*1000
      
      def checkStatusIterator(){
        if (System.currentTimeMillis >= endTime){
          throw new RuntimeException("Stream "+myStreamName+" never went active.")
        } 
        else {

          try{
            Thread.sleep(20*1000)
          } catch {
            case ex : Exception => 
              println("Trouble sleeping")
          }

          try{
            val describeStreamResponse = client.describeStream( describeStreamRequest )
            val streamStatus = describeStreamResponse.getStreamDescription.getStreamStatus
            if ( streamStatus.equals("ACTIVE") ) {
              println("Stream is active")
            }
            else if ( streamStatus.equals("DELETING") ) {
              println("Stream is deleting")
            }
            else {
              checkStatusIterator()
            }
            try {
              Thread.sleep( 1000 );
            } catch {
              case ex: Exception => {}
            }           
          } catch {
            case ex: ResourceNotFoundException => {}
          }
          
        }
      }
      checkStatusIterator()
    }

    def createAStream() {
      try {
        val createStreamRequest = new CreateStreamRequest
        createStreamRequest.setStreamName(myStreamName)
        createStreamRequest.setShardCount(myStreamSize)

        client.createStream(createStreamRequest)

        checkStatus()
      }
    }

    def deleteAStream() {

      val deleteStreamRequest = new DeleteStreamRequest
      deleteStreamRequest.setStreamName( myStreamName )

      client.deleteStream( deleteStreamRequest )

      checkStatus()

    }

    def listTheStreams() {
      
      val listStreamRequest = new ListStreamsRequest
      listStreamRequest.setLimit(20)

      val listStreamResult = client.listStreams( listStreamRequest )
      val streamNames = listStreamResult.getStreamNames

      streamNames.foreach(x => println("Stream name is: ", x) )

    }

    def getShardInfo( streamName: String ): List[Shard] = {
      
      val describeStreamRequest = new DescribeStreamRequest
      describeStreamRequest.setStreamName ( streamName )
      
      def getShardId(shards: List[Shard], exclusiveStartShardId: String): List[Shard] = {

        if (!exclusiveStartShardId.isEmpty) {
          describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId )
        }

        val describeStreamResult = client.describeStream( describeStreamRequest )
        val shardsNew = List.concat( shards, describeStreamResult.getStreamDescription.getShards)

        if (describeStreamResult.getStreamDescription.getHasMoreShards && shardsNew.size > 0){
          getShardId(shardsNew, shardsNew.get(shardsNew.size - 1).getShardId)
        } else {
          shardsNew
        }

      }
      val emptyList = List[Shard]() 
      getShardId(emptyList, "")
    }

    def putARecord() {
      val results = 
        for (i <- 0 to 99) yield {
          val putRecordRequest = new PutRecordRequest
          putRecordRequest.setStreamName( myStreamName )
          val dataString = "Data entry no:" + i.toString
          putRecordRequest.setData(ByteBuffer.wrap(dataString.getBytes))
          putRecordRequest.setPartitionKey("Partion key:"+i.toString)
          val putRecordResult = client.putRecord( putRecordRequest ) 
          (putRecordRequest, putRecordResult)
        }

      //results.foreach( x => println(x._1.toString, x._2.toString))
    }

    def getAShard(): String = {
      val getShardIteratorRequest = new GetShardIteratorRequest
      val listOfShards: List[Shard] = getShardInfo( myStreamName )
      val shard: Shard = listOfShards(0)
      getShardIteratorRequest.setStreamName(myStreamName)
      getShardIteratorRequest.setShardId(shard.getShardId)
      getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
      val result = client.getShardIterator(getShardIteratorRequest)
      result.getShardIterator
    }

    def getSomeRecords(): GetRecordsResult = {
      val getRecordsRequest = new GetRecordsRequest
      getRecordsRequest.setShardIterator(getAShard())
      getRecordsRequest.setLimit(25)

      client.getRecords( getRecordsRequest )
    }

    def printTheRecords( records: GetRecordsResult ){
      println("This bit being called")
      val recordList = records.getRecords
      recordList.foreach{ record => 
        val byteBuffer: ByteBuffer = record.getData
        val recordBytes: Array[Byte] = byteBuffer.array
        val recordString: String = new String(recordBytes)
        println("recordString: ", recordString)
      }
    }

    //def putSomeRecords() {
      
      //val putRecordsRequest = new PutRecordsRequest
      //putRecordsRequest.setStreamName( myStreamName )
      //val putRecordsRequestEntryList = 
        //for {i <- 0 to 99} yield {
          //val putRecordsRequestEntry = new PutRecordsRequestEntry
          //val dataString = "Data entry no:" + i.toString
          //putRecordsRequestEntry.setData(ByteBuffer.wrap(dataString.getBytes))
          //putRecordsRequestEntry.setPartitionKey("Partion key:"+i.toString)
          //putRecordsRequestEntry
        //}
      //println("putRecordsRequestEntryList: ", putRecordsRequestEntryList)
      //putRecordsRequest.setRecords(putRecordsRequestEntryList)
      //val putRecordsResult = client.putRecords(putRecordsRequest)
      //println( "putRecordsResult: ", putRecordsResult )

    //}

    //createAStream()
    //listTheStreams()
    //getShardInfo( myStreamName )
    //putARecord()
    //Thread.sleep(5*1000)
    //println( getAShard() )
    //val records: GetRecordsResult = getSomeRecords()
    //printTheRecords( records )
    //deleteAStream()

  }

}

