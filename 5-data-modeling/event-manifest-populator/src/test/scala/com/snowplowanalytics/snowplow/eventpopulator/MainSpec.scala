/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.eventpopulator

// joda time
import org.joda.time.DateTime

// specs2
import org.specs2.Specification

class MainSpec extends Specification { def is = s2"""

  This is a specification for the Event Populator CLI parser

    Main class should correctly parse S3 path and dynamodb options $e1
    Main class should correctly parse since option $e2
    Main class should reject dynamodb config invalidated by Iglu resolver $e3
  """

  def e1 = {
    val argv =
      ("--enriched-archive s3://enriched/archive " +
       "--storage-config eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy5zdG9yYWdlL2FtYXpvbl9keW5hbW9kYl9jb25maWcvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsibmFtZSI6IkFXUyBEeW5hbW9EQiBkdXBsaWNhdGVzIHN0b3JhZ2UiLCJhY2Nlc3NLZXlJZCI6IkFERCBIRVJFIiwic2VjcmV0QWNjZXNzS2V5IjoiQUREIEhFUkUiLCJhd3NSZWdpb24iOiJBREQgSEVSRSIsImR5bmFtb2RiVGFibGUiOiJBREQgSEVSRSIsInB1cnBvc2UiOiJEVVBMSUNBVEVfVFJBQ0tJTkcifX0= " +
       "--resolver eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0yIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="
      ).split(" ")

    val jobConf = Main.parse(argv).flatMap(_.toOption)
    val expectedS3Path = "enriched/archive/"
    val expectedStorageName = "AWS DynamoDB duplicates storage"

    val pathResult = jobConf.map(_.enrichedInBucket) must beSome(expectedS3Path)
    val nameResult = jobConf.map(_.storageConfig.asInstanceOf[DuplicateStorage.DynamoDbConfig].name) must beSome(expectedStorageName)

    pathResult.and(nameResult)
  }

  def e2 = {
    val argv =
      ("--enriched-archive s3://enriched/archive " +
        "--since 2016-12-10 " +
        "--storage-config eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy5zdG9yYWdlL2FtYXpvbl9keW5hbW9kYl9jb25maWcvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsibmFtZSI6IkFXUyBEeW5hbW9EQiBkdXBsaWNhdGVzIHN0b3JhZ2UiLCJhY2Nlc3NLZXlJZCI6IkFERCBIRVJFIiwic2VjcmV0QWNjZXNzS2V5IjoiQUREIEhFUkUiLCJhd3NSZWdpb24iOiJBREQgSEVSRSIsImR5bmFtb2RiVGFibGUiOiJBREQgSEVSRSIsInB1cnBvc2UiOiJEVVBMSUNBVEVfVFJBQ0tJTkcifX0= " +
        "--resolver eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0yIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="
        ).split(" ")

    val expectedSince = DateTime.parse("2016-12-10")

    val since = Main.parse(argv).flatMap(_.toOption).flatMap(_.since)

    since must beSome(expectedSince)
  }

  def e3 = {
    val argv =
      ("--enriched-archive s3://enriched/archive " +
        "--storage-config eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cuc3RvcmFnZS9hbWF6b25fZHluYW1vZGJfY29uZmlnL2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IHt9fQ== " +
        "--resolver eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0yIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUwMCwicmVwb3NpdG9yaWVzIjpbeyJuYW1lIjoiSWdsdSBDZW50cmFsIiwicHJpb3JpdHkiOjAsInZlbmRvclByZWZpeGVzIjpbImNvbS5zbm93cGxvd2FuYWx5dGljcyJdLCJjb25uZWN0aW9uIjp7Imh0dHAiOnsidXJpIjoiaHR0cDovL2lnbHVjZW50cmFsLmNvbSJ9fX1dfX0="
        ).split(" ")

    val jobConf = Main.parse(argv).get.toEither

    jobConf must beLeft
  }
}
