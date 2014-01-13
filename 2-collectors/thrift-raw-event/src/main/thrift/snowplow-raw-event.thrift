/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

namespace java com.snowplowanalytics.snowplow.collectors.thrift

enum PayloadProtocol {
  Http = 1
}

enum PayloadFormat {
  HttpGet = 1
  HttpPostUrlencodedForm = 10
  HttpPostMultipartForm = 11
}

typedef string PayloadData

struct TrackerPayload {
  1: PayloadProtocol protocol
  2: PayloadFormat format
  3: PayloadData data
}

struct SnowplowRawEvent {
  01: i64 timestamp // Milliseconds since epoch.
  20: string collector // Collector name/version.
  30: string encoding
  40: string ipAddress
  41: optional TrackerPayload payload
  45: optional string hostname
  50: optional string userAgent
  60: optional string refererUri
  70: optional list<string> headers
  80: optional string networkUserId
}
