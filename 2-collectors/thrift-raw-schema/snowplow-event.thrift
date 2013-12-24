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

namespace java com.snowplowanalytics.generated

struct GetPayload { /* TODO */ }
struct NVGetPayload { /* TODO */ }
struct JsonPayload { /* TODO */ }
enum PayloadType { GET = 1, NVGET = 2, JSON = 3 }

union TrackerPayload {
  1: GetPayload getPayload,
  2: NVGetPayload nvGetPayload,
  3: JsonPayload jsonPayload,
}

struct SnowplowEvent {
  01: i64 timestamp // Seconds since epoch.
  10: TrackerPayload payload
  11: PayloadType payloadType
  20: string collector // Collector name/version.
  30: string encoding
  40: optional string hostname
  41: optional string ipAddress
  50: optional string userAgent
  60: optional string refererUri
  70: optional list<string> headers
  80: optional string userId
}
