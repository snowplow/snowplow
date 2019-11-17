/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package good

import org.specs2.mutable.Specification

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatMailgunEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2017-11-08  17:23:47  - - 52.205.206.237  POST  52.205.206.237  /com.mailgun/v1 200 - - &cv=clj-1.1.0-tom-0.2.0&nuid=856c2ed4-addc-45f5-bfe3-027256cf0057 - - - application%2Fx-www-form-urlencoded ZG9tYWluPXNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZyZteV92YXJfMT1NYWlsZ3VuK1ZhcmlhYmxlKyUyMzEmbXktdmFyLTI9YXdlc29tZSZtZXNzYWdlLWhlYWRlcnM9JTVCJTVCJTIyUmVjZWl2ZWQlMjIlMkMrJTIyYnkrbHVuYS5tYWlsZ3VuLm5ldCt3aXRoK1NNVFArbWdydCs4NzM0NjYzMzExNzMzJTNCK0ZyaSUyQyswMytNYXkrMjAxMysxOCUzQTI2JTNBMjcrJTJCMDAwMCUyMiU1RCUyQyslNUIlMjJDb250ZW50LVR5cGUlMjIlMkMrJTVCJTIybXVsdGlwYXJ0JTJGYWx0ZXJuYXRpdmUlMjIlMkMrJTdCJTIyYm91bmRhcnklMjIlM0ErJTIyZWI2NjNkNzNhZTBhNGQ2YzkxNTNjYzBhZWM4Yjc1MjAlMjIlN0QlNUQlNUQlMkMrJTVCJTIyTWltZS1WZXJzaW9uJTIyJTJDKyUyMjEuMCUyMiU1RCUyQyslNUIlMjJTdWJqZWN0JTIyJTJDKyUyMlRlc3QrZGVsaXZlcit3ZWJob29rJTIyJTVEJTJDKyU1QiUyMkZyb20lMjIlMkMrJTIyQm9iKyUzQ2JvYiU0MHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZyUzRSUyMiU1RCUyQyslNUIlMjJUbyUyMiUyQyslMjJBbGljZSslM0NhbGljZSU0MGV4YW1wbGUuY29tJTNFJTIyJTVEJTJDKyU1QiUyMk1lc3NhZ2UtSWQlMjIlMkMrJTIyJTNDMjAxMzA1MDMxODI2MjYuMTg2NjYuMTY1NDAlNDBzYW5kYm94NTcwNzAwNzIwNzVkNGNmZDkwMDhkNDMzMjEwODczNGMubWFpbGd1bi5vcmclM0UlMjIlNUQlMkMrJTVCJTIyWC1NYWlsZ3VuLVZhcmlhYmxlcyUyMiUyQyslMjIlN0IlNUMlMjJteV92YXJfMSU1QyUyMiUzQSslNUMlMjJNYWlsZ3VuK1ZhcmlhYmxlKyUyMzElNUMlMjIlMkMrJTVDJTIybXktdmFyLTIlNUMlMjIlM0ErJTVDJTIyYXdlc29tZSU1QyUyMiU3RCUyMiU1RCUyQyslNUIlMjJEYXRlJTIyJTJDKyUyMkZyaSUyQyswMytNYXkrMjAxMysxOCUzQTI2JTNBMjcrJTJCMDAwMCUyMiU1RCUyQyslNUIlMjJTZW5kZXIlMjIlMkMrJTIyYm9iJTQwc2FuZGJveDU3MDcwMDcyMDc1ZDRjZmQ5MDA4ZDQzMzIxMDg3MzRjLm1haWxndW4ub3JnJTIyJTVEJTVEJk1lc3NhZ2UtSWQ9JTNDMjAxMzA1MDMxODI2MjYuMTg2NjYuMTY1NDAlNDBzYW5kYm94NTcwNzAwNzIwNzVkNGNmZDkwMDhkNDMzMjEwODczNGMubWFpbGd1bi5vcmclM0UmcmVjaXBpZW50PWFsaWNlJTQwZXhhbXBsZS5jb20mZXZlbnQ9ZGVsaXZlcmVkJnRpbWVzdGFtcD0xNTEwMTYxODI3JnRva2VuPWNkODdmNWEzMDAwMjc5NGUzN2FhNDllNjdmYjQ2OTkwZTU3OGIxZTkxOTc3NzNkODE3JnNpZ25hdHVyZT1jOTAyZmY5ZTNkZWE1NGMyZGJlMTg3MWY5MDQxNjUzMjkyZWE5Njg5ZDNkMmIyZDJlY2ZhOTk2ZjAyNWI5NjY5JmJvZHktcGxhaW49",
    "2017-11-08  17:24:22  - - 52.205.206.237  POST  52.205.206.237  /com.mailgun/v1 200 - - &cv=clj-1.1.0-tom-0.2.0&nuid=8bc04475-779b-4ed4-bec3-555e737fac74 - - - multipart%2Fform-data%3B+boundary%3D353d603f-eede-4b49-97ac-724fbc54ea3c  LS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iTWVzc2FnZS1JZCINCg0KPDIwMTMwNTAzMTkyNjU5LjEzNjUxLjIwMjg3QHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZz4NCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9IlgtTWFpbGd1bi1TaWQiDQoNCld5SXdOekk1TUNJc0lDSnBaRzkxWW5SMGFHbHpiMjVsWlhocGMzUnpRR2R0WVdsc0xtTnZiU0lzSUNJMklsMD0NCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9ImF0dGFjaG1lbnQtY291bnQiDQoNCjENCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9ImJvZHktcGxhaW4iDQoNCg0KLS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iY29kZSINCg0KNjA1DQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJkZXNjcmlwdGlvbiINCg0KTm90IGRlbGl2ZXJpbmcgdG8gcHJldmlvdXNseSBib3VuY2VkIGFkZHJlc3MNCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9ImRvbWFpbiINCg0Kc2FuZGJveDU3MDcwMDcyMDc1ZDRjZmQ5MDA4ZDQzMzIxMDg3MzRjLm1haWxndW4ub3JnDQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJldmVudCINCg0KZHJvcHBlZA0KLS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0ibWVzc2FnZS1oZWFkZXJzIg0KDQpbWyJSZWNlaXZlZCIsICJieSBsdW5hLm1haWxndW4ubmV0IHdpdGggU01UUCBtZ3J0IDg3NTU1NDY3NTE0MDU7IEZyaSwgMDMgTWF5IDIwMTMgMTk6MjY6NTkgKzAwMDAiXSwgWyJDb250ZW50LVR5cGUiLCBbIm11bHRpcGFydC9hbHRlcm5hdGl2ZSIsIHsiYm91bmRhcnkiOiAiMjMwNDFiY2RmYWU1NGFhZmI4MDFhOGRhMDI4M2FmODUifV1dLCBbIk1pbWUtVmVyc2lvbiIsICIxLjAiXSwgWyJTdWJqZWN0IiwgIlRlc3QgZHJvcCB3ZWJob29rIl0sIFsiRnJvbSIsICJCb2IgPGJvYkBzYW5kYm94NTcwNzAwNzIwNzVkNGNmZDkwMDhkNDMzMjEwODczNGMubWFpbGd1bi5vcmc-Il0sIFsiVG8iLCAiQWxpY2UgPGFsaWNlQGV4YW1wbGUuY29tPiJdLCBbIk1lc3NhZ2UtSWQiLCAiPDIwMTMwNTAzMTkyNjU5LjEzNjUxLjIwMjg3QHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZz4iXSwgWyJMaXN0LVVuc3Vic2NyaWJlIiwgIjxtYWlsdG86dStuYTZ0bXkzZWdlNHRnbmxkbXl5dHFvanFtZnNkZW1ieW1lM3RteTNjaGE0d2NuZGJnYXlkcXlyZ29pNndzemRwb3ZyaGk1ZGluZnp3NjN0Zm12NGdzNDN1b21zdGltZGhudnF3czNib21ueHcyanR1aHVzdGVxamdtcTZ0bUBzYW5kYm94NTcwNzAwNzIwNzVkNGNmZDkwMDhkNDMzMjEwODczNGMubWFpbGd1bi5vcmc-Il0sIFsiWC1NYWlsZ3VuLVNpZCIsICJXeUl3TnpJNU1DSXNJQ0pwWkc5MVluUjBhR2x6YjI1bFpYaHBjM1J6UUdkdFlXbHNMbU52YlNJc0lDSTJJbDA9Il0sIFsiWC1NYWlsZ3VuLVZhcmlhYmxlcyIsICJ7XCJteV92YXJfMVwiOiBcIk1haWxndW4gVmFyaWFibGUgIzFcIiwgXCJteS12YXItMlwiOiBcImF3ZXNvbWVcIn0iXSwgWyJEYXRlIiwgIkZyaSwgMDMgTWF5IDIwMTMgMTk6MjY6NTkgKzAwMDAiXSwgWyJTZW5kZXIiLCAiYm9iQHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZyJdXQ0KLS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0ibXktdmFyLTIiDQoNCmF3ZXNvbWUNCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9Im15X3Zhcl8xIg0KDQpNYWlsZ3VuIFZhcmlhYmxlICMxDQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJyZWFzb24iDQoNCmhhcmRmYWlsDQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJyZWNpcGllbnQiDQoNCmFsaWNlQGV4YW1wbGUuY29tDQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJzaWduYXR1cmUiDQoNCjcxZjgxMjQ4NWFlM2ZiMzk4ZGU4ZDFhODZiMTM5ZjI0MzkxZDYwNGZkOTRkYWI1OWU3Yzk5Y2ZjZDUwNjg4NWMNCi0tMzUzZDYwM2YtZWVkZS00YjQ5LTk3YWMtNzI0ZmJjNTRlYTNjDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9InRpbWVzdGFtcCINCg0KMTUxMDE2MTg2Mg0KLS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0idG9rZW4iDQoNCjllM2ZmZmM3ZWJhNTdlMjgyZTg5ZjdhZmNmMjQzNTYzODY4ZTlkZTRlY2ZlYTc4YzA5DQotLTM1M2Q2MDNmLWVlZGUtNGI0OS05N2FjLTcyNGZiYzU0ZWEzYw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJhdHRhY2htZW50LTEiOyBmaWxlbmFtZT0ibWVzc2FnZS5taW1lIg0KQ29udGVudC1UeXBlOiBhcHBsaWNhdGlvbi9vY3RldC1zdHJlYW0NCkNvbnRlbnQtTGVuZ3RoOiAxMzg2DQoNClJlY2VpdmVkOiBieSBsdW5hLm1haWxndW4ubmV0IHdpdGggU01UUCBtZ3J0IDg3NTU1NDY3NTE0MDU7IEZyaSwgMDMgTWF5IDIwMTMKIDE5OjI2OjU5ICswMDAwCkNvbnRlbnQtVHlwZTogbXVsdGlwYXJ0L2FsdGVybmF0aXZlOyBib3VuZGFyeT0iMjMwNDFiY2RmYWU1NGFhZmI4MDFhOGRhMDI4M2FmODUiCk1pbWUtVmVyc2lvbjogMS4wClN1YmplY3Q6IFRlc3QgZHJvcCB3ZWJob29rCkZyb206IEJvYiA8Ym9iQHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZz4KVG86IEFsaWNlIDxhbGljZUBleGFtcGxlLmNvbT4KTWVzc2FnZS1JZDogPDIwMTMwNTAzMTkyNjU5LjEzNjUxLjIwMjg3QHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZz4KTGlzdC1VbnN1YnNjcmliZTogPG1haWx0bzp1K25hNnRteTNlZ2U0dGdubGRteXl0cW9qcW1mc2RlbWJ5bWUzdG15M2NoYTR3Y25kYmdheWRxeXJnb2k2d3N6ZHBvdnJoaTVkaW5menc2M3RmbXY0Z3M0M3VvbXN0aW1kaG52cXdzM2JvbW54dzJqdHVodXN0ZXFqZ21xNnRtQHNhbmRib3g1NzA3MDA3MjA3NWQ0Y2ZkOTAwOGQ0MzMyMTA4NzM0Yy5tYWlsZ3VuLm9yZz4KWC1NYWlsZ3VuLVNpZDogV3lJd056STVNQ0lzSUNKcFpHOTFZblIwYUdsemIyNWxaWGhwYzNSelFHZHRZV2xzTG1OdmJTSXNJQ0kySWwwPQpYLU1haWxndW4tVmFyaWFibGVzOiB7Im15X3Zhcl8xIjogIk1haWxndW4gVmFyaWFibGUgIzEiLCAibXktdmFyLTIiOiAiYXdlc29tZSJ9CkRhdGU6IEZyaSwgMDMgTWF5IDIwMTMgMTk6MjY6NTkgKzAwMDAKU2VuZGVyOiBib2JAc2FuZGJveDU3MDcwMDcyMDc1ZDRjZmQ5MDA4ZDQzMzIxMDg3MzRjLm1haWxndW4ub3JnCgotLTIzMDQxYmNkZmFlNTRhYWZiODAxYThkYTAyODNhZjg1Ck1pbWUtVmVyc2lvbjogMS4wCkNvbnRlbnQtVHlwZTogdGV4dC9wbGFpbjsgY2hhcnNldD0iYXNjaWkiCkNvbnRlbnQtVHJhbnNmZXItRW5jb2Rpbmc6IDdiaXQKCkhpIEFsaWNlLCBJIHNlbnQgYW4gZW1haWwgdG8gdGhpcyBhZGRyZXNzIGJ1dCBpdCB3YXMgYm91bmNlZC4KCi0tMjMwNDFiY2RmYWU1NGFhZmI4MDFhOGRhMDI4M2FmODUKTWltZS1WZXJzaW9uOiAxLjAKQ29udGVudC1UeXBlOiB0ZXh0L2h0bWw7IGNoYXJzZXQ9ImFzY2lpIgpDb250ZW50LVRyYW5zZmVyLUVuY29kaW5nOiA3Yml0Cgo8aHRtbD4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxib2R5PkhpIEFsaWNlLCBJIHNlbnQgYW4gZW1haWwgdG8gdGhpcyBhZGRyZXNzIGJ1dCBpdCB3YXMgYm91bmNlZC4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxicj4KPC9ib2R5PjwvaHRtbD4KLS0yMzA0MWJjZGZhZTU0YWFmYjgwMWE4ZGEwMjgzYWY4NS0tCg0KLS0zNTNkNjAzZi1lZWRlLTRiNDktOTdhYy03MjRmYmM1NGVhM2MtLQ0K"
  )

  val expected1 = List(
    null,
    "srv",
    etlTimestamp,
    "2017-11-08 17:23:47.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.mailgun-v1",
    "clj-1.1.0-tom-0.2.0",
    etlVersion,
    null, // No user_id set
    "5c2999403ebf6d9019488092d42bd0cb3062a9a1",
    null,
    null,
    null,
    "856c2ed4-addc-45f5-bfe3-027256cf0057", // TODO: fix this, https://github.com/snowplow/snowplow/issues/1133
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailgun/message_delivered/jsonschema/1-0-0","data":{"recipient":"b4a671bd13d37fef13b4aa170f22bf6d5bf2f6a3","timestamp":"2017-11-08T17:23:47.000Z","domain":"sandbox57070072075d4cfd9008d4332108734c.mailgun.org","signature":"c902ff9e3dea54c2dbe1871f9041653292ea9689d3d2b2d2ecfa996f025b9669","messageHeaders":"[[\"Received\", \"by luna.mailgun.net with SMTP mgrt 8734663311733; Fri, 03 May 2013 18:26:27 +0000\"], [\"Content-Type\", [\"multipart/alternative\", {\"boundary\": \"eb663d73ae0a4d6c9153cc0aec8b7520\"}]], [\"Mime-Version\", \"1.0\"], [\"Subject\", \"Test deliver webhook\"], [\"From\", \"Bob <bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"To\", \"Alice <alice@example.com>\"], [\"Message-Id\", \"<20130503182626.18666.16540@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"X-Mailgun-Variables\", \"{\\\"my_var_1\\\": \\\"Mailgun Variable #1\\\", \\\"my-var-2\\\": \\\"awesome\\\"}\"], [\"Date\", \"Fri, 03 May 2013 18:26:27 +0000\"], [\"Sender\", \"bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org\"]]","myVar1":"Mailgun Variable #1","token":"cd87f5a30002794e37aa49e67fb46990e578b1e9197773d817","messageId":"<20130503182626.18666.16540@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>","myVar2":"awesome"}}}""",
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )

  val expected2 = expected1
    .updated(3, "2017-11-08 17:24:22.000")
    .updated(17, "8bc04475-779b-4ed4-bec3-555e737fac74")
    .updated(
      58,
      """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailgun/message_dropped/jsonschema/1-0-0","data":{"recipient":"alice@example.com","timestamp":"2017-11-08T17:24:22.000Z","xMailgunSid":"WyIwNzI5MCIsICJpZG91YnR0aGlzb25lZXhpc3RzQGdtYWlsLmNvbSIsICI2Il0=","description":"Not delivering to previously bounced address","domain":"sandbox57070072075d4cfd9008d4332108734c.mailgun.org","signature":"71f812485ae3fb398de8d1a86b139f24391d604fd94dab59e7c99cfcd506885c","reason":"hardfail","messageHeaders":"[[\"Received\", \"by luna.mailgun.net with SMTP mgrt 8755546751405; Fri, 03 May 2013 19:26:59 +0000\"], [\"Content-Type\", [\"multipart/alternative\", {\"boundary\": \"23041bcdfae54aafb801a8da0283af85\"}]], [\"Mime-Version\", \"1.0\"], [\"Subject\", \"Test drop webhook\"], [\"From\", \"Bob <bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"To\", \"Alice <alice@example.com>\"], [\"Message-Id\", \"<20130503192659.13651.20287@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"List-Unsubscribe\", \"<mailto:u+na6tmy3ege4tgnldmyytqojqmfsdembyme3tmy3cha4wcndbgaydqyrgoi6wszdpovrhi5dinfzw63tfmv4gs43uomstimdhnvqws3bomnxw2jtuhusteqjgmq6tm@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"X-Mailgun-Sid\", \"WyIwNzI5MCIsICJpZG91YnR0aGlzb25lZXhpc3RzQGdtYWlsLmNvbSIsICI2Il0=\"], [\"X-Mailgun-Variables\", \"{\\\"my_var_1\\\": \\\"Mailgun Variable #1\\\", \\\"my-var-2\\\": \\\"awesome\\\"}\"], [\"Date\", \"Fri, 03 May 2013 19:26:59 +0000\"], [\"Sender\", \"bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org\"]]","code":"605","myVar1":"Mailgun Variable #1","attachment1":"Received: by luna.mailgun.net with SMTP mgrt 8755546751405; Fri, 03 May 2013\n 19:26:59 +0000\nContent-Type: multipart/alternative; boundary=\"23041bcdfae54aafb801a8da0283af85\"\nMime-Version: 1.0\nSubject: Test drop webhook\nFrom: Bob <bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\nTo: Alice <alice@example.com>\nMessage-Id: <20130503192659.13651.20287@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\nList-Unsubscribe: <mailto:u+na6tmy3ege4tgnldmyytqojqmfsdembyme3tmy3cha4wcndbgaydqyrgoi6wszdpovrhi5dinfzw63tfmv4gs43uomstimdhnvqws3bomnxw2jtuhusteqjgmq6tm@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\nX-Mailgun-Sid: WyIwNzI5MCIsICJpZG91YnR0aGlzb25lZXhpc3RzQGdtYWlsLmNvbSIsICI2Il0=\nX-Mailgun-Variables: {\"my_var_1\": \"Mailgun Variable #1\", \"my-var-2\": \"awesome\"}\nDate: Fri, 03 May 2013 19:26:59 +0000\nSender: bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org\n\n--23041bcdfae54aafb801a8da0283af85\nMime-Version: 1.0\nContent-Type: text/plain; charset=\"ascii\"\nContent-Transfer-Encoding: 7bit\n\nHi Alice, I sent an email to this address but it was bounced.\n\n--23041bcdfae54aafb801a8da0283af85\nMime-Version: 1.0\nContent-Type: text/html; charset=\"ascii\"\nContent-Transfer-Encoding: 7bit\n\n<html>\n                            <body>Hi Alice, I sent an email to this address but it was bounced.\n                            <br>\n</body></html>\n--23041bcdfae54aafb801a8da0283af85--","token":"9e3fffc7eba57e282e89f7afcf243563868e9de4ecfea78c09","messageId":"<20130503192659.13651.20287@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>","attachmentCount":1,"myVar2":"awesome"}}}"""
    )
  val expected = List(expected1, expected2)
}

class CljTomcatMailgunEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-mailgun-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing 2 valid " +
    "completed call" should {
    runEnrichJob(CljTomcatMailgunEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 2 completed calls" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== CljTomcatMailgunEventSpec.expected.length
      for (line_idx <- goods.indices) {
        val actual = goods(line_idx).split("\t").map(s => if (s.isEmpty()) null else s)
        for (idx <- CljTomcatMailgunEventSpec.expected(line_idx).indices) {
          actual(idx) must BeFieldEqualTo(CljTomcatMailgunEventSpec.expected(line_idx)(idx), idx)
        }
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
