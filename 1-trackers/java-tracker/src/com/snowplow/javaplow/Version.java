/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplow.javaplow;

/**
 * The version class is used for documentation purposes.
 * It follows the python convention of creating a version identity class
 *
 * @version 0.1.0
 * @author Kevin Gleason
 */

public class Version {
    static final String VERSION = "0.1.0";
}
/**
 * JavaPlow com.snowplow.javaplow.Tracker

    Instructions to Use:

     Instantiate a com.snowplow.javaplow.PayloadMap and a com.snowplow.javaplow.Tracker:
      com.snowplow.javaplow.PayloadMap pd = new com.snowplow.javaplow.PayloadMapC();
      com.snowplow.javaplow.Tracker t1 = new com.snowplow.javaplow.TrackerC("collector_uri","namespace");

     Configure payload if needed:
      pd.add_json("{'Movie':'Shawshank Redemption', 'Time':'100 Minutes' }");

     Attach the payload to the com.snowplow.javaplow.Tracker:
      t1.setPayload(pd);

     Configure the payload as you must:
      t1.setUserID("Kevin");
      t1.setLanguage("eng");
      t1.setPlatform("cnsl");
      t1.setScreenResolution(1260, 1080);
      t1.track();

     Call the track function when configured:
      t1.track()

    A HttpGet request is configured based on the parameters passed in
     and sent to the collector URI to be stored in a database.

    Current supported platforms include "pc", "tv", "mob", "cnsl", and "iot".

    Current Tracking commands:
     t1.track()
       Submits the current payload to CloudFront server

     track_page_view(page_url, page_title, referrer, context)
       All strings, null context or empty string allowed.
       Configures the URI further adding page data to payload

    Notes:
     Dictionary and JSON context values should be in String format or Map<String,Object> e.g. "{'name':'Kevin', ...}"
     I would recommend using one tracker for one tracking instance type.
       This is because only certain fields are refreshed every loop to reduce overhead at high iteration speed.
     Arrays, Dictionaries, JSON contest is all homogeneous, must all be of the String type.

*/
