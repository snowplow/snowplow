/* 
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive;

// Java
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

// Java Library for User-Agent Information
import nl.bitwalker.useragentutils.*;

import javax.el.ArrayELResolver;

/**
 * SnowPlowEventStruct represents the Hive struct for a SnowPlow event or page view.
 *
 * Contains a parse() method to perform an update-in-place for this instance
 * based on the current row's contents.
 *
 * Constructor is empty because we do updates-in-place for performance reasons.
 */
public class SnowPlowEventStruct {

  // -------------------------------------------------------------------------------------------------------------------
  // Mutable properties for this Hive struct
  // -------------------------------------------------------------------------------------------------------------------

  // Date/time
  public String dt;
  public String tm;

  // User and visit
  public String user_id;
  public String user_ipaddress;
  public int visit_id; // TODO: check type

  // Page
  public String page_url;
  public String page_title;
  public String page_referrer;

  // Marketing
  public String mkt_source;
  public String mkt_medium;
  public String mkt_term;
  public String mkt_content;
  public String mkt_name;

  // Event
  public String ev_category;
  public String ev_action;
  public String ev_label;
  public String ev_property;
  public String ev_value;

  // Browser (from user-agent)
  public String br_name;
  public String br_family;
  public String br_version;
  public String br_type;
  public String br_renderengine;

  // Browser (from querystring)
  public String br_lang;
  public ArrayList<String> br_features; // TODO: check type
  public boolean br_cookies; // TODO: check type
  public int br_screenwidth; // TODO: check type
  public int br_screenheight; // TODO: check type

  // OS (from user-agent)
  public String os_name;
  public String os_family;
  public String os_manufacturer;
  
  // Device/Hardware (from user-agent) 
  public boolean dvce_ismobile; // TODO: check type
  public String dvce_type;

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private static final String w = "[\\s]+"; // Whitespace regex
  private static final Pattern cfRegex = Pattern.compile("([\\S]+)"  // Date          / date
                                                   + w + "([\\S]+)"  // Time          / time
                                                   + w + "([\\S]+)"  // EdgeLocation  / x-edge-location
                                                   + w + "([\\S]+)"  // BytesSent     / sc-bytes
                                                   + w + "([\\S]+)"  // IPAddress     / c-ip
                                                   + w + "([\\S]+)"  // Operation     / cs-method
                                                   + w + "([\\S]+)"  // Domain        / cs(Host)
                                                   + w + "([\\S]+)"  // Object        / cs-uri-stem
                                                   + w + "([\\S]+)"  // HttpStatus    / sc-status
                                                   + w + "([\\S]+)"  // Referrer      / cs(Referer)
                                                   + w + "([\\S]+)"  // UserAgent     / cs(User Agent)
                                                   + w + "(.+)");    // Querystring   / cs-uri-query

  // -------------------------------------------------------------------------------------------------------------------
  // Deserialization logic
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Parses the input row String into a Java object.
   * For performance reasons this works in-place updating the fields
   * within this SnowPlowEventStruct, rather than creating a new one.
   * 
   * @param row The raw String containing the row contents
   * @return This struct with all values updated
   * @throws SerDeException For any exception during parsing
   */
  public Object parse(String row) throws SerDeException {
    
    // We have to handle any header rows
    if (row.startsWith("#Version:") || row.startsWith("#Fields:")) {
      return null; // Empty row will be discarded by Hive
    }

    final Matcher m = cfRegex.matcher(row);
    
    try {
      // 0. First check our row is kosher

      // Is CloudFront from CloudFront? Throw an exception if not
      m.matches();

      // Was generated by sp.js? If not (e.g. favicon.ico request), then silently return
      String object = m.group(8);
      String querystring = m.group(12);
      if (!object.equals("/ice.png") || isNullField(querystring)) {
        return null;
      }

      // 1. Now we retrieve the fields which get directly passed through
      this.dt = m.group(1);
      this.tm = m.group(2); // CloudFront date format matches Hive's
      this.user_ipaddress = m.group(5);

      // 2. Next we dis-assemble the user agent...
      String ua = m.group(11);
      UserAgent userAgent = UserAgent.parseUserAgentString(ua);

      // -> browser fields
      Browser b = userAgent.getBrowser();
      this.br_name = b.getName();
      this.br_family = b.getGroup().getName();
      this.br_version = userAgent.getBrowserVersion().getVersion();
      this.br_type = b.getBrowserType().getName();
      this.br_renderengine = b.getRenderingEngine().toString();
      
      // -> OS-related fields
      OperatingSystem os = userAgent.getOperatingSystem();
      this.os_name = os.getName();
      this.os_family = os.getGroup().getName(); 
      this.os_manufacturer = os.getManufacturer().getName();
      
      // -> device/hardware-related fields
      this.dvce_ismobile = os.isMobileDevice();
      this.dvce_type = os.getDeviceType().getName();

      // 3. Now we dis-assemble the querystring
      String qsUrl = null;
      // TODO

      // 4. Finally construct the page_url
      String cfUrl = m.group(10);
      if (isNullField(cfUrl)) { // CloudFront didn't provide the URL as cs(Referer)
        this.page_url = qsUrl; // Use the querystring URL
      } else {
        this.page_url = cfUrl; // Use the CloudFront URL
      }

    } catch (Exception e) {
      throw new SerDeException("Could not parse row: " + row, e);
    }

    return this; // Return the SnowPlowEventStruct
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Datatype conversions
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Checks whether a String is a hyphen "-", which
   * is used by CloudFront to signal a null. Package
   * private for unit testing.
   *
   * @param s The String to check
   * @return True if the String was a hyphen "-"
   */
  static boolean isNullField(String s) { return (s == null || s.equals("") || s.equals("-")); }
}
