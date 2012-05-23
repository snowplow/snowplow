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
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

// Java Library for User-Agent Information
import nl.bitwalker.useragentutils.*;

// Apache URLEncodedUtils 
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

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

  // Transaction (i.e. this logging event)
  public String txn_id;

  // User and visit
  public String user_id;
  public String user_ipaddress;
  public Integer visit_id; // TODO: check type

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
  public Boolean br_cookies; // TODO: check type
  public Integer br_screenwidth; // TODO: check type
  public Integer br_screenheight; // TODO: check type

  // OS (from user-agent)
  public String os_name;
  public String os_family;
  public String os_manufacturer;
  
  // Device/Hardware (from user-agent) 
  public Boolean dvce_ismobile; // TODO: check type
  public String dvce_type;

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  // An enum of all the fields we're expecting in the querystring
  private enum Fields { TID, UID, VID, LANG, RES, COOKIE, REFR, URL, PAGE, EV_CA, EV_AC, EV_LA, EV_PR, EV_VA }

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

  // private static final Charset utf8 = Charset.forName("UTF-8"); TODO: use this when httpclient 4.2 comes out of beta http://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient

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
    
    // Null everything before we start
    this.dt = null;
    this.tm = null;
    this.txn_id = null;
    this.user_id = null;
    this.user_ipaddress = null;
    this.visit_id = null;
    this.page_url = null;
    this.page_title = null;
    this.page_referrer = null;
    this.mkt_source = null;
    this.mkt_medium = null;
    this.mkt_term = null;
    this.mkt_content = null;
    this.mkt_name = null;
    this.ev_category = null;
    this.ev_action = null;
    this.ev_label = null;
    this.ev_property = null;
    this.ev_value = null;
    this.br_name = null;
    this.br_family = null;
    this.br_version = null;
    this.br_type = null;
    this.br_renderengine = null;
    this.br_lang = null;
    this.br_features = null;
    this.br_cookies = null;
    this.br_screenwidth = null;
    this.br_screenheight = null;
    this.os_name = null;
    this.os_family = null;
    this.os_manufacturer = null;
    this.dvce_ismobile = null; // TODO: check type
    this.dvce_type = null;

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

      // Was the status code other than 200 or 304? Should never happen, throw an exception
      // TODO

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
      // List<NameValuePair> params = URLEncodedUtils.parse(querystring, utf8); TODO: use this when httpclient 4.2 comes out of beta http://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
      List<NameValuePair> params = URLEncodedUtils.parse(URI.create("http://localhost/?" + querystring), "UTF-8");

      // For performance, don't convert to a map, just loop through and match to our variables as we go
      for (NameValuePair pair : params) {
        
        String name = pair.getName();

        // Handle features (f_fla etc) separately
        if (name.startsWith("f_")) {
          // TODO
        
        } else {

          Fields field = Fields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
          // TODO: put exception handling in for case of a non-supported field
          switch (field) {

            case TID: 
              this.txn_id = pair.getValue();
              break;
            case UID: 
              this.user_id = pair.getValue();
              break;
            case VID:
              this.visit_id = Integer.parseInt(pair.getValue());
              break;
            case LANG:
              this.br_lang = pair.getValue();
              break;
            /* case COOKIE:
              this.br_cookies = TODO;
              break;
            case RES:
              // TODO
              this.br_screenheight */
            case REFR:
              this.page_referrer = pair.getValue();
              break;
            case URL:
              qsUrl = pair.getValue(); // We might use this later for the page URL
              break;
            
            // Page-view only
            case PAGE:
              this.page_title = pair.getValue();
              break;

            // Event only
            case EV_CA:
              this.ev_category = pair.getValue();
              break;         
            case EV_AC:
              this.ev_action = pair.getValue();
              break; 
            case EV_LA:
              this.ev_label = pair.getValue();
              break; 
            case EV_PR:
              this.ev_property = pair.getValue();
              break; 
             case EV_VA:
              this.ev_value = pair.getValue();
              break;
          }
        }
      }

      // 4. Construct the page_url
      String cfUrl = m.group(10);
      if (isNullField(cfUrl)) { // CloudFront didn't provide the URL as cs(Referer)
        this.page_url = qsUrl; // Use the querystring URL
      } else {
        this.page_url = cfUrl; // Use the CloudFront URL
      }

      // 5. Finally handle the marketing fields in the page_url
      // TODO

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
