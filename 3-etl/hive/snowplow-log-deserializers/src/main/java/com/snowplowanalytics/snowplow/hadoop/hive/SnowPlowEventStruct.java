/* 
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
import java.net.URLDecoder;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
  public Integer visit_id;

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
  public ArrayList<String> br_features = new ArrayList<String>(); // So we don't have to create new on each row.
  public Boolean br_cookies;

  // OS (from user-agent)
  public String os_name;
  public String os_family;
  public String os_manufacturer;
  
  // Device/Hardware (from user-agent) 
  public String dvce_type;
  public Boolean dvce_ismobile;

  // Device (from querystring)
  public Integer dvce_screenwidth;
  public Integer dvce_screenheight;

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  private static final String cfEncoding = "UTF-8";

  // An enum of all the fields we're expecting in the querystring
  private static enum Fields { TID, UID, VID, LANG, COOKIE, RES, REFR, URL, PAGE, EV_CA, EV_AC, EV_LA, EV_PR, EV_VA }

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
    this.br_features.clear(); // Empty the ArrayList, don't destroy it
    this.br_cookies = null;
    this.os_name = null;
    this.os_family = null;
    this.os_manufacturer = null;
    this.dvce_type = null;
    this.dvce_ismobile = null;
    this.dvce_screenwidth = null;
    this.dvce_screenheight = null;

    // We have to handle any header rows
    if (row.startsWith("#Version:") || row.startsWith("#Fields:")) {
      return null; // Empty row will be discarded by Hive
    }

    final Matcher m = cfRegex.matcher(row);
    
    try {
      // 0. First check our row is kosher

      // -> is row from a CloudFront log? Will throw an exception if not
      try {
        m.matches();
      } catch (PatternSyntaxException pse) {
        throw new SerDeException("Row does not match expected CloudFront regexp pattern");
      }

      // -> was generated by sp.js? If not (e.g. favicon.ico request), then silently return
      final String object = m.group(8);
      final String querystring = m.group(12);
      if (!object.equals("/ice.png") || isNullField(querystring)) {
        return null;
      }

      // 1. Now we retrieve the fields which get directly passed through
      this.dt = m.group(1);
      this.tm = m.group(2); // CloudFront date format matches Hive's
      this.user_ipaddress = m.group(5);

      // 2. Next we dis-assemble the user agent...
      final String ua = m.group(11);
      final UserAgent userAgent = UserAgent.parseUserAgentString(ua);

      // -> browser fields
      final Browser b = userAgent.getBrowser();
      this.br_name = b.getName();
      this.br_family = b.getGroup().getName();
      final Version v = userAgent.getBrowserVersion();
      this.br_version = (v == null) ? null : v.getVersion();
      this.br_type = b.getBrowserType().getName();
      this.br_renderengine = b.getRenderingEngine().toString();
      
      // -> OS-related fields
      final OperatingSystem os = userAgent.getOperatingSystem();
      this.os_name = os.getName();
      this.os_family = os.getGroup().getName(); 
      this.os_manufacturer = os.getManufacturer().getName();
      
      // -> device/hardware-related fields
      this.dvce_type = os.getDeviceType().getName();
      this.dvce_ismobile = os.isMobileDevice();

      // 3. Now we dis-assemble the querystring
      String qsUrl = null;
      // List<NameValuePair> params = URLEncodedUtils.parse(querystring, utf8); TODO: use this when httpclient 4.2 comes out of beta http://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
      List<NameValuePair> params = URLEncodedUtils.parse(URI.create("http://localhost/?" + querystring), "UTF-8");

      // For performance, don't convert to a map, just loop through and match to our variables as we go
      for (NameValuePair pair : params) {
        
        final String name = pair.getName();
        final String value = pair.getValue();

        // Handle browser features (f_fla etc) separately (i.e. no enum value for these)
        if (name.startsWith("f_")) {
          // Only add it to our array of browser features if it's set to "1"
          if (stringToBoolean(value)) {
            // Drop the "f_" prefix and add to our array
            this.br_features.add(name.substring(2));
          }
        } else {

          try {
            final Fields field = Fields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
            switch (field) {

              // Common fields
              case TID: 
                this.txn_id = value;
                break;
              case UID: 
                this.user_id = value;
                break;
              case VID:
                this.visit_id = Integer.parseInt(value);
                break;
              case LANG:
                this.br_lang = value;
                break;
              case COOKIE:
                this.br_cookies = stringToBoolean(value);
                break;
              case RES:
                try {
                  String[] resolution = value.split("x");
                  this.dvce_screenwidth = Integer.parseInt(resolution[0]);
                  this.dvce_screenheight = Integer.parseInt(resolution[1]); 
                } catch (Exception e) {
                  // Return a null row on invalid data
                  return null;
                }
                break;
              case REFR:
                this.page_referrer = URLDecoder.decode(value, cfEncoding);
                break;
              case URL:
                qsUrl = pair.getValue(); // We might use this later for the page URL
                break;
              
              // Page-view only
              case PAGE:
                this.page_title = URLDecoder.decode(value, cfEncoding);
                break;

              // Event only
              case EV_CA:
                this.ev_category = URLDecoder.decode(value, cfEncoding);
                break;         
              case EV_AC:
                this.ev_action = URLDecoder.decode(value, cfEncoding);
                break; 
              case EV_LA:
                this.ev_label = URLDecoder.decode(value, cfEncoding);
                break; 
              case EV_PR:
                this.ev_property = URLDecoder.decode(value, cfEncoding);
                break; 
               case EV_VA:
                this.ev_value = URLDecoder.decode(value, cfEncoding);
                break;
            }
          } catch (IllegalArgumentException iae) {
            // Return a null row on invalid data
            return null;
          }
        }
      }

      // 4. Choose the page_url
      final String cfUrl = m.group(10);
      if (isNullField(cfUrl)) { // CloudFront didn't provide the URL as cs(Referer)
        this.page_url = URLDecoder.decode(qsUrl, cfEncoding); // Use the decoded querystring URL
      } else { // Otherwise default to...
        this.page_url = cfUrl; // The CloudFront cs(Referer) URL
      }

      // 5. Finally handle the marketing fields in the page_url
      // TODO

    } catch (Exception e) {
      throw new SerDeException("Could not parse row: \"" + row + "\"", e);
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

  /**
   * Converts a String of value "1" or "0" to true
   * or false respectively.
   * 
   * @param s The String to check
   * @return True for "1", false for "0"
   * @throws IllegalArgumentException if the string is not "1" or "0"
   */ 
  static boolean stringToBoolean(String s) throws IllegalArgumentException {
    if (s.equals("1"))
      return true;
    if (s.equals("0"))
      return false;
    throw new IllegalArgumentException("Could not convert \"" + s + "\" to boolean, only 1 or 0.");
  }
}
