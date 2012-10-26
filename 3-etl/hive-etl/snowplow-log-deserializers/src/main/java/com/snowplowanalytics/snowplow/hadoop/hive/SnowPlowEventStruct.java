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
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang.StringUtils;

// Commons Logging
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

// Java Library for User-Agent Information
// TODO: we may well update this to a better lib
import nl.bitwalker.useragentutils.*;

// Apache URLEncodedUtils
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

// Get our project settings
import com.snowplowanalytics.snowplow.hadoop.hive.generated.ProjectSettings;

/**
 * SnowPlowEventStruct represents the Hive struct for a SnowPlow event or page view.
 *
 * Contains a parse() method to perform an update-in-place for this instance
 * based on the current row's contents.
 *
 * Constructor is empty because we do updates-in-place for performance reasons.
 */
public class SnowPlowEventStruct {

  // Setup logging - instantiated lazily
  private static Log _LOG;

  // -------------------------------------------------------------------------------------------------------------------
  // Mutable properties for this Hive struct
  // -------------------------------------------------------------------------------------------------------------------

  // Date/time
  public String dt;
  public String tm;

  // Transaction (i.e. this logging event)
  public String txn_id;

  // The application (site, game, app etc) this event belongs to
  public String app_id;

  // User and visit
  public String user_id;
  public String user_ipaddress;
  public Integer visit_id;

  // Page
  public String page_url;
  public String page_title;
  public String page_referrer;

  // Marketing
  public String mkt_medium;
  public String mkt_source;
  public String mkt_term;
  public String mkt_content;
  public String mkt_campaign;

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
  // Individual feature fields for non-Hive targets (e.g. Infobright)
  public Boolean br_features_pdf;
  public Boolean br_features_flash;
  public Boolean br_features_java;
  public Boolean br_features_director;
  public Boolean br_features_quicktime;
  public Boolean br_features_realplayer;
  public Boolean br_features_windowsmedia;
  public Boolean br_features_gears;
  public Boolean br_features_silverlight;
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

  // Ecommerce transaction (from querystring)
  public String tr_orderid;
  public String tr_affiliation;
  public String tr_total;
  public String tr_tax;
  public String tr_shipping;
  public String tr_city;
  public String tr_state;
  public String tr_country;

  // Ecommerce transaction item (from querystring)
  public String ti_orderid;
  public String ti_sku;
  public String ti_name;
  public String ti_category;
  public String ti_price;
  public String ti_quantity;

  // Versioning
  public String v_tracker;
  public String v_collector;
  public String v_etl;

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  private static final String collectorVersion = "cf"; // Collector is CloudFront. Update when we support other collector formats.

  private static final String cfEncoding = "UTF-8";

  // An enum of all the fields we're expecting in the querystring
  private static enum QuerystringFields { TID, AID, UID, VID, TSTAMP, TV, LANG, F_PDF, F_FLA, F_JAVA, F_DIR, F_QT, F_REALP, F_WMA, F_GEARS, F_AG, COOKIE, RES, REFR, URL, PAGE, EV_CA, EV_AC, EV_LA, EV_PR, EV_VA, TR_ID, TR_AF, TR_TT, TR_TX, TR_SH, TR_CI, TR_ST, TR_CO, TI_ID, TI_SK, TI_NA, TI_CA, TI_PR, TI_QU }

  // An enum for the marketing attribution fields we might find
  // attached to the page URL.
  private static enum MarketingFields { UTM_SOURCE, UTM_MEDIUM, UTM_CAMPAIGN, UTM_TERM, UTM_CONTENT }

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private static final String w = "[\\s]+"; // Whitespace regex
  private static final String ow = "(?:" + w; // Optional whitespace begins
  private static final Pattern cfRegex = Pattern.compile("([\\S]+)"   // Date          / date
                                                   + w + "([\\S]+)"   // Time          / time
                                                   + w + "([\\S]+)"   // EdgeLocation  / x-edge-location
                                                   + w + "([\\S]+)"   // BytesSent     / sc-bytes
                                                   + w + "([\\S]+)"   // IPAddress     / c-ip
                                                   + w + "([\\S]+)"   // Operation     / cs-method
                                                   + w + "([\\S]+)"   // Domain        / cs(Host)
                                                   + w + "([\\S]+)"   // Object        / cs-uri-stem
                                                   + w + "([\\S]+)"   // HttpStatus    / sc-status
                                                   + w + "([\\S]+)"   // Referrer      / cs(Referer)
                                                   + w + "([\\S]+)"   // UserAgent     / cs(User Agent)
                                                   + w + "([\\S]+)"   // Querystring   / cs-uri-query
                                                   + ow + "[\\S]+"    // CookieHeader  / cs(Cookie)         added 12 Sep 2012
                                                   + w +  "[\\S]+"    // ResultType    / x-edge-result-type added 12 Sep 2012
                                                   + w +  "[\\S]+)?"  // X-Amz-Cf-Id   / x-edge-request-id  added 12 Sep 2012
                                         );

  // Logging helper instantiated lazily
  private static Log getLog() {
    if (_LOG == null)
      _LOG = LogFactory.getLog(SnowPlowEventStruct.class.getName());

    return _LOG;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Deserialization logic
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Parses the input row String into a Java object.
   * For performance reasons this works in-place updating the fields
   * within this SnowPlowEventStruct, rather than creating a new one.
   *
   * @param row The raw String containing the row contents
   * @return true if row was successfully parsed, false otherwise
   * @throws SerDeException For any exception during parsing
   */
  public boolean updateByParsing(String row) throws SerDeException {

    // First we reset the object's fields
    nullify();

    // We have to handle any header rows
    if (row.startsWith("#Version:") || row.startsWith("#Fields:")) {
      return false; // Empty row will be discarded by Hive
    }

    final Matcher m = cfRegex.matcher(row);

    // 0. First check our row is kosher

    // -> is row from a CloudFront log? Will throw an exception if not
    if (!m.matches())
      throw new SerDeException("Row does not match expected CloudFront regexp pattern");

    // -> was generated by sp.js? If not (e.g. favicon.ico request), then silently return
    final String object = m.group(8);
    final String querystring = m.group(12);
    if (!(object.startsWith("/ice.png") || object.equals("/i") || object.startsWith("/i?")) || isNullField(querystring)) { // Also works if Forward Query String = yes
      return false;
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

    // 3. Now for the versioning
    this.v_collector = collectorVersion;
    this.v_etl = "serde-" + ProjectSettings.VERSION;

    // 4. Now we dis-assemble the querystring
    String qsUrl = null;
    List<NameValuePair> params = URLEncodedUtils.parse(URI.create("http://localhost/?" + querystring), "UTF-8");

    // For performance, don't convert to a map, just loop through and match to our variables as we go
    for (NameValuePair pair : params) {

      final String name = pair.getName();
      final String value = pair.getValue();

      try {
        final QuerystringFields field = QuerystringFields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
        switch (field) {

          // Common fields
          case TID:
            this.txn_id = value;
            break;
          case AID:
            this.app_id = value;
            break;
          case UID:
            this.user_id = value;
            break;
          case VID:
            this.visit_id = Integer.parseInt(value);
            break;
          case TSTAMP:
            // Replace our timestamp fields with the client's timestamp
            String[] timestamp = value.split(" ");
            this.dt = timestamp[0];
            this.tm = timestamp[1];
            break;
          case TV:
            this.v_tracker = value;
            break;
          case LANG:
            this.br_lang = value;
            break;
          case F_PDF:
            if (this.br_features_pdf = stringToBoolean(value)) // Intentional =
              this.br_features.add("pdf");
            break;
          case F_FLA:
            if (this.br_features_flash = stringToBoolean(value)) // Intentional =
              this.br_features.add("fla");
            break;
          case F_JAVA:
            if (this.br_features_java = stringToBoolean(value)) // Intentional =
              this.br_features.add("java");
            break;
          case F_DIR:
            if (this.br_features_director = stringToBoolean(value)) // Intentional =
              this.br_features.add("dir");
            break;
          case F_QT:
            if (this.br_features_quicktime = stringToBoolean(value)) // Intentional =
              this.br_features.add("qt");
            break;
          case F_REALP:
            if (this.br_features_realplayer = stringToBoolean(value)) // Intentional =
              this.br_features.add("realp");
            break;
          case F_WMA:
            if (this.br_features_windowsmedia = stringToBoolean(value)) // Intentional =
              this.br_features.add("wma");
            break;
          case F_GEARS:
            if (this.br_features_gears = stringToBoolean(value)) // Intentional =
              this.br_features.add("gears");
            break;
          case F_AG:
            if (this.br_features_silverlight = stringToBoolean(value)) // Intentional =
              this.br_features.add("ag");
            break;
          case COOKIE:
            this.br_cookies = stringToBoolean(value);
            break;
          case RES:
            String[] resolution = value.split("x");
            if (resolution.length != 2)
              throw new Exception("Couldn't parse res field");
            this.dvce_screenwidth = Integer.parseInt(resolution[0]);
            this.dvce_screenheight = Integer.parseInt(resolution[1]);
            break;
          case REFR:
            this.page_referrer = decodeSafeString(value);
            break;
          case URL:
            qsUrl = pair.getValue(); // We might use this later for the page URL
            break;

          // Page-view only
          case PAGE:
            this.page_title = decodeSafeString(value);
            break;

          // Event only
          case EV_CA:
            this.ev_category = decodeSafeString(value);
            break;
          case EV_AC:
            this.ev_action = decodeSafeString(value);
            break;
          case EV_LA:
            this.ev_label = decodeSafeString(value);
            break;
          case EV_PR:
            this.ev_property = decodeSafeString(value);
            break;
          case EV_VA:
            this.ev_value = decodeSafeString(value);
            break;

          // Ecommerce
          case TR_ID:
            this.tr_orderid = decodeSafeString(value);
            break;
          case TR_AF:
            this.tr_affiliation = decodeSafeString(value);
            break;
          case TR_TT:
            this.tr_total = decodeSafeString(value);
            break;
          case TR_TX:
            this.tr_tax = decodeSafeString(value);
            break;
          case TR_SH:
            this.tr_shipping = decodeSafeString(value);
            break;
          case TR_CI:
            this.tr_city = decodeSafeString(value);
            break;
          case TR_ST:
            this.tr_state = decodeSafeString(value);
            break;
          case TR_CO:
            this.tr_country = decodeSafeString(value);
            break;
          case TI_ID:
            this.ti_orderid = decodeSafeString(value);
            break;
          case TI_SK:
            this.ti_sku = decodeSafeString(value);
            break;
          case TI_NA:
            this.ti_name = decodeSafeString(value);
            break;
          case TI_CA:
            this.ti_category = decodeSafeString(value);
            break;
          case TI_PR:
            this.ti_price = decodeSafeString(value);
            break;
          case TI_QU:
            this.ti_quantity = decodeSafeString(value);
            break;            
        }
      } catch (Exception e) {
        getLog().warn(e.getClass().getSimpleName() + " on { " + name + ": " + value + "}");
      }
    }

    // 4. Choose the page_url
    final String cfUrl = m.group(10);
    if (isNullField(cfUrl)) { // CloudFront didn't provide the URL as cs(Referer)
      try {
        this.page_url = decodeSafeString(qsUrl); // Use the decoded querystring URL
      } catch (Exception e) {
        getLog().warn(e.getClass().getSimpleName() + " on { qsUrl: " + qsUrl + "}");
      }
    } else { // Otherwise default to...
      this.page_url = cfUrl; // The CloudFront cs(Referer) URL
    }

    // 5. Finally handle the marketing fields in the page_url
    // Re-use params to avoid creating another object
    try {
      params = URLEncodedUtils.parse(URI.create(this.page_url), "UTF-8");
  
      // For performance, don't convert to a map, just loop through and match to our variables as we go
      for (NameValuePair pair : params) {
  
        final String name = pair.getName();
        final String value = pair.getValue();
  
        try {
          final MarketingFields field = MarketingFields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
  
          switch (field) {
  
            // Marketing fields
            case UTM_MEDIUM:
              this.mkt_medium = decodeSafeString(value);
              break;
            case UTM_SOURCE:
              this.mkt_source = decodeSafeString(value);
              break;
            case UTM_TERM:
              this.mkt_term = decodeSafeString(value);
              break;
            case UTM_CONTENT:
              this.mkt_content = decodeSafeString(value);
              break;
            case UTM_CAMPAIGN:
              this.mkt_campaign = decodeSafeString(value);
              break;
          }
        } catch (Exception e) {
          // Do nothing: non-marketing related querystring fields are not an issue.
        }
      }
    } catch (IllegalArgumentException e) {
      getLog().warn("Couldn't parse page_url: " + page_url);
    }

    return true; // Successfully updated the row.
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Initializer & scrubber
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Because we are always re-using the same object
   * (for performance reasons), we need a mechanism
   * for nulling all the fields. We null all the
   * fields at two points in time:
   * 1. When we're re-initializing the object ready
   *    to parse a new row into it
   * 2. If the parsing failed part way through and we
   *    we need to null all the fields again (so that
   *    Hive silently discards the row)
   */
  private void nullify() {

    // Null all the things
    this.dt = null;
    this.tm = null;
    this.txn_id = null;
    this.app_id = null;
    this.user_id = null;
    this.user_ipaddress = null;
    this.visit_id = null;
    this.page_url = null;
    this.page_title = null;
    this.page_referrer = null;
    this.mkt_medium = null;
    this.mkt_source = null;
    this.mkt_term = null;
    this.mkt_content = null;
    this.mkt_campaign = null;
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
    this.br_features_pdf = null;
    this.br_features_flash = null;
    this.br_features_java = null;
    this.br_features_director = null;
    this.br_features_quicktime = null;
    this.br_features_realplayer = null;
    this.br_features_windowsmedia = null;
    this.br_features_gears = null;
    this.br_features_silverlight = null;
    this.br_cookies = null;
    this.br_cookies = null;
    this.os_name = null;
    this.os_family = null;
    this.os_manufacturer = null;
    this.dvce_type = null;
    this.dvce_ismobile = null;
    this.dvce_screenwidth = null;
    this.dvce_screenheight = null;
    this.tr_orderid = null;
    this.tr_affiliation = null;
    this.tr_total = null;
    this.tr_tax = null;
    this.tr_shipping = null;
    this.tr_city = null;
    this.tr_state = null;
    this.tr_country = null;
    this.ti_orderid = null;
    this.ti_sku = null;
    this.ti_name = null;
    this.ti_category = null;
    this.ti_price = null;
    this.ti_quantity = null;
    this.v_tracker = null;
    this.v_collector = null;
    this.v_etl = null;
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
  static boolean isNullField(String s) {
    return (s == null || s.equals("") || s.equals("-"));
  }

  /**
   * Decodes a String using UTF8, also removing:
   * - Newlines - because they will break Hive
   * - Tabs - because they will break non-Hive
   *          targets (e.g. Infobright)
   *
   * @param s The String to decode
   * @return The decoded String
   * @throws UnsupportedEncodingException if the Character Encoding is not supported
   * @throws IllegalArgumentException if the string cannot be parsed
   */
  static String decodeSafeString(String s) throws UnsupportedEncodingException, IllegalArgumentException {

    if (s == null) return null;
    String decoded = URLDecoder.decode(cleanUrlString(s), cfEncoding);
    if (decoded == null) return null;

    return decoded.replaceAll("(\\r|\\n)", "").replaceAll("\\t", "    ");
}

  /**
   * Cleans a string to try and make it parsable by URLDecoder.decode.
   *
   * @param s The String to clean
   * @return The cleaned string
   */
  static String cleanUrlString(String s)
  {
    // The '%' character seems to be appended to the end of some URLs in 
    // the CloudFront logs, causing Exceptions when using URLDecoder.decode
    // Perhaps a CloudFront bug?
    return StringUtils.removeEnd(s, "%");
  }

  /**
   * Converts a String of value "1" or "0" to true
   * or false respectively.
   *
   * @param s The String to check
   * @return True for "1", false for "0"
   * @throws IllegalArgumentException if the string is not "1" or "0"
   */
  static boolean stringToBoolean(String s) throws IllegalArgumentException {
    if (s.equals("1")) return true;
    if (s.equals("0")) return false;
    throw new IllegalArgumentException("Could not convert \"" + s + "\" to boolean, only 1 or 0.");
  }
}
