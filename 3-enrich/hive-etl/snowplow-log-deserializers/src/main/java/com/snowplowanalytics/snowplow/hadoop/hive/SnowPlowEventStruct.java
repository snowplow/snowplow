/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
import java.util.UUID;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.util.Date;
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

  // The application (site, game, app etc) this event belongs to, and the tracker platform
  public String app_id;
  public String platform;

  // Date/time
  public String dt; // Leave in because still used in the Hive output for partioning
  public String collector_dt;
  public String collector_tm;
  public String dvce_dt;
  public String dvce_tm;
  public Long dvce_epoch;

  // Event and transaction
  public String event;
  public String event_vendor;
  public String event_id;
  public String txn_id;

  // User and visit
  public String user_id;
  public String user_ipaddress;
  public String user_fingerprint;
  public String domain_userid;
  public Integer domain_sessionidx;
  public String network_userid;

  // Page
  public String page_url;
  public String page_title;
  public String page_referrer;

  // Page URL components
  public String page_urlscheme;
  public String page_urlhost;
  public Integer page_urlport;
  public String page_urlpath;
  public String page_urlquery;
  public String page_urlfragment;

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

  // User Agent
  public String useragent;

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
  // Note: we use objects (not primitives) because we want to be able to set to null
  public Byte br_features_pdf;
  public Byte br_features_flash;
  public Byte br_features_java;
  public Byte br_features_director;
  public Byte br_features_quicktime;
  public Byte br_features_realplayer;
  public Byte br_features_windowsmedia;
  public Byte br_features_gears;
  public Byte br_features_silverlight;
  // Non-Hive uses Byte version
  public Boolean br_cookies;
  public Byte br_cookies_bt;
  public String br_colordepth;
  public Integer br_viewwidth;
  public Integer br_viewheight;

  // OS (from user-agent)
  public String os_name;
  public String os_family;
  public String os_manufacturer;
  public String os_timezone;

  // Device/Hardware (from user-agent)
  public String dvce_type;
  // Non-Hive uses Byte version
  public Boolean dvce_ismobile;
  public Byte dvce_ismobile_bt;

  // Device (from querystring)
  public Integer dvce_screenwidth;
  public Integer dvce_screenheight;

  // Document fields
  public String doc_charset;
  public Integer doc_width;
  public Integer doc_height;

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

  // Page pings
  public Integer pp_xoffset_min;
  public Integer pp_xoffset_max;
  public Integer pp_yoffset_min;
  public Integer pp_yoffset_max;

  // Versioning
  public String v_tracker;
  public String v_collector;
  public String v_etl;

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  private static final String collectorVersion = "cf"; // Collector is CloudFront. TODO: update when we support other collector formats.

  private static final String cfEncoding = "UTF-8";

  private static final String eventVendor = "com.snowplowanalytics"; // Assume all events are from SnowPlow vendor for now.

  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

  // An enum of all the fields we're expecting in the querystring
  // See https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol for details
  private static enum QuerystringFields { E, IP, AID, P, TID, UID, DUID, NUID, FP, VID, DTM, TV, LANG, CS, VP, DS, F_PDF, F_FLA, F_JAVA, F_DIR, F_QT, F_REALP, F_WMA, F_GEARS, F_AG, COOKIE, RES, CD, TZ, REFR, URL, PAGE, EV_CA, EV_AC, EV_LA, EV_PR, EV_VA, TR_ID, TR_AF, TR_TT, TR_TX, TR_SH, TR_CI, TR_ST, TR_CO, TI_ID, TI_SK, TI_NA, TI_CA, TI_PR, TI_QU, PP_MIX, PP_MAX, PP_MIY, PP_MAY }

  // An enum for the marketing attribution fields we might find
  // attached to the page URL.
  private static enum MarketingFields { UTM_SOURCE, UTM_MEDIUM, UTM_CAMPAIGN, UTM_TERM, UTM_CONTENT }

  // An enum for the event codes we might find in our e= querystring parameter
  private static enum EventCodes { EV, SE, AD, TR, TI, PV, PP }

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
    this.collector_dt = this.dt;
    this.collector_tm = m.group(2); // CloudFront date format matches Hive's
    this.user_ipaddress = m.group(5);

    // 2. Now grab the user agent
    final String ua = m.group(11);
    try {
      this.useragent = decodeSafeString(ua);
    } catch (Exception e) {
      getLog().warn(e.getClass().getSimpleName() + " on { useragent: " + ua + " }");
    }

    // 3. Next we dis-assemble the user agent...
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
    this.dvce_ismobile_bt = (byte)(this.dvce_ismobile ? 1 : 0);

    // 4. Now for the versioning (tracker versioning is handled below)
    this.v_collector = collectorVersion;
    this.v_etl = "serde-" + ProjectSettings.VERSION;

    // 5. Now we generate the event vendor and ID
    this.event_vendor = eventVendor; // TODO: this becomes part of the protocol eventually
    this.event_id = generateEventId();

    // 6. Now we dis-assemble the querystring.
    String qsUrl = null; // We use this in the block below, and afterwards
    List<NameValuePair> params = null; // We re-use this for efficiency
    try {
      params = URLEncodedUtils.parse(URI.create("http://localhost/?" + querystring), cfEncoding);

      // For performance, don't convert to a map, just loop through and match to our variables as we go
      for (NameValuePair pair : params) {

        final String name = pair.getName();
        final String value = pair.getValue();

        final QuerystringFields field;
        try {
          field = QuerystringFields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
        } catch(IllegalArgumentException e) {
          getLog().warn("Unexpected params { " + name + ": " + value + " }");
          continue;
        }

        try {
          switch (field) {
            // Common fields
            case E:
              this.event = asEventType(value);
              break;
            case IP:
              this.user_ipaddress = value;
              break;
            case AID:
              this.app_id = value;
              break;
            case P:
              this.platform = value;
              break;
            case TID:
              this.txn_id = value;
              break;
            case UID:
              this.user_id = value;
              break;
            case DUID:
              this.domain_userid = value;
              break;
            case NUID:
              this.network_userid = value;
              break;
            case FP:
              this.user_fingerprint = value;
              break;
            case VID:
              this.domain_sessionidx = Integer.parseInt(value);
              break;
            case DTM:
              // Set our dvce_dt, _tm and _epoch fields
              this.dvce_epoch = Long.parseLong(value);
              Date deviceDate = new Date(this.dvce_epoch);
              this.dvce_dt = dateFormat.format(deviceDate);
              this.dvce_tm = timeFormat.format(deviceDate);
              break;
            case TV:
              this.v_tracker = value;
              break;
            case LANG:
              this.br_lang = value;
              break;
            case CS:
              this.doc_charset = value;
              break;
            case VP:
              String[] viewport = value.split("x");
              if (viewport.length != 2)
                throw new Exception("Couldn't parse vp field");
              this.br_viewwidth = Integer.parseInt(viewport[0]);
              this.br_viewheight = Integer.parseInt(viewport[1]);
              break;
            case DS:
              String[] docsize = value.split("x");
              if (docsize.length != 2)
                throw new Exception("Couldn't parse ds field");
              this.doc_width = Integer.parseInt(docsize[0]);
              this.doc_height = Integer.parseInt(docsize[1]);
              break;
            case F_PDF:
              if ((this.br_features_pdf = stringToByte(value)) == 1)
                this.br_features.add("pdf");
              break;
            case F_FLA:
              if ((this.br_features_flash = stringToByte(value)) == 1)
                this.br_features.add("fla");
              break;
            case F_JAVA:
              if ((this.br_features_java = stringToByte(value)) == 1)
                this.br_features.add("java");
              break;
            case F_DIR:
              if ((this.br_features_director = stringToByte(value)) == 1)
                this.br_features.add("dir");
              break;
            case F_QT:
              if ((this.br_features_quicktime = stringToByte(value)) == 1)
                this.br_features.add("qt");
              break;
            case F_REALP:
              if ((this.br_features_realplayer = stringToByte(value)) == 1)
                this.br_features.add("realp");
              break;
            case F_WMA:
              if ((this.br_features_windowsmedia = stringToByte(value)) == 1)
                this.br_features.add("wma");
              break;
            case F_GEARS:
              if ((this.br_features_gears = stringToByte(value)) == 1)
                this.br_features.add("gears");
              break;
            case F_AG:
              if ((this.br_features_silverlight = stringToByte(value)) == 1)
                this.br_features.add("ag");
              break;
            case COOKIE:
              this.br_cookies = stringToBoolean(value);
              this.br_cookies_bt = (byte)(this.br_cookies ? 1 : 0);
              break;
            case RES:
              String[] resolution = value.split("x");
              if (resolution.length != 2)
                throw new Exception("Couldn't parse res field");
              this.dvce_screenwidth = Integer.parseInt(resolution[0]);
              this.dvce_screenheight = Integer.parseInt(resolution[1]);
              break;
            case CD:
              this.br_colordepth = value;
              break;
            case TZ:
              this.os_timezone = decodeSafeString(value);
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

            // Page pings
            case PP_MIX:
              this.pp_xoffset_min = Integer.parseInt(value);
              break;
            case PP_MAX:
              this.pp_xoffset_max = Integer.parseInt(value);
              break;
            case PP_MIY:
              this.pp_yoffset_min = Integer.parseInt(value);
              break;
            case PP_MAY:
              this.pp_yoffset_max = Integer.parseInt(value);
              break;              
          }
        } catch (Exception e) {
          getLog().warn(e.getClass().getSimpleName() + " on { " + name + ": " + value + " }");
        }
      }
    } catch (IllegalArgumentException e) {
      getLog().warn("Corrupted querystring { " + querystring + " }");    
    }

    // 7. Choose the page_url
    final String cfUrl = m.group(10);
    if (!isNullField(cfUrl)) { // CloudFront didn't provide the URL as cs(Referer)
      this.page_url = cfUrl; // The CloudFront cs(Referer) URL
    } else {
      try {
        this.page_url = decodeSafeString(qsUrl); // Use the decoded querystring URL. Might be null (returned as null)
      } catch (Exception e) {
        getLog().warn(e.getClass().getSimpleName() + " on { qsUrl: " + qsUrl + " }");
      }
    }

    // 8. Now try to convert the page_url into a valid Java URI and store the 6 out of 9 components we are interested in:
    try {
      final URI pageUri = URI.create(this.page_url);
      this.page_urlscheme = pageUri.getScheme();
      this.page_urlhost = pageUri.getHost();
      final Integer port = pageUri.getPort();
      this.page_urlport = (port == -1) ? 80 : port;
      this.page_urlpath = pageUri.getPath();
      this.page_urlquery = pageUri.getQuery();
      this.page_urlfragment = pageUri.getFragment();
    } catch (Exception e) {
      getLog().warn("Could not parse page_url " + this.page_url + " }");
    }

    // 9. Finally handle the marketing fields in the page_url
    // Re-use params to avoid creating another object
    if (this.page_url != null) {
      params = null;
      try {
        params = URLEncodedUtils.parse(URI.create(this.page_url), "UTF-8");
      } catch (IllegalArgumentException e) {
        getLog().warn("Couldn't parse page_url: " + page_url);
      }
      if (params != null) {
        // For performance, don't convert to a map, just loop through and match to our variables as we go
        for (NameValuePair pair : params) {

          final String name = pair.getName();
          final String value = pair.getValue();

          final MarketingFields field;
          try {
            field = MarketingFields.valueOf(name.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
          } catch(IllegalArgumentException e) {
            // Do nothing: non-marketing related querystring fields are not an issue.
            continue;
          }

          try {
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
            getLog().warn(e.getClass().getSimpleName() + " on { " + name + ": " + value + " }");
          }
        }
      }
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
    this.app_id = null;
    this.platform = null;
    this.dt = null;
    this.collector_dt = null;
    this.collector_tm = null;
    this.dvce_dt = null;
    this.dvce_tm = null;
    this.dvce_epoch = null;
    this.event = null;
    this.event_vendor = null;
    this.event_id = null;
    this.txn_id = null;
    this.user_id = null;
    this.user_ipaddress = null;
    this.user_fingerprint = null;
    this.domain_userid = null;
    this.domain_sessionidx = null;
    this.network_userid = null;
    this.page_url = null;
    this.page_title = null;
    this.page_referrer = null;
    this.page_urlscheme = null;
    this.page_urlhost = null;
    this.page_urlport = null;
    this.page_urlpath = null;
    this.page_urlquery = null;
    this.page_urlfragment = null;
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
    this.useragent = null;
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
    this.br_cookies_bt = null;
    this.br_colordepth = null;
    this.br_viewwidth = null;
    this.br_viewheight = null;
    this.os_name = null;
    this.os_family = null;
    this.os_manufacturer = null;
    this.os_timezone = null;
    this.dvce_type = null;
    this.dvce_ismobile = null;
    this.dvce_ismobile_bt = null;
    this.dvce_screenwidth = null;
    this.dvce_screenheight = null;
    this.doc_charset = null;
    this.doc_width = null;
    this.doc_height = null;
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
    this.pp_xoffset_min = null;
    this.pp_xoffset_max = null;
    this.pp_yoffset_min = null;
    this.pp_yoffset_max = null;
    this.v_tracker = null;
    this.v_collector = null;
    this.v_etl = null;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Enrichments
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Turns an event code into a valid event type,
   * e.g. "pv" -> "page_view"
   *
   * @param eventCode The event code
   * @return The event type
   * @throws IllegalArgumentException if the event code is not recognised
   */
  static String asEventType(String eventCode) {

    final EventCodes e = EventCodes.valueOf(eventCode.toUpperCase()); // Java pre-7 can't switch on a string, so hash the string
    
    final String eventType;
    switch (e) {
      case EV: // TODO: remove this in the future.
        eventType = "struct";
        break;
      case SE:
        eventType = "struct";
        break;
      case AD:
        eventType = "ad_impression";
        break;
      case TR:
        eventType = "transaction";
        break;
      case TI:
        eventType = "transaction_item";
        break;
      case PV:
        eventType = "page_view";
        break;
      case PP:
        eventType = "page_ping";
        break;
      default: // Should never happen
        eventType = "";
        break;
    }

    return eventType;
  }

  /**
   * Returns a unique event ID. ID is 
   * generated as a type 4 UUID, then
   * converted to a String.
   *
   * @return The event ID
   */
  static String generateEventId() {
    return UUID.randomUUID().toString();
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

  /**
   * Converts a String of value "1" or "0" to a Byte
   * of 1 or 0 respectively.
   *
   * @param s The String to check
   * @return 1 for "1", 0 for "0"
   * @throws IllegalArgumentException if the string is not "1" or "0"
   */
  static byte stringToByte(String s) throws IllegalArgumentException {
    if (s.equals("1")) return (byte)1;
    if (s.equals("0")) return (byte)0;
    throw new IllegalArgumentException("Could not convert \"" + s + "\" to byte, only 1 or 0.");
  } 
}