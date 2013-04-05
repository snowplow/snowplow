import com.snowplowanalytics.snowplow.storage.avro._

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 05/04/13
 * Time: 14:43
 * To change this template use File | Settings | File Templates.
 */
class PagePingCfLineTest {
  def getTestLine: FlattenedAvro = {
    // Create object with test data
    val t = new FlattenedAvro

    t.app_id = "pbzsite"
    t.platform = "web"

    t.collector_tstamp = "2013-03-25 02:04:00.000"
    t.dvce_tstamp = "2013-03-25 02:03:37.34"

    t.event = "page_ping"
    t.event_vendor = "com_snowplowanalytics"
    t.event_id = "long-unique-string"
    t.txn_id = "128574"

    t.v_tracker = "js-0.11.1"
    t.v_collector = "cloudfront"
    t.v_etl = "hadoop-0.1.0"

    t.user_id = null
    t.user_ipaddress = "201.17.84.32"
    t.user_fingerprint = "1640945579"
    t.domain_userid = "132e226e3359a9cd"
    t.domain_sessionidx = 1
    t.network_userid = null

    t.page_url = "http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?n=48"
    t.page_title = "Tarot cards - Psychic Bazaar"
    t.page_referrer = "http://www.psychicbazaar.com"

    t.page_urlscheme = "http"
    t.page_urlhost = "www.psychicbazaar.com"
    t.page_urlport = 80
    t.page_urlpath = "/2-tarot-cards/genre/all/type/all"
    t.page_urlquery = "n=48"
    t.page_urlfragment = null

    // All intervening fields are none until we get to the page ping fields

    t.pp_xoffset_min = 21
    t.pp_xoffset_max = 214
    t.pp_yoffset_min = 251
    t.pp_yoffset_max = 517
    t.useragent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22"

    t.br_name = "Chrome"
    t.br_family = "Chrome"
    t.br_version = "25.0.1364.172"

    t.br_type = "browser"
    t.br_renderengine = "WEBKIT"

    t.br_lang = "pt-BR"
    t.br_features = Array("pdf", "realp", "fla", "java")
    t.br_cookies = 1
    t.br_colordepth = 32
    t.br_viewwidth = 1366
    t.br_viewheight = 768

    t.os_name = "Windows"
    t.os_family = "Windows"
    t.os_manufacturer = "Microsoft Corporation"
    t.os_timezone = "America/Sao_Paulo"

    t.dvce_type = "Computer"
    t.dvce_ismobile = 0
    t.dvce_screenwidth = 1349
    t.dvce_screenheight = 3787

    return t
  }

  def createEventObject(f: FlattenedAvro):Event = {
    // Create an object to populate
    val e = new Event

    // 1st populate the event ID, collector_tstamp, device_tstamp
    e.setEventId(f.event_id)
    e.setCollectorTstamp((132165464).asInstanceOf[Timestamp]) // dummy data for the moment
    e.setDeviceTstamp(null)         // dummy data for the moment

    // 2nd populate the user object
    val u = new User
    u.setBusinessUserId(f.user_id)
    u.setIpAddress(f.user_ipaddress)
    e.setUser(u)

    // 3rd populate the snowplow_version object
    val v = new SnowPlowVersion
    v.setTrackerVersion(f.v_tracker)
    v.setCollectorVersion(f.v_collector)
    v.setEtlVersion(f.v_etl)
    e.setSnowplowVersion(v)

    // 4th populate the event_type object
    val et = new EventType
    val ev = new EventVendor
    val s = new com_SnowplowAnalytics



    // 5th populate the platform

    return e
  }

}
