import sys, csv, gzip, json, re, glob, os
from optparse import OptionParser
from datetime import datetime
import arrow
from pytz import timezone

fieldnames = [
    'app_id',
    'platform',
    'etl_tstamp',
    'collector_tstamp',
    'dvce_tstamp',
    'event',
    'event_id',
    'txn_id',
    'name_tracker',
    'v_tracker',
    'v_collector',
    'v_etl',
    'user_id',
    'user_ipaddress',
    'user_fingerprint',
    'domain_userid',
    'domain_sessionidx',
    'network_userid',
    'geo_country',
    'geo_region',
    'geo_city',
    'geo_zipcode',
    'geo_latitude',
    'geo_longitude',
    'geo_region_name',
    'ip_isp',
    'ip_organization',
    'ip_domain',
    'ip_netspeed',
    'page_url',
    'page_title',
    'page_referrer',
    'page_urlscheme',
    'page_urlhost',
    'page_urlport',
    'page_urlpath',
    'page_urlquery',
    'page_urlfragment',
    'refr_urlscheme',
    'refr_urlhost',
    'refr_urlport',
    'refr_urlpath',
    'refr_urlquery',
    'refr_urlfragment',
    'refr_medium',
    'refr_source',
    'refr_term',
    'mkt_medium',
    'mkt_source',
    'mkt_term',
    'mkt_content',
    'mkt_campaign',
    'contexts',
    'se_category',
    'se_action',
    'se_label',
    'se_property',
    'se_value',
    'unstruct_event',
    'tr_orderid',
    'tr_affiliation',
    'tr_total',
    'tr_tax',
    'tr_shipping',
    'tr_city',
    'tr_state',
    'tr_country',
    'ti_orderid',
    'ti_sku',
    'ti_name',
    'ti_category',
    'ti_price',
    'ti_quantity',
    'pp_xoffset_min',
    'pp_xoffset_max',
    'pp_yoffset_min',
    'pp_yoffset_max',
    'useragent',
    'br_name',
    'br_family',
    'br_version',
    'br_type',
    'br_renderengine',
    'br_lang',
    'br_features_pdf',
    'br_features_flash',
    'br_features_java',
    'br_features_director',
    'br_features_quicktime',
    'br_features_realplayer',
    'br_features_windowsmedia',
    'br_features_gears',
    'br_features_silverlight',
    'br_cookies',
    'br_colordepth',
    'br_viewwidth',
    'br_viewheight',
    'os_name',
    'os_family',
    'os_manufacturer',
    'os_timezone',
    'dvce_type',
    'dvce_ismobile',
    'dvce_screenwidth',
    'dvce_screenheight',
    'doc_charset',
    'doc_width',
    'doc_height',
    'tr_currency',
    'tr_total_base',
    'tr_tax_base',
    'tr_shipping_base',
    'ti_currency',
    'ti_price_base',
    'base_currency',
    'geo_timezone',
    'mkt_clickid',
    'mkt_network',
    'etl_tags',
    'dvce_sent_tstamp',
    'refr_domain_useri',
    'refr_dvce_tstamp',
    'derived_contexts',
    'domain_sessionid',
    'derived_tstamp',
    'event_vendor',
    'event_name',
    'event_format',
    'event_version',
    'event_fingerprint',
    'true_tstamp'
]

def process(in_dir, out_dir):
    LINE_COUNT = 0

    DAY_STATS = {}
    out_files = {}

    in_glob = os.path.join(in_dir, '**/*.gzip')
    try:
        for in_file in list(glob.glob(in_glob)):
            print 'Processing', in_file
            with gzip.open(in_file, 'rb') as f:
                reader = csv.DictReader(f, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_ALL)
                for line in reader:
                    LINE_COUNT = LINE_COUNT + 1
                    collector_tstamp = line["collector_tstamp"]
                    row_ts = arrow.get(collector_tstamp).datetime
                    row_ts_ist = row_ts.astimezone(timezone('Asia/Kolkata'))
                    row_day_ist = row_ts_ist.date().strftime("%Y-%m-%d")
                    if row_day_ist in DAY_STATS:
                        DAY_STATS[row_day_ist] = DAY_STATS[row_day_ist] + 1
                    else:
                        DAY_STATS[row_day_ist] = 1

                    out = {}
                    out["_unstruct_event"] = {}
                    for col, val in line.iteritems():
                        process_column(col,val,out)

                    out_fd = out_files.setdefault(row_day_ist, None)
                    if out_fd is None:
                        out_file = os.path.join(out_dir, row_day_ist + '.gzip')
                        out_files[row_day_ist] = gzip.open(out_file, 'wb+')

                    out_files[row_day_ist].write(json.dumps(out) + '\n')


                    # data[row_day_ist].append(out)


            # for k, v in data.items():
            #     out_file = os.path.join(out_dir, k + '.gzip')
            #     with gzip.open(out_file, 'wb+') as output:
            #         for l in v:
            #             output.write(json.dumps(l) + '\n')

    except Exception, e:
        print e
    finally:
        print "LINE_COUNT: {}".format(LINE_COUNT)
        print "DAY_STATS: {}".format(DAY_STATS)

        for k, v in out_files.items():
            if v is not None:
                v.close()

def process_column(col, val, out):
    #output the original value
    out[col] = val

    # process unstruct_event
    if col == "unstruct_event":
        val_obj = val_to_json(val)
        if "data" in val_obj:
            schema_name = None
            schema_name_match = re.findall(r'iglu:[^/]+/([^/]+)/jsonschema/[0-9]+-[0-9]+-[0-9]+',val_obj["data"]["schema"])
            if schema_name_match:
                schema_name = schema_name_match[0]
            else:
                print "Error regex failed! {}".format(val_obj["data"]["schema"])

            out["_unstruct_event"][schema_name] = val_obj["data"]["data"]
            out["_unstruct_event"][schema_name]["_schema"] = val_obj["data"]["schema"]

def val_to_json(val):
    if val:
        json_val = json.loads(val)
        return json_val
    else:
        json_val = "{}"
        return json_val

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--in", dest="in_dir")
    parser.add_option("-o", "--out", dest="out_dir")

    (options, args) = parser.parse_args()
    process(in_dir=options.in_dir, out_dir=options.out_dir)