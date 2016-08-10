from glob import glob
import json
from optparse import OptionParser

EVENT_FIELDS = ['app_id','platform','etl_tstamp','collector_tstamp','dvce_tstamp','event','event_id','txn_id','name_tracker','v_tracker','v_collector','v_etl','user_id','user_ipaddress','user_fingerprint','domain_userid','domain_sessionidx','network_userid','geo_country','geo_region','geo_city','geo_zipcode','geo_latitude','geo_longitude','geo_region_name','ip_isp','ip_organization','ip_domain','ip_netspeed','page_url','page_title','page_referrer','page_urlscheme','page_urlhost','page_urlport','page_urlpath','page_urlquery','page_urlfragment','refr_urlscheme','refr_urlhost','refr_urlport','refr_urlpath','refr_urlquery','refr_urlfragment','refr_medium','refr_source','refr_term','mkt_medium','mkt_source','mkt_term','mkt_content','mkt_campaign','contexts','se_category','se_action','se_label','se_property','se_value','unstruct_event','tr_orderid','tr_affiliation','tr_total','tr_tax','tr_shipping','tr_city','tr_state','tr_country','ti_orderid','ti_sku','ti_name','ti_category','ti_price','ti_quantity','pp_xoffset_min','pp_xoffset_max','pp_yoffset_min','pp_yoffset_max','useragent','br_name','br_family','br_version','br_type','br_renderengine','br_lang','br_features_pdf','br_features_flash','br_features_java','br_features_director','br_features_quicktime','br_features_realplayer','br_features_windowsmedia','br_features_gears','br_features_silverlight','br_cookies','br_colordepth','br_viewwidth','br_viewheight','os_name','os_family','os_manufacturer','os_timezone','dvce_type','dvce_ismobile','dvce_screenwidth','dvce_screenheight','doc_charset','doc_width','doc_height','tr_currency','tr_total_base','tr_tax_base','tr_shipping_base','ti_currency','ti_price_base','base_currency','geo_timezone','mkt_clickid','mkt_network','etl_tags','dvce_sent_tstamp','refr_domain_useri','refr_dvce_tstamp','derived_contexts','domain_sessionid','derived_tstamp','event_vendor','event_name','event_format','event_version','event_fingerprint','true_tstamp']

DATA_TYPES = {
	'integer': 'INTEGER',
	'number': 'FLOAT',
	'string': 'STRING',
	'boolean': 'BOOLEAN'
}

def main(iglu_dir):
	all_fields = []
	fields = {}
	fields_cache = {}
	paths = glob(iglu_dir + 'schemas/com.bd.snowplow/*/jsonschema/*')

	for path in paths:
		with open(path, 'r') as f:
			schema = json.loads(f.read())
			name = schema['self']['name']
			fields.setdefault(name, [])
			for field, _type in schema['properties'].items():
				cache = fields_cache.setdefault(name + field, False)

				if cache:
					continue

				fields[name].append({
					'name': field,
					'type': DATA_TYPES[_type['type']]
				})

				fields_cache[name + field] = True


	for field in EVENT_FIELDS:
		all_fields.append({
			'name': field,
			'type': 'STRING'
		})


	tool_fields = []

	for tool, field in fields.items():
		tool_fields.append({
			'name': tool,
			'type': 'RECORD',
			'fields': field	
		})

	all_fields.append({
		'name': '_unstruct_event',
		'type': 'RECORD',
		'fields': tool_fields
	})

	print json.dumps(all_fields, indent=4)
	
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--iglu", dest="iglu_dir")

    (options, args) = parser.parse_args()
    main(iglu_dir=options.iglu_dir)