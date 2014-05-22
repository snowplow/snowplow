
define([
	'intern!object',
	'intern/chai!expect',
	'intern/dojo/node!z-schema',
	'intern/dojo/node!../com.snowplowanalytics.self-desc/schema',
	'intern/dojo/node!../com.snowplowanalytics.self-desc/instance',	
	'intern/dojo/node!../com.snowplowanalytics.snowplow/payload_data',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/contexts',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/unstruct_event',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/ad_impression',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/ad_click',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/ad_conversion',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/link_click',
	'intern/dojo/node!../com.snowplowanalytics.snowplow/screen_view'
], function(registerSuite, expect, ZSchema, schema, instance, trackerProtocol, customContexts, unstructEvent, adImpression, adClick, adConversion, linkClick, screenView) {

	var schemas = [trackerProtocol, customContexts, unstructEvent, schema, instance, trackerProtocol, customContexts, unstructEvent, adImpression, adClick, adConversion, linkClick, screenView],
		validator = new ZSchema({sync:true}),
		i;

	registerSuite({

		name: 'Validate schemas',

		'Validate schemas': function() {

			for (i = 0; i < schemas.length; i++) {

				expect(function(){
					valid = validator.validateSchema(schemas[i])
				}).to.not.throw();
			}
		}
	});

});
