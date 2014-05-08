
define([
	'intern!object',
	'intern/chai!expect',
	'intern/dojo/node!z-schema',
	'intern/dojo/node!../com.snowplowanalytics/tracker_protocol',
	'intern/dojo/node!../com.snowplowanalytics/us_contexts',
	'intern/dojo/node!../com.snowplowanalytics/ad_impression',
	'intern/dojo/node!../com.snowplowanalytics/ad_click',
	'intern/dojo/node!../com.snowplowanalytics/ad_conversion',
	'intern/dojo/node!../com.snowplowanalytics/link_click',
	'intern/dojo/node!../com.snowplowanalytics/screen_view'
], function(registerSuite, expect, ZSchema, trackerProtocol, customContexts, adImpression, adClick, adConversion, linkClick, screenView) {

	var schemas = [trackerProtocol, customContexts, adImpression, adClick, adConversion, linkClick, screenView],
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
