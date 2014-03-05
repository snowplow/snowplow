
define([
	'intern!object',
	'intern/chai!assert',
	'intern/chai!expect',
	'intern/dojo/node!chai',
	'intern/dojo/node!chai-json-schema',
	'intern/dojo/node!../schemas/ad_conversion.json',
	'intern/dojo/node!z-schema'
], function(registerSuite, assert, expect, chai, chaiJsonSchema, adConversionSchema, ZSchema) {

	chai.use(chaiJsonSchema);

	registerSuite({

		name: 'Ad impression schema validation',

		'Validate schema': function() {
			
			var conversionJson = {
				"conversion_id": "100",
				"cost_if_cpa": 22,
				"category": "business",
				"action": "purchase",
				"initial_value": 1000,
				"banner_id": "b",
				"zone_id": "z",
				"impression_id": "i",
				"advertiser_id": "a",
				"cost_model": "cpc",
				"campaign_id": "c"
			}

			var validator = new ZSchema({sync: true}),
				valid = validator.validate(conversionJson, adConversionSchema);

			assert.strictEqual(valid, true);

		}

	});
});
