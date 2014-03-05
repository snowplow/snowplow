
define([
	'intern!object',
	'intern/chai!assert',
	'intern/chai!expect',
	'intern/dojo/node!chai',
	'intern/dojo/node!chai-json-schema',
	'intern/dojo/node!../schemas/ad_click.json',
	'intern/dojo/node!z-schema'
], function(registerSuite, assert, expect, chai, chaiJsonSchema, adClickSchema, ZSchema) {

	chai.use(chaiJsonSchema);

	registerSuite({

		name: 'Ad impression schema validation',

		'Validate schema': function() {
			
			var clickJson = {
				"click_id": "100",
				"cost_if_cpc": 22,
				"target_url": "www.example.com",
				"banner_id": "b",
				"zone_id": "z",
				"impression_id": "i",
				"advertiser_id": "a",
				"cost_model": "cpc",
				"campaign_id": "c"
			}

			var validator = new ZSchema({sync: true}),
			valid = validator.validate(clickJson, adClickSchema);

			assert.strictEqual(valid, true);

		}

	});
});
