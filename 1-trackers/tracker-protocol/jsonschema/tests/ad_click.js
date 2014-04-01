
define([
	'intern!object',
	'intern/chai!assert',
	'intern/dojo/node!../schemas/ad_click.json',
	'intern/dojo/node!z-schema'
], function(registerSuite, assert, adClickSchema, ZSchema) {

	registerSuite({

		name: 'Ad click schema validation',

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
				"campaign_id": "c",
				"EXTRANEOUS": 100
			}

			var validator = new ZSchema({sync: true}),
				valid = validator.validate(clickJson, adClickSchema);

			assert.strictEqual(valid, true);

		}

	});
});
