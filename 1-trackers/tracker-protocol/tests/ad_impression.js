
define([
	'intern!object',
	'intern/chai!assert',
	'intern/dojo/node!../schemas/ad_impression.json',
	'intern/dojo/node!z-schema'
], function(registerSuite, assert, adImpressionSchema, ZSchema) {

	registerSuite({
		name: 'Ad impression schema validation',
		'Validate schema': function() {
			var impressionJson = {
				"impression_id": "100",
				"cost_if_cpm": 22,
				"banner_id": "3",
				"zone_id": "5",
				"advertiser_id": "5",
				"cost_model": "cpc",
				"campaign_id": "88"
			}

			var validator = new ZSchema({sync: true}),
				valid = validator.validate(impressionJson, adImpressionSchema);

			assert.strictEqual(valid, true);
		}
	});
});

