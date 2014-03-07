
define([
	'intern!object',
	'intern/chai!assert',
	'intern/dojo/node!z-schema',
	'intern/dojo/node!../schemas/ad_impression',
	'intern/dojo/node!../schemas/ad_click',
	'intern/dojo/node!../schemas/ad_conversion',
	'intern/dojo/node!../schemas/link_click',
	'intern/dojo/node!./resources/ad_impression',
	'intern/dojo/node!./resources/ad_click',
	'intern/dojo/node!./resources/ad_conversion',
	'intern/dojo/node!./resources/link_click',
], function(registerSuite, assert, ZSchema,
			adImpressionSchema, adClickSchema, adConversionSchema, linkClickSchema,
			adImpressionJson,   adClickJson,   adConversionJson,   linkClickJson) {

	var validator = new ZSchema({sync: true});

	function testSchema(schema, json) {
		registerSuite({
			name: json.name + ' schema validation',

			'Validate JSONs should pass validation': function() {
				var validJsons = json.valid,
					valid,
					i;
				for (i = 0; i < validJsons.length; i++) {
					valid = validator.validate(validJsons[i], schema);
					assert.strictEqual(valid, true, 'Valid JSON #' + (i+1) + ' should be validated');
				}	
			},

			'Invalid JSONs should fail validation': function() {
				var invalidJsons = json.invalid,
					valid,
					i;
				for (i = 0; i < invalidJsons.length; i++) {
					valid = validator.validate(invalidJsons[i], schema);
					assert.strictEqual(valid, false, 'Invalid JSON #' + (i+1) + ' should be invalidated');
				}
			}
		})
	}

	testSchema(adImpressionSchema, adImpressionJson);
	testSchema(adClickSchema, adClickJson);
	testSchema(adConversionSchema, adConversionJson);
	testSchema(linkClickSchema, linkClickJson);
});
