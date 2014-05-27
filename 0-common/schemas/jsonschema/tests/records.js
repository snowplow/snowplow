define([
	'intern!object',
	'intern/chai!assert',
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
	'intern/dojo/node!../com.snowplowanalytics.snowplow/screen_view',
	'intern/dojo/node!../com.snowplowanalytics.website/page_context',
	'intern/dojo/node!./resources/schema',
	'intern/dojo/node!./resources/instance',
	'intern/dojo/node!./resources/payload_data',
	'intern/dojo/node!./resources/contexts',
	'intern/dojo/node!./resources/unstruct_event',
	'intern/dojo/node!./resources/ad_impression',
	'intern/dojo/node!./resources/ad_click',
	'intern/dojo/node!./resources/ad_conversion',
	'intern/dojo/node!./resources/link_click',
	'intern/dojo/node!./resources/screen_view',
	'intern/dojo/node!./resources/page_context'
], function(registerSuite, assert, ZSchema,
			schemaSchema,
			instanceSchema, payloadDataSchema,
			contextsSchema, unstructEventSchema,
			adImpressionSchema, adClickSchema, adConversionSchema, linkClickSchema, screenViewSchema,
			pageContextSchema,
			schemaJsons,
			instanceJsons, payloadDataJsons,
			contextsJsons, unstructEventJsons,
			adImpressionJsons,  adClickJsons,  adConversionJsons,  linkClickJsons,  screenViewJsons,
			pageContextJsons) {

	var validator = new ZSchema({sync: true, strictUris: true}),
		testArray = [
			[schemaSchema, schemaJsons],
			[instanceSchema, instanceJsons],
			[payloadDataSchema, payloadDataJsons],
			[contextsSchema, contextsJsons],
			[unstructEventSchema, unstructEventJsons],
			[adImpressionSchema, adImpressionJsons],
			[adClickSchema, adClickJsons],
			[adConversionSchema, adConversionJsons],
			[linkClickSchema, linkClickJsons],
			[screenViewSchema, screenViewJsons],
			[pageContextSchema, pageContextJsons]
		],
		j;

	function testSchema(schema, json) { console.log(typeof schema, typeof json)
		registerSuite({
			name: json.name + ' schema validation',

			'Valid JSONs should pass validation': function() {
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

	for (var j = 0; j < testArray.length; j++) {
		testSchema(testArray[j][0], testArray[j][1])
	}

});
