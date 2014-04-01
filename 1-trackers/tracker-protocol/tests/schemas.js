
define([
	'intern!object',
	'intern/chai!expect',
	'intern/dojo/node!z-schema',
	'intern/dojo/node!../schemas/ad_impression',
	'intern/dojo/node!../schemas/ad_click',
	'intern/dojo/node!../schemas/ad_conversion',
	'intern/dojo/node!../schemas/link_click',
	'intern/dojo/node!../schemas/screen_view'
], function(registerSuite, expect, ZSchema, adImpression, adClick, adConversion, linkClick, screenView) {

	var schemas = [adImpression, adClick, adConversion, linkClick, screenView],
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
