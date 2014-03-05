
define([
	'intern!object',
	'intern/chai!assert',
	'intern/chai!expect',
	'intern/dojo/node!z-schema',
	'intern/dojo/node!../schemas/ad_click.json'
], function(registerSuite, assert, expect, ZSchema, adClick) {

	registerSuite({

		name: 'Validate schemas',

		'Validate schemas': function() {
			
			var validator = new ZSchema({sync:true})

			expect(function(){
				valid = validator.validateSchema(adClick)
			}).to.not.throw();

		}

	});
});
